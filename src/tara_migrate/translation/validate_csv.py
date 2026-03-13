"""Consolidated CSV validation: clean, verify, validate, and generate todo lists.

Merges functionality from three standalone scripts:
- validate_csv.py   — AI-powered validation using Claude Haiku (row-shift detection)
- verify_translation.py — Coverage + quality check + todo generation
- clean_translation_csv.py — CSV row cleaning / deduplication

Four modes:
  clean    — Remove non-translatable rows, dedup, fix formatting
  verify   — Check translation completeness (gaps, truncation, Arabic presence)
  validate — Full pipeline: rule-based + script analysis + duplicates + heuristic + AI
  todo     — Generate JSON todo list for re-translation

Usage:
    python -m tara_migrate.translation.validate_csv --input file.csv --mode clean
    python -m tara_migrate.translation.validate_csv --input file.csv --mode verify
    python -m tara_migrate.translation.validate_csv --input file.csv --mode validate --skip-ai
    python -m tara_migrate.translation.validate_csv --input file.csv --mode todo
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv

from tara_migrate.core.csv_utils import is_non_translatable, is_keep_as_is
from tara_migrate.core.language import has_arabic, count_chars, ARABIC_REGEX, LATIN_REGEX
from tara_migrate.core.rich_text import extract_text, is_rich_text_json
from tara_migrate.translation.toon import to_toon, from_toon

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MODEL = "claude-haiku-4-5-20251001"
HAIKU_INPUT_COST = 0.80   # $/1M input tokens
HAIKU_OUTPUT_COST = 4.00  # $/1M output tokens

# Additional field-level patterns for the validate pipeline's untranslatable check.
# These supplement csv_utils.is_non_translatable with theme-level field patterns.
_SKIP_FIELD_PATTERNS = re.compile("|".join([
    r"\.image(_\d(_mobile)?)?$", r"\.icon:", r"\.link$", r"_url$",
    r"\.logo", r"\.favicon", r"google_maps", r"form_id",
    r"portal_id", r"anchor_id", r"worker_url",
    r"default_la[tn]", r"default_lng", r"max_(height|width)",
    r"\.video(_url)?$", r"\.color(_|$)", r"color_scheme",
    r"\.(opacity|padding|margin)$", r"font_size", r"border_radius",
]))

_UNTRANS_VALUE_PATTERNS = [
    (re.compile(r"^(shopify://|https?://|/|gid://)"), None),
    (re.compile(r"^-?\d+\.?\d*$"), None),
    (re.compile(r"^#?[0-9a-fA-F]{3,}$"), None),
    (re.compile(r"^\d+(\.\d+)?(px|rem|em|%|vh|vw|s|ms)$"), None),
]

_BOOL_VALUES = frozenset(("true", "false", "yes", "no", "none", "null"))

# Spanish detection (used in verify mode)
_SPANISH_CHARS = re.compile(r"[áéíóúñ¿¡ü]", re.IGNORECASE)
_SPANISH_WORDS = re.compile(
    r"\b(de|del|los|las|con|para|por|una|que|cabello|capilar|"
    r"champú|tratamiento|colección|más|también|productos?|cuidado)\b",
    re.IGNORECASE,
)

# Text extraction helpers
_STRIP_TAGS_RE = re.compile(r"<[^>]+>")
_STYLE_RE = re.compile(r"<style[^>]*>.*?</style>", re.DOTALL | re.IGNORECASE)
_SCRIPT_RE = re.compile(r"<script[^>]*>.*?</script>", re.DOTALL | re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")
_ENTITY_MAP = {"&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">"}
_ENTITY_NUM_RE = re.compile(r"&#\d+;")

MAX_SHIFT_OFFSET = 5


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _is_untranslatable_extended(field, value):
    """Extended untranslatable check for the validate pipeline.

    Supplements ``csv_utils.is_non_translatable`` with theme-level field
    patterns, CSS-only blocks, bool values, and hex colours.

    Returns ``(should_remove, reason)``.
    """
    v = (value or "").strip()
    if not v:
        return True, "empty"
    if _SKIP_FIELD_PATTERNS.search(field):
        return True, "field_pattern"
    if v.lower() in _BOOL_VALUES:
        return True, "untranslatable_value"
    for pat, _ in _UNTRANS_VALUE_PATTERNS:
        if pat.match(v):
            return True, "untranslatable_value"
    # JSON arrays of GIDs/IDs
    if v.startswith("[") and v.endswith("]"):
        try:
            parsed = json.loads(v)
            if isinstance(parsed, list) and all(
                isinstance(x, str) and (x.startswith("gid://") or x.isdigit())
                for x in parsed
            ):
                return True, "untranslatable_value"
        except (json.JSONDecodeError, TypeError):
            pass
    # Config JSON
    if v.startswith("{") and any(k in v for k in ('"reviewCount"', '"formId"')):
        return True, "untranslatable_value"
    # Pure CSS with no visible text
    if "<style>" in v.lower() and "</style>" in v.lower() and len(v) > 200:
        stripped = re.sub(r"<style>.*?</style>", "", v, flags=re.DOTALL | re.IGNORECASE)
        if not re.sub(r"<[^>]+>", " ", stripped).strip():
            return True, "untranslatable_value"
    return False, ""


def _read_csv(path):
    """Read a CSV file with BOM handling. Returns (fieldnames, rows)."""
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    return fieldnames, rows


def _write_csv(path, fieldnames, rows):
    """Write CSV with BOM for Excel compatibility."""
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def extract_visible_text(html_or_text, max_chars=300):
    """Extract visible text from HTML/rich_text for comparison."""
    if not html_or_text:
        return ""
    text = html_or_text.strip()

    # Rich text JSON -- extract text nodes via library
    if text.startswith("{") and '"type"' in text:
        extracted = extract_text(text)
        if extracted:
            text = extracted

    text = _STYLE_RE.sub("", text)
    text = _SCRIPT_RE.sub("", text)
    text = _STRIP_TAGS_RE.sub(" ", text)
    for entity, repl in _ENTITY_MAP.items():
        text = text.replace(entity, repl)
    text = _ENTITY_NUM_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text[:max_chars]


def arabic_ratio(text):
    """Fraction of alphabetic chars that are Arabic (0.0-1.0)."""
    if not text:
        return 0.0
    ar, la = count_chars(text)
    return ar / (ar + la) if (ar + la) else 0.0


def classify_content(field, text):
    """Classify field into content category for AI hints."""
    fl = field.lower()
    if any(p in fl for p in (
        ".title", ".heading", ".label", ".name",
        "button_label", "button_text", "cta_text",
        ".tab_", ".menu_",
    )):
        return "heading"
    if any(p in fl for p in (
        ".body", ".description", ".content",
        ".rich_text", ".paragraph", ".details",
    )):
        return "body"
    if text and re.match(r"^[A-Z][a-z]+(\s[A-Z][a-z]+)*(,\s*[A-Z])", text):
        return "ingredients"
    return "text"


def _detect_language(text):
    """Detect if text is Arabic, English, Spanish, or mixed.

    Returns: 'ar', 'en', 'es', or 'mixed'.
    """
    # Try rich_text JSON first
    if text.startswith("{") and '"type"' in text:
        extracted = extract_text(text)
        if extracted and extracted.strip():
            text = extracted

    # Strip HTML/CSS for detection
    stripped = re.sub(r"<[^>]+>", " ", text)
    stripped = re.sub(r"\{[^}]*\}", " ", stripped)
    stripped = stripped.strip()
    if not stripped:
        return "en"

    ar, la = count_chars(stripped)
    total_alpha = ar + la

    if total_alpha == 0:
        return "en"
    if ar / total_alpha >= 0.3:
        return "ar"

    # It's mostly Latin -- is it Spanish or English?
    spanish_chars = len(_SPANISH_CHARS.findall(stripped))
    spanish_words = len(_SPANISH_WORDS.findall(stripped))
    if spanish_chars >= 2 or spanish_words >= 2:
        return "es"

    return "en"


# ---------------------------------------------------------------------------
# RowCache for validate pipeline
# ---------------------------------------------------------------------------

class RowCache:
    """Cache extracted text per row to avoid redundant extraction."""

    def __init__(self, rows):
        self.rows = rows
        self._eng = {}
        self._ar = {}

    def eng(self, i, max_chars=300):
        key = (i, max_chars)
        if key not in self._eng:
            self._eng[key] = extract_visible_text(
                self.rows[i].get("Default content", ""), max_chars)
        return self._eng[key]

    def ar(self, i, max_chars=300):
        key = (i, max_chars)
        if key not in self._ar:
            self._ar[key] = extract_visible_text(
                self.rows[i].get("Translated content", ""), max_chars)
        return self._ar[key]

    def field(self, i):
        return self.rows[i].get("Field", "")

    def resource_key(self, i):
        r = self.rows[i]
        return (r.get("Type", ""), r.get("Identification", ""))


def _build_mismatch(cache, idx, reason, source, severity="medium", confidence=None):
    """Build a standardized mismatch dict."""
    row = cache.rows[idx]
    m = {
        "row_index": idx,
        "type": row.get("Type", ""),
        "identification": row.get("Identification", ""),
        "field": cache.field(idx),
        "english": cache.eng(idx, 120),
        "arabic": cache.ar(idx, 120),
        "reason": reason,
        "source": source,
    }
    if confidence:
        m["confidence"] = confidence
    else:
        m["severity"] = severity
    return m


# ---------------------------------------------------------------------------
# Misalignment detection (from clean_translation_csv.py)
# ---------------------------------------------------------------------------

def _detect_misalignment(row):
    """Detect if a translation appears to be in the wrong field."""
    default = row.get("Default content", "").strip()
    translated = row.get("Translated content", "").strip()

    if not translated or not default:
        return None

    # Rich text JSON in a non-JSON field
    if translated.startswith("{") and not default.startswith("{"):
        if '"type"' in translated:
            return "rich_text_json_in_plain_field"

    # Truncated JSON translation
    if default.startswith("{") and '"type"' in default:
        if not translated.startswith("{"):
            extracted = extract_text(default)
            if extracted and len(extracted) > 20 and len(translated) < 10:
                return "truncated_json_translation"

    # Heading translation too long
    heading_patterns = [
        "Key Benefits", "Key Ingredients", "How to Use",
        "How To Use", "Free Of", "Free of", "Fragrance",
    ]
    if default in heading_patterns and len(translated) > 50:
        return "heading_got_content_translation"

    # Review JSON leaked
    if '"reviewCount"' in translated and '"reviewCount"' not in default:
        return "review_json_leaked"

    return None


# ---------------------------------------------------------------------------
# Script / structural heuristic detection (validate pipeline layer 2)
# ---------------------------------------------------------------------------

def _detect_script_issues(cache):
    """Detect issues via script analysis. Returns dict of {idx: mismatch}."""
    issues = {}
    for i in range(len(cache.rows)):
        eng = cache.eng(i)
        ar = cache.ar(i)
        field = cache.field(i)
        if not eng or not ar:
            continue

        # Untranslated: Arabic identical to English
        if eng == ar and len(eng) > 2:
            is_liquid = bool(re.match(r"^\{\{.*\}\}$", eng.strip()))
            is_inci = bool(re.search(r"[™®©]", eng)) or bool(
                re.match(r"^[A-Z][a-z]+\s*\(.*\)$", eng))
            words = eng.split()
            if len(words) == 1:
                is_inci = is_inci or not eng[0].isupper() or len(eng) <= 3
            if not is_liquid and not is_inci:
                issues[i] = _build_mismatch(cache, i,
                    "untranslated: Arabic identical to English",
                    "script_analysis", "high")
                continue

        # No Arabic script at all
        ar_r = arabic_ratio(ar)
        if ar_r == 0.0 and len(ar) > 10:
            is_inci = bool(re.search(r"[™®©]", ar)) or bool(
                re.match(r"^[A-Z][a-z]+\s*\(.*\)$", ar))
            if not is_inci and not re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+", ar):
                issues[i] = _build_mismatch(cache, i,
                    "no Arabic script in translation",
                    "script_analysis", "high")
                continue

        # Very low Arabic ratio
        if ar_r < 0.15 and len(ar) > 50 and len(ARABIC_REGEX.findall(ar)) < 5:
            issues[i] = _build_mismatch(cache, i,
                f"only {ar_r:.0%} Arabic chars in translation",
                "script_analysis", "medium")
            continue

        # Corrupted rich_text JSON
        raw_ar = cache.rows[i].get("Translated content", "")
        if raw_ar.strip().startswith("{") and '"type"' in raw_ar:
            try:
                json.loads(raw_ar)
            except json.JSONDecodeError:
                issues[i] = _build_mismatch(cache, i,
                    "corrupted rich_text JSON (invalid JSON structure)",
                    "script_analysis", "high")
                continue

        # Length anomalies
        eng_len, ar_len = len(eng), len(ar)
        if eng_len > 20 and ar_len > 20:
            ratio = ar_len / eng_len
            if ratio > 5.0:
                issues[i] = _build_mismatch(cache, i,
                    f"Arabic is {ratio:.1f}x longer than English",
                    "script_analysis", "medium")
            elif ratio < 0.1:
                issues[i] = _build_mismatch(cache, i,
                    f"Arabic is {ratio:.1f}x shorter than English",
                    "script_analysis", "medium")
        elif (classify_content(field, eng) == "heading"
              and len(eng.split()) <= 4 and ar_len > 200):
            issues[i] = _build_mismatch(cache, i,
                f"heading has paragraph-length translation ({ar_len} chars)",
                "script_analysis", "medium")

    return issues


# ---------------------------------------------------------------------------
# Duplicate translation detection (validate pipeline layer 3)
# ---------------------------------------------------------------------------

def _detect_duplicates(cache):
    """Flag identical Arabic for substantially different English."""
    issues = {}
    ar_to_rows = defaultdict(list)

    for i in range(len(cache.rows)):
        ar = cache.ar(i, 500)
        if len(ar) > 20:
            ar_to_rows[ar].append(i)

    for ar_text, indices in ar_to_rows.items():
        if len(indices) < 2:
            continue
        eng_words = [set(cache.eng(i, 200).lower().split()) for i in indices]
        flagged = set()
        for a in range(len(indices)):
            for b in range(a + 1, len(indices)):
                wa, wb = eng_words[a], eng_words[b]
                if len(wa) < 3 or len(wb) < 3:
                    continue
                overlap = len(wa & wb) / max(len(wa), len(wb))
                if overlap < 0.3:
                    flagged.update((indices[a], indices[b]))
        for idx in flagged:
            if idx not in issues:
                issues[idx] = _build_mismatch(cache, idx,
                    "duplicate Arabic for different English",
                    "duplicate", "medium")

    return issues


# ---------------------------------------------------------------------------
# Multi-offset heuristic shift detection (validate pipeline layer 4)
# ---------------------------------------------------------------------------

def _detect_shifts(cache):
    """Detect row shifts by cross-matching within offsets per resource.

    Returns set of shifted row indices.
    """
    shifted = set()
    by_resource = defaultdict(list)

    for i in range(len(cache.rows)):
        eng = cache.eng(i, 150)
        ar = cache.ar(i, 150)
        if eng and ar and len(eng) >= 10 and len(ar) >= 5:
            by_resource[cache.resource_key(i)].append(i)

    for key, indices in by_resource.items():
        if len(indices) < 2:
            continue

        for pos, i in enumerate(indices):
            eng_words = set(cache.eng(i, 150).lower().split())
            if len(eng_words) < 3:
                continue
            ar_latin = set(re.findall(r"[a-zA-Z]{2,}", cache.ar(i, 150).lower()))
            if not ar_latin:
                continue
            overlap_self = len(ar_latin & eng_words)

            for delta in range(-MAX_SHIFT_OFFSET, MAX_SHIFT_OFFSET + 1):
                if delta == 0:
                    continue
                other_pos = pos + delta
                if not (0 <= other_pos < len(indices)):
                    continue
                j = indices[other_pos]
                other_words = set(cache.eng(j, 150).lower().split())
                if len(other_words) < 3:
                    continue
                overlap_other = len(ar_latin & other_words)
                if overlap_other > overlap_self and overlap_other >= 3:
                    shifted.add(i)
                    break

    return shifted


# ---------------------------------------------------------------------------
# AI validation: resource-grouped, two-pass + back-translation
# ---------------------------------------------------------------------------

_FEW_SHOT_EXAMPLES = """Examples:

OK pairs:
- EN: "Award-Winning Haircare: Botanical Extracts + Advanced Science"
  AR: "عناية بالشعر حاصلة على جوائز: مستخلصات نباتية + علم متقدم"
  -> OK (same meaning)
- EN: "Activated Charcoal Face Wash" / AR: "غسول الوجه بالفحم المنشط" -> OK
- EN: "Free Of" / AR: "خالٍ من" -> OK
- EN: "Aqua, Glycerin, Cetearyl Alcohol" / AR: "Aqua, Glycerin, Cetearyl Alcohol" -> OK (INCI kept in Latin)
- EN: "Our gentle formula cleanses without stripping natural oils"
  AR: "تركيبتنا اللطيفة تنظف دون إزالة الزيوت الطبيعية" -> OK

MISMATCH pairs (row shift):
- EN: "Hydrating Face Cream with Hyaluronic Acid"
  AR: "شامبو مقوي للشعر بالكيراتين" -> MISMATCH (hair shampoo != face cream)
- EN: "Key Benefits"
  AR: "ينظف البشرة بعمق ويزيل الشوائب والزيوت الزائدة" -> MISMATCH (heading got body text)
- EN: "How to Use"
  AR: "زبدة الشيا العضوية تغذي وترطب البشرة الجافة" -> MISMATCH (usage heading got ingredient desc)
- EN: "Rose Water Toner helps balance skin pH"
  AR: "كريم الليل بالريتينول يجدد البشرة" -> MISMATCH (toner != night cream)
"""


def _estimate_cost(num_pairs, batch_size):
    """Estimate total API cost including recheck pass."""
    batches = (num_pairs + batch_size - 1) // batch_size
    inp = num_pairs * 60 + batches * 800
    out = num_pairs * 8
    rc = int(num_pairs * 0.1)
    rc_b = max(1, (rc + 15) // 15)
    inp += rc * 120 + rc_b * 800
    out += rc * 15
    return (inp / 1e6 * HAIKU_INPUT_COST + out / 1e6 * HAIKU_OUTPUT_COST), batches


def _parse_json_response(text):
    """Extract JSON array from model response, handling code fences."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        end = -1 if lines[-1].strip() == "```" else len(lines)
        text = "\n".join(lines[1:end]).strip()
    result = json.loads(text)
    return result if isinstance(result, list) else []


def _call_haiku(client, prompt, retries=3):
    """Call Haiku with retries and rate limiting, return parsed JSON list."""
    for attempt in range(retries):
        try:
            resp = client.messages.create(
                model=MODEL, max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            result = _parse_json_response(resp.content[0].text)
            time.sleep(1)
            return result
        except json.JSONDecodeError:
            if attempt < retries - 1:
                print(f" json-retry", end="", flush=True)
                time.sleep(2 ** attempt)
        except Exception as e:
            if "rate" in str(e).lower() or "429" in str(e):
                wait = 2 ** (attempt + 2)
                print(f" rate-limited({wait}s)", end="", flush=True)
                time.sleep(wait)
            elif attempt < retries - 1:
                print(f" err-retry", end="", flush=True)
                time.sleep(2 ** attempt)
            else:
                print(f" ERROR({e})", end="", flush=True)
    return []


def _build_pass1_prompt(pairs, fields, categories):
    """Build the primary validation prompt with content-category hints."""
    lines = []
    for i, (eng, ara) in enumerate(pairs):
        cat = categories[i] if categories else ""
        fld = fields[i] if fields else ""
        hint = f" [{cat}:{fld}]" if cat and fld else (f" [{fld}]" if fld else "")
        lines.append(f"{i}.{hint} EN: {eng}")
        lines.append(f"   AR: {ara}")

    return (
        "You are a translation QA checker for Tara, a skincare/haircare brand. "
        "Check each Arabic<>English pair.\n\n"
        "Content categories in brackets: heading (short title/label), body (description/paragraph), "
        "ingredients (INCI list), text (other).\n\n"
        "MISMATCH only if:\n"
        "- Arabic is about a COMPLETELY different topic/product (row shift)\n"
        "- Arabic is a translation of a different English text\n"
        "- A heading got a body-length translation or vice versa\n"
        "- Content categories clearly don't match (ingredients <> usage instructions)\n\n"
        "OK if:\n"
        "- Reasonable translation, even imperfect/paraphrased\n"
        "- Same topic, different wording\n"
        "- INCI names kept in Latin within Arabic = CORRECT\n"
        "- Brand name Tara/تارا in both = fine\n"
        "- Minor omissions, additions, formatting diffs = fine\n\n"
        + _FEW_SHOT_EXAMPLES +
        "\nConfidence: high (clearly wrong), medium (likely wrong), low (uncertain).\n"
        "Return JSON array of MISMATCHES ONLY:\n"
        '[{"i":<n>,"ok":false,"confidence":"high"|"medium"|"low","reason":"brief"}]\n'
        "All OK -> return []\n\n"
        "Pairs:\n" + "\n".join(lines)
    )


def _build_recheck_prompt(items, cache):
    """Build context-enriched recheck prompt."""
    lines = []
    for bi, m in enumerate(items):
        idx = m["row_index"]
        eng = cache.eng(idx, 400)
        ara = cache.ar(idx, 400)
        field = cache.field(idx)

        ctx = []
        rk = cache.resource_key(idx)
        for delta in (-2, -1, 1, 2):
            ci = idx + delta
            if 0 <= ci < len(cache.rows) and cache.resource_key(ci) == rk:
                ce, ca = cache.eng(ci, 100), cache.ar(ci, 100)
                if ce and ca:
                    d = "PREV" if delta < 0 else "NEXT"
                    ctx.append(f"     ({d}{abs(delta)}) EN: {ce} -> AR: {ca}")

        lines.append(f"{bi}. [{field}] EN: {eng}")
        lines.append(f"   AR: {ara}")
        lines.append(f"   (Flagged: {m.get('reason', '?')})")
        lines.extend(ctx)
        lines.append("")

    return (
        "Recheck these uncertain translation flags. Context rows are provided.\n"
        "Be LENIENT -- only confirm MISMATCH if clearly wrong product/topic.\n"
        "Creative/liberal translations covering the same topic = OK.\n\n"
        "For each pair respond:\n"
        '- Confirmed: {"i":<n>,"ok":false,"reason":"..."}\n'
        '- False alarm: {"i":<n>,"ok":true}\n'
        "Include ALL pairs.\n\n" + "\n".join(lines)
    )


def _build_backtranslate_prompt(items, cache):
    """Build back-translation prompt for final-resort verification."""
    lines = []
    for bi, m in enumerate(items):
        idx = m["row_index"]
        ara = cache.ar(idx, 400)
        lines.append(f"{bi}. AR: {ara}")

    return (
        "Translate each Arabic text below back to English. "
        "Keep it literal -- preserve the topic, product names, and key details.\n"
        'Return a JSON array: [{"i": <n>, "en": "back-translation"}]\n\n'
        + "\n".join(lines)
    )


def _run_ai_validation(client, cache, batch_size, no_recheck, workers):
    """Run the full AI validation pipeline. Returns list of confirmed mismatches."""
    pairs, indices, fields, categories = [], [], [], []
    for i in range(len(cache.rows)):
        eng = cache.eng(i)
        ar = cache.ar(i)
        raw_default = cache.rows[i].get("Default content", "").strip()
        raw_translated = cache.rows[i].get("Translated content", "").strip()
        if not raw_default or not raw_translated or raw_default == raw_translated:
            continue
        if len(eng) < 3 or len(ar) < 2:
            continue
        pairs.append((eng, ar))
        indices.append(i)
        fields.append(cache.field(i))
        categories.append(classify_content(cache.field(i), eng))

    n = len(pairs)
    print(f"  Pairs to validate: {n}")
    if n == 0:
        return []

    est_cost, est_batches = _estimate_cost(n, batch_size)
    print(f"  Estimated: {est_batches} batches, ~${est_cost:.3f}")

    # --- Pass 1: parallel batch validation ---
    print(f"  Pass 1: batch validation ({workers} workers)...")

    batches = []
    for start in range(0, n, batch_size):
        end = min(start + batch_size, n)
        batches.append({
            "pairs": pairs[start:end],
            "indices": indices[start:end],
            "fields": fields[start:end],
            "categories": categories[start:end],
            "num": start // batch_size + 1,
        })

    ai_mismatches = []

    def process_batch(b):
        prompt = _build_pass1_prompt(b["pairs"], b["fields"], b["categories"])
        results = _call_haiku(client, prompt)
        found = []
        for r in results:
            idx_in_batch = r.get("i", -1)
            if 0 <= idx_in_batch < len(b["indices"]) and not r.get("ok", True):
                row_idx = b["indices"][idx_in_batch]
                m = _build_mismatch(cache, row_idx,
                    r.get("reason", ""), "ai",
                    confidence=r.get("confidence", "medium"))
                found.append(m)
        return b["num"], found

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(process_batch, b): b["num"] for b in batches}
        for future in as_completed(futures):
            bnum, found = future.result()
            ai_mismatches.extend(found)
            status = f" {len(found)} flagged" if found else " OK"
            print(f"    Batch {bnum}/{est_batches}...{status}")

    print(f"  Pass 1: {len(ai_mismatches)} flagged")

    if not ai_mismatches:
        return []

    # --- Pass 2: recheck uncertain with context ---
    if not no_recheck:
        uncertain = [m for m in ai_mismatches if m.get("confidence") in ("medium", "low")]
        high_conf = [m for m in ai_mismatches if m.get("confidence") == "high"]

        if uncertain:
            print(f"\n  Pass 2: rechecking {len(uncertain)} uncertain with context...")
            confirmed = []
            cleared = 0
            rc_size = 15

            for start in range(0, len(uncertain), rc_size):
                batch = uncertain[start:start + rc_size]
                bnum = start // rc_size + 1
                total_b = (len(uncertain) + rc_size - 1) // rc_size
                print(f"    Recheck {bnum}/{total_b}...", end="", flush=True)

                prompt = _build_recheck_prompt(batch, cache)
                results = _call_haiku(client, prompt)
                rmap = {r.get("i", -1): r for r in results if isinstance(r, dict)}

                bc, bclr = 0, 0
                for bi, m in enumerate(batch):
                    r = rmap.get(bi)
                    if r and r.get("ok", False):
                        bclr += 1
                        cleared += 1
                    else:
                        updated = dict(m)
                        if r and r.get("reason"):
                            updated["reason"] = r["reason"]
                        updated["confidence"] = "confirmed"
                        confirmed.append(updated)
                        bc += 1

                print(f" {bc} confirmed, {bclr} cleared")
                time.sleep(0.2)

            ai_mismatches = high_conf + confirmed
            print(f"  Pass 2: {cleared} false alarms removed")

            # --- Pass 3: back-translate remaining uncertain ---
            still_uncertain = [m for m in confirmed if m.get("confidence") == "confirmed"]
            if still_uncertain and len(still_uncertain) <= 30:
                print(f"\n  Pass 3: back-translating {len(still_uncertain)} for verification...")
                prompt = _build_backtranslate_prompt(still_uncertain, cache)
                bt_results = _call_haiku(client, prompt)
                bt_map = {r.get("i", -1): r.get("en", "") for r in bt_results if isinstance(r, dict)}

                rescued = 0
                final_confirmed = []
                for bi, m in enumerate(still_uncertain):
                    bt_en = bt_map.get(bi, "")
                    orig_en = cache.eng(m["row_index"], 300)
                    if bt_en:
                        orig_words = set(orig_en.lower().split())
                        bt_words = set(bt_en.lower().split())
                        if orig_words and bt_words:
                            overlap = len(orig_words & bt_words) / max(len(orig_words), len(bt_words))
                            if overlap > 0.3:
                                rescued += 1
                                continue
                    final_confirmed.append(m)

                if rescued:
                    print(f"  Pass 3: rescued {rescued} false alarms via back-translation")
                    ai_mismatches = high_conf + final_confirmed
        else:
            print(f"\n  All {len(ai_mismatches)} are high-confidence, skipping recheck")

    print(f"  Final AI mismatches: {len(ai_mismatches)}")
    return ai_mismatches


# ---------------------------------------------------------------------------
# HTML integrity checks (from verify_translation.py)
# ---------------------------------------------------------------------------

def _check_html_integrity(default, translated):
    """Check HTML tag integrity between source and translation."""
    issues = []
    default_tags = re.findall(r"</?[a-zA-Z][^>]*>", default)
    translated_tags = re.findall(r"</?[a-zA-Z][^>]*>", translated)
    if len(default_tags) != len(translated_tags):
        issues.append(f"tag count: {len(default_tags)} -> {len(translated_tags)}")
    open_tags = re.findall(r"<([a-zA-Z]+)", translated)
    close_tags = re.findall(r"</([a-zA-Z]+)", translated)
    for tag in set(open_tags):
        if open_tags.count(tag) != close_tags.count(tag):
            if tag.lower() not in ("br", "hr", "img", "input", "meta", "link"):
                issues.append(f"unclosed <{tag}>")
    return issues


def _check_truncation(default, translated):
    """Check if translation appears truncated."""
    if len(default) > 50 and len(translated) < len(default) * 0.2:
        return f"possibly truncated ({len(translated)} vs {len(default)} chars)"
    return None


# ---------------------------------------------------------------------------
# AI spot-check using OpenAI (from verify_translation.py)
# ---------------------------------------------------------------------------

_SPOT_CHECK_PROMPT = """You are a translation QA checker for Tara, a Saudi skincare brand.

Check these English->Arabic translation pairs. For EACH one, respond in TOON format (id|verdict):
- PASS if correct
- ISSUE: <brief reason> if problematic

Flag: wrong meaning, missing content, "Tara" translated instead of kept, broken HTML, English left in Arabic, wrong tone for luxury skincare."""


def _spot_check_batch(client, model, samples):
    """Run AI spot-check on a batch of samples using OpenAI. Returns (verdicts, tokens)."""
    entries = []
    for s in samples:
        entries.append({
            "id": s["id"],
            "value": f"EN: {s['default'][:200]} ||| AR: {s['translated'][:200]}",
        })
    toon_input = to_toon(entries)

    try:
        response = client.responses.create(
            model=model,
            input=f"{_SPOT_CHECK_PROMPT}\n\n{toon_input}",
            reasoning={"effort": "minimal"},
        )
        result = ""
        for item in response.output:
            if item.type == "message":
                for content in item.content:
                    if content.type == "output_text":
                        result += content.text
        result = result.strip()
        if result.startswith("```"):
            lines = result.split("\n")
            if lines[-1].strip() == "```":
                result = "\n".join(lines[1:-1])
            else:
                result = "\n".join(lines[1:])

        verdicts = from_toon(result)
        usage = response.usage
        tokens = (usage.input_tokens or 0) + (usage.output_tokens or 0)
        return verdicts, tokens
    except Exception as e:
        print(f"  AI error: {e}")
        return [], 0


# ---------------------------------------------------------------------------
# Printing helpers
# ---------------------------------------------------------------------------

def _print_issues(issues, cache, limit):
    """Print a summary of detected issues."""
    if not issues:
        print("  None found")
        return
    by_sev = Counter(m.get("severity", "?") for m in issues.values())
    parts = ", ".join(f"{c} {s}" for s, c in by_sev.most_common())
    print(f"  Found: {len(issues)} ({parts})")
    for idx in sorted(issues)[:limit]:
        m = issues[idx]
        print(f"    [{m.get('severity', '?')}] {m['reason']}")
        print(f"      EN: {m['english'][:70]}")
        print(f"      AR: {m['arabic'][:70]}")
    if len(issues) > limit:
        print(f"    ... and {len(issues) - limit} more")


# ===================================================================
# PUBLIC API: Four modes
# ===================================================================

def clean_csv(input_path, output_path=None, *, fix_misaligned=False, keep_all_rows=False):
    """Remove non-translatable rows, dedup, fix formatting.

    Args:
        input_path: Input CSV file path.
        output_path: Output CSV path (default: ``<input>_clean.csv``).
        fix_misaligned: Clear translations that appear to be in the wrong field.
        keep_all_rows: Keep non-translatable rows (just clear their translations).

    Returns:
        dict with ``input_rows``, ``output_rows``, ``removed``, ``cleared`` counts.
    """
    if not output_path:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_clean{ext}"

    fieldnames, rows = _read_csv(input_path)
    print(f"Read {len(rows)} rows from {input_path}\n")

    removed = Counter()
    cleared = Counter()
    misaligned_rows = []
    clean_rows = []

    for i, row in enumerate(rows):
        if is_non_translatable(row):
            # Determine specific reason for reporting
            default = row.get("Default content", "").strip()
            if not default:
                removed["non_translatable_empty"] += 1
            elif row.get("Field", "") == "handle":
                removed["non_translatable_handle"] += 1
            else:
                removed["non_translatable_value"] += 1
            if keep_all_rows:
                row["Translated content"] = ""
                clean_rows.append(row)
            continue

        if is_keep_as_is(row):
            removed["keep_as_is"] += 1
            if keep_all_rows:
                clean_rows.append(row)
            continue

        # Fake translations (identical, no Arabic)
        default = row.get("Default content", "").strip()
        translated = row.get("Translated content", "").strip()

        if translated and translated == default and not has_arabic(translated):
            cleared["identical_not_translated"] += 1
            row["Translated content"] = ""

        # Misalignment
        if fix_misaligned and translated:
            misalign = _detect_misalignment(row)
            if misalign:
                misaligned_rows.append({
                    "row": i,
                    "type": row.get("Type", ""),
                    "id": row.get("Identification", ""),
                    "field": row.get("Field", ""),
                    "default": default[:60],
                    "translated": translated[:60],
                    "issue": misalign,
                })
                cleared[f"misaligned_{misalign}"] += 1
                row["Translated content"] = ""

        clean_rows.append(row)

    _write_csv(output_path, fieldnames, clean_rows)

    # Report
    print(f"{'=' * 60}")
    print(f"  CLEANING REPORT")
    print(f"{'=' * 60}")
    print(f"  Input rows:     {len(rows)}")
    print(f"  Output rows:    {len(clean_rows)}")
    print(f"  Removed:        {len(rows) - len(clean_rows)}")

    if removed:
        print(f"\n  Removed (non-translatable):")
        for reason, count in removed.most_common():
            print(f"    {reason}: {count}")

    if cleared:
        print(f"\n  Cleared translations:")
        for reason, count in cleared.most_common():
            print(f"    {reason}: {count}")

    if misaligned_rows:
        print(f"\n  Misaligned translations ({len(misaligned_rows)}):")
        for m in misaligned_rows[:20]:
            print(f"    [{m['type']}] {m['field']}: {m['issue']}")
            print(f"      Default:    {m['default']}")
            print(f"      Translated: {m['translated']}")
        if len(misaligned_rows) > 20:
            print(f"    ... and {len(misaligned_rows) - 20} more")

        report_file = os.path.splitext(output_path)[0] + "_misaligned.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(misaligned_rows, f, ensure_ascii=False, indent=2)
        print(f"\n  Misaligned report: {report_file}")

    print(f"\n  Clean CSV: {output_path}")
    print(f"{'=' * 60}")

    needs_translation = sum(
        1 for r in clean_rows
        if r.get("Default content", "").strip()
        and not r.get("Translated content", "").strip()
    )
    has_translation = sum(
        1 for r in clean_rows
        if r.get("Translated content", "").strip()
        and has_arabic(r["Translated content"])
    )
    print(f"\n  Translated (Arabic): {has_translation}")
    print(f"  Needs translation:   {needs_translation}")

    return {
        "input_rows": len(rows),
        "output_rows": len(clean_rows),
        "removed": dict(removed),
        "cleared": dict(cleared),
        "misaligned": misaligned_rows,
    }


def verify_coverage(input_path, *, no_ai=True, samples=30, model="gpt-5-nano",
                    verbose=False):
    """Check translation completeness: coverage, gaps, quality issues.

    Args:
        input_path: Translated CSV file path.
        no_ai: Skip AI spot-check (default True).
        samples: Number of samples for AI spot-check.
        model: Model for AI spot-check.
        verbose: Show all issue details.

    Returns:
        dict with ``coverage_pct``, ``translated``, ``gaps``, ``issues``, ``todos``.
    """
    fieldnames, rows = _read_csv(input_path)
    print(f"Read {len(rows)} rows from {input_path}\n")

    translated = []
    gaps = []
    keep_as_is_count = 0
    non_translatable_count = 0
    empty_count = 0
    todos = []

    for i, row in enumerate(rows):
        default = row.get("Default content", "").strip()
        trans = row.get("Translated content", "").strip()

        if not default:
            empty_count += 1
        elif is_non_translatable(row):
            non_translatable_count += 1
        elif is_keep_as_is(row):
            keep_as_is_count += 1
        elif trans:
            translated.append(i)
        else:
            gaps.append(i)

    total_translatable = len(translated) + len(gaps)
    coverage = (len(translated) / total_translatable * 100) if total_translatable else 100

    print(f"{'=' * 60}")
    print(f"  COVERAGE REPORT")
    print(f"{'=' * 60}")
    print(f"  Total rows:           {len(rows)}")
    print(f"  Translatable:         {total_translatable}")
    print(f"  Translated:           {len(translated)}  ({coverage:.1f}%)")
    print(f"  GAPS (missing):       {len(gaps)}")
    print(f"  Keep-as-is:           {keep_as_is_count}")
    print(f"  Non-translatable:     {non_translatable_count}")
    print(f"  Empty (no source):    {empty_count}")
    print(f"{'=' * 60}\n")

    # Add gaps to todo
    for idx in gaps:
        r = rows[idx]
        field_id = f"{r['Type']}|{r['Identification']}|{r['Field']}"
        todos.append({
            "action": "translate",
            "field_id": field_id,
            "row": idx,
            "type": r["Type"],
            "id": r["Identification"],
            "field": r["Field"],
            "default": r["Default content"],
            "issues": ["missing translation"],
        })

    if gaps:
        print(f"GAPS -- {len(gaps)} fields missing translation:\n")
        gap_by_type = Counter(rows[i]["Type"] for i in gaps)
        for t, c in gap_by_type.most_common():
            print(f"  {t}: {c}")
        print()

    # Local quality checks + language detection
    print(f"Local checks on {len(translated)} translated fields...\n")

    issues_by_category = Counter()
    all_issues = []
    spanish_source_count = 0

    for idx in translated:
        row = rows[idx]
        default = row["Default content"].strip()
        trans = row["Translated content"].strip()
        field_id = f"{row['Type']}|{row['Identification']}|{row['Field']}"

        row_issues = []
        todo_action = None

        source_lang = _detect_language(default)
        trans_lang = _detect_language(trans)

        # Spanish in Arabic column
        if trans_lang == "es":
            row_issues.append("Spanish in Arabic column")
            issues_by_category["spanish_not_translated"] += 1
            todo_action = "translate_es_to_ar" if source_lang == "es" else "translate"

        # English in Arabic column (not translated)
        elif trans_lang == "en":
            if default == trans or (len(default) > 3 and re.match(r"^[a-zA-Z\s]+$", default) and default == trans):
                row_issues.append("not translated (EN=AR)")
                issues_by_category["identical_not_translated"] += 1
                todo_action = "translate"
            else:
                ar_check = _detect_language(trans)
                if ar_check != "ar":
                    row_issues.append("English in Arabic column")
                    issues_by_category["english_not_translated"] += 1
                    todo_action = "translate"

        # Source is Spanish but translation is Arabic
        if source_lang == "es" and trans_lang == "ar":
            spanish_source_count += 1
            todos.append({
                "action": "fix_default_es_to_en",
                "field_id": field_id,
                "row": idx,
                "type": row["Type"],
                "id": row["Identification"],
                "field": row["Field"],
                "default": default,
                "issues": ["default content is Spanish, needs English"],
            })

        # HTML integrity
        if "<" in default:
            html_issues = _check_html_integrity(default, trans)
            if html_issues:
                row_issues.extend(html_issues)
                for h in html_issues:
                    issues_by_category["html_" + h.split(":")[0].split("(")[0].strip()] += 1

        # Truncation
        trunc = _check_truncation(default, trans)
        if trunc:
            row_issues.append(trunc)
            issues_by_category["truncated"] += 1
            if not todo_action:
                todo_action = "translate"

        if row_issues:
            all_issues.append((idx, row_issues))

        if todo_action:
            todos.append({
                "action": todo_action,
                "field_id": field_id,
                "row": idx,
                "type": row["Type"],
                "id": row["Identification"],
                "field": row["Field"],
                "default": default,
                "translated": trans,
                "issues": row_issues,
            })

    if all_issues:
        print(f"  {len(all_issues)} rows with issues:\n")
        for cat, count in issues_by_category.most_common():
            print(f"    {cat}: {count}")
        if spanish_source_count:
            print(f"\n    {spanish_source_count} rows have Spanish in 'Default content'")
            print(f"    (Arabic OK, but English column needs fixing)")

        if verbose:
            print()
            for idx, issues in all_issues[:50]:
                r = rows[idx]
                src_lang = _detect_language(r["Default content"])
                lang_tag = f" [{src_lang.upper()}]" if src_lang != "en" else ""
                print(f"    [{r['Type']}] {r['Field']}{lang_tag}")
                print(f"      SRC: {r['Default content'][:80]}")
                print(f"      AR:  {r['Translated content'][:80]}")
                for iss in issues:
                    print(f"      >> {iss}")
    else:
        print("  All local checks passed!")

    # AI spot-check
    if not no_ai:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            print("\nOPENAI_API_KEY not set -- skipping AI spot-check")
        else:
            import random
            from openai import OpenAI
            client = OpenAI(api_key=api_key)

            random.seed(42)
            by_type = {}
            for idx in translated:
                by_type.setdefault(rows[idx]["Type"], []).append(idx)

            sample_indices = []
            per_type = max(1, samples // len(by_type)) if by_type else 0
            for t, type_indices in by_type.items():
                sample_indices.extend(random.sample(type_indices, min(per_type, len(type_indices))))
            remaining = samples - len(sample_indices)
            if remaining > 0:
                pool = [i for i in translated if i not in set(sample_indices)]
                sample_indices.extend(random.sample(pool, min(remaining, len(pool))))

            check_samples = []
            for idx in sample_indices[:samples]:
                r = rows[idx]
                check_samples.append({
                    "id": f"{r['Type']}|{r['Identification']}|{r['Field']}",
                    "default": r["Default content"],
                    "translated": r["Translated content"],
                })

            print(f"\nAI spot-check: {len(check_samples)} samples via {model} "
                  f"(reasoning: minimal)...")

            ai_issues = []
            total_tokens = 0
            BATCH = 30
            for start in range(0, len(check_samples), BATCH):
                batch = check_samples[start:start + BATCH]
                verdicts, tokens = _spot_check_batch(client, model, batch)
                total_tokens += tokens
                for v in verdicts:
                    val = v["value"].strip()
                    if not val.upper().startswith("PASS"):
                        ai_issues.append((v["id"], val))

            print(f"  {len(check_samples)} samples checked ({total_tokens:,} tokens)")

            if ai_issues:
                print(f"\n  AI flagged {len(ai_issues)} issues:\n")
                for field_id, verdict in ai_issues:
                    print(f"    {field_id}")
                    print(f"      {verdict}")
            else:
                print("  All samples passed AI review!")
    else:
        print("\nSkipping AI spot-check (--no-ai)")

    # Verdict
    todo_by_action = Counter(t["action"] for t in todos)
    print(f"\n{'=' * 60}")
    if not todos:
        print("  RESULT: ALL GOOD -- CSV is complete and clean")
    else:
        print(f"  RESULT: {len(todos)} items need fixing")
        print()
        for action, count in todo_by_action.most_common():
            label = {
                "translate": "Translate to Arabic (missing/bad)",
                "translate_es_to_ar": "Translate Spanish -> Arabic",
                "fix_default_es_to_en": "Fix 'Default content': Spanish -> English",
            }.get(action, action)
            print(f"    {label}: {count}")
    print(f"{'=' * 60}")

    return {
        "coverage_pct": coverage,
        "total_rows": len(rows),
        "translatable": total_translatable,
        "translated": len(translated),
        "gaps": len(gaps),
        "issues": len(all_issues),
        "todos": todos,
    }


def validate_csv(input_path, output_path=None, *, skip_ai=False, workers=3,
                 batch_size=50, skip_heuristic=False, no_recheck=False,
                 dry_run=False):
    """Full validation pipeline: rule-based + script + duplicates + heuristic + AI.

    Args:
        input_path: Input CSV file path.
        output_path: Output CSV path (default: ``<input>_validated.csv``).
        skip_ai: Skip AI alignment check.
        workers: Parallel API workers for AI validation.
        batch_size: Pairs per AI batch.
        skip_heuristic: Skip heuristic shift detection.
        no_recheck: Skip pass 2+3 recheck of uncertain mismatches.
        dry_run: Report issues without writing output.

    Returns:
        dict with ``input_rows``, ``translatable``, ``mismatches`` list, etc.
    """
    if not output_path:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_validated{ext}"

    fieldnames, rows = _read_csv(input_path)
    print(f"Read {len(rows)} rows from {input_path}\n")

    # --- Step 1: Remove untranslatable rows ---
    print("Step 1: Removing untranslatable rows...")
    translatable = []
    removed = Counter()
    for row in rows:
        skip, reason = _is_untranslatable_extended(
            row.get("Field", ""), row.get("Default content", ""))
        if skip:
            removed[reason] += 1
        else:
            translatable.append(row)

    total_removed = len(rows) - len(translatable)
    print(f"  Removed: {total_removed}")
    for reason, count in removed.most_common():
        print(f"    {reason}: {count}")
    print(f"  Remaining: {len(translatable)}")

    cache = RowCache(translatable)

    # --- Step 2: Script analysis ---
    print(f"\nStep 2: Script & structural analysis...")
    script_issues = _detect_script_issues(cache)
    _print_issues(script_issues, cache, 5)

    # --- Step 3: Duplicate detection ---
    print(f"\nStep 3: Duplicate translation detection...")
    dup_issues = _detect_duplicates(cache)
    _print_issues(dup_issues, cache, 3)

    # --- Step 4: Heuristic shift detection ---
    heuristic_shifts = set()
    if not skip_heuristic:
        print(f"\nStep 4: Heuristic shift detection (+/-{MAX_SHIFT_OFFSET} offsets)...")
        heuristic_shifts = _detect_shifts(cache)
        if heuristic_shifts:
            print(f"  Potential shifts: {len(heuristic_shifts)}")
            for idx in sorted(heuristic_shifts)[:5]:
                print(f"    [{cache.rows[idx].get('Type', '')}] {cache.field(idx)}")
                print(f"      EN: {cache.eng(idx, 60)}")
                print(f"      AR: {cache.ar(idx, 60)}")
            if len(heuristic_shifts) > 5:
                print(f"    ... and {len(heuristic_shifts) - 5} more")
        else:
            print("  No shifts detected")

    # --- Step 5: AI validation ---
    ai_mismatches = []
    if not skip_ai:
        print(f"\nStep 5: AI alignment check ({MODEL})...")
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            print("  ERROR: Set ANTHROPIC_API_KEY in .env (or use --skip-ai)")
            sys.exit(1)
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        ai_mismatches = _run_ai_validation(
            client, cache, batch_size, no_recheck, workers)
    else:
        print("\nSkipping AI alignment check (--skip-ai)")

    # --- Merge all findings (AI > script > heuristic > duplicates) ---
    all_flagged = {}
    for idx, m in dup_issues.items():
        all_flagged[idx] = m
    for idx in heuristic_shifts:
        if idx not in all_flagged:
            all_flagged[idx] = _build_mismatch(cache, idx,
                "heuristic: adjacent row cross-match", "heuristic")
    for idx, m in script_issues.items():
        if idx not in all_flagged or m.get("severity") == "high":
            all_flagged[idx] = m
    for m in ai_mismatches:
        all_flagged[m["row_index"]] = m

    mismatches = sorted(all_flagged.values(), key=lambda m: m["row_index"])

    # --- Report ---
    print(f"\n{'=' * 60}")
    print(f"  VALIDATION REPORT")
    print(f"{'=' * 60}")
    print(f"  Input rows:         {len(rows)}")
    print(f"  Untranslatable:     {total_removed} (removed)")
    print(f"  Translatable:       {len(translatable)}")
    print(f"  -- By layer --")
    print(f"  Script/structural:  {len(script_issues)}")
    print(f"  Duplicate Arabic:   {len(dup_issues)}")
    print(f"  Heuristic shifts:   {len(heuristic_shifts)}")
    print(f"  AI mismatches:      {len(ai_mismatches)}")
    print(f"  -- Merged --")
    print(f"  Total flagged:      {len(mismatches)}")
    for src, cnt in Counter(m.get("source", "?") for m in mismatches).most_common():
        print(f"    {src}: {cnt}")

    if mismatches:
        print(f"\n  Flagged rows:")
        for m in mismatches[:30]:
            conf = m.get("confidence", m.get("severity", ""))
            print(f"    [{m['source']}] ({conf}) [{m['type']}] {m['field']}")
            print(f"      EN: {m['english'][:80]}")
            print(f"      AR: {m['arabic'][:80]}")
            if m.get("reason"):
                print(f"      Why: {m['reason']}")
        if len(mismatches) > 30:
            print(f"    ... and {len(mismatches) - 30} more")

        mismatch_set = {m["row_index"] for m in mismatches}
        for idx in mismatch_set:
            translatable[idx]["Translated content"] = ""
        print(f"\n  Cleared {len(mismatch_set)} mismatched translations")

        report_path = os.path.splitext(output_path)[0] + "_mismatches.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(mismatches, f, ensure_ascii=False, indent=2)
        print(f"  Report: {report_path}")

    if dry_run:
        print(f"\n  DRY RUN -- no output written")
    else:
        _write_csv(output_path, fieldnames, translatable)
        print(f"\n  Output: {output_path}")

    has = sum(1 for r in translatable if r.get("Translated content", "").strip())
    print(f"\n  With translation:   {has}")
    print(f"  Needs translation:  {len(translatable) - has}")
    print(f"{'=' * 60}")

    return {
        "input_rows": len(rows),
        "translatable": len(translatable),
        "removed": total_removed,
        "script_issues": len(script_issues),
        "duplicate_issues": len(dup_issues),
        "heuristic_shifts": len(heuristic_shifts),
        "ai_mismatches": len(ai_mismatches),
        "total_flagged": len(mismatches),
        "mismatches": mismatches,
    }


def generate_todo(input_path, output_path=None):
    """Generate JSON todo list for re-translation.

    Runs ``verify_coverage`` and writes the todo list to a JSON file.

    Args:
        input_path: Translated CSV file path.
        output_path: Output JSON path (default: ``<input>_todo.json``).

    Returns:
        list of todo items.
    """
    if not output_path:
        base, _ext = os.path.splitext(input_path)
        output_path = f"{base}_todo.json"

    result = verify_coverage(input_path, no_ai=True)
    todos = result["todos"]

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(todos, f, ensure_ascii=False, indent=2)

    todo_by_action = Counter(t["action"] for t in todos)
    print(f"\nTo-do file: {output_path}")
    if todos:
        print(f"\n  {len(todos)} items:")
        for action, count in todo_by_action.most_common():
            label = {
                "translate": "Translate to Arabic (missing/bad)",
                "translate_es_to_ar": "Translate Spanish -> Arabic",
                "fix_default_es_to_en": "Fix 'Default content': Spanish -> English",
            }.get(action, action)
            print(f"    {label}: {count}")

    return todos


# ===================================================================
# CLI entry point
# ===================================================================

def main():
    parser = argparse.ArgumentParser(
        description="CSV validation: clean, verify, validate, or generate todo")
    parser.add_argument("--input", required=True, help="Input CSV file")
    parser.add_argument("--output", default=None, help="Output path (default: auto)")
    parser.add_argument("--mode", default="validate",
                        choices=("clean", "verify", "validate", "todo"),
                        help="Operation mode (default: validate)")
    parser.add_argument("--skip-ai", action="store_true",
                        help="Skip AI validation (validate/verify mode)")
    parser.add_argument("--workers", type=int, default=3,
                        help="Parallel AI workers (default: 3)")
    parser.add_argument("--batch-size", type=int, default=50,
                        help="Pairs per AI batch (default: 50)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report without writing output")
    parser.add_argument("--skip-heuristic", action="store_true",
                        help="Skip heuristic shift detection (validate mode)")
    parser.add_argument("--no-recheck", action="store_true",
                        help="Skip pass 2+3 recheck (validate mode)")
    parser.add_argument("--fix-misaligned", action="store_true",
                        help="Clear misaligned translations (clean mode)")
    parser.add_argument("--keep-all-rows", action="store_true",
                        help="Keep non-translatable rows (clean mode)")
    parser.add_argument("--samples", type=int, default=30,
                        help="AI spot-check sample count (verify mode)")
    parser.add_argument("--model", default="gpt-5-nano",
                        help="AI spot-check model (verify mode)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Show all issue details")
    args = parser.parse_args()

    load_dotenv()

    if args.mode == "clean":
        clean_csv(args.input, args.output,
                  fix_misaligned=args.fix_misaligned,
                  keep_all_rows=args.keep_all_rows)

    elif args.mode == "verify":
        verify_coverage(args.input,
                        no_ai=args.skip_ai,
                        samples=args.samples,
                        model=args.model,
                        verbose=args.verbose)

    elif args.mode == "validate":
        validate_csv(args.input, args.output,
                     skip_ai=args.skip_ai,
                     workers=args.workers,
                     batch_size=args.batch_size,
                     skip_heuristic=args.skip_heuristic,
                     no_recheck=args.no_recheck,
                     dry_run=args.dry_run)

    elif args.mode == "todo":
        generate_todo(args.input, args.output)


if __name__ == "__main__":
    main()
