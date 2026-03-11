#!/usr/bin/env python3
"""Validate translation CSV: detect misaligned rows and remove untranslatable data.

Uses Claude Haiku 4.5 to verify each English↔Arabic pair actually corresponds
(catches row shifts where translations slid up/down). Also strips rows that
contain non-translatable data (URLs, IDs, config JSON, images, etc.).

Validation pipeline:
1. Rule-based: remove untranslatable rows (URLs, IDs, images, config)
2. Script analysis: detect missing Arabic, untranslated copies, length anomalies
3. Duplicate detection: flag identical Arabic for different English sources
4. Heuristic: detect systematic row shifts (N±1..N±5 cross-matching)
5. AI (Haiku 4.5): resource-grouped two-pass verification
   - Pass 1: batch check with confidence + content-category hints
   - Pass 2: re-verify uncertain mismatches with neighboring-row context
   - Pass 3 (optional): back-translate suspicious pairs for final verdict

Usage:
    python validate_csv.py --input Arabic/translations.csv
    python validate_csv.py --input Arabic/translations.csv --dry-run
    python validate_csv.py --input Arabic/translations.csv --skip-ai
    python validate_csv.py --input Arabic/translations.csv --workers 4
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

import anthropic
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

MODEL = "claude-haiku-4-5-20251001"
HAIKU_INPUT_COST = 0.80   # $/1M input tokens
HAIKU_OUTPUT_COST = 4.00  # $/1M output tokens

# ---------------------------------------------------------------------------
# Rule-based untranslatable detection
# ---------------------------------------------------------------------------

SKIP_FIELD_PATTERNS = re.compile("|".join([
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


def is_untranslatable(field, value):
    """Return (should_remove, reason) for a field+value pair."""
    v = (value or "").strip()
    if not v:
        return True, "empty"
    if SKIP_FIELD_PATTERNS.search(field):
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


# ---------------------------------------------------------------------------
# Text extraction & caching
# ---------------------------------------------------------------------------

_STRIP_TAGS_RE = re.compile(r"<[^>]+>")
_STYLE_RE = re.compile(r"<style[^>]*>.*?</style>", re.DOTALL | re.IGNORECASE)
_SCRIPT_RE = re.compile(r"<script[^>]*>.*?</script>", re.DOTALL | re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")
_ENTITY_MAP = {"&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">"}
_ENTITY_NUM_RE = re.compile(r"&#\d+;")
_ARABIC_RE = re.compile(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]")
_LATIN_RE = re.compile(r"[a-zA-Z]")


def extract_visible_text(html_or_text, max_chars=300):
    """Extract visible text from HTML/rich_text for comparison."""
    if not html_or_text:
        return ""
    text = html_or_text.strip()

    # Rich text JSON → extract text nodes
    if text.startswith("{") and '"type"' in text:
        try:
            data = json.loads(text)
            parts = []
            def walk(node):
                if isinstance(node, dict):
                    if node.get("type") == "text" and "value" in node:
                        parts.append(node["value"])
                    for child in node.get("children", []):
                        walk(child)
                elif isinstance(node, list):
                    for item in node:
                        walk(item)
            walk(data)
            if parts:
                text = " ".join(parts)
        except (json.JSONDecodeError, TypeError):
            pass

    text = _STYLE_RE.sub("", text)
    text = _SCRIPT_RE.sub("", text)
    text = _STRIP_TAGS_RE.sub(" ", text)
    for entity, repl in _ENTITY_MAP.items():
        text = text.replace(entity, repl)
    text = _ENTITY_NUM_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text[:max_chars]


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


def arabic_ratio(text):
    """Fraction of alphabetic chars that are Arabic (0.0–1.0)."""
    if not text:
        return 0.0
    ar = len(_ARABIC_RE.findall(text))
    la = len(_LATIN_RE.findall(text))
    return ar / (ar + la) if (ar + la) else 0.0


def classify_content(field, text):
    """Classify field into content category for AI hints."""
    fl = field.lower()
    if any(p in fl for p in (".title", ".heading", ".label", ".name",
                              "button_label", "button_text", "cta_text",
                              ".tab_", ".menu_")):
        return "heading"
    if any(p in fl for p in (".body", ".description", ".content",
                              ".rich_text", ".paragraph", ".details")):
        return "body"
    # Check text content for ingredients (comma-separated Latin words)
    if text and re.match(r"^[A-Z][a-z]+(\s[A-Z][a-z]+)*(,\s*[A-Z])", text):
        return "ingredients"
    return "text"


def build_mismatch(cache, idx, reason, source, severity="medium", confidence=None):
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
# Layer 2: Script & structural heuristics
# ---------------------------------------------------------------------------

def detect_script_issues(cache):
    """Detect issues via script analysis. Returns dict of {idx: mismatch}."""
    issues = {}
    for i in range(len(cache.rows)):
        eng = cache.eng(i)
        ar = cache.ar(i)
        field = cache.field(i)
        if not eng or not ar:
            continue

        # Untranslated: Arabic identical to English (>2 words)
        if eng == ar and len(eng) > 5 and len(eng.split()) > 2:
            issues[i] = build_mismatch(cache, i,
                "untranslated: Arabic identical to English",
                "script_analysis", "high")
            continue

        # No Arabic script at all (allow INCI / genus-species)
        ar_r = arabic_ratio(ar)
        if ar_r == 0.0 and len(ar) > 10:
            if not re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+", ar):
                issues[i] = build_mismatch(cache, i,
                    "no Arabic script in translation",
                    "script_analysis", "high")
                continue

        # Very low Arabic ratio in substantial text (likely mixed up)
        if ar_r < 0.15 and len(ar) > 50 and len(_ARABIC_RE.findall(ar)) < 5:
            issues[i] = build_mismatch(cache, i,
                f"only {ar_r:.0%} Arabic chars in translation",
                "script_analysis", "medium")
            continue

        # Length anomalies
        eng_len, ar_len = len(eng), len(ar)
        if eng_len > 20 and ar_len > 20:
            ratio = ar_len / eng_len
            if ratio > 5.0:
                issues[i] = build_mismatch(cache, i,
                    f"Arabic is {ratio:.1f}x longer than English",
                    "script_analysis", "medium")
            elif ratio < 0.1:
                issues[i] = build_mismatch(cache, i,
                    f"Arabic is {ratio:.1f}x shorter than English",
                    "script_analysis", "medium")
        elif classify_content(field, eng) == "heading" and len(eng.split()) <= 4 and ar_len > 200:
            issues[i] = build_mismatch(cache, i,
                f"heading has paragraph-length translation ({ar_len} chars)",
                "script_analysis", "medium")

    return issues


# ---------------------------------------------------------------------------
# Layer 3: Duplicate translation detection
# ---------------------------------------------------------------------------

def detect_duplicates(cache):
    """Flag identical Arabic for substantially different English. Returns {idx: mismatch}."""
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
                issues[idx] = build_mismatch(cache, idx,
                    "duplicate Arabic for different English",
                    "duplicate", "medium")

    return issues


# ---------------------------------------------------------------------------
# Layer 4: Multi-offset heuristic shift detection
# ---------------------------------------------------------------------------

MAX_SHIFT_OFFSET = 5


def detect_shifts(cache):
    """Detect row shifts by cross-matching within ±5 offsets per resource.
    Returns set of shifted row indices."""
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
                    break  # one match is enough

    return shifted


# ---------------------------------------------------------------------------
# AI validation: resource-grouped, two-pass + back-translation
# ---------------------------------------------------------------------------

FEW_SHOT_EXAMPLES = """Examples:

OK pairs:
- EN: "Award-Winning Haircare: Botanical Extracts + Advanced Science"
  AR: "عناية بالشعر حاصلة على جوائز: مستخلصات نباتية + علم متقدم"
  → OK (same meaning)
- EN: "Activated Charcoal Face Wash" / AR: "غسول الوجه بالفحم المنشط" → OK
- EN: "Free Of" / AR: "خالٍ من" → OK
- EN: "Aqua, Glycerin, Cetearyl Alcohol" / AR: "Aqua, Glycerin, Cetearyl Alcohol" → OK (INCI kept in Latin)
- EN: "Our gentle formula cleanses without stripping natural oils"
  AR: "تركيبتنا اللطيفة تنظف دون إزالة الزيوت الطبيعية" → OK

MISMATCH pairs (row shift):
- EN: "Hydrating Face Cream with Hyaluronic Acid"
  AR: "شامبو مقوي للشعر بالكيراتين" → MISMATCH (hair shampoo ≠ face cream)
- EN: "Key Benefits"
  AR: "ينظف البشرة بعمق ويزيل الشوائب والزيوت الزائدة" → MISMATCH (heading got body text)
- EN: "How to Use"
  AR: "زبدة الشيا العضوية تغذي وترطب البشرة الجافة" → MISMATCH (usage heading got ingredient desc)
- EN: "Rose Water Toner helps balance skin pH"
  AR: "كريم الليل بالريتينول يجدد البشرة" → MISMATCH (toner ≠ night cream)
"""


def estimate_cost(num_pairs, batch_size):
    """Estimate total API cost including recheck pass."""
    batches = (num_pairs + batch_size - 1) // batch_size
    inp = num_pairs * 60 + batches * 800
    out = num_pairs * 8
    # Recheck ~10%
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
    """Call Haiku with retries, return parsed JSON list."""
    for attempt in range(retries):
        try:
            resp = client.messages.create(
                model=MODEL, max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            return _parse_json_response(resp.content[0].text)
        except json.JSONDecodeError:
            if attempt < retries - 1:
                print(f" json-retry", end="", flush=True)
                time.sleep(1)
        except Exception as e:
            if attempt < retries - 1:
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
        "Check each Arabic↔English pair.\n\n"
        "Content categories in brackets: heading (short title/label), body (description/paragraph), "
        "ingredients (INCI list), text (other).\n\n"
        "MISMATCH only if:\n"
        "- Arabic is about a COMPLETELY different topic/product (row shift)\n"
        "- Arabic is a translation of a different English text\n"
        "- A heading got a body-length translation or vice versa\n"
        "- Content categories clearly don't match (ingredients ↔ usage instructions)\n\n"
        "OK if:\n"
        "- Reasonable translation, even imperfect/paraphrased\n"
        "- Same topic, different wording\n"
        "- INCI names kept in Latin within Arabic = CORRECT\n"
        "- Brand name Tara/تارا in both = fine\n"
        "- Minor omissions, additions, formatting diffs = fine\n\n"
        + FEW_SHOT_EXAMPLES +
        "\nConfidence: high (clearly wrong), medium (likely wrong), low (uncertain).\n"
        "Return JSON array of MISMATCHES ONLY:\n"
        '[{"i":<n>,"ok":false,"confidence":"high"|"medium"|"low","reason":"brief"}]\n'
        "All OK → return []\n\n"
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
                    ctx.append(f"     ({d}{abs(delta)}) EN: {ce} → AR: {ca}")

        lines.append(f"{bi}. [{field}] EN: {eng}")
        lines.append(f"   AR: {ara}")
        lines.append(f"   (Flagged: {m.get('reason', '?')})")
        lines.extend(ctx)
        lines.append("")

    return (
        "Recheck these uncertain translation flags. Context rows are provided.\n"
        "Be LENIENT — only confirm MISMATCH if clearly wrong product/topic.\n"
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
        "Keep it literal — preserve the topic, product names, and key details.\n"
        "Return a JSON array: [{\"i\": <n>, \"en\": \"back-translation\"}]\n\n"
        + "\n".join(lines)
    )


def run_ai_validation(client, cache, batch_size, no_recheck, workers):
    """Run the full AI validation pipeline. Returns list of confirmed mismatches."""
    # Collect pairs
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

    est_cost, est_batches = estimate_cost(n, batch_size)
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
                m = build_mismatch(cache, row_idx,
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

            # --- Pass 3: back-translate remaining uncertain for final check ---
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
                        # Check if back-translation topic matches original
                        orig_words = set(orig_en.lower().split())
                        bt_words = set(bt_en.lower().split())
                        if orig_words and bt_words:
                            overlap = len(orig_words & bt_words) / max(len(orig_words), len(bt_words))
                            if overlap > 0.3:
                                # Back-translation matches → false alarm
                                rescued += 1
                                continue
                    final_confirmed.append(m)

                if rescued:
                    print(f"  Pass 3: rescued {rescued} false alarms via back-translation")
                    # Rebuild: high_conf + final_confirmed
                    ai_mismatches = high_conf + final_confirmed
        else:
            print(f"\n  All {len(ai_mismatches)} are high-confidence, skipping recheck")

    print(f"  Final AI mismatches: {len(ai_mismatches)}")
    return ai_mismatches


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Validate translation CSV: detect misaligned rows and remove untranslatable data")
    parser.add_argument("--input", required=True, help="Input CSV file")
    parser.add_argument("--output", default=None,
                        help="Output CSV (default: <input>_validated.csv)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report issues without writing output")
    parser.add_argument("--batch-size", type=int, default=50,
                        help="Pairs per AI batch (default: 50)")
    parser.add_argument("--workers", type=int, default=3,
                        help="Parallel API workers (default: 3)")
    parser.add_argument("--skip-ai", action="store_true",
                        help="Skip AI alignment check")
    parser.add_argument("--skip-heuristic", action="store_true",
                        help="Skip heuristic shift detection")
    parser.add_argument("--no-recheck", action="store_true",
                        help="Skip pass 2+3 recheck of uncertain mismatches")
    args = parser.parse_args()

    if not args.output:
        base, ext = os.path.splitext(args.input)
        args.output = f"{base}_validated{ext}"

    load_dotenv()

    # Read CSV
    with open(args.input, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    print(f"Read {len(rows)} rows from {args.input}\n")

    # --- Step 1: Remove untranslatable rows ---
    print("Step 1: Removing untranslatable rows...")
    translatable = []
    removed = Counter()
    for row in rows:
        skip, reason = is_untranslatable(row.get("Field", ""), row.get("Default content", ""))
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
    script_issues = detect_script_issues(cache)
    _print_issues(script_issues, cache, 5)

    # --- Step 3: Duplicate detection ---
    print(f"\nStep 3: Duplicate translation detection...")
    dup_issues = detect_duplicates(cache)
    _print_issues(dup_issues, cache, 3)

    # --- Step 4: Heuristic shift detection ---
    heuristic_shifts = set()
    if not args.skip_heuristic:
        print(f"\nStep 4: Heuristic shift detection (±{MAX_SHIFT_OFFSET} offsets)...")
        heuristic_shifts = detect_shifts(cache)
        if heuristic_shifts:
            print(f"  Potential shifts: {len(heuristic_shifts)}")
            for idx in sorted(heuristic_shifts)[:5]:
                print(f"    [{cache.rows[idx].get('Type','')}] {cache.field(idx)}")
                print(f"      EN: {cache.eng(idx, 60)}")
                print(f"      AR: {cache.ar(idx, 60)}")
            if len(heuristic_shifts) > 5:
                print(f"    ... and {len(heuristic_shifts) - 5} more")
        else:
            print(f"  No shifts detected")

    # --- Step 5: AI validation ---
    ai_mismatches = []
    if not args.skip_ai:
        print(f"\nStep 5: AI alignment check ({MODEL})...")
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            print("  ERROR: Set ANTHROPIC_API_KEY in .env (or use --skip-ai)")
            sys.exit(1)
        client = anthropic.Anthropic(api_key=api_key)
        ai_mismatches = run_ai_validation(
            client, cache, args.batch_size, args.no_recheck, args.workers)
    else:
        print("\nSkipping AI alignment check (--skip-ai)")

    # --- Merge all findings (AI > script > heuristic > duplicates) ---
    all_flagged = {}
    for idx, m in dup_issues.items():
        all_flagged[idx] = m
    for idx in heuristic_shifts:
        if idx not in all_flagged:
            all_flagged[idx] = build_mismatch(cache, idx,
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
    print(f"  ── By layer ──")
    print(f"  Script/structural:  {len(script_issues)}")
    print(f"  Duplicate Arabic:   {len(dup_issues)}")
    print(f"  Heuristic shifts:   {len(heuristic_shifts)}")
    print(f"  AI mismatches:      {len(ai_mismatches)}")
    print(f"  ── Merged ──")
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

        report_path = os.path.splitext(args.output)[0] + "_mismatches.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(mismatches, f, ensure_ascii=False, indent=2)
        print(f"  Report: {report_path}")

    if args.dry_run:
        print(f"\n  DRY RUN — no output written")
    else:
        with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(translatable)
        print(f"\n  Output: {args.output}")

    has = sum(1 for r in translatable if r.get("Translated content", "").strip())
    print(f"\n  With translation:   {has}")
    print(f"  Needs translation:  {len(translatable) - has}")
    print(f"{'=' * 60}")


def _print_issues(issues, cache, limit):
    """Print a summary of detected issues."""
    if not issues:
        print(f"  None found")
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


if __name__ == "__main__":
    main()
