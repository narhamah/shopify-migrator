#!/usr/bin/env python3
"""Validate translation CSV: detect misaligned rows and remove untranslatable data.

Uses Claude Haiku 4.5 to verify each English↔Arabic pair actually corresponds
(catches row shifts where translations slid up/down). Also strips rows that
contain non-translatable data (URLs, IDs, config JSON, images, etc.).

Five-layer validation:
1. Rule-based: remove untranslatable rows (URLs, IDs, images, config)
2. Script analysis: detect missing Arabic, untranslated copies, length anomalies
3. Duplicate detection: flag identical Arabic for different English sources
4. Heuristic: detect systematic row shifts (N+1/N-1 cross-matching)
5. AI (Haiku 4.5): two-pass verification with confidence scoring
   - Pass 1: batch check all pairs, collect mismatches with confidence
   - Pass 2: re-verify low/medium confidence mismatches with more context

Usage:
    python validate_csv.py --input Arabic/translations.csv
    python validate_csv.py --input Arabic/translations.csv --dry-run
    python validate_csv.py --input Arabic/translations.csv --skip-ai
    python validate_csv.py --input Arabic/translations.csv --batch-size 50
    python validate_csv.py --input Arabic/translations.csv --no-recheck
"""

import argparse
import csv
import json
import math
import os
import re
import sys
import time
from collections import Counter, defaultdict

import anthropic
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

MODEL = "claude-haiku-4-5-20251001"

# Approximate Haiku 4.5 pricing (input/output per 1M tokens)
HAIKU_INPUT_COST = 0.80   # $/1M input tokens
HAIKU_OUTPUT_COST = 4.00  # $/1M output tokens

# ---------------------------------------------------------------------------
# Untranslatable detection (rule-based, no API needed)
# ---------------------------------------------------------------------------

SKIP_FIELD_PATTERNS = [
    r"\.image$", r"\.image_\d", r"\.image_\d_mobile", r"\.icon:",
    r"\.link$", r"_url$", r"\.logo", r"\.favicon",
    r"google_maps", r"form_id", r"portal_id", r"anchor_id",
    r"worker_url", r"default_lat", r"default_lng",
    r"max_height", r"max_width", r"\.video$", r"\.video_url",
    r"\.color$", r"\.color_", r"color_scheme",
    r"\.opacity", r"\.padding", r"\.margin",
    r"font_size", r"border_radius",
]


def is_untranslatable_field(field):
    """Return True if this field key should not be translated."""
    for pat in SKIP_FIELD_PATTERNS:
        if re.search(pat, field):
            return True
    return False


def is_untranslatable_value(value):
    """Return True if this value is not translatable text."""
    if not value or not value.strip():
        return True
    v = value.strip()
    # URLs, paths, GIDs
    if v.startswith(("shopify://", "http://", "https://", "/", "gid://")):
        return True
    # Pure numbers (including decimals, negatives)
    if re.match(r"^-?\d+\.?\d*$", v):
        return True
    # Hex IDs / color codes
    if re.match(r"^#?[0-9a-fA-F]{6,}$", v):
        return True
    # Short hex (3-char colors)
    if re.match(r"^#[0-9a-fA-F]{3}$", v):
        return True
    # JSON arrays of GIDs/IDs
    if v.startswith("[") and v.endswith("]"):
        try:
            parsed = json.loads(v)
            if isinstance(parsed, list) and all(
                isinstance(x, str) and (x.startswith("gid://") or re.match(r"^\d+$", x))
                for x in parsed
            ):
                return True
        except (json.JSONDecodeError, TypeError):
            pass
    # Config JSON
    if v.startswith("{") and ('"reviewCount"' in v or '"formId"' in v):
        return True
    # Pure CSS/style blocks with no visible text
    if v.strip().startswith("<style>") and "</style>" in v and len(v) > 200:
        no_style = re.sub(r"<style>.*?</style>", "", v, flags=re.DOTALL)
        no_tags = re.sub(r"<[^>]+>", " ", no_style).strip()
        if not no_tags:
            return True
    # Boolean-like values
    if v.lower() in ("true", "false", "yes", "no", "none", "null"):
        return True
    # CSS values (px, rem, em, %, vh, vw)
    if re.match(r"^\d+(\.\d+)?(px|rem|em|%|vh|vw|s|ms)$", v):
        return True
    return False


def is_untranslatable_row(row):
    """Check if a row should be removed entirely."""
    field = row.get("Field", "")
    default = row.get("Default content", "").strip()

    if not default:
        return True, "empty"
    if is_untranslatable_field(field):
        return True, "field_pattern"
    if is_untranslatable_value(default):
        return True, "untranslatable_value"
    return False, ""


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------

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

    # Strip CSS blocks first, then HTML tags
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    # Clean up whitespace and entities
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&#\d+;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


# ---------------------------------------------------------------------------
# Script analysis helpers
# ---------------------------------------------------------------------------

_ARABIC_RE = re.compile(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]")
_LATIN_RE = re.compile(r"[a-zA-Z]")


def arabic_char_ratio(text):
    """Return fraction of alphabetic characters that are Arabic."""
    if not text:
        return 0.0
    arabic = len(_ARABIC_RE.findall(text))
    latin = len(_LATIN_RE.findall(text))
    total = arabic + latin
    if total == 0:
        return 0.0
    return arabic / total


def text_length_ratio(eng_text, ar_text):
    """Return the length ratio between Arabic and English visible text.

    Arabic text is typically 0.6x–1.5x the English length (in chars).
    Extreme ratios suggest misalignment.
    """
    eng_len = len(eng_text.strip())
    ar_len = len(ar_text.strip())
    if eng_len == 0:
        return float("inf") if ar_len > 0 else 1.0
    return ar_len / eng_len


def is_field_type_heading(field):
    """Check if field name suggests a short heading/title."""
    heading_patterns = [
        r"\.title$", r"\.heading$", r"\.label$", r"\.name$",
        r"\.button_label$", r"\.button_text$", r"\.cta_text$",
        r"\.tab_", r"\.menu_",
    ]
    return any(re.search(p, field) for p in heading_patterns)


def is_field_type_body(field):
    """Check if field name suggests body/description content."""
    body_patterns = [
        r"\.body$", r"\.description$", r"\.content$", r"\.text$",
        r"\.rich_text$", r"\.paragraph$", r"\.details$",
    ]
    return any(re.search(p, field) for p in body_patterns)


# ---------------------------------------------------------------------------
# Script & structural heuristics (layer 2 - no API cost)
# ---------------------------------------------------------------------------

def detect_script_issues(rows):
    """Detect translation issues using script analysis.

    Returns dict of {row_index: {"reason": str, "severity": str}}
    """
    issues = {}

    for i, row in enumerate(rows):
        default = row.get("Default content", "").strip()
        translated = row.get("Translated content", "").strip()
        field = row.get("Field", "")

        if not default or not translated:
            continue

        eng_text = extract_visible_text(default)
        ar_text = extract_visible_text(translated)

        if not eng_text or not ar_text:
            continue

        # 1) Translation is identical to English (not actually translated)
        if eng_text == ar_text and len(eng_text) > 5:
            # Exception: brand names, INCI ingredients, single words that might be the same
            if len(eng_text.split()) > 2:
                issues[i] = {
                    "reason": "untranslated: Arabic identical to English",
                    "severity": "high",
                }
                continue

        # 2) "Arabic" translation has no Arabic characters at all
        ar_ratio = arabic_char_ratio(ar_text)
        if ar_ratio == 0.0 and len(ar_text) > 10:
            # Allow pure-Latin scientific names, INCI lists
            if not re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+", ar_text):  # Genus species
                issues[i] = {
                    "reason": f"no Arabic script in translation (all Latin)",
                    "severity": "high",
                }
                continue

        # 3) Length ratio anomaly
        ratio = text_length_ratio(eng_text, ar_text)
        eng_words = len(eng_text.split())

        # Short heading (1-4 words) mapped to long paragraph
        if is_field_type_heading(field) and eng_words <= 4 and len(ar_text) > 200:
            issues[i] = {
                "reason": f"heading field has paragraph-length translation ({len(ar_text)} chars)",
                "severity": "medium",
            }
            continue

        # Extreme length ratio (only flag if enough text to be meaningful)
        if len(eng_text) > 20 and len(ar_text) > 20:
            if ratio > 5.0:
                issues[i] = {
                    "reason": f"Arabic is {ratio:.1f}x longer than English",
                    "severity": "medium",
                }
            elif ratio < 0.1:
                issues[i] = {
                    "reason": f"Arabic is {ratio:.1f}x shorter than English",
                    "severity": "medium",
                }

    return issues


# ---------------------------------------------------------------------------
# Duplicate translation detection (layer 3 - no API cost)
# ---------------------------------------------------------------------------

def detect_duplicate_translations(rows):
    """Find cases where identical Arabic text maps to very different English text.

    This catches copy-paste errors or systematic fill-down mistakes.
    Returns dict of {row_index: {"reason": str, "severity": str}}
    """
    issues = {}

    # Group rows by normalized Arabic translation
    ar_to_rows = defaultdict(list)
    for i, row in enumerate(rows):
        translated = row.get("Translated content", "").strip()
        if not translated:
            continue
        ar_text = extract_visible_text(translated, 500)
        # Only consider substantial translations (>20 chars)
        if len(ar_text) > 20:
            ar_to_rows[ar_text].append(i)

    for ar_text, indices in ar_to_rows.items():
        if len(indices) < 2:
            continue

        # Get the English texts for these rows
        eng_texts = []
        for idx in indices:
            eng = extract_visible_text(rows[idx].get("Default content", ""), 200)
            eng_texts.append(eng)

        # Check if the English texts are substantially different
        # Use word overlap to measure similarity
        for a in range(len(indices)):
            for b in range(a + 1, len(indices)):
                words_a = set(eng_texts[a].lower().split())
                words_b = set(eng_texts[b].lower().split())
                if not words_a or not words_b:
                    continue
                overlap = len(words_a & words_b) / max(len(words_a), len(words_b))
                # If English texts share less than 30% words but have identical Arabic
                if overlap < 0.3 and len(words_a) >= 3 and len(words_b) >= 3:
                    for idx in [indices[a], indices[b]]:
                        if idx not in issues:
                            issues[idx] = {
                                "reason": "duplicate Arabic for different English sources",
                                "severity": "medium",
                            }

    return issues


# ---------------------------------------------------------------------------
# Heuristic shift detection (layer 4)
# ---------------------------------------------------------------------------

def detect_sequential_shifts(rows):
    """Detect systematic row shifts by cross-matching adjacent rows.

    If row N's Arabic matches row N+1's English better than row N's English,
    that indicates a shift. Returns set of row indices that are shifted.
    """
    shifted = set()

    indexed = []
    for i, row in enumerate(rows):
        eng = extract_visible_text(row.get("Default content", ""), 150)
        ara = extract_visible_text(row.get("Translated content", ""), 150)
        if eng and ara and len(eng) >= 10 and len(ara) >= 5:
            indexed.append((i, eng, ara))

    if len(indexed) < 3:
        return shifted

    # Group by resource type + ID to only compare within same resource
    by_resource = {}
    for idx, (i, eng, ara) in enumerate(indexed):
        row = rows[i]
        key = (row.get("Type", ""), row.get("Identification", ""))
        if key not in by_resource:
            by_resource[key] = []
        by_resource[key].append((i, eng, ara, idx))

    for key, group in by_resource.items():
        if len(group) < 2:
            continue

        for pos in range(len(group)):
            i, eng, ara, _ = group[pos]
            eng_words = set(eng.lower().split())
            ara_latin = set(re.findall(r"[a-zA-Z]+", ara.lower()))

            # Check: does this row's Arabic match the NEXT row's English?
            if pos + 1 < len(group):
                _, next_eng, _, _ = group[pos + 1]
                next_words = set(next_eng.lower().split())

                if len(eng_words) >= 3 and len(next_words) >= 3:
                    overlap_current = len(ara_latin & eng_words)
                    overlap_next = len(ara_latin & next_words)
                    if overlap_next > overlap_current and overlap_next >= 3:
                        shifted.add(i)

            # Check: does this row's Arabic match the PREVIOUS row's English?
            if pos > 0:
                _, prev_eng, _, _ = group[pos - 1]
                prev_words = set(prev_eng.lower().split())

                if len(eng_words) >= 3 and len(prev_words) >= 3:
                    overlap_current = len(ara_latin & eng_words)
                    overlap_prev = len(ara_latin & prev_words)
                    if overlap_prev > overlap_current and overlap_prev >= 3:
                        shifted.add(i)

    return shifted


# ---------------------------------------------------------------------------
# AI-based alignment validation (Claude Haiku 4.5)
# ---------------------------------------------------------------------------

FEW_SHOT_EXAMPLES = """Examples:

CORRECT (OK) pairs:
- EN: "Award-Winning Haircare: Botanical Extracts + Advanced Science"
  AR: "عناية بالشعر حاصلة على جوائز: مستخلصات نباتية + علم متقدم"
  → OK, confidence: high (same meaning, same structure)

- EN: "Activated Charcoal Face Wash"
  AR: "غسول الوجه بالفحم المنشط"
  → OK, confidence: high (same product name)

- EN: "Free Of"
  AR: "خالٍ من"
  → OK, confidence: high (heading translation)

- EN: "Aqua, Glycerin, Cetearyl Alcohol, Butyrospermum Parkii"
  AR: "Aqua, Glycerin, Cetearyl Alcohol, Butyrospermum Parkii"
  → OK, confidence: high (INCI list — kept in Latin is correct)

- EN: "Our gentle formula cleanses without stripping natural oils"
  AR: "تركيبتنا اللطيفة تنظف دون إزالة الزيوت الطبيعية"
  → OK, confidence: high (accurate translation, cosmetics context)

MISMATCHED pairs (row shift):
- EN: "Hydrating Face Cream with Hyaluronic Acid"
  AR: "شامبو مقوي للشعر بالكيراتين"
  → MISMATCH, confidence: high (Arabic says "keratin hair shampoo" — different product)

- EN: "Key Benefits"
  AR: "ينظف البشرة بعمق ويزيل الشوائب والزيوت الزائدة"
  → MISMATCH, confidence: high (Arabic is a product description, not a heading)

- EN: "How to Use"
  AR: "زبدة الشيا العضوية تغذي وترطب البشرة الجافة"
  → MISMATCH, confidence: high (Arabic describes shea butter benefits, not usage)

- EN: "Rose Water Toner helps balance skin pH and tighten pores"
  AR: "كريم الليل بالريتينول يجدد البشرة أثناء النوم"
  → MISMATCH, confidence: high (Arabic says "retinol night cream" — completely different product)
"""


def estimate_cost(num_pairs, batch_size, recheck_ratio=0.1):
    """Estimate API cost for validation including potential recheck pass."""
    num_batches = (num_pairs + batch_size - 1) // batch_size
    # Pass 1: ~60 tokens per pair input + ~600 tokens system/few-shot per batch
    input_tokens = num_pairs * 60 + num_batches * 800
    output_tokens = num_pairs * 8

    # Pass 2 recheck: ~10% of pairs, more context
    recheck_pairs = int(num_pairs * recheck_ratio)
    recheck_batches = max(1, (recheck_pairs + 15) // 15)
    input_tokens += recheck_pairs * 120 + recheck_batches * 800
    output_tokens += recheck_pairs * 15

    cost = (input_tokens / 1_000_000 * HAIKU_INPUT_COST +
            output_tokens / 1_000_000 * HAIKU_OUTPUT_COST)
    return cost, num_batches


def validate_batch(client, pairs, fields=None):
    """Send a batch of (english, arabic) pairs to Haiku for alignment check.

    Returns list of {"i": int, "ok": false, "confidence": str, "reason": str}.
    """
    lines = []
    for i, (eng, ara) in enumerate(pairs):
        field_hint = f" [{fields[i]}]" if fields and i < len(fields) else ""
        lines.append(f"{i}.{field_hint} EN: {eng}")
        lines.append(f"   AR: {ara}")

    prompt = (
        "You are a translation QA checker for Tara, a skincare/haircare brand. "
        "Check if each Arabic translation correctly corresponds to its English source.\n\n"
        "Flag as MISMATCH ONLY if:\n"
        "- Arabic is about a COMPLETELY DIFFERENT topic/product (row shift)\n"
        "- Arabic is clearly a translation of a different English text\n"
        "- A short heading (1-3 words) received a long paragraph as translation\n"
        "- Content categories don't match (e.g., ingredient list vs. usage instructions)\n\n"
        "Flag as OK if:\n"
        "- Arabic is a reasonable translation (even if imperfect or paraphrased)\n"
        "- Same topic/product, even with different wording or emphasis\n"
        "- INCI/scientific names kept in English within Arabic — this is CORRECT\n"
        "- Minor omissions, additions, or style differences are fine\n"
        "- Brand name 'Tara'/'تارا' appearing in both is fine\n"
        "- HTML entities or formatting differences are fine\n\n"
        + FEW_SHOT_EXAMPLES +
        "\nFor each pair, assess confidence: \"high\" (clearly mismatch), \"medium\" (likely mismatch), \"low\" (uncertain).\n\n"
        "Respond ONLY with a JSON array of mismatches:\n"
        '[{\"i\": <number>, \"ok\": false, \"confidence\": \"high\"|\"medium\"|\"low\", \"reason\": \"brief reason\"}]\n'
        "If ALL pairs are OK, return exactly: []\n"
        "Do NOT include OK pairs. Only flag clear mismatches.\n\n"
        "Pairs to check:\n" + "\n".join(lines)
    )

    for attempt in range(3):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )

            text = response.content[0].text.strip()
            # Strip markdown code fences
            if text.startswith("```"):
                lines_r = text.split("\n")
                if lines_r[-1].strip() == "```":
                    text = "\n".join(lines_r[1:-1])
                else:
                    text = "\n".join(lines_r[1:])
                text = text.strip()

            results = json.loads(text)
            if not isinstance(results, list):
                results = []
            return results

        except json.JSONDecodeError as e:
            if attempt < 2:
                print(f" retry({e})", end="", flush=True)
                time.sleep(1)
            else:
                print(f" ERROR parsing", end="", flush=True)
                return []
        except Exception as e:
            if attempt < 2:
                print(f" retry({e})", end="", flush=True)
                time.sleep(2 ** attempt)
            else:
                print(f" ERROR({e})", end="", flush=True)
                return []

    return []


def recheck_mismatches(client, mismatches, translatable_rows):
    """Re-verify uncertain mismatches with surrounding context.

    Takes mismatches with medium/low confidence and rechecks them
    by providing 1-2 neighboring rows as context.
    """
    to_recheck = [
        m for m in mismatches
        if m.get("confidence", "high") in ("medium", "low")
    ]

    if not to_recheck:
        return mismatches

    print(f"\n  Pass 2: Re-checking {len(to_recheck)} uncertain mismatches with context...")

    confirmed = []
    cleared = 0
    batch_size = 15  # smaller batches for context-rich rechecks

    for batch_start in range(0, len(to_recheck), batch_size):
        batch = to_recheck[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (len(to_recheck) + batch_size - 1) // batch_size

        # Build context-enriched pairs
        lines = []
        for bi, m in enumerate(batch):
            row_idx = m["row_index"]
            row = translatable_rows[row_idx]
            eng = extract_visible_text(row.get("Default content", ""), 400)
            ara = extract_visible_text(row.get("Translated content", ""), 400)
            field = row.get("Field", "")

            # Add context: 1 row before and 1 row after (same resource)
            context_lines = []
            resource_key = (row.get("Type", ""), row.get("Identification", ""))
            for delta in [-1, 1]:
                ctx_idx = row_idx + delta
                if 0 <= ctx_idx < len(translatable_rows):
                    ctx_row = translatable_rows[ctx_idx]
                    ctx_key = (ctx_row.get("Type", ""), ctx_row.get("Identification", ""))
                    if ctx_key == resource_key:
                        ctx_eng = extract_visible_text(ctx_row.get("Default content", ""), 100)
                        ctx_ara = extract_visible_text(ctx_row.get("Translated content", ""), 100)
                        direction = "PREV" if delta == -1 else "NEXT"
                        if ctx_eng and ctx_ara:
                            context_lines.append(
                                f"     ({direction} row) EN: {ctx_eng} → AR: {ctx_ara}"
                            )

            lines.append(f"{bi}. [{field}] EN: {eng}")
            lines.append(f"   AR: {ara}")
            lines.append(f"   (First-pass reason: {m.get('reason', 'unknown')})")
            for cl in context_lines:
                lines.append(cl)
            lines.append("")

        prompt = (
            "You are rechecking potential translation mismatches that were flagged with "
            "UNCERTAIN confidence. For each pair, look at the context (neighboring rows) "
            "and determine if this is truly a mismatch or a false alarm.\n\n"
            "Be MORE LENIENT in this pass — only confirm as MISMATCH if you're quite sure.\n"
            "Consider: maybe the translation is just creative/liberal, or covers the same "
            "topic from a different angle. Context from neighboring rows can help.\n\n"
            "Respond with a JSON array. For each pair:\n"
            '- Confirmed mismatch: {\"i\": <n>, \"ok\": false, \"reason\": \"...\"}\n'
            '- False alarm (actually OK): {\"i\": <n>, \"ok\": true}\n'
            "Include ALL pairs in the response.\n\n"
            "Pairs to recheck:\n" + "\n".join(lines)
        )

        print(f"    Recheck batch {batch_num}/{total_batches}...", end="", flush=True)

        for attempt in range(3):
            try:
                response = client.messages.create(
                    model=MODEL,
                    max_tokens=4096,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = response.content[0].text.strip()
                if text.startswith("```"):
                    text_lines = text.split("\n")
                    if text_lines[-1].strip() == "```":
                        text = "\n".join(text_lines[1:-1])
                    else:
                        text = "\n".join(text_lines[1:])
                    text = text.strip()

                results = json.loads(text)
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(1)
                    results = []
                else:
                    results = []

        batch_cleared = 0
        confirmed_in_batch = 0
        result_map = {r.get("i", -1): r for r in results if isinstance(r, dict)}

        for bi, m in enumerate(batch):
            r = result_map.get(bi)
            if r and r.get("ok", False):
                # False alarm — don't add to confirmed
                batch_cleared += 1
                cleared += 1
            else:
                # Confirmed mismatch or no response (keep flagged)
                updated = dict(m)
                if r and r.get("reason"):
                    updated["reason"] = r["reason"]
                updated["confidence"] = "confirmed"
                confirmed.append(updated)
                confirmed_in_batch += 1

        print(f" {confirmed_in_batch} confirmed, {batch_cleared} cleared")
        time.sleep(0.3)

    # Combine: high-confidence (kept from pass 1) + confirmed from pass 2
    high_confidence = [m for m in mismatches if m.get("confidence") == "high"]
    final = high_confidence + confirmed

    print(f"  Recheck result: {cleared} false alarms removed, {len(confirmed)} confirmed")
    return final


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
                        help="Pairs per AI validation batch (default: 50)")
    parser.add_argument("--skip-ai", action="store_true",
                        help="Only remove untranslatable rows, skip AI alignment check")
    parser.add_argument("--skip-heuristic", action="store_true",
                        help="Skip heuristic shift detection")
    parser.add_argument("--no-recheck", action="store_true",
                        help="Skip the second-pass recheck of uncertain mismatches")
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

    # -----------------------------------------------------------------------
    # Step 1: Remove untranslatable rows
    # -----------------------------------------------------------------------
    print("Step 1: Removing untranslatable rows...")
    translatable_rows = []
    removed_reasons = {}

    for row in rows:
        should_remove, reason = is_untranslatable_row(row)
        if should_remove:
            removed_reasons[reason] = removed_reasons.get(reason, 0) + 1
        else:
            translatable_rows.append(row)

    total_removed = len(rows) - len(translatable_rows)
    print(f"  Removed: {total_removed} untranslatable rows")
    for reason, count in sorted(removed_reasons.items(), key=lambda x: -x[1]):
        print(f"    {reason}: {count}")
    print(f"  Remaining: {len(translatable_rows)} translatable rows")

    # -----------------------------------------------------------------------
    # Step 2: Script & structural analysis
    # -----------------------------------------------------------------------
    print(f"\nStep 2: Script & structural analysis...")
    script_issues = detect_script_issues(translatable_rows)
    if script_issues:
        high_issues = sum(1 for v in script_issues.values() if v["severity"] == "high")
        med_issues = sum(1 for v in script_issues.values() if v["severity"] == "medium")
        print(f"  Found: {len(script_issues)} issues ({high_issues} high, {med_issues} medium)")
        # Show a few examples
        shown = 0
        for idx, info in sorted(script_issues.items()):
            if shown >= 5:
                break
            row = translatable_rows[idx]
            eng = extract_visible_text(row.get("Default content", ""), 60)
            ara = extract_visible_text(row.get("Translated content", ""), 60)
            print(f"    [{info['severity']}] {info['reason']}")
            print(f"      EN: {eng}")
            print(f"      AR: {ara}")
            shown += 1
        if len(script_issues) > 5:
            print(f"    ... and {len(script_issues) - 5} more")
    else:
        print(f"  No script/structural issues found")

    # -----------------------------------------------------------------------
    # Step 3: Duplicate translation detection
    # -----------------------------------------------------------------------
    print(f"\nStep 3: Duplicate translation detection...")
    dup_issues = detect_duplicate_translations(translatable_rows)
    if dup_issues:
        print(f"  Found: {len(dup_issues)} rows with duplicate Arabic for different English")
        shown = 0
        for idx, info in sorted(dup_issues.items()):
            if shown >= 3:
                break
            row = translatable_rows[idx]
            eng = extract_visible_text(row.get("Default content", ""), 60)
            ara = extract_visible_text(row.get("Translated content", ""), 60)
            print(f"    EN: {eng}")
            print(f"    AR: {ara}")
            shown += 1
        if len(dup_issues) > 3:
            print(f"    ... and {len(dup_issues) - 3} more")
    else:
        print(f"  No duplicate translations found")

    # -----------------------------------------------------------------------
    # Step 4: Heuristic shift detection
    # -----------------------------------------------------------------------
    heuristic_shifts = set()
    if not args.skip_heuristic:
        print(f"\nStep 4: Heuristic shift detection...")
        heuristic_shifts = detect_sequential_shifts(translatable_rows)
        if heuristic_shifts:
            print(f"  Potential shifts: {len(heuristic_shifts)} rows")
            for idx in sorted(list(heuristic_shifts))[:5]:
                row = translatable_rows[idx]
                eng = extract_visible_text(row.get("Default content", ""), 60)
                ara = extract_visible_text(row.get("Translated content", ""), 60)
                print(f"    [{row.get('Type', '')}] {row.get('Field', '')}")
                print(f"      EN: {eng}")
                print(f"      AR: {ara}")
            if len(heuristic_shifts) > 5:
                print(f"    ... and {len(heuristic_shifts) - 5} more")
        else:
            print(f"  No systematic shifts detected")

    # -----------------------------------------------------------------------
    # Step 5: AI alignment check (two-pass)
    # -----------------------------------------------------------------------
    ai_mismatches = []

    if args.skip_ai:
        print("\nSkipping AI alignment check (--skip-ai)")
    else:
        print(f"\nStep 5: AI alignment check with {MODEL}...")

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            print("  ERROR: Set ANTHROPIC_API_KEY in .env")
            print("  Use --skip-ai to skip alignment check")
            sys.exit(1)

        client = anthropic.Anthropic(api_key=api_key)

        # Collect rows that have both English and Arabic
        pairs_to_check = []
        pair_indices = []
        pair_fields = []

        for i, row in enumerate(translatable_rows):
            default = row.get("Default content", "").strip()
            translated = row.get("Translated content", "").strip()
            if not default or not translated:
                continue
            if default == translated:
                continue

            eng_text = extract_visible_text(default)
            ar_text = extract_visible_text(translated)

            if len(eng_text) < 3 or len(ar_text) < 2:
                continue

            pairs_to_check.append((eng_text, ar_text))
            pair_indices.append(i)
            pair_fields.append(row.get("Field", ""))

        print(f"  Pairs to validate: {len(pairs_to_check)}")

        est_cost, est_batches = estimate_cost(len(pairs_to_check), args.batch_size)
        print(f"  Estimated: {est_batches} batches, ~${est_cost:.3f}")
        print(f"  Pass 1: batch validation...")

        total_checked = 0

        for batch_start in range(0, len(pairs_to_check), args.batch_size):
            batch_pairs = pairs_to_check[batch_start:batch_start + args.batch_size]
            batch_indices = pair_indices[batch_start:batch_start + args.batch_size]
            batch_fields = pair_fields[batch_start:batch_start + args.batch_size]
            batch_num = batch_start // args.batch_size + 1

            print(f"    Batch {batch_num}/{est_batches}...", end="", flush=True)
            results = validate_batch(client, batch_pairs, fields=batch_fields)
            total_checked += len(batch_pairs)

            mismatch_count = 0
            for r in results:
                idx_in_batch = r.get("i", -1)
                if 0 <= idx_in_batch < len(batch_indices) and not r.get("ok", True):
                    row_idx = batch_indices[idx_in_batch]
                    row = translatable_rows[row_idx]
                    confidence = r.get("confidence", "medium")
                    ai_mismatches.append({
                        "row_index": row_idx,
                        "type": row.get("Type", ""),
                        "identification": row.get("Identification", ""),
                        "field": row.get("Field", ""),
                        "english": extract_visible_text(row.get("Default content", ""), 150),
                        "arabic": extract_visible_text(row.get("Translated content", ""), 150),
                        "reason": r.get("reason", ""),
                        "confidence": confidence,
                        "source": "ai",
                    })
                    mismatch_count += 1

            print(f" {mismatch_count} flagged" if mismatch_count else " OK")
            time.sleep(0.3)

        print(f"\n  Pass 1 complete: {total_checked} checked, {len(ai_mismatches)} flagged")

        # Pass 2: recheck uncertain mismatches
        if not args.no_recheck and ai_mismatches:
            uncertain = sum(1 for m in ai_mismatches if m.get("confidence") in ("medium", "low"))
            if uncertain > 0:
                ai_mismatches = recheck_mismatches(client, ai_mismatches, translatable_rows)
            else:
                print(f"\n  All {len(ai_mismatches)} mismatches are high-confidence, skipping recheck")

        print(f"  Final AI mismatches: {len(ai_mismatches)}")

    # -----------------------------------------------------------------------
    # Merge all findings
    # -----------------------------------------------------------------------
    all_flagged = {}  # row_index → mismatch info (deduplicated, highest severity wins)

    # Priority: AI (most reliable) > script issues > heuristic > duplicates
    for idx, info in dup_issues.items():
        all_flagged[idx] = {
            "row_index": idx,
            "type": translatable_rows[idx].get("Type", ""),
            "identification": translatable_rows[idx].get("Identification", ""),
            "field": translatable_rows[idx].get("Field", ""),
            "english": extract_visible_text(translatable_rows[idx].get("Default content", ""), 120),
            "arabic": extract_visible_text(translatable_rows[idx].get("Translated content", ""), 120),
            "reason": info["reason"],
            "source": "duplicate",
            "severity": info["severity"],
        }

    for idx in heuristic_shifts:
        if idx not in all_flagged:
            row = translatable_rows[idx]
            all_flagged[idx] = {
                "row_index": idx,
                "type": row.get("Type", ""),
                "identification": row.get("Identification", ""),
                "field": row.get("Field", ""),
                "english": extract_visible_text(row.get("Default content", ""), 120),
                "arabic": extract_visible_text(row.get("Translated content", ""), 120),
                "reason": "heuristic: adjacent row cross-match",
                "source": "heuristic",
                "severity": "medium",
            }

    for idx, info in script_issues.items():
        if idx not in all_flagged or info["severity"] == "high":
            all_flagged[idx] = {
                "row_index": idx,
                "type": translatable_rows[idx].get("Type", ""),
                "identification": translatable_rows[idx].get("Identification", ""),
                "field": translatable_rows[idx].get("Field", ""),
                "english": extract_visible_text(translatable_rows[idx].get("Default content", ""), 120),
                "arabic": extract_visible_text(translatable_rows[idx].get("Translated content", ""), 120),
                "reason": info["reason"],
                "source": "script_analysis",
                "severity": info["severity"],
            }

    for m in ai_mismatches:
        idx = m["row_index"]
        all_flagged[idx] = m  # AI overrides others

    mismatches = sorted(all_flagged.values(), key=lambda m: m["row_index"])

    # -----------------------------------------------------------------------
    # Report and output
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 60}")
    print(f"  VALIDATION REPORT")
    print(f"{'=' * 60}")
    print(f"  Input rows:           {len(rows)}")
    print(f"  Untranslatable:       {total_removed} (removed)")
    print(f"  Translatable:         {len(translatable_rows)}")
    print(f"  ─── Detections by layer ───")
    print(f"  Script/structural:    {len(script_issues)}")
    print(f"  Duplicate Arabic:     {len(dup_issues)}")
    print(f"  Heuristic shifts:     {len(heuristic_shifts)}")
    print(f"  AI mismatches:        {len(ai_mismatches)}")
    print(f"  ─── After dedup ───")
    print(f"  Total flagged:        {len(mismatches)}")

    # Breakdown by source
    source_counts = Counter(m.get("source", "?") for m in mismatches)
    for source, count in source_counts.most_common():
        print(f"    {source}: {count}")

    if mismatches:
        print(f"\n  Flagged rows:")
        mismatch_set = {m["row_index"] for m in mismatches}
        for m in mismatches[:30]:
            source_tag = f"[{m.get('source', '?')}]"
            conf = f" ({m.get('confidence', m.get('severity', '?'))})" if m.get('confidence') or m.get('severity') else ""
            print(f"    {source_tag}{conf} [{m['type']}] {m['field']}")
            print(f"      EN: {m['english'][:80]}")
            print(f"      AR: {m['arabic'][:80]}")
            if m.get("reason"):
                print(f"      Why: {m['reason']}")
        if len(mismatches) > 30:
            print(f"    ... and {len(mismatches) - 30} more")

        # Clear mismatched translations
        for idx in mismatch_set:
            translatable_rows[idx]["Translated content"] = ""

        print(f"\n  Cleared {len(mismatch_set)} mismatched translations")

        # Save report
        report_path = os.path.splitext(args.output)[0] + "_mismatches.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(mismatches, f, ensure_ascii=False, indent=2)
        print(f"  Mismatch report: {report_path}")

    if args.dry_run:
        print(f"\n  DRY RUN — no output file written")
    else:
        with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(translatable_rows)
        print(f"\n  Output: {args.output}")

    # Summary
    has_translation = sum(
        1 for r in translatable_rows
        if r.get("Translated content", "").strip()
    )
    needs_translation = len(translatable_rows) - has_translation
    print(f"\n  With translation:   {has_translation}")
    print(f"  Needs translation:  {needs_translation}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
