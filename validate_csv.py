#!/usr/bin/env python3
"""Validate translation CSV: detect misaligned rows and remove untranslatable data.

Uses Claude Haiku 4.5 to verify each English↔Arabic pair actually corresponds
(catches row shifts where translations slid up/down). Also strips rows that
contain non-translatable data (URLs, IDs, config JSON, images, etc.).

Three-layer validation:
1. Rule-based: remove untranslatable rows (URLs, IDs, images, config)
2. Heuristic: detect systematic row shifts (N+1/N-1 cross-matching)
3. AI (Haiku 4.5): verify remaining pairs with few-shot examples

Usage:
    python validate_csv.py --input Arabic/translations.csv
    python validate_csv.py --input Arabic/translations.csv --dry-run
    python validate_csv.py --input Arabic/translations.csv --skip-ai
    python validate_csv.py --input Arabic/translations.csv --batch-size 50
"""

import argparse
import csv
import json
import os
import re
import sys
import time

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
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


def _has_arabic(text):
    """Check if text contains Arabic characters."""
    return bool(re.search(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", text or ""))


# ---------------------------------------------------------------------------
# Heuristic shift detection
# ---------------------------------------------------------------------------

def detect_sequential_shifts(rows):
    """Detect systematic row shifts by cross-matching adjacent rows.

    If row N's Arabic matches row N+1's English better than row N's English,
    that indicates a shift. Returns set of row indices that are shifted.
    """
    shifted = set()

    # Build list of (index, english_text, arabic_text) for rows with both
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
            # Check: does this row's Arabic match the NEXT row's English?
            if pos + 1 < len(group):
                _, next_eng, _, _ = group[pos + 1]
                # Simple word overlap check
                eng_words = set(eng.lower().split())
                ara_latin = set(re.findall(r"[a-zA-Z]+", ara.lower()))
                next_words = set(next_eng.lower().split())

                if len(eng_words) >= 3 and len(next_words) >= 3:
                    # If Arabic contains Latin words that match next row better
                    overlap_current = len(ara_latin & eng_words)
                    overlap_next = len(ara_latin & next_words)
                    if overlap_next > overlap_current and overlap_next >= 3:
                        shifted.add(i)

            # Check: does this row's Arabic match the PREVIOUS row's English?
            if pos > 0:
                _, prev_eng, _, _ = group[pos - 1]
                eng_words = set(eng.lower().split())
                ara_latin = set(re.findall(r"[a-zA-Z]+", ara.lower()))
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
  → OK (same meaning)

- EN: "Activated Charcoal Face Wash"
  AR: "غسول الوجه بالفحم المنشط"
  → OK (same product)

- EN: "Free Of"
  AR: "خالٍ من"
  → OK (heading translation)

MISMATCHED pairs (row shift):
- EN: "Hydrating Face Cream with Hyaluronic Acid"
  AR: "شامبو مقوي للشعر بالكيراتين"
  → MISMATCH (Arabic says "keratin hair shampoo" — different product entirely)

- EN: "Key Benefits"
  AR: "ينظف البشرة بعمق ويزيل الشوائب والزيوت الزائدة"
  → MISMATCH (Arabic is a product description, not a heading translation)

- EN: "How to Use"
  AR: "زبدة الشيا العضوية تغذي وترطب البشرة الجافة"
  → MISMATCH (Arabic describes shea butter benefits, not usage instructions)
"""


def estimate_cost(num_pairs, batch_size):
    """Estimate API cost for validation."""
    num_batches = (num_pairs + batch_size - 1) // batch_size
    # ~50 tokens per pair input + ~200 tokens system/few-shot overhead per batch
    input_tokens = num_pairs * 50 + num_batches * 500
    # ~5 tokens per pair output (most are OK → empty array)
    output_tokens = num_pairs * 5
    cost = (input_tokens / 1_000_000 * HAIKU_INPUT_COST +
            output_tokens / 1_000_000 * HAIKU_OUTPUT_COST)
    return cost, num_batches


def validate_batch(client, pairs):
    """Send a batch of (english, arabic) pairs to Haiku for alignment check.

    Returns list of {"i": int, "ok": false, "reason": str} for mismatches only.
    """
    lines = []
    for i, (eng, ara) in enumerate(pairs):
        lines.append(f"{i}. EN: {eng}")
        lines.append(f"   AR: {ara}")

    prompt = (
        "You are a translation QA checker for a skincare/haircare brand (Tara). "
        "Check if each Arabic translation corresponds to its English source.\n\n"
        "Flag as MISMATCH ONLY if:\n"
        "- The Arabic is about a COMPLETELY DIFFERENT topic/product than the English\n"
        "- The Arabic is clearly a translation of a different English text (row shift)\n"
        "- A short heading (like 'Key Benefits') got a long content translation\n\n"
        "Flag as OK if:\n"
        "- The Arabic is a reasonable translation (even if imperfect or paraphrased)\n"
        "- The Arabic covers the same topic, product, or concept\n"
        "- INCI/scientific names are kept in English within Arabic text — this is CORRECT\n"
        "- Minor omissions or additions are fine\n"
        "- Brand name 'Tara' or 'تارا' appearing in both is fine\n\n"
        + FEW_SHOT_EXAMPLES +
        "\nRespond ONLY with a JSON array of mismatches. Each element:\n"
        '{\"i\": <number>, \"ok\": false, \"reason\": \"brief reason\"}\n'
        "If ALL pairs are OK, return exactly: []\n"
        "Do NOT include OK pairs in the output.\n\n"
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
                print(f" ERROR parsing response", end="", flush=True)
                return []
        except Exception as e:
            if attempt < 2:
                print(f" retry({e})", end="", flush=True)
                time.sleep(2 ** attempt)
            else:
                print(f" ERROR({e})", end="", flush=True)
                return []

    return []


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
    # Step 2: Heuristic shift detection
    # -----------------------------------------------------------------------
    heuristic_shifts = set()
    if not args.skip_heuristic:
        print(f"\nStep 2: Heuristic shift detection...")
        heuristic_shifts = detect_sequential_shifts(translatable_rows)
        if heuristic_shifts:
            print(f"  Potential shifts detected: {len(heuristic_shifts)} rows")
            for idx in sorted(list(heuristic_shifts))[:10]:
                row = translatable_rows[idx]
                eng = extract_visible_text(row.get("Default content", ""), 60)
                ara = extract_visible_text(row.get("Translated content", ""), 60)
                print(f"    [{row.get('Type', '')}] {row.get('Field', '')}")
                print(f"      EN: {eng}")
                print(f"      AR: {ara}")
            if len(heuristic_shifts) > 10:
                print(f"    ... and {len(heuristic_shifts) - 10} more")
        else:
            print(f"  No systematic shifts detected")

    # -----------------------------------------------------------------------
    # Step 3: AI alignment check
    # -----------------------------------------------------------------------
    mismatches = []

    if args.skip_ai:
        print("\nSkipping AI alignment check (--skip-ai)")
    else:
        step_num = "3" if not args.skip_heuristic else "2"
        print(f"\nStep {step_num}: AI alignment check with {MODEL}...")

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            print("  ERROR: Set ANTHROPIC_API_KEY in .env")
            print("  Use --skip-ai to skip alignment check")
            sys.exit(1)

        client = anthropic.Anthropic(api_key=api_key)

        # Collect rows that have both English and Arabic
        pairs_to_check = []
        pair_indices = []

        for i, row in enumerate(translatable_rows):
            default = row.get("Default content", "").strip()
            translated = row.get("Translated content", "").strip()
            if not default or not translated:
                continue
            if default == translated:
                continue

            eng_text = extract_visible_text(default)
            ar_text = extract_visible_text(translated)

            # Skip very short pairs
            if len(eng_text) < 3 or len(ar_text) < 2:
                continue

            pairs_to_check.append((eng_text, ar_text))
            pair_indices.append(i)

        print(f"  Pairs to validate: {len(pairs_to_check)}")

        # Cost estimate
        est_cost, est_batches = estimate_cost(len(pairs_to_check), args.batch_size)
        print(f"  Estimated: {est_batches} batches, ~${est_cost:.3f}")

        total_checked = 0
        total_input_tokens = 0
        total_output_tokens = 0

        for batch_start in range(0, len(pairs_to_check), args.batch_size):
            batch_pairs = pairs_to_check[batch_start:batch_start + args.batch_size]
            batch_indices = pair_indices[batch_start:batch_start + args.batch_size]
            batch_num = batch_start // args.batch_size + 1

            print(f"  Batch {batch_num}/{est_batches}...", end="", flush=True)
            results = validate_batch(client, batch_pairs)
            total_checked += len(batch_pairs)

            mismatch_count = 0
            for r in results:
                idx_in_batch = r.get("i", -1)
                if 0 <= idx_in_batch < len(batch_indices) and not r.get("ok", True):
                    row_idx = batch_indices[idx_in_batch]
                    row = translatable_rows[row_idx]
                    mismatches.append({
                        "row_index": row_idx,
                        "type": row.get("Type", ""),
                        "identification": row.get("Identification", ""),
                        "field": row.get("Field", ""),
                        "english": extract_visible_text(row.get("Default content", ""), 120),
                        "arabic": extract_visible_text(row.get("Translated content", ""), 120),
                        "reason": r.get("reason", ""),
                        "source": "ai",
                    })
                    mismatch_count += 1

            print(f" {mismatch_count} mismatches" if mismatch_count else " OK")
            time.sleep(0.3)

        print(f"\n  Checked: {total_checked} pairs")
        print(f"  AI mismatches: {len(mismatches)}")

    # Add heuristic shifts that weren't already caught by AI
    ai_indices = {m["row_index"] for m in mismatches}
    for idx in heuristic_shifts:
        if idx not in ai_indices:
            row = translatable_rows[idx]
            mismatches.append({
                "row_index": idx,
                "type": row.get("Type", ""),
                "identification": row.get("Identification", ""),
                "field": row.get("Field", ""),
                "english": extract_visible_text(row.get("Default content", ""), 120),
                "arabic": extract_visible_text(row.get("Translated content", ""), 120),
                "reason": "heuristic: adjacent row cross-match",
                "source": "heuristic",
            })

    # -----------------------------------------------------------------------
    # Report and output
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 60}")
    print(f"  VALIDATION REPORT")
    print(f"{'=' * 60}")
    print(f"  Input rows:           {len(rows)}")
    print(f"  Untranslatable:       {total_removed} (removed)")
    print(f"  Translatable:         {len(translatable_rows)}")
    print(f"  Heuristic shifts:     {len(heuristic_shifts)}")
    ai_count = sum(1 for m in mismatches if m.get("source") == "ai")
    print(f"  AI mismatches:        {ai_count}")
    print(f"  Total misaligned:     {len(mismatches)}")

    if mismatches:
        print(f"\n  Misaligned rows:")
        mismatch_set = {m["row_index"] for m in mismatches}
        for m in mismatches[:30]:
            source_tag = f"[{m.get('source', '?')}]"
            print(f"    {source_tag} [{m['type']}] {m['field']}")
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
