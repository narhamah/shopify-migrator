#!/usr/bin/env python3
"""Validate translation CSV: detect misaligned rows and remove untranslatable data.

Uses Claude Haiku 4.5 to verify each English↔Arabic pair actually corresponds
(catches row shifts where translations slid up/down). Also strips rows that
contain non-translatable data (URLs, IDs, config JSON, images, etc.).

Usage:
    python validate_csv.py --input Arabic/translations.csv
    python validate_csv.py --input Arabic/translations.csv --dry-run
    python validate_csv.py --input Arabic/translations.csv --batch-size 40
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

# ---------------------------------------------------------------------------
# Untranslatable detection (rule-based, no API needed)
# ---------------------------------------------------------------------------

SKIP_FIELD_PATTERNS = [
    r"\.image$", r"\.image_\d", r"\.image_\d_mobile", r"\.icon:",
    r"\.link$", r"_url$", r"\.logo", r"\.favicon",
    r"google_maps", r"form_id", r"portal_id", r"anchor_id",
    r"worker_url", r"default_lat", r"default_lng",
    r"max_height", r"max_width", r"\.video$", r"\.video_url",
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
    # Pure numbers
    if re.match(r"^-?\d+\.?\d*$", v):
        return True
    # Hex IDs
    if re.match(r"^[0-9a-f]{8,}$", v):
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
    # Config JSON (reviewCount, etc.)
    if v.startswith("{") and ('"reviewCount"' in v or '"formId"' in v):
        return True
    # Pure CSS/style blocks
    if v.strip().startswith("<style>") and "</style>" in v and len(v) > 200:
        # If ONLY CSS with no visible text
        no_style = re.sub(r"<style>.*?</style>", "", v, flags=re.DOTALL)
        no_tags = re.sub(r"<[^>]+>", " ", no_style).strip()
        if not no_tags:
            return True
    return False


def is_untranslatable_row(row):
    """Check if a row should be removed entirely.

    Returns (should_remove, reason) tuple.
    """
    field = row.get("Field", "")
    default = row.get("Default content", "").strip()

    if not default:
        return True, "empty"
    if is_untranslatable_field(field):
        return True, f"field_pattern:{field}"
    if is_untranslatable_value(default):
        return True, "untranslatable_value"
    return False, ""


# ---------------------------------------------------------------------------
# AI-based alignment validation (Claude Haiku 4.5)
# ---------------------------------------------------------------------------

def extract_visible_text(html_or_text, max_chars=200):
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
            text = " ".join(parts)
        except (json.JSONDecodeError, TypeError):
            pass
    # Strip HTML tags
    text = re.sub(r"<style>.*?</style>", "", text, flags=re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


def validate_batch(client, pairs):
    """Send a batch of (english, arabic) pairs to Haiku for alignment check.

    Returns list of {"index": int, "ok": bool, "reason": str} dicts.
    """
    # Build a numbered list for the prompt
    lines = []
    for i, (eng, ara) in enumerate(pairs):
        lines.append(f"{i}. EN: {eng}")
        lines.append(f"   AR: {ara}")
        lines.append("")

    prompt = (
        "You are a translation QA checker. For each numbered pair below, determine if "
        "the Arabic text is a valid translation of the English text.\n\n"
        "Flag as MISMATCH if:\n"
        "- The Arabic is clearly about a DIFFERENT topic/product than the English\n"
        "- The Arabic appears to be a translation of a completely different English text (row shift)\n"
        "- The Arabic contains content that has no relation to the English\n\n"
        "Flag as OK if:\n"
        "- The Arabic is a reasonable translation (even if imperfect)\n"
        "- The Arabic covers the same topic even if wording differs\n"
        "- Minor style differences are fine\n\n"
        "Respond ONLY with a JSON array. Each element: {\"i\": <number>, \"ok\": true/false, \"reason\": \"brief reason if mismatch\"}\n"
        "Only include mismatches in the array. If all are OK, return []\n\n"
        "Pairs:\n" + "\n".join(lines)
    )

    for attempt in range(3):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )

            text = response.content[0].text.strip()
            # Extract JSON from response
            if text.startswith("```"):
                text = "\n".join(text.split("\n")[1:])
                if text.endswith("```"):
                    text = text[:-3]
                text = text.strip()

            results = json.loads(text)
            return results

        except (json.JSONDecodeError, Exception) as e:
            if attempt < 2:
                print(f"    Retry {attempt + 1}: {e}")
                time.sleep(2 ** attempt)
            else:
                print(f"    ERROR: {e}")
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
    parser.add_argument("--batch-size", type=int, default=30,
                        help="Pairs per AI validation batch (default: 30)")
    parser.add_argument("--skip-ai", action="store_true",
                        help="Only remove untranslatable rows, skip AI alignment check")
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
    # Step 2: AI alignment check
    # -----------------------------------------------------------------------
    if args.skip_ai:
        print("\nSkipping AI alignment check (--skip-ai)")
        mismatches = []
    else:
        print(f"\nStep 2: Checking English↔Arabic alignment with {MODEL}...")
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            print("  ERROR: Set ANTHROPIC_API_KEY in .env")
            print("  Use --skip-ai to skip alignment check")
            sys.exit(1)

        client = anthropic.Anthropic(api_key=api_key)

        # Collect rows that have both English and Arabic
        pairs_to_check = []
        pair_indices = []  # index into translatable_rows

        for i, row in enumerate(translatable_rows):
            default = row.get("Default content", "").strip()
            translated = row.get("Translated content", "").strip()
            if not default or not translated:
                continue
            # Skip if they're identical (already flagged elsewhere)
            if default == translated:
                continue

            eng_text = extract_visible_text(default)
            ar_text = extract_visible_text(translated)

            # Skip very short pairs (not enough context to validate)
            if len(eng_text) < 5 or len(ar_text) < 3:
                continue

            pairs_to_check.append((eng_text, ar_text))
            pair_indices.append(i)

        print(f"  Pairs to validate: {len(pairs_to_check)}")

        mismatches = []  # list of {row_index, field, english, arabic, reason}
        total_checked = 0

        for batch_start in range(0, len(pairs_to_check), args.batch_size):
            batch_pairs = pairs_to_check[batch_start:batch_start + args.batch_size]
            batch_indices = pair_indices[batch_start:batch_start + args.batch_size]
            batch_num = batch_start // args.batch_size + 1
            total_batches = (len(pairs_to_check) + args.batch_size - 1) // args.batch_size

            print(f"  Batch {batch_num}/{total_batches} ({len(batch_pairs)} pairs)...", end="", flush=True)
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
                        "english": extract_visible_text(row.get("Default content", ""), 100),
                        "arabic": extract_visible_text(row.get("Translated content", ""), 100),
                        "reason": r.get("reason", ""),
                    })
                    mismatch_count += 1

            print(f" {mismatch_count} mismatches" if mismatch_count else " OK")
            time.sleep(0.5)

        print(f"\n  Checked: {total_checked} pairs")
        print(f"  Mismatches found: {len(mismatches)}")

    # -----------------------------------------------------------------------
    # Step 3: Report and output
    # -----------------------------------------------------------------------
    print(f"\n{'=' * 60}")
    print(f"  VALIDATION REPORT")
    print(f"{'=' * 60}")
    print(f"  Input rows:         {len(rows)}")
    print(f"  Untranslatable:     {total_removed} (removed)")
    print(f"  Translatable:       {len(translatable_rows)}")
    print(f"  Misaligned:         {len(mismatches)}")

    if mismatches:
        print(f"\n  Misaligned rows (English↔Arabic mismatch):")
        # Clear mismatched translations
        mismatch_set = {m["row_index"] for m in mismatches}
        for m in mismatches[:30]:
            print(f"    [{m['type']}] {m['field']}")
            print(f"      EN: {m['english'][:80]}")
            print(f"      AR: {m['arabic'][:80]}")
            print(f"      Reason: {m['reason']}")
        if len(mismatches) > 30:
            print(f"    ... and {len(mismatches) - 30} more")

        # Clear mismatched translations in output
        for idx in mismatch_set:
            translatable_rows[idx]["Translated content"] = ""

        cleared_count = len(mismatch_set)
        print(f"\n  Cleared {cleared_count} mismatched translations")

        # Save mismatches report
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

    # Stats
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
