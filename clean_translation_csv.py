#!/usr/bin/env python3
"""Clean translation CSV: remove non-translatable rows, fix misaligned translations.

Produces a clean CSV with only translatable rows, and flags/removes rows where
the translation appears to be in the wrong field.

Usage:
    python clean_translation_csv.py --input Arabic/Tara_Saudi_translations_Mar-10-2026.csv
    python clean_translation_csv.py --input Arabic/file.csv --fix-misaligned
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter

from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


def _extract_rich_text(text):
    """Extract plain text from Shopify rich_text JSON."""
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None
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
    return " ".join(parts) if parts else None


def _has_arabic(text, min_ratio=0.3):
    if not text:
        return False
    if text.startswith("{") and '"type"' in text:
        extracted = _extract_rich_text(text)
        if extracted and extracted.strip():
            text = extracted
    stripped = re.sub(r"<[^>]+>", " ", text)
    stripped = re.sub(r"\{[^}]*\}", " ", stripped)
    stripped = stripped.strip()
    if not stripped:
        return True
    arabic = len(re.findall(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", stripped))
    alpha = len(re.findall(r"[a-zA-ZÀ-ÿ\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", stripped))
    if alpha == 0:
        return True
    return arabic / alpha >= min_ratio


def _is_non_translatable(row):
    default = row.get("Default content", "").strip()
    field = row.get("Field", "")
    if not default:
        return True, "empty"
    if field == "handle":
        return True, "handle"
    if default.startswith(("shopify://", "http://", "https://", "/", "gid://")):
        return True, "url"
    if re.match(r"^-?\d+\.?\d*$", default):
        return True, "number"
    if re.match(r"^[0-9a-f]{8,}$", default):
        return True, "hex_id"
    if default.startswith("[") and default.endswith("]"):
        try:
            parsed = json.loads(default)
            if isinstance(parsed, list) and all(
                isinstance(v, str) and (v.startswith("gid://") or re.match(r"^\d+$", v))
                for v in parsed
            ):
                return True, "gid_array"
        except (json.JSONDecodeError, TypeError):
            pass
    return False, ""


def _is_keep_as_is(row):
    field = row.get("Field", "")
    keep_patterns = [
        "facebook_url", "instagram_url", "tiktok_url", "twitter_url",
        "google_maps_api_key", "form_id", "portal_id", "region",
        "anchor_id", "worker_url", "default_lat", "default_lng",
        "custom_max_height", "custom_max_width",
    ]
    for pat in keep_patterns:
        if pat in field:
            return True
    if field.endswith(".link") or field.endswith("_url"):
        return True
    if field.endswith(".image") or field.endswith(".image_1") or field.endswith(".image_2"):
        return True
    if ".image_1:" in field or ".image_2:" in field:
        return True
    if ".image_1_mobile:" in field or ".image_2_mobile:" in field:
        return True
    if field in ("general.logo", "general.logo_inverse", "general.favicon"):
        return True
    if ".icon:" in field:
        return True
    return False


def _detect_misalignment(row):
    """Detect if a translation appears to be in the wrong field."""
    default = row.get("Default content", "").strip()
    translated = row.get("Translated content", "").strip()
    field = row.get("Field", "")

    if not translated or not default:
        return None

    # Check if rich_text JSON was put in a non-JSON field
    if translated.startswith("{") and not default.startswith("{"):
        if '"type"' in translated:
            return "rich_text_json_in_plain_field"

    # Check if plain text was put in a JSON field
    if default.startswith("{") and '"type"' in default:
        if not translated.startswith("{"):
            # Could be OK if it's a short extracted value
            extracted = _extract_rich_text(default)
            if extracted and len(extracted) > 20 and len(translated) < 10:
                return "truncated_json_translation"

    # Check if a heading translation has content-length text (> 50 chars)
    # Headings like "Key Benefits", "How to Use" should translate to short Arabic
    heading_patterns = ["Key Benefits", "Key Ingredients", "How to Use",
                        "How To Use", "Free Of", "Free of", "Fragrance"]
    if default in heading_patterns and len(translated) > 50:
        return f"heading_got_content_translation"

    # Check if review JSON leaked into a non-review field
    if '"reviewCount"' in translated and '"reviewCount"' not in default:
        return "review_json_leaked"

    return None


def main():
    parser = argparse.ArgumentParser(description="Clean translation CSV")
    parser.add_argument("--input", required=True, help="Input CSV")
    parser.add_argument("--output", default=None, help="Output CSV (default: <input>_clean.csv)")
    parser.add_argument("--fix-misaligned", action="store_true",
                        help="Clear translations that appear to be in the wrong field")
    parser.add_argument("--keep-all-rows", action="store_true",
                        help="Keep non-translatable rows (just clear their translations)")
    args = parser.parse_args()

    if not args.output:
        base, ext = os.path.splitext(args.input)
        args.output = f"{base}_clean{ext}"

    load_dotenv()

    with open(args.input, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    print(f"Read {len(rows)} rows from {args.input}\n")

    removed = Counter()
    cleared = Counter()
    misaligned_rows = []
    clean_rows = []

    for i, row in enumerate(rows):
        non_trans, reason = _is_non_translatable(row)

        if non_trans:
            removed[f"non_translatable_{reason}"] += 1
            if args.keep_all_rows:
                row["Translated content"] = ""
                clean_rows.append(row)
            continue

        if _is_keep_as_is(row):
            removed["keep_as_is"] += 1
            if args.keep_all_rows:
                clean_rows.append(row)
            continue

        # Check for fake translations (identical, no Arabic)
        default = row.get("Default content", "").strip()
        translated = row.get("Translated content", "").strip()

        if translated and translated == default and not _has_arabic(translated):
            cleared["identical_not_translated"] += 1
            row["Translated content"] = ""  # Clear fake translation

        # Check for misalignment
        if args.fix_misaligned and translated:
            misalign = _detect_misalignment(row)
            if misalign:
                misaligned_rows.append({
                    "row": i,
                    "type": row["Type"],
                    "id": row["Identification"],
                    "field": row["Field"],
                    "default": default[:60],
                    "translated": translated[:60],
                    "issue": misalign,
                })
                cleared[f"misaligned_{misalign}"] += 1
                row["Translated content"] = ""  # Clear misaligned translation

        clean_rows.append(row)

    # Write clean CSV
    with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clean_rows)

    print(f"{'='*60}")
    print(f"  CLEANING REPORT")
    print(f"{'='*60}")
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

        # Save misaligned report
        report_file = os.path.splitext(args.output)[0] + "_misaligned.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(misaligned_rows, f, ensure_ascii=False, indent=2)
        print(f"\n  Misaligned report: {report_file}")

    print(f"\n  Clean CSV: {args.output}")
    print(f"{'='*60}")

    # Summary of what's left to translate
    needs_translation = sum(
        1 for r in clean_rows
        if r.get("Default content", "").strip()
        and not r.get("Translated content", "").strip()
    )
    has_translation = sum(
        1 for r in clean_rows
        if r.get("Translated content", "").strip()
        and _has_arabic(r["Translated content"])
    )
    print(f"\n  Translated (Arabic): {has_translation}")
    print(f"  Needs translation:   {needs_translation}")
    print(f"\nNext steps:")
    print(f"  1. Translate missing fields:")
    print(f"     python translate_tara_ar.py --input {args.output}")
    print(f"  2. Upload via GraphQL (recommended):")
    print(f"     python upload_translations_graphql.py --input {args.output} --dry-run")


if __name__ == "__main__":
    main()
