#!/usr/bin/env python3
"""Verify translated CSV: coverage, gaps, and AI spot-check.

Checks:
  1. Coverage — every translatable field has Arabic content
  2. Gaps — lists missing translations by type/field
  3. Local checks — HTML integrity, truncation, Arabic presence
  4. AI spot-check — GPT-5-nano (reasoning: minimal) flags quality issues

Usage:
    python verify_translation.py --input Arabic/Tara_Saudi_translations_Mar-10-2026.csv
    python verify_translation.py --input Arabic/export.csv --no-ai
    python verify_translation.py --input Arabic/export.csv --samples 50
    python verify_translation.py --input Arabic/export.csv -v
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

from tara_migrate.translation.toon import from_toon, to_toon  # noqa: E402


# =====================================================================
# Row classification (same as translate_tara_ar.py)
# =====================================================================

def _is_non_translatable(row):
    default = row.get("Default content", "").strip()
    field = row.get("Field", "")
    if not default:
        return True
    if field == "handle":
        return True
    if default.startswith(("shopify://", "http://", "https://", "/", "gid://")):
        return True
    if re.match(r"^-?\d+\.?\d*$", default):
        return True
    if re.match(r"^[0-9a-f]{8,}$", default):
        return True
    if default.startswith("[") and default.endswith("]"):
        try:
            parsed = json.loads(default)
            if isinstance(parsed, list) and all(
                isinstance(v, str) and (v.startswith("gid://") or re.match(r"^\d+$", v))
                for v in parsed
            ):
                return True
        except (json.JSONDecodeError, TypeError):
            pass
    return False


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


# =====================================================================
# Local quality checks
# =====================================================================

def check_html_integrity(default, translated):
    issues = []
    default_tags = re.findall(r"</?[a-zA-Z][^>]*>", default)
    translated_tags = re.findall(r"</?[a-zA-Z][^>]*>", translated)
    if len(default_tags) != len(translated_tags):
        issues.append(f"tag count: {len(default_tags)} → {len(translated_tags)}")
    open_tags = re.findall(r"<([a-zA-Z]+)", translated)
    close_tags = re.findall(r"</([a-zA-Z]+)", translated)
    for tag in set(open_tags):
        if open_tags.count(tag) != close_tags.count(tag):
            if tag.lower() not in ("br", "hr", "img", "input", "meta", "link"):
                issues.append(f"unclosed <{tag}>")
    return issues


def check_untranslated(default, translated):
    if default == translated and len(default) > 3 and re.match(r"^[a-zA-Z\s]+$", default):
        return "identical to English"
    return None


def check_arabic_present(translated):
    arabic = len(re.findall(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", translated))
    alpha = len(re.findall(r"[a-zA-Z\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", translated))
    if alpha > 0 and arabic / alpha < 0.3:
        return f"low Arabic ratio ({arabic}/{alpha})"
    return None


def check_truncation(default, translated):
    if len(default) > 50 and len(translated) < len(default) * 0.2:
        return f"possibly truncated ({len(translated)} vs {len(default)} chars)"
    return None


# =====================================================================
# AI spot-check
# =====================================================================

SPOT_CHECK_PROMPT = """You are a translation QA checker for Tara, a Saudi skincare brand.

Check these English→Arabic translation pairs. For EACH one, respond in TOON format (id|verdict):
- PASS if correct
- ISSUE: <brief reason> if problematic

Flag: wrong meaning, missing content, "Tara" translated instead of kept, broken HTML, English left in Arabic, wrong tone for luxury skincare."""


def spot_check_batch(client, model, samples):
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
            input=f"{SPOT_CHECK_PROMPT}\n\n{toon_input}",
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


# =====================================================================
# Main
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="Verify translated Shopify CSV")
    parser.add_argument("--input", required=True, help="Translated CSV file")
    parser.add_argument("--no-ai", action="store_true", help="Skip AI spot-check")
    parser.add_argument("--samples", type=int, default=30,
                        help="Samples for AI spot-check (default: 30)")
    parser.add_argument("--model", default="gpt-5-nano", help="Model (default: gpt-5-nano)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show all details")
    args = parser.parse_args()

    load_dotenv()

    # 1. Read CSV
    with open(args.input, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"Read {len(rows)} rows from {args.input}\n")

    # 2. Classify
    translated = []
    gaps = []
    keep_as_is_count = 0
    non_translatable_count = 0
    empty_count = 0

    for i, row in enumerate(rows):
        default = row.get("Default content", "").strip()
        trans = row.get("Translated content", "").strip()

        if not default:
            empty_count += 1
        elif _is_non_translatable(row):
            non_translatable_count += 1
        elif _is_keep_as_is(row):
            keep_as_is_count += 1
        elif trans:
            translated.append(i)
        else:
            gaps.append(i)

    total_translatable = len(translated) + len(gaps)
    coverage = (len(translated) / total_translatable * 100) if total_translatable else 100

    print(f"{'='*60}")
    print(f"  COVERAGE REPORT")
    print(f"{'='*60}")
    print(f"  Total rows:           {len(rows)}")
    print(f"  Translatable:         {total_translatable}")
    print(f"  Translated:           {len(translated)}  ({coverage:.1f}%)")
    print(f"  GAPS (missing):       {len(gaps)}")
    print(f"  Keep-as-is:           {keep_as_is_count}")
    print(f"  Non-translatable:     {non_translatable_count}")
    print(f"  Empty (no source):    {empty_count}")
    print(f"{'='*60}\n")

    # 3. Show gaps
    if gaps:
        print(f"GAPS — {len(gaps)} fields missing translation:\n")
        gap_by_type = Counter(rows[i]["Type"] for i in gaps)
        for t, c in gap_by_type.most_common():
            print(f"  {t}: {c}")

        gap_by_field = Counter(rows[i]["Field"] for i in gaps)
        print(f"\n  By field (top 15):")
        for f, c in gap_by_field.most_common(15):
            print(f"    {f}: {c}")

        if args.verbose:
            print(f"\n  All gaps:")
            for idx in gaps:
                r = rows[idx]
                print(f"    [{r['Type']}] {r['Identification']} / {r['Field']}: "
                      f"{r['Default content'][:60]}")
        print()

    # 4. Local quality checks
    print(f"Local checks on {len(translated)} translated fields...\n")

    issues_by_type = Counter()
    all_issues = []

    for idx in translated:
        row = rows[idx]
        default = row["Default content"].strip()
        trans = row["Translated content"].strip()

        row_issues = []

        if "<" in default:
            row_issues.extend(check_html_integrity(default, trans))

        untrans = check_untranslated(default, trans)
        if untrans:
            row_issues.append(untrans)

        ar_check = check_arabic_present(trans)
        if ar_check:
            row_issues.append(ar_check)

        trunc = check_truncation(default, trans)
        if trunc:
            row_issues.append(trunc)

        if row_issues:
            for issue in row_issues:
                issues_by_type[issue.split("(")[0].strip()] += 1
            all_issues.append((idx, row_issues))

    if all_issues:
        print(f"  {len(all_issues)} rows with potential issues:\n")
        for issue_type, count in issues_by_type.most_common():
            print(f"    {issue_type}: {count}")

        if args.verbose:
            print()
            for idx, issues in all_issues[:50]:
                r = rows[idx]
                print(f"    [{r['Type']}] {r['Field']}")
                print(f"      EN: {r['Default content'][:80]}")
                print(f"      AR: {r['Translated content'][:80]}")
                for iss in issues:
                    print(f"      >> {iss}")
    else:
        print("  All local checks passed!")

    # 5. AI spot-check
    if not args.no_ai:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            print("\nOPENAI_API_KEY not set — skipping AI spot-check")
        else:
            from openai import OpenAI
            import random
            client = OpenAI(api_key=api_key)

            random.seed(42)
            by_type = {}
            for idx in translated:
                by_type.setdefault(rows[idx]["Type"], []).append(idx)

            sample_indices = []
            per_type = max(1, args.samples // len(by_type)) if by_type else 0
            for t, indices in by_type.items():
                sample_indices.extend(random.sample(indices, min(per_type, len(indices))))
            remaining = args.samples - len(sample_indices)
            if remaining > 0:
                pool = [i for i in translated if i not in set(sample_indices)]
                sample_indices.extend(random.sample(pool, min(remaining, len(pool))))

            samples = []
            for idx in sample_indices[:args.samples]:
                r = rows[idx]
                samples.append({
                    "id": f"{r['Type']}|{r['Identification']}|{r['Field']}",
                    "default": r["Default content"],
                    "translated": r["Translated content"],
                })

            print(f"\nAI spot-check: {len(samples)} samples via {args.model} "
                  f"(reasoning: minimal)...")

            ai_issues = []
            total_tokens = 0
            BATCH = 30
            for start in range(0, len(samples), BATCH):
                batch = samples[start:start + BATCH]
                verdicts, tokens = spot_check_batch(client, args.model, batch)
                total_tokens += tokens
                for v in verdicts:
                    val = v["value"].strip()
                    if not val.upper().startswith("PASS"):
                        ai_issues.append((v["id"], val))

            print(f"  {len(samples)} samples checked ({total_tokens:,} tokens)")

            if ai_issues:
                print(f"\n  AI flagged {len(ai_issues)} issues:\n")
                for field_id, verdict in ai_issues:
                    print(f"    {field_id}")
                    print(f"      {verdict}")
            else:
                print("  All samples passed AI review!")
    else:
        print("\nSkipping AI spot-check (--no-ai)")

    # 6. Verdict
    print(f"\n{'='*60}")
    if not gaps and not all_issues:
        print("  RESULT: ALL GOOD — CSV is complete and clean")
    elif not gaps:
        print(f"  RESULT: COMPLETE but {len(all_issues)} quality warnings")
    else:
        print(f"  RESULT: {len(gaps)} GAPS — re-run translate_tara_ar.py")
    print(f"{'='*60}")

    if gaps:
        gaps_file = os.path.splitext(args.input)[0] + "_gaps.json"
        gap_data = []
        for idx in gaps:
            r = rows[idx]
            gap_data.append({
                "type": r["Type"],
                "id": r["Identification"],
                "field": r["Field"],
                "default": r["Default content"],
            })
        with open(gaps_file, "w", encoding="utf-8") as f:
            json.dump(gap_data, f, ensure_ascii=False, indent=2)
        print(f"\nGaps saved to {gaps_file}")
        print(f"Fix: python translate_tara_ar.py --input {args.input}")


if __name__ == "__main__":
    main()
