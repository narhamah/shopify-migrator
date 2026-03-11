#!/usr/bin/env python3
"""Verify translated CSV: coverage, gaps, quality, and generate to-do file.

Checks:
  1. Coverage — every translatable field has Arabic content
  2. Gaps — fields missing translation entirely
  3. Local checks — HTML integrity, truncation, Arabic presence, Spanish detection
  4. AI spot-check — GPT-5-nano (reasoning: minimal) flags quality issues
  5. To-do file — actionable JSON consumed by translate_tara_ar.py --todo

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
# Language detection
# =====================================================================

# Spanish-specific characters and common words
_SPANISH_CHARS = re.compile(r"[áéíóúñ¿¡ü]", re.IGNORECASE)
_SPANISH_WORDS = re.compile(
    r"\b(de|del|los|las|con|para|por|una|que|cabello|capilar|"
    r"champú|tratamiento|colección|más|también|productos?|cuidado)\b",
    re.IGNORECASE
)


def _detect_language(text):
    """Detect if text is Arabic, English, Spanish, or mixed.

    Returns: 'ar', 'en', 'es', or 'mixed'
    """
    # Strip HTML/CSS for detection
    stripped = re.sub(r"<[^>]+>", " ", text)
    stripped = re.sub(r"\{[^}]*\}", " ", stripped)
    stripped = stripped.strip()
    if not stripped:
        return "en"  # structural content, treat as OK

    arabic = len(re.findall(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", stripped))
    latin = len(re.findall(r"[a-zA-ZÀ-ÿ]", stripped))
    total_alpha = arabic + latin

    if total_alpha == 0:
        return "en"

    if arabic / total_alpha >= 0.3:
        return "ar"

    # It's mostly Latin — is it Spanish or English?
    spanish_chars = len(_SPANISH_CHARS.findall(stripped))
    spanish_words = len(_SPANISH_WORDS.findall(stripped))

    if spanish_chars >= 2 or spanish_words >= 2:
        return "es"

    return "en"


def _has_arabic(text, min_ratio=0.3):
    return _detect_language(text) == "ar"


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

    # 2. Classify and build to-do list
    translated = []
    gaps = []
    keep_as_is_count = 0
    non_translatable_count = 0
    empty_count = 0

    # To-do items
    todos = []  # each: {action, field_id, type, id, field, default, translated, issues}

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

    # 3. Add gaps to to-do
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
        print(f"GAPS — {len(gaps)} fields missing translation:\n")
        gap_by_type = Counter(rows[i]["Type"] for i in gaps)
        for t, c in gap_by_type.most_common():
            print(f"  {t}: {c}")
        print()

    # 4. Local quality checks + language detection
    print(f"Local checks on {len(translated)} translated fields...\n")

    issues_by_category = Counter()
    all_issues = []
    spanish_source_count = 0
    untranslated_count = 0

    for idx in translated:
        row = rows[idx]
        default = row["Default content"].strip()
        trans = row["Translated content"].strip()
        field_id = f"{row['Type']}|{row['Identification']}|{row['Field']}"

        row_issues = []
        todo_action = None

        # Detect source language
        source_lang = _detect_language(default)

        # Detect translation language
        trans_lang = _detect_language(trans)

        # Case 1: "Translated" content is still Spanish
        if trans_lang == "es":
            row_issues.append(f"Spanish in Arabic column")
            issues_by_category["spanish_not_translated"] += 1
            if source_lang == "es":
                # Source is Spanish, translation is also Spanish = not translated at all
                todo_action = "translate_es_to_ar"
            else:
                # Source is English but translation is Spanish (weird)
                todo_action = "translate"

        # Case 2: "Translated" content is English (not translated)
        elif trans_lang == "en":
            untrans = check_untranslated(default, trans)
            if untrans or (default == trans):
                row_issues.append("not translated (EN=AR)")
                issues_by_category["identical_not_translated"] += 1
                todo_action = "translate"
            else:
                # Different English text — check if it's actually a problem
                ar_check = _detect_language(trans)
                if ar_check != "ar":
                    row_issues.append("English in Arabic column")
                    issues_by_category["english_not_translated"] += 1
                    todo_action = "translate"

        # Case 3: Source is Spanish but translation is Arabic — might need
        # English "Default content" fixed too
        if source_lang == "es" and trans_lang == "ar":
            spanish_source_count += 1
            # The Arabic is OK but the "Default content" is Spanish
            # Add a separate to-do to fix the English
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
            html_issues = check_html_integrity(default, trans)
            if html_issues:
                row_issues.extend(html_issues)
                for h in html_issues:
                    issues_by_category["html_" + h.split(":")[0].split("(")[0].strip()] += 1

        # Truncation
        trunc = check_truncation(default, trans)
        if trunc:
            row_issues.append(trunc)
            issues_by_category["truncated"] += 1
            if not todo_action:
                todo_action = "translate"  # re-translate truncated content

        if row_issues:
            all_issues.append((idx, row_issues))

        # Add to-do if action needed
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

    # Print summary
    if all_issues:
        print(f"  {len(all_issues)} rows with issues:\n")
        for cat, count in issues_by_category.most_common():
            print(f"    {cat}: {count}")
        if spanish_source_count:
            print(f"\n    {spanish_source_count} rows have Spanish in 'Default content'")
            print(f"    (Arabic OK, but English column needs fixing)")

        if args.verbose:
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

    # 6. Write to-do file
    # Categorize to-dos by action
    todo_by_action = Counter(t["action"] for t in todos)

    todo_file = os.path.splitext(args.input)[0] + "_todo.json"
    with open(todo_file, "w", encoding="utf-8") as f:
        json.dump(todos, f, ensure_ascii=False, indent=2)

    # 7. Verdict
    print(f"\n{'='*60}")
    if not todos:
        print("  RESULT: ALL GOOD — CSV is complete and clean")
    else:
        print(f"  RESULT: {len(todos)} items need fixing")
        print()
        for action, count in todo_by_action.most_common():
            label = {
                "translate": "Translate to Arabic (missing/bad)",
                "translate_es_to_ar": "Translate Spanish → Arabic",
                "fix_default_es_to_en": "Fix 'Default content': Spanish → English",
            }.get(action, action)
            print(f"    {label}: {count}")
    print(f"{'='*60}")

    if todos:
        print(f"\nTo-do file: {todo_file}")
        print(f"\nFix commands:")
        if any(t["action"] in ("translate", "translate_es_to_ar") for t in todos):
            print(f"  python translate_tara_ar.py --input {args.input} --todo {todo_file}")
        if any(t["action"] == "fix_default_es_to_en" for t in todos):
            print(f"  python translate_tara_ar.py --input {args.input} --todo {todo_file} --fix-spanish")


if __name__ == "__main__":
    main()
