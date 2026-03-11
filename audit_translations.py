#!/usr/bin/env python3
"""Comprehensive translation audit via Shopify GraphQL API.

Scans ALL translatable resources, checks every field for:
- Missing translations
- Identical (untranslated) content
- Mixed language (English/Spanish mixed with target language)
- Corrupted JSON (rich_text fields)
- Outdated translations

Outputs a JSON fix list that fix_translations.py can consume.

Usage:
    python audit_translations.py                              # Full audit
    python audit_translations.py --verbose                    # Show every problem
    python audit_translations.py --fix-json audit_fix.json    # Output fix list
    python audit_translations.py --locale ar                  # Specify locale
    python audit_translations.py --type PRODUCT               # Audit one type
"""

import argparse
import json
import os
import re
import sys
import time

from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.core.language import count_chars, detect_mixed_language, has_arabic
from tara_migrate.core.rich_text import extract_text
from tara_migrate.core.shopify_fields import (
    TRANSLATABLE_RESOURCE_TYPES,
    is_skippable_field,
    is_skippable_value,
)

TRANSLATABLE_RESOURCES_QUERY = """
query($resourceType: TranslatableResourceType!, $first: Int!, $after: String) {
  translatableResources(resourceType: $resourceType, first: $first, after: $after) {
    edges {
      node {
        resourceId
        translatableContent {
          key
          value
          digest
          locale
        }
        translations(locale: "%LOCALE%") {
          key
          value
          outdated
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


def classify_translation(key, english_value, translated_value, outdated=False):
    """Classify a field's translation status.

    Returns: (status, detail)
    Status: OK, MISSING, IDENTICAL, NOT_ARABIC, MIXED_LANGUAGE,
            CORRUPTED_JSON, OUTDATED, SKIP
    """
    if is_skippable_field(key):
        return "SKIP", "non-translatable field"
    if is_skippable_value(english_value):
        return "SKIP", "non-translatable value"

    if not translated_value:
        return "MISSING", "no translation"

    # Extract text for analysis (handles rich_text JSON)
    en_text = english_value
    ar_text = translated_value
    is_json = False

    if english_value.strip().startswith("{") and '"type"' in english_value:
        is_json = True
        en_extracted = extract_text(english_value)
        ar_extracted = extract_text(translated_value)
        if en_extracted:
            en_text = en_extracted
        if ar_extracted:
            ar_text = ar_extracted
        elif translated_value.strip().startswith("{"):
            try:
                json.loads(translated_value)
            except (json.JSONDecodeError, TypeError):
                return "CORRUPTED_JSON", "invalid JSON in translation"

    # Strip HTML and CSS for text analysis
    en_clean = re.sub(r"<[^>]+>", " ", en_text)
    ar_clean = re.sub(r"<[^>]+>", " ", ar_text)
    en_clean = re.sub(r"\{[^}]*\}", " ", en_clean).strip()
    ar_clean = re.sub(r"\{[^}]*\}", " ", ar_clean).strip()

    if not en_clean:
        return "SKIP", "structural/CSS-only content"

    # Check if identical
    if translated_value == english_value:
        ar_chars, _ = count_chars(ar_clean)
        if ar_chars > 0:
            return "OK", "already in target language"
        return "IDENTICAL", "translation identical to source"

    # Check language ratio
    ar_chars, lat_chars = count_chars(ar_clean)
    total = ar_chars + lat_chars

    if total == 0:
        return "OK", "no alpha content (numbers/symbols)"
    if ar_chars == 0:
        return "NOT_ARABIC", "translation has no target-language characters"

    # Mixed language detection
    is_mixed, lang = detect_mixed_language(ar_clean)
    if is_mixed:
        return "MIXED_LANGUAGE", (
            f"significant {lang} text ({lat_chars} Latin / {ar_chars} Arabic chars)"
        )

    if outdated:
        return "OUTDATED", "translation is outdated"

    return "OK", ""


def audit_resource_type(client, resource_type, locale, verbose=False):
    """Audit all resources of a given type. Returns (problems, stats, resource_count)."""
    query = TRANSLATABLE_RESOURCES_QUERY.replace("%LOCALE%", locale)
    problems = []
    stats = {"total": 0, "ok": 0, "missing": 0, "identical": 0,
             "not_arabic": 0, "mixed": 0, "corrupted": 0, "outdated": 0, "skip": 0}

    cursor = None
    page = 0
    total_resources = 0

    while True:
        page += 1
        try:
            data = client._graphql(query, {
                "resourceType": resource_type,
                "first": 50,
                "after": cursor,
            })
        except Exception as e:
            print(f"    ERROR on page {page}: {e}")
            break

        edges = data["translatableResources"]["edges"]
        page_info = data["translatableResources"]["pageInfo"]
        total_resources += len(edges)

        for edge in edges:
            node = edge["node"]
            resource_id = node["resourceId"]
            translations = {t["key"]: t for t in node["translations"]}

            for field in node["translatableContent"]:
                key = field["key"]
                value = field["value"] or ""
                trans = translations.get(key)
                ar_value = trans["value"] if trans else None
                outdated = trans.get("outdated", False) if trans else False

                status, detail = classify_translation(key, value, ar_value, outdated)

                if status == "SKIP":
                    stats["skip"] += 1
                    continue

                stats["total"] += 1
                stat_key = {
                    "OK": "ok", "MISSING": "missing", "IDENTICAL": "identical",
                    "NOT_ARABIC": "not_arabic", "MIXED_LANGUAGE": "mixed",
                    "CORRUPTED_JSON": "corrupted", "OUTDATED": "outdated",
                }.get(status, "ok")
                stats[stat_key] += 1

                if status != "OK":
                    problem = {
                        "resource_id": resource_id,
                        "resource_type": resource_type,
                        "key": key,
                        "status": status,
                        "detail": detail,
                        "english": value[:200],
                        "arabic": (ar_value or "")[:200],
                        "digest": field["digest"],
                    }
                    problems.append(problem)

                    if verbose:
                        en_preview = value[:60]
                        ar_preview = (ar_value or "(none)")[:60]
                        if value.startswith("{") and '"type"' in value:
                            extracted = extract_text(value)
                            if extracted:
                                en_preview = f"[json] {extracted[:55]}"
                        if ar_value and ar_value.startswith("{") and '"type"' in ar_value:
                            extracted = extract_text(ar_value)
                            if extracted:
                                ar_preview = f"[json] {extracted[:55]}"
                        print(f"    [{status:15s}] {resource_id}")
                        print(f"      {key}: {en_preview}")
                        if ar_value:
                            print(f"      TR: {ar_preview}")

        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
        time.sleep(0.3)

    return problems, stats, total_resources


def main():
    parser = argparse.ArgumentParser(
        description="Comprehensive translation audit via Shopify GraphQL")
    parser.add_argument("--locale", default="ar",
                        help="Target locale to audit (default: ar)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show every problem found")
    parser.add_argument("--fix-json", default=None,
                        help="Output fix list as JSON (for fix_translations.py)")
    parser.add_argument("--type", default=None,
                        help="Audit only one type (PRODUCT, COLLECTION, etc.)")
    parser.add_argument("--shop-url-env", default="SAUDI_SHOP_URL",
                        help="Env var name for shop URL (default: SAUDI_SHOP_URL)")
    parser.add_argument("--token-env", default="SAUDI_ACCESS_TOKEN",
                        help="Env var name for access token (default: SAUDI_ACCESS_TOKEN)")
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ.get(args.shop_url_env)
    token = os.environ.get(args.token_env)
    if not shop_url or not token:
        print(f"ERROR: Set {args.shop_url_env} and {args.token_env} in .env")
        sys.exit(1)

    client = ShopifyClient(shop_url, token)

    resource_types = TRANSLATABLE_RESOURCE_TYPES
    if args.type:
        resource_types = [args.type.upper()]

    all_problems = []
    total_stats = {"total": 0, "ok": 0, "missing": 0, "identical": 0,
                   "not_arabic": 0, "mixed": 0, "corrupted": 0, "outdated": 0, "skip": 0}

    print("=" * 70)
    print(f"  TRANSLATION AUDIT (locale: {args.locale})")
    print("=" * 70)

    for rtype in resource_types:
        print(f"\n  Scanning {rtype}...")
        problems, stats, n_resources = audit_resource_type(
            client, rtype, args.locale, verbose=args.verbose)
        all_problems.extend(problems)

        for k in total_stats:
            total_stats[k] += stats[k]

        pct = (stats["ok"] / stats["total"] * 100) if stats["total"] else 100
        n_problems = stats["total"] - stats["ok"]
        print(f"    {n_resources} resources | {stats['total']} fields | "
              f"{stats['ok']} OK ({pct:.0f}%) | {n_problems} problems")
        if n_problems:
            parts = []
            for key, label in [("missing", "missing"), ("identical", "identical"),
                               ("not_arabic", "not_translated"),
                               ("mixed", "mixed_lang"), ("corrupted", "corrupted_json"),
                               ("outdated", "outdated")]:
                if stats[key]:
                    parts.append(f"{label}={stats[key]}")
            print(f"    Breakdown: {', '.join(parts)}")

    # Summary
    pct = (total_stats["ok"] / total_stats["total"] * 100) if total_stats["total"] else 100
    print(f"\n{'=' * 70}")
    print(f"  TOTAL: {total_stats['total']} translatable fields")
    print(f"  OK:       {total_stats['ok']} ({pct:.0f}%)")
    print(f"  PROBLEMS: {total_stats['total'] - total_stats['ok']}")
    print(f"    Missing:        {total_stats['missing']}")
    print(f"    Identical:      {total_stats['identical']}")
    print(f"    Not translated: {total_stats['not_arabic']}")
    print(f"    Mixed language: {total_stats['mixed']}")
    print(f"    Corrupted JSON: {total_stats['corrupted']}")
    print(f"    Outdated:       {total_stats['outdated']}")
    print(f"  Skipped: {total_stats['skip']} (non-translatable)")
    print(f"{'=' * 70}")

    if args.fix_json and all_problems:
        fixable = [p for p in all_problems if p["status"] in
                   ("MISSING", "IDENTICAL", "NOT_ARABIC", "MIXED_LANGUAGE", "CORRUPTED_JSON")]
        with open(args.fix_json, "w", encoding="utf-8") as f:
            json.dump(fixable, f, ensure_ascii=False, indent=2)
        print(f"\n  Fix list: {len(fixable)} fields -> {args.fix_json}")
        print(f"  Run: python fix_translations.py --audit {args.fix_json} --locale {args.locale}")


if __name__ == "__main__":
    main()
