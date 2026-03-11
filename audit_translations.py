#!/usr/bin/env python3
"""Comprehensive Arabic translation audit via Shopify GraphQL API.

Scans ALL translatable resources, checks every field for:
- Missing translations
- Identical (untranslated) content
- Mixed language (English/Spanish mixed with Arabic)
- Corrupted JSON (rich_text fields)
- Outdated translations

Outputs a JSON fix list that fix_remaining_ar.py can consume.

Usage:
    python audit_translations.py                    # Full audit, summary only
    python audit_translations.py --verbose          # Show every problem
    python audit_translations.py --fix-json audit_fix.json  # Output fix list
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

LOCALE = "ar"

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
        translations(locale: "ar") {
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

# Fields that should NOT be translated (URLs, IDs, config, images)
SKIP_FIELD_PATTERNS = [
    r"\.image$", r"\.image_\d", r"\.icon:", r"\.link$", r"_url$",
    r"\.logo", r"\.favicon", r"google_maps", r"form_id", r"portal_id",
    r"anchor_id", r"worker_url", r"default_lat", r"default_lng",
    r"max_height", r"max_width",
]


def _is_skippable_field(key):
    """Return True if this field key should not be translated."""
    for pat in SKIP_FIELD_PATTERNS:
        if re.search(pat, key):
            return True
    return False


def _is_skippable_value(value):
    """Return True if this value should not be translated."""
    if not value or not value.strip():
        return True
    v = value.strip()
    # URLs, GIDs, file refs
    if v.startswith(("shopify://", "http://", "https://", "/", "gid://")):
        return True
    # Pure numbers
    if re.match(r"^-?\d+\.?\d*$", v):
        return True
    # Hex strings (hashes, IDs)
    if re.match(r"^[0-9a-f]{8,}$", v):
        return True
    # JSON arrays of IDs
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
    # JSON config objects (reviewCount, etc.)
    if v.startswith("{") and '"reviewCount"' in v:
        return True
    return False


def _extract_text_from_json(json_str):
    """Extract plain text values from rich_text JSON."""
    try:
        data = json.loads(json_str)
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


def _count_chars(text):
    """Count Arabic vs Latin characters in text."""
    if not text:
        return 0, 0
    arabic = len(re.findall(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", text))
    latin = len(re.findall(r"[a-zA-ZÀ-ÿ]", text))
    return arabic, latin


def classify_translation(key, english_value, arabic_value, outdated=False):
    """Classify a field's translation status.

    Returns: (status, detail)
    Status: OK, MISSING, IDENTICAL, NOT_ARABIC, MIXED_LANGUAGE,
            CORRUPTED_JSON, OUTDATED, SKIP
    """
    if _is_skippable_field(key):
        return "SKIP", "non-translatable field"
    if _is_skippable_value(english_value):
        return "SKIP", "non-translatable value"

    if not arabic_value:
        return "MISSING", "no Arabic translation"

    # Extract text for analysis
    en_text = english_value
    ar_text = arabic_value
    is_json = False

    if english_value.strip().startswith("{") and '"type"' in english_value:
        is_json = True
        en_extracted = _extract_text_from_json(english_value)
        ar_extracted = _extract_text_from_json(arabic_value)
        if en_extracted:
            en_text = en_extracted
        if ar_extracted:
            ar_text = ar_extracted
        elif arabic_value.strip().startswith("{"):
            # JSON but couldn't extract — might be corrupted
            try:
                json.loads(arabic_value)
            except (json.JSONDecodeError, TypeError):
                return "CORRUPTED_JSON", "invalid JSON in translation"

    # Strip HTML for text analysis
    en_clean = re.sub(r"<[^>]+>", " ", en_text)
    ar_clean = re.sub(r"<[^>]+>", " ", ar_text)
    # Strip CSS
    en_clean = re.sub(r"\{[^}]*\}", " ", en_clean).strip()
    ar_clean = re.sub(r"\{[^}]*\}", " ", ar_clean).strip()

    if not en_clean:
        return "SKIP", "structural/CSS-only content"

    # Check if identical
    if arabic_value == english_value:
        ar_chars, _ = _count_chars(ar_clean)
        if ar_chars > 0:
            return "OK", "already Arabic"
        return "IDENTICAL", "translation identical to source"

    # Check Arabic ratio
    ar_chars, lat_chars = _count_chars(ar_clean)
    total = ar_chars + lat_chars

    if total == 0:
        return "OK", "no alpha content (numbers/symbols)"

    if ar_chars == 0:
        return "NOT_ARABIC", "translation has no Arabic characters"

    ar_ratio = ar_chars / total
    lat_ratio = lat_chars / total

    # Mixed language: has Arabic but also significant Latin
    if lat_ratio > 0.25 and lat_chars > 10:
        # Check if the Latin chars are just INCI/scientific names or brand terms
        # by looking for multi-word English phrases
        en_words = re.findall(r"[A-Z][a-z]+ [A-Z][a-z]+", ar_clean)
        es_indicators = re.findall(r"(?:ción|ante|ador|mente|miento|ular|ficante)\b", ar_clean, re.IGNORECASE)
        if en_words or es_indicators:
            lang = "Spanish" if es_indicators else "English"
            return "MIXED_LANGUAGE", f"significant {lang} text in Arabic ({lat_chars} Latin / {ar_chars} Arabic chars)"

    if outdated:
        return "OUTDATED", "translation is outdated"

    return "OK", ""


def audit_resource_type(client, resource_type, verbose=False):
    """Audit all resources of a given type. Returns list of problems."""
    problems = []
    stats = {"total": 0, "ok": 0, "missing": 0, "identical": 0,
             "not_arabic": 0, "mixed": 0, "corrupted": 0, "outdated": 0, "skip": 0}

    cursor = None
    page = 0
    total_resources = 0

    while True:
        page += 1
        try:
            data = client._graphql(TRANSLATABLE_RESOURCES_QUERY, {
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

                if status == "OK":
                    stats["ok"] += 1
                elif status == "MISSING":
                    stats["missing"] += 1
                elif status == "IDENTICAL":
                    stats["identical"] += 1
                elif status == "NOT_ARABIC":
                    stats["not_arabic"] += 1
                elif status == "MIXED_LANGUAGE":
                    stats["mixed"] += 1
                elif status == "CORRUPTED_JSON":
                    stats["corrupted"] += 1
                elif status == "OUTDATED":
                    stats["outdated"] += 1

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
                            extracted = _extract_text_from_json(value)
                            if extracted:
                                en_preview = f"[json] {extracted[:55]}"
                        if ar_value and ar_value.startswith("{") and '"type"' in ar_value:
                            extracted = _extract_text_from_json(ar_value)
                            if extracted:
                                ar_preview = f"[json] {extracted[:55]}"
                        print(f"    [{status:15s}] {resource_id}")
                        print(f"      {key}: {en_preview}")
                        if ar_value:
                            print(f"      AR: {ar_preview}")
                        if detail:
                            print(f"      → {detail}")

        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
        time.sleep(0.3)

    return problems, stats, total_resources


def main():
    parser = argparse.ArgumentParser(
        description="Comprehensive Arabic translation audit")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show every problem found")
    parser.add_argument("--fix-json", default=None,
                        help="Output fix list as JSON (for fix_remaining_ar.py)")
    parser.add_argument("--type", default=None,
                        help="Audit only one type (PRODUCT, COLLECTION, etc.)")
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ.get("SAUDI_SHOP_URL")
    token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not token:
        print("ERROR: Set SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN in .env")
        sys.exit(1)

    client = ShopifyClient(shop_url, token)

    resource_types = ["PRODUCT", "COLLECTION", "METAFIELD", "METAOBJECT",
                      "ONLINE_STORE_THEME", "PAGE"]
    if args.type:
        resource_types = [args.type.upper()]

    all_problems = []
    total_stats = {"total": 0, "ok": 0, "missing": 0, "identical": 0,
                   "not_arabic": 0, "mixed": 0, "corrupted": 0, "outdated": 0, "skip": 0}

    print("=" * 70)
    print("  ARABIC TRANSLATION AUDIT")
    print("=" * 70)

    for rtype in resource_types:
        print(f"\n  Scanning {rtype}...")
        problems, stats, n_resources = audit_resource_type(
            client, rtype, verbose=args.verbose)
        all_problems.extend(problems)

        for k in total_stats:
            total_stats[k] += stats[k]

        pct = (stats["ok"] / stats["total"] * 100) if stats["total"] else 100
        n_problems = stats["total"] - stats["ok"]
        print(f"    {n_resources} resources | {stats['total']} fields | "
              f"{stats['ok']} OK ({pct:.0f}%) | {n_problems} problems")
        if n_problems:
            parts = []
            if stats["missing"]:
                parts.append(f"missing={stats['missing']}")
            if stats["identical"]:
                parts.append(f"identical={stats['identical']}")
            if stats["not_arabic"]:
                parts.append(f"not_arabic={stats['not_arabic']}")
            if stats["mixed"]:
                parts.append(f"mixed_lang={stats['mixed']}")
            if stats["corrupted"]:
                parts.append(f"corrupted_json={stats['corrupted']}")
            if stats["outdated"]:
                parts.append(f"outdated={stats['outdated']}")
            print(f"    Breakdown: {', '.join(parts)}")

    # Summary
    pct = (total_stats["ok"] / total_stats["total"] * 100) if total_stats["total"] else 100
    print(f"\n{'=' * 70}")
    print(f"  TOTAL: {total_stats['total']} translatable fields")
    print(f"  OK:      {total_stats['ok']} ({pct:.0f}%)")
    print(f"  PROBLEMS: {total_stats['total'] - total_stats['ok']}")
    print(f"    Missing:        {total_stats['missing']}")
    print(f"    Identical:      {total_stats['identical']}")
    print(f"    Not Arabic:     {total_stats['not_arabic']}")
    print(f"    Mixed language: {total_stats['mixed']}")
    print(f"    Corrupted JSON: {total_stats['corrupted']}")
    print(f"    Outdated:       {total_stats['outdated']}")
    print(f"  Skipped: {total_stats['skip']} (non-translatable)")
    print(f"{'=' * 70}")

    if args.fix_json and all_problems:
        # Filter to actionable problems (exclude SKIP, OUTDATED-only)
        fixable = [p for p in all_problems if p["status"] in
                   ("MISSING", "IDENTICAL", "NOT_ARABIC", "MIXED_LANGUAGE", "CORRUPTED_JSON")]
        with open(args.fix_json, "w", encoding="utf-8") as f:
            json.dump(fixable, f, ensure_ascii=False, indent=2)
        print(f"\n  Fix list: {len(fixable)} fields → {args.fix_json}")
        print(f"  Run: python fix_remaining_ar.py --audit {args.fix_json}")


if __name__ == "__main__":
    main()
