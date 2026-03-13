#!/usr/bin/env python3
"""Consolidated translation audit, investigation, and CSV upload via Shopify GraphQL.

Merges three capabilities:
1. **audit** — Scan all translatable resources for missing/broken/identical/outdated translations
2. **investigate** — Query specific resources or types for detailed translation state
3. **upload** — Upload translations from a Shopify CSV export via GraphQL

Usage:
    # Full audit
    python -m tara_migrate.audit.audit_translations --mode audit
    python -m tara_migrate.audit.audit_translations --mode audit --verbose --type PRODUCT
    python -m tara_migrate.audit.audit_translations --mode audit --fix-json audit_fix.json

    # Investigate specific resources
    python -m tara_migrate.audit.audit_translations --mode investigate --type PRODUCT
    python -m tara_migrate.audit.audit_translations --mode investigate --resource-id gid://shopify/Product/123

    # Upload from CSV
    python -m tara_migrate.audit.audit_translations --mode upload --csv translations.csv
    python -m tara_migrate.audit.audit_translations --mode upload --csv translations.csv --dry-run
"""

import argparse
import csv
import json
import os
import re
import sys
import time

from dotenv import load_dotenv

from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.core.graphql_queries import (
    FETCH_DIGESTS_QUERY,
    REGISTER_TRANSLATIONS_MUTATION,
    TRANSLATABLE_RESOURCES_QUERY,
    fetch_translatable_resources,
    paginate_query,
    upload_translations,
)
from tara_migrate.core.language import count_chars, detect_mixed_language, has_arabic
from tara_migrate.core.rich_text import extract_text, is_rich_text_json
from tara_migrate.core.shopify_fields import (
    TRANSLATABLE_RESOURCE_TYPES,
    is_skippable_field,
    is_skippable_value,
)
from tara_migrate.core.utils import sanitize_rich_text_json


# ---------------------------------------------------------------------------
# GraphQL query for fetching resources by ID (used by investigate)
# ---------------------------------------------------------------------------

TRANSLATABLE_BY_IDS_QUERY = """
query($resourceIds: [ID!]!, $first: Int!) {
  translatableResourcesByIds(resourceIds: $resourceIds, first: $first) {
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
  }
}
"""

# CSV Type -> Shopify GID prefix mapping (for upload mode)
TYPE_TO_GID_PREFIX = {
    "PRODUCT": "gid://shopify/Product/",
    "COLLECTION": "gid://shopify/Collection/",
    "METAFIELD": "gid://shopify/Metafield/",
    "METAOBJECT": "gid://shopify/Metaobject/",
    "ONLINE_STORE_THEME": "gid://shopify/OnlineStoreTheme/",
    "PAGE": "gid://shopify/Page/",
    "DELIVERY_METHOD_DEFINITION": "gid://shopify/DeliveryMethodDefinition/",
    "COOKIE_BANNER": "gid://shopify/CookieBanner/",
    "MEDIA_IMAGE": "gid://shopify/MediaImage/",
}

# CSV field patterns that should be copied as-is (not translated)
_KEEP_AS_IS_PATTERNS = [
    "facebook_url", "instagram_url", "tiktok_url", "twitter_url",
    "google_maps_api_key", "form_id", "portal_id", "region",
    "anchor_id", "worker_url", "default_lat", "default_lng",
    "custom_max_height", "custom_max_width",
]


# ═══════════════════════════════════════════════════════════════════════════
# 1. AUDIT — classify_translation + audit_translations
# ═══════════════════════════════════════════════════════════════════════════

def classify_translation(english, arabic, key=None, outdated=False):
    """Classify a translation pair.

    Args:
        english: Source (English) value.
        arabic: Translated value (may be None if missing).
        key: Optional field key for skip detection.
        outdated: Whether Shopify marks the translation as outdated.

    Returns:
        (status, detail) where status is one of:
        OK, MISSING, IDENTICAL, NOT_ARABIC, MIXED_LANGUAGE,
        CORRUPTED_JSON, OUTDATED, SKIP
    """
    if key and is_skippable_field(key):
        return "SKIP", "non-translatable field"
    if is_skippable_value(english):
        return "SKIP", "non-translatable value"

    if not arabic:
        return "MISSING", "no translation"

    # Extract text for analysis (handles rich_text JSON)
    en_text = english
    ar_text = arabic

    if english and english.strip().startswith("{") and '"type"' in english:
        en_extracted = extract_text(english)
        ar_extracted = extract_text(arabic)
        if en_extracted:
            en_text = en_extracted
        if ar_extracted:
            ar_text = ar_extracted
        elif arabic.strip().startswith("{"):
            try:
                json.loads(arabic)
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
    if arabic == english:
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


def _audit_resource_type(client, resource_type, locale, verbose=False):
    """Audit all resources of a given type.

    Returns (problems, stats, resource_count).
    """
    query = TRANSLATABLE_RESOURCES_QUERY.replace("%LOCALE%", locale)
    problems = []
    stats = {
        "total": 0, "ok": 0, "missing": 0, "identical": 0,
        "not_arabic": 0, "mixed": 0, "corrupted": 0, "outdated": 0, "skip": 0,
    }

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

                status, detail = classify_translation(
                    value, ar_value, key=key, outdated=outdated,
                )

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


def audit_translations(client, locale="ar", resource_types=None, verbose=False):
    """Scan all translatable resources for missing/broken/identical/outdated translations.

    Args:
        client: ShopifyClient instance.
        locale: Target locale to audit (default: "ar").
        resource_types: List of resource types to audit (default: all).
        verbose: Print every problem found.

    Returns:
        (all_problems, total_stats) where total_stats has keys:
        total, ok, missing, identical, not_arabic, mixed, corrupted, outdated, skip
    """
    if resource_types is None:
        resource_types = TRANSLATABLE_RESOURCE_TYPES

    all_problems = []
    total_stats = {
        "total": 0, "ok": 0, "missing": 0, "identical": 0,
        "not_arabic": 0, "mixed": 0, "corrupted": 0, "outdated": 0, "skip": 0,
    }

    print("=" * 70)
    print(f"  TRANSLATION AUDIT (locale: {locale})")
    print("=" * 70)

    for rtype in resource_types:
        print(f"\n  Scanning {rtype}...")
        problems, stats, n_resources = _audit_resource_type(
            client, rtype, locale, verbose=verbose,
        )
        all_problems.extend(problems)

        for k in total_stats:
            total_stats[k] += stats[k]

        pct = (stats["ok"] / stats["total"] * 100) if stats["total"] else 100
        n_problems = stats["total"] - stats["ok"]
        print(
            f"    {n_resources} resources | {stats['total']} fields | "
            f"{stats['ok']} OK ({pct:.0f}%) | {n_problems} problems"
        )
        if n_problems:
            parts = []
            for key, label in [
                ("missing", "missing"), ("identical", "identical"),
                ("not_arabic", "not_translated"), ("mixed", "mixed_lang"),
                ("corrupted", "corrupted_json"), ("outdated", "outdated"),
            ]:
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

    return all_problems, total_stats


# ═══════════════════════════════════════════════════════════════════════════
# 2. INVESTIGATE — analyze_resource + investigate_translations
# ═══════════════════════════════════════════════════════════════════════════

def _analyze_resource(node):
    """Analyze a single translatable resource node.

    Returns (resource_id, fields) where each field dict has:
    key, value, digest, translation, status.
    """
    resource_id = node["resourceId"]
    content = node["translatableContent"]
    translations = {t["key"]: t for t in node["translations"]}

    fields = []
    for field in content:
        key = field["key"]
        value = field["value"] or ""
        digest = field["digest"]
        trans = translations.get(key)

        # Extract text from rich_text JSON for display
        display_value = value[:80]
        if value.startswith("{") and '"type"' in value:
            extracted = extract_text(value)
            if extracted:
                display_value = f"[rich_text] {extracted[:70]}"

        if trans:
            trans_value = trans["value"] or ""
            trans_display = trans_value[:80]
            if trans_value.startswith("{") and '"type"' in trans_value:
                extracted = extract_text(trans_value)
                if extracted:
                    trans_display = f"[rich_text] {extracted[:70]}"

            ar = has_arabic(trans_value) or (
                trans_value.startswith("{")
                and has_arabic(extract_text(trans_value) or "")
            )
            outdated = trans.get("outdated", False)
            status = "OK" if ar else "NOT_ARABIC"
            if outdated:
                status += " [OUTDATED]"
            if trans_value == value and not ar:
                status = "IDENTICAL"
        else:
            trans_display = "(missing)"
            status = "MISSING"

        fields.append({
            "key": key,
            "value": display_value,
            "digest": digest,
            "translation": trans_display,
            "status": status,
        })

    return resource_id, fields


def _print_field(f, key_width=20):
    """Print a single field's investigation result."""
    icon = {"OK": "+", "MISSING": "X", "IDENTICAL": "=", "NOT_ARABIC": "!"}.get(
        f["status"].split()[0], "?"
    )
    print(f"  {icon} [{f['status']:12s}] {f['key']:{key_width}s} | EN: {f['value'][:50]}")
    if f["translation"] != "(missing)":
        pad = " " * (key_width + 20)
        print(f"  {pad} | AR: {f['translation'][:50]}")


def investigate_translations(client, locale="ar", resource_type=None,
                             resource_id=None, resource_ids=None,
                             limit=10, all_pages=False, json_out=None):
    """Query specific resources for detailed translation state.

    Modes:
    - resource_id with "Product" in it: fetch product + its metafields
    - resource_ids: fetch specific GIDs
    - resource_type: browse all of that type
    - None: summary of all types

    Args:
        client: ShopifyClient instance.
        locale: Target locale (default: "ar").
        resource_type: Shopify resource type (PRODUCT, COLLECTION, etc.).
        resource_id: Single resource GID or numeric product ID.
        resource_ids: List of resource GIDs.
        limit: Resources per page.
        all_pages: Fetch all pages (vs just first).
        json_out: Path to save results as JSON.

    Returns:
        List of {"resourceId": ..., "fields": [...]} dicts.
    """
    query_by_ids = TRANSLATABLE_BY_IDS_QUERY.replace("%LOCALE%", locale)
    query_by_type = TRANSLATABLE_RESOURCES_QUERY.replace("%LOCALE%", locale)

    # --- Mode: specific product (by numeric ID) ---
    if resource_id and not resource_id.startswith("gid://"):
        product_gid = f"gid://shopify/Product/{resource_id}"
        print(f"Fetching product {product_gid} and its metafields...\n")

        data = client._graphql(query_by_ids, {
            "resourceIds": [product_gid],
            "first": 1,
        })
        edges = data["translatableResourcesByIds"]["edges"]
        if not edges:
            print(f"Product {product_gid} not found")
            return []

        rid, fields = _analyze_resource(edges[0]["node"])
        print(f"=== {rid} ===")
        for f in fields:
            _print_field(f)

        # Also fetch metafields
        print(f"\nFetching metafields for product {resource_id}...")
        results = [{"resourceId": rid, "fields": fields}]
        try:
            metafields = client.get_metafields("products", resource_id)
            if metafields:
                mf_gids = [f"gid://shopify/Metafield/{mf['id']}" for mf in metafields]
                print(f"Found {len(metafields)} metafields, checking translations...\n")

                for i in range(0, len(mf_gids), 10):
                    batch = mf_gids[i:i + 10]
                    data = client._graphql(query_by_ids, {
                        "resourceIds": batch,
                        "first": len(batch),
                    })
                    for edge in data["translatableResourcesByIds"]["edges"]:
                        mrid, mf_fields = _analyze_resource(edge["node"])
                        mf_match = next(
                            (m for m in metafields
                             if f"gid://shopify/Metafield/{m['id']}" == mrid),
                            None,
                        )
                        ns_key = (
                            f"{mf_match['namespace']}.{mf_match['key']}"
                            if mf_match else mrid
                        )
                        for f in mf_fields:
                            _print_field(f, key_width=40)
                        results.append({"resourceId": mrid, "fields": mf_fields})
            else:
                print("  No metafields found via REST (may need GraphQL)")
        except Exception as e:
            print(f"  Error fetching metafields: {e}")

        return results

    # --- Mode: specific GIDs ---
    if resource_ids:
        print(f"Fetching {len(resource_ids)} specific resources...\n")
        data = client._graphql(query_by_ids, {
            "resourceIds": resource_ids,
            "first": len(resource_ids),
        })
        results = []
        for edge in data["translatableResourcesByIds"]["edges"]:
            rid, fields = _analyze_resource(edge["node"])
            results.append({"resourceId": rid, "fields": fields})
            print(f"=== {rid} ===")
            for f in fields:
                _print_field(f)
        return results

    # --- Mode: specific resource by GID ---
    if resource_id and resource_id.startswith("gid://"):
        return investigate_translations(
            client, locale, resource_ids=[resource_id],
            json_out=json_out,
        )

    # --- Mode: summary of all types ---
    if not resource_type:
        summary_types = [
            "PRODUCT", "COLLECTION", "METAFIELD", "METAOBJECT",
            "ONLINE_STORE_THEME", "PAGE",
        ]
        print("=== TRANSLATION COVERAGE SUMMARY ===\n")

        for rtype in summary_types:
            try:
                data = client._graphql(query_by_type, {
                    "resourceType": rtype,
                    "first": 50,
                })
                edges = data["translatableResources"]["edges"]
                has_more = data["translatableResources"]["pageInfo"]["hasNextPage"]

                total_fields = 0
                translated_ok = 0
                missing = 0
                identical = 0
                not_arabic = 0
                outdated = 0

                for edge in edges:
                    _, fields = _analyze_resource(edge["node"])
                    for f in fields:
                        if not f["value"] or f["value"] == "(empty)":
                            continue
                        total_fields += 1
                        s = f["status"]
                        if s.startswith("OK"):
                            translated_ok += 1
                        elif s == "MISSING":
                            missing += 1
                        elif s == "IDENTICAL":
                            identical += 1
                        elif s.startswith("NOT_ARABIC"):
                            not_arabic += 1
                        if "OUTDATED" in s:
                            outdated += 1

                pct = (translated_ok / total_fields * 100) if total_fields else 0
                more = "+" if has_more else ""
                print(
                    f"  {rtype:25s} | {len(edges)}{more:2s} resources | "
                    f"{translated_ok}/{total_fields} fields OK ({pct:.0f}%) | "
                    f"missing={missing} identical={identical} not_arabic={not_arabic} "
                    f"outdated={outdated}"
                )
            except Exception as e:
                print(f"  {rtype:25s} | ERROR: {e}")

        print("\nRun with --type <TYPE> for details, or --resource-id <ID> for a specific product.")
        return []

    # --- Mode: detailed browse of a specific type ---
    print(f"Fetching {resource_type} translations...\n")
    cursor = None
    page = 0
    all_results = []

    while True:
        data = client._graphql(query_by_type, {
            "resourceType": resource_type,
            "first": limit,
            "after": cursor,
        })
        edges = data["translatableResources"]["edges"]
        page_info = data["translatableResources"]["pageInfo"]

        for edge in edges:
            rid, fields = _analyze_resource(edge["node"])
            all_results.append({"resourceId": rid, "fields": fields})

            print(f"=== {rid} ===")
            for f in fields:
                if not f["value"]:
                    continue
                _print_field(f)
                if f["translation"] != "(missing)" and f["status"] != "OK":
                    pass  # already printed by _print_field
            print()

        page += 1
        if not page_info["hasNextPage"] or not all_pages:
            break
        cursor = page_info["endCursor"]

    if json_out:
        with open(json_out, "w", encoding="utf-8") as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)
        print(f"\nSaved to {json_out}")

    return all_results


# ═══════════════════════════════════════════════════════════════════════════
# 3. UPLOAD — upload_from_csv
# ═══════════════════════════════════════════════════════════════════════════

def _is_csv_non_translatable(row):
    """Return True if this CSV row should never be translated."""
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
    """Check if a CSV row's value should be copied as-is (not translated)."""
    field = row.get("Field", "")
    for pat in _KEEP_AS_IS_PATTERNS:
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


def _has_arabic_for_upload(text, min_ratio=0.3):
    """Check if text contains sufficient Arabic characters (for upload validation)."""
    if not text:
        return False
    check_text = text
    if text.startswith("{") and '"type"' in text:
        extracted = extract_text(text)
        if extracted and extracted.strip():
            check_text = extracted
    stripped = re.sub(r"<[^>]+>", " ", check_text)
    stripped = re.sub(r"\{[^}]*\}", " ", stripped).strip()
    if not stripped:
        return True
    arabic = len(re.findall(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", stripped))
    alpha = len(re.findall(r"[a-zA-Z\u00C0-\u00FF\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", stripped))
    if alpha == 0:
        return True
    return arabic / alpha >= min_ratio


def _build_gid(csv_type, identification):
    """Build Shopify GID from CSV Type and Identification columns."""
    prefix = TYPE_TO_GID_PREFIX.get(csv_type)
    if not prefix:
        return None
    clean_id = identification.lstrip("'")
    return f"{prefix}{clean_id}"


def upload_from_csv(client, locale, csv_path, type_filter=None,
                    dry_run=False, fix_misaligned=False, force=False,
                    skip_identical=True, batch_size=10):
    """Upload translations from a Shopify CSV export via GraphQL.

    Args:
        client: ShopifyClient instance.
        locale: Target locale (e.g. "ar").
        csv_path: Path to the translated CSV file.
        type_filter: Only upload a specific type (e.g. "PRODUCT").
        dry_run: Show what would be uploaded without making changes.
        fix_misaligned: Detect and skip misaligned translations.
        force: Upload even if translation doesn't have Arabic.
        skip_identical: Skip rows where Translated == Default (default: True).
        batch_size: Resources per GraphQL batch (default: 10).

    Returns:
        (uploaded, errors) counts.
    """
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"Read {len(rows)} rows from {csv_path}")

    # Group translatable rows by resource GID
    resources = {}
    skipped = {
        "empty": 0, "non_translatable": 0, "keep_as_is": 0,
        "no_arabic": 0, "identical": 0, "unknown_type": 0, "type_filter": 0,
    }

    for row in rows:
        csv_type = row.get("Type", "").strip()
        identification = row.get("Identification", "").strip()
        field = row.get("Field", "").strip()
        default = row.get("Default content", "").strip()
        translated = row.get("Translated content", "").strip()

        if type_filter and csv_type != type_filter:
            skipped["type_filter"] += 1
            continue
        if _is_csv_non_translatable(row):
            skipped["non_translatable"] += 1
            continue
        if _is_keep_as_is(row):
            skipped["keep_as_is"] += 1
            continue
        if not translated:
            skipped["empty"] += 1
            continue
        if skip_identical and translated == default and not _has_arabic_for_upload(translated):
            skipped["identical"] += 1
            continue
        if not force and not _has_arabic_for_upload(translated):
            skipped["no_arabic"] += 1
            continue

        gid = _build_gid(csv_type, identification)
        if not gid:
            skipped["unknown_type"] += 1
            continue

        if gid not in resources:
            resources[gid] = []
        resources[gid].append({
            "field": field,
            "default": default,
            "translated": translated,
            "csv_type": csv_type,
        })

    total_fields = sum(len(fields) for fields in resources.values())
    print(f"\nTo upload: {total_fields} fields across {len(resources)} resources")
    print(f"Skipped: {json.dumps(skipped, indent=2)}")

    if dry_run:
        print("\n--- DRY RUN ---")
        for gid, fields in list(resources.items())[:5]:
            print(f"\n  {gid}:")
            for f in fields:
                ar_preview = f["translated"][:50]
                print(f"    {f['field']:20s} -> {ar_preview}")
        if len(resources) > 5:
            print(f"\n  ... and {len(resources) - 5} more resources")
        return 0, 0

    # Upload translations via GraphQL
    fetch_query = FETCH_DIGESTS_QUERY.replace("%LOCALE%", locale)

    uploaded = 0
    errors = 0
    skipped_digest = 0
    misaligned = 0
    gid_list = list(resources.keys())

    for batch_start in range(0, len(gid_list), batch_size):
        batch_gids = gid_list[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (len(gid_list) + batch_size - 1) // batch_size

        print(
            f"\nBatch {batch_num}/{total_batches}: "
            f"fetching digests for {len(batch_gids)} resources..."
        )

        # Step 1: Fetch current digests from Shopify
        try:
            data = client._graphql(fetch_query, {
                "resourceIds": batch_gids,
                "first": len(batch_gids),
            })
        except Exception as e:
            print(f"  ERROR fetching digests: {e}")
            errors += len(batch_gids)
            continue

        edges = data.get("translatableResourcesByIds", {}).get("edges", [])
        digest_map = {}
        for edge in edges:
            node = edge["node"]
            rid = node["resourceId"]
            digest_map[rid] = {}
            for tc in node["translatableContent"]:
                digest_map[rid][tc["key"]] = {
                    "digest": tc["digest"],
                    "value": tc["value"],
                }

        # Step 2: Register translations for each resource
        for gid in batch_gids:
            csv_fields = resources[gid]

            if gid not in digest_map:
                print(f"  SKIP {gid}: not found in Shopify (deleted?)")
                skipped_digest += len(csv_fields)
                continue

            translations_input = []
            for cf in csv_fields:
                field_key = cf["field"]
                shopify_field = digest_map[gid].get(field_key)

                if not shopify_field:
                    skipped_digest += 1
                    continue

                # Misalignment check
                if fix_misaligned and shopify_field["value"]:
                    shopify_value = shopify_field["value"].strip()
                    csv_default = cf["default"].strip()
                    if shopify_value != csv_default:
                        misaligned += 1
                        continue

                # Sanitize rich_text JSON
                translated_value = cf["translated"]
                if translated_value.strip().startswith("{") and '"type"' in translated_value:
                    translated_value = sanitize_rich_text_json(translated_value)

                translations_input.append({
                    "locale": locale,
                    "key": field_key,
                    "value": translated_value,
                    "translatableContentDigest": shopify_field["digest"],
                })

            if not translations_input:
                continue

            try:
                result = client._graphql(REGISTER_TRANSLATIONS_MUTATION, {
                    "resourceId": gid,
                    "translations": translations_input,
                })
                user_errors = result.get("translationsRegister", {}).get("userErrors", [])
                if user_errors:
                    print(f"  ERRORS for {gid}:")
                    for ue in user_errors:
                        print(f"    {ue['field']}: {ue['message']}")
                    errors += len(user_errors)
                    uploaded += len(translations_input) - len(user_errors)
                else:
                    uploaded += len(translations_input)
            except Exception as e:
                print(f"  ERROR uploading {gid}: {e}")
                errors += len(translations_input)

        # Brief pause between batches
        if batch_start + batch_size < len(gid_list):
            time.sleep(0.5)

    print(f"\n{'=' * 60}")
    print(f"  UPLOAD COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Uploaded:          {uploaded} fields")
    print(f"  Errors:            {errors}")
    print(f"  Skipped (digest):  {skipped_digest}")
    if misaligned:
        print(f"  Misaligned:        {misaligned} (content changed since CSV export)")
    print(f"{'=' * 60}")

    return uploaded, errors


# ═══════════════════════════════════════════════════════════════════════════
# CLI main
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Translation audit, investigation, and CSV upload via Shopify GraphQL",
    )
    parser.add_argument(
        "--mode", default="audit", choices=["audit", "investigate", "upload"],
        help="Operation mode (default: audit)",
    )
    parser.add_argument("--locale", default="ar", help="Target locale (default: ar)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show every problem found")
    parser.add_argument("--type", default=None, help="Resource type filter (PRODUCT, COLLECTION, etc.)")
    parser.add_argument("--fix-json", default=None, help="Output fix list as JSON (audit mode)")
    parser.add_argument("--resource-id", default=None, help="Specific resource GID or product ID (investigate mode)")
    parser.add_argument("--resource-ids", nargs="+", default=None, help="Specific resource GIDs (investigate mode)")
    parser.add_argument("--limit", type=int, default=10, help="Resources per page (investigate mode, default: 10)")
    parser.add_argument("--all-pages", action="store_true", help="Fetch all pages (investigate mode)")
    parser.add_argument("--json-out", default=None, help="Save results to JSON file")
    parser.add_argument("--csv", default=None, help="Translated CSV file (upload mode)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be uploaded (upload mode)")
    parser.add_argument("--fix-misaligned", action="store_true", help="Skip misaligned translations (upload mode)")
    parser.add_argument("--force", action="store_true", help="Upload even without Arabic (upload mode)")
    parser.add_argument("--batch-size", type=int, default=10, help="Resources per batch (upload mode)")
    parser.add_argument("--shop-url-env", default="SAUDI_SHOP_URL", help="Env var for shop URL")
    parser.add_argument("--token-env", default="SAUDI_ACCESS_TOKEN", help="Env var for access token")
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ.get(args.shop_url_env)
    token = os.environ.get(args.token_env)
    if not shop_url or not token:
        print(f"ERROR: Set {args.shop_url_env} and {args.token_env} in .env")
        sys.exit(1)

    client = ShopifyClient(shop_url, token)

    if args.mode == "audit":
        resource_types = [args.type.upper()] if args.type else None
        all_problems, total_stats = audit_translations(
            client, locale=args.locale, resource_types=resource_types,
            verbose=args.verbose,
        )

        if args.fix_json and all_problems:
            fixable = [
                p for p in all_problems
                if p["status"] in (
                    "MISSING", "IDENTICAL", "NOT_ARABIC",
                    "MIXED_LANGUAGE", "CORRUPTED_JSON",
                )
            ]
            with open(args.fix_json, "w", encoding="utf-8") as f:
                json.dump(fixable, f, ensure_ascii=False, indent=2)
            print(f"\n  Fix list: {len(fixable)} fields -> {args.fix_json}")
            print(f"  Run: python fix_translations.py --audit {args.fix_json} --locale {args.locale}")

    elif args.mode == "investigate":
        investigate_translations(
            client, locale=args.locale,
            resource_type=args.type.upper() if args.type else None,
            resource_id=args.resource_id,
            resource_ids=args.resource_ids,
            limit=args.limit,
            all_pages=args.all_pages,
            json_out=args.json_out,
        )

    elif args.mode == "upload":
        if not args.csv:
            print("ERROR: --csv is required for upload mode")
            sys.exit(1)
        upload_from_csv(
            client, locale=args.locale, csv_path=args.csv,
            type_filter=args.type.upper() if args.type else None,
            dry_run=args.dry_run,
            fix_misaligned=args.fix_misaligned,
            force=args.force,
            batch_size=args.batch_size,
        )


if __name__ == "__main__":
    main()
