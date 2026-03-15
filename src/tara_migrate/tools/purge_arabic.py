#!/usr/bin/env python3
"""Purge all Arabic translations from the Saudi store.

This script removes all Arabic translations via Shopify's translationsRemove
GraphQL mutation. After purging, re-run the existing translation pipeline:

    python purge_arabic.py --skip-theme                    # Purge Arabic translations
    python translate_gaps.py --lang ar                     # Retranslate (Magento scrape + AI for gaps)
    python import_arabic.py                                # Upload to Shopify

Usage:
    python purge_arabic.py                                 # Purge all Arabic translations
    python purge_arabic.py --dry-run                       # Preview what would be purged
    python purge_arabic.py --skip-theme                    # Skip ONLINE_STORE_THEME (4000+ keys)
    python purge_arabic.py --type PRODUCT,COLLECTION       # Only specific resource types
"""

import argparse
import time

from dotenv import load_dotenv

from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.core import config
from tara_migrate.core.graphql_queries import TRANSLATABLE_RESOURCES_QUERY
from tara_migrate.core.shopify_fields import TRANSLATABLE_RESOURCE_TYPES


LOCALE = "ar"

REMOVE_TRANSLATIONS_MUTATION = """
mutation translationsRemove($resourceId: ID!, $translationKeys: [String!]!, $locales: [String!]!) {
  translationsRemove(resourceId: $resourceId, translationKeys: $translationKeys, locales: $locales) {
    userErrors {
      message
      field
    }
  }
}
"""


def fetch_all_translations(client, resource_types, locale=LOCALE):
    """Fetch all translatable fields with their Arabic translations.

    Returns list of dicts: [{resource_id, resource_type, key, arabic}, ...]
    """
    query = TRANSLATABLE_RESOURCES_QUERY.replace("%LOCALE%", locale)
    all_fields = []

    for rtype in resource_types:
        count = 0
        field_count = 0
        has_translation = 0
        cursor = None

        while True:
            try:
                data = client._graphql(query, {
                    "resourceType": rtype,
                    "first": 50,
                    "after": cursor,
                })
            except Exception as e:
                print(f"  ERROR fetching {rtype}: {e}")
                break

            container = data.get("translatableResources", {})
            edges = container.get("edges", [])
            page_info = container.get("pageInfo", {})

            for edge in edges:
                node = edge["node"]
                rid = node["resourceId"]
                translations = {t["key"]: t for t in node.get("translations", [])}
                count += 1

                for field in node.get("translatableContent", []):
                    key = field["key"]
                    trans = translations.get(key)
                    arabic = trans["value"] if trans else None

                    all_fields.append({
                        "resource_id": rid,
                        "resource_type": rtype,
                        "key": key,
                        "arabic": arabic,
                    })
                    field_count += 1
                    if arabic:
                        has_translation += 1

            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            time.sleep(0.3)

        print(f"  {rtype}: {count} resources, {field_count} fields, "
              f"{has_translation} translated")

    return all_fields


def purge_translations(client, fields, dry_run=False, locale=LOCALE):
    """Remove all Arabic translations.

    Groups by resource_id and sends batched translationsRemove mutations.
    """
    by_resource = {}
    for f in fields:
        if f["arabic"]:
            rid = f["resource_id"]
            if rid not in by_resource:
                by_resource[rid] = []
            by_resource[rid].append(f["key"])

    total_to_remove = sum(len(keys) for keys in by_resource.values())
    print(f"\n  {total_to_remove} translations across {len(by_resource)} resources to purge")

    if dry_run:
        by_type = {}
        for f in fields:
            if f["arabic"]:
                rtype = f["resource_type"]
                by_type[rtype] = by_type.get(rtype, 0) + 1
        for rtype, count in sorted(by_type.items()):
            print(f"    {rtype}: {count}")
        return total_to_remove, 0

    removed = 0
    errors = 0

    for i, (rid, keys) in enumerate(by_resource.items()):
        for j in range(0, len(keys), 50):
            batch = keys[j:j + 50]
            try:
                result = client._graphql(REMOVE_TRANSLATIONS_MUTATION, {
                    "resourceId": rid,
                    "translationKeys": batch,
                    "locales": [locale],
                })
                user_errors = result.get("translationsRemove", {}).get("userErrors", [])
                if user_errors:
                    for ue in user_errors:
                        print(f"    ERROR: {rid}: {ue['message']}")
                    errors += len(batch)
                else:
                    removed += len(batch)
            except Exception as e:
                print(f"    ERROR removing from {rid}: {e}")
                errors += len(batch)

            time.sleep(0.3)

        if (i + 1) % 20 == 0:
            print(f"  Purged {removed} / {total_to_remove} "
                  f"({i + 1}/{len(by_resource)} resources)...")

    print(f"\n  Purge complete: removed={removed}, errors={errors}")
    return removed, errors


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Purge all Arabic translations from the destination store"
    )
    parser.add_argument("--skip-theme", action="store_true",
                        help="Skip ONLINE_STORE_THEME (4000+ keys)")
    parser.add_argument("--type", type=str,
                        help="Comma-separated resource types (e.g. PRODUCT,COLLECTION)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without making changes")
    args = parser.parse_args()

    if args.type:
        resource_types = [t.strip() for t in args.type.split(",")]
    else:
        resource_types = list(TRANSLATABLE_RESOURCE_TYPES)
        if args.skip_theme:
            resource_types = [t for t in resource_types
                              if t != "ONLINE_STORE_THEME"]

    shop_url = config.get_dest_shop_url()
    access_token = config.get_dest_access_token()
    client = ShopifyClient(shop_url, access_token)

    print("=" * 60)
    print("ARABIC TRANSLATION PURGE")
    print(f"  Store:           {shop_url}")
    print(f"  Resource types:  {', '.join(resource_types)}")
    print(f"  Mode:            {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)

    # ── Step 1: Fetch all translations ──
    print("\nStep 1: Fetching all translations...")
    fields = fetch_all_translations(client, resource_types)

    translated_count = sum(1 for f in fields if f["arabic"])
    print(f"\n  Total fields: {len(fields)}")
    print(f"  Currently translated: {translated_count}")

    # ── Step 2: Purge ──
    print(f"\n{'=' * 60}")
    print("Step 2: PURGING all Arabic translations")
    print("=" * 60)

    if translated_count == 0:
        print("  Nothing to purge — no existing translations found.")
    else:
        removed, purge_errors = purge_translations(
            client, fields, dry_run=args.dry_run
        )

    # ── Next steps ──
    print(f"\n{'=' * 60}")
    print("NEXT STEPS")
    print("=" * 60)
    print("  1. python translate_gaps.py --lang ar    # Retranslate (Magento scrape + AI gaps)")
    print("  2. python import_arabic.py               # Upload to Shopify")


if __name__ == "__main__":
    main()
