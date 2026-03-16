#!/usr/bin/env python3
"""Export existing translations from a Shopify store.

Queries the source store for all translatable resources of each type,
fetches their translations for the specified locale, and saves the data
in the same format that ``import_arabic.py`` expects — both a flat
progress file and per-resource-type JSON files.

This is the bridge for cross-store migration: export Arabic (or any
locale) from one store, then import it into another destination store.

Usage:
    # Export Arabic translations from the source store
    python export_translations.py --locale ar

    # Preview what would be exported (no files written)
    python export_translations.py --locale ar --dry-run

    # Export to a specific directory
    python export_translations.py --locale ar --output-dir data/kuwait_ar

    # Export only specific resource types
    python export_translations.py --locale ar --resource-type PRODUCT
"""

import argparse
import json
import os
import time

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, load_json, save_json
from tara_migrate.core.graphql_queries import TRANSLATABLE_RESOURCES_QUERY, paginate_query


# Resource types that can carry translations
RESOURCE_TYPES = [
    "PRODUCT",
    "COLLECTION",
    "PAGE",
    "ARTICLE",
    "BLOG",
    "METAOBJECT",
]

# Shopify GID prefix → resource type label for progress keys
GID_PREFIX_MAP = {
    "Product": "prod",
    "Collection": "coll",
    "Page": "page",
    "Article": "art",
    "Blog": "blog",
    "Metaobject": "mo",
}


def _gid_type(gid):
    """Extract the type name from a Shopify GID string."""
    # gid://shopify/Product/12345 → Product
    parts = gid.split("/")
    return parts[3] if len(parts) >= 4 else ""


def _extract_handle(translatable_content):
    """Extract the English handle from translatable content fields."""
    for tc in translatable_content:
        if tc["key"] == "handle":
            return tc.get("value", "")
    return ""


def _extract_metaobject_type(translatable_content):
    """Extract the metaobject type from translatable content (if present)."""
    for tc in translatable_content:
        if tc["key"] == "type":
            return tc.get("value", "")
    return ""


def export_resource_type(client, resource_type, locale, dry_run=False):
    """Export translations for a single resource type.

    Returns:
        (progress_entries, resource_items)
        - progress_entries: dict of flat progress keys → translated values
        - resource_items: list of full resource dicts (handle, fields, etc.)
    """
    query = TRANSLATABLE_RESOURCES_QUERY.replace("%LOCALE%", locale)
    progress_entries = {}
    resource_items = []

    print(f"\n{'='*60}")
    print(f"Exporting {resource_type} translations (locale: {locale})...")
    print(f"{'='*60}")

    count = 0
    translated = 0

    for node in paginate_query(client, query, "translatableResources",
                                variables={"resourceType": resource_type}):
        gid = node["resourceId"]
        content = node.get("translatableContent", [])
        translations = node.get("translations", [])
        count += 1

        if not translations:
            continue

        # Build translation lookup: key → value
        trans_map = {t["key"]: t["value"] for t in translations if t.get("value")}
        if not trans_map:
            continue

        translated += 1
        handle = _extract_handle(content)
        gid_type = _gid_type(gid)

        # Build progress keys
        if resource_type == "METAOBJECT":
            # Metaobject progress keys: mo.{type}.{handle}.{field}
            mo_type = ""
            for tc in content:
                if tc["key"] == "type":
                    mo_type = tc.get("value", "")
                    break
            if not mo_type:
                # Try to infer from handle pattern
                mo_type = "unknown"
            for key, value in trans_map.items():
                if key in ("handle", "type"):
                    continue
                progress_entries[f"mo.{mo_type}.{handle}.{key}"] = value
        else:
            # Standard resource: {prefix}.{handle}.{field}
            prefix_map = {
                "PRODUCT": "prod",
                "COLLECTION": "coll",
                "PAGE": "page",
                "ARTICLE": "art",
                "BLOG": "blog",
            }
            prefix = prefix_map.get(resource_type, resource_type.lower())
            for key, value in trans_map.items():
                if key == "handle":
                    continue
                progress_entries[f"{prefix}.{handle}.{key}"] = value

        # Build full resource item for JSON export
        item = {
            "id": gid,
            "handle": handle,
        }
        # Include English content as reference
        for tc in content:
            if tc["key"] != "handle":
                item[f"en_{tc['key']}"] = tc.get("value", "")
        # Include translations
        for key, value in trans_map.items():
            item[key] = value
        resource_items.append(item)

    print(f"  Found {count} resources, {translated} with translations "
          f"({len(progress_entries)} field entries)")
    return progress_entries, resource_items


def main():
    parser = argparse.ArgumentParser(
        description="Export translations from a Shopify store")
    parser.add_argument("--locale", default="ar",
                        help="Locale to export (default: ar)")
    parser.add_argument("--output-dir",
                        help="Output directory (default: data/{dest}/arabic or data/arabic)")
    parser.add_argument("--resource-type",
                        help="Export only this resource type (e.g. PRODUCT)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without writing files")
    args = parser.parse_args()

    load_dotenv()

    # Connect to the SOURCE store (we're exporting FROM it)
    shop_url = config.get_source_shop_url()
    access_token = config.get_source_access_token()
    client = ShopifyClient(shop_url, access_token)

    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = config.get_ar_dir()

    if not args.dry_run:
        os.makedirs(output_dir, exist_ok=True)

    print(f"Exporting {args.locale} translations from {shop_url}")
    print(f"Output directory: {output_dir}")

    # Determine which resource types to export
    types_to_export = RESOURCE_TYPES
    if args.resource_type:
        types_to_export = [args.resource_type.upper()]

    all_progress = {}
    all_items_by_type = {}

    for resource_type in types_to_export:
        progress, items = export_resource_type(
            client, resource_type, args.locale, dry_run=args.dry_run)
        all_progress.update(progress)
        all_items_by_type[resource_type] = items

    # Save progress file (flat key-value, same format as _translation_progress_ar.json)
    progress_file = os.path.join(output_dir, f"_translation_progress_{args.locale}.json")
    if not args.dry_run:
        save_json(all_progress, progress_file)
        print(f"\nSaved {len(all_progress)} progress entries → {progress_file}")

    # Save per-type JSON files
    type_to_filename = {
        "PRODUCT": "products.json",
        "COLLECTION": "collections.json",
        "PAGE": "pages.json",
        "ARTICLE": "articles.json",
        "BLOG": "blogs.json",
        "METAOBJECT": "metaobjects.json",
    }
    for resource_type, items in all_items_by_type.items():
        if not items:
            continue
        filename = type_to_filename.get(resource_type, f"{resource_type.lower()}.json")
        filepath = os.path.join(output_dir, filename)
        if not args.dry_run:
            save_json(items, filepath)
            print(f"Saved {len(items)} {resource_type} items → {filepath}")

    # Summary
    total_items = sum(len(items) for items in all_items_by_type.values())
    print(f"\n{'='*60}")
    print(f"Export complete: {total_items} resources, {len(all_progress)} translation fields")
    if args.dry_run:
        print("(DRY RUN — no files written)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
