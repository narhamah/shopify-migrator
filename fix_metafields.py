#!/usr/bin/env python3
"""Backfill product metafields on already-imported Shopify products.

Reads the translated English product data, finds the corresponding Shopify
product via id_map or SKU match, and sets all text/rich_text metafields
using the GraphQL metafieldsSet API.

This fixes products that were imported but whose metafields were not
saved (e.g. due to REST API inline metafield limitations).

Usage:
    # Dry run — show what would be set
    python fix_metafields.py --dry-run

    # Backfill all product metafields
    python fix_metafields.py

    # Only backfill empty metafields (skip if already set)
    python fix_metafields.py --only-empty
"""

import argparse
import json
import os

from dotenv import load_dotenv
from shopify_client import ShopifyClient

from utils import load_json, save_json, sanitize_rich_text_json


# Metafield keys that are text-based (not references, not file_reference)
TEXT_METAFIELD_KEYS = {
    "custom.tagline",
    "custom.short_description",
    "custom.size_ml",
    "custom.key_benefits_heading",
    "custom.key_benefits_content",
    "custom.clinical_results_heading",
    "custom.clinical_results_content",
    "custom.how_to_use_heading",
    "custom.how_to_use_content",
    "custom.whats_inside_heading",
    "custom.whats_inside_content",
    "custom.free_of_heading",
    "custom.free_of_content",
    "custom.awards_heading",
    "custom.awards_content",
    "custom.fragrance_heading",
    "custom.fragrance_content",
}


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Backfill product metafields on Shopify")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be set")
    parser.add_argument("--only-empty", action="store_true",
                        help="Only set metafields that are currently empty on Shopify")
    args = parser.parse_args()

    shop_url = os.environ.get("SAUDI_SHOP_URL")
    access_token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not access_token:
        print("ERROR: SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN must be set in .env")
        return

    client = ShopifyClient(shop_url, access_token)

    # Load source data
    en_products = load_json("data/english/products.json")
    if not en_products:
        print("ERROR: data/english/products.json is empty or missing")
        return

    # Load ID map
    id_map = load_json("data/id_map.json", default={})
    if isinstance(id_map, list):
        id_map = {}
    product_map = id_map.get("products", {})

    # Build SKU → dest product ID map from Shopify as fallback
    print("Fetching Shopify products for SKU matching...")
    shopify_products = client.get_products()
    sku_to_dest = {}
    dest_product_ids = set()
    for sp in shopify_products:
        dest_product_ids.add(str(sp["id"]))
        for v in sp.get("variants", []):
            sku = v.get("sku", "")
            if sku:
                sku_to_dest[sku] = str(sp["id"])

    print(f"  {len(shopify_products)} Shopify products, {len(sku_to_dest)} SKUs")
    print(f"  {len(product_map)} products in id_map")
    print(f"  {len(en_products)} products in source data\n")

    # Optionally fetch existing metafields to check what's empty
    existing_metafields = {}  # dest_product_id → set of ns.key that have values
    if args.only_empty:
        print("Fetching existing metafields from Shopify...")
        for i, sp in enumerate(shopify_products):
            pid = sp["id"]
            try:
                mfs = client.get_metafields("products", pid)
                filled = set()
                for mf in mfs:
                    if mf.get("value"):
                        filled.add(f"{mf['namespace']}.{mf['key']}")
                existing_metafields[str(pid)] = filled
            except Exception:
                pass
            if (i + 1) % 50 == 0:
                print(f"  Checked {i+1}/{len(shopify_products)} products")
        print(f"  Done checking existing metafields\n")

    updated = 0
    skipped = 0
    errors = 0

    for product in en_products:
        source_id = str(product.get("id", ""))
        title = product.get("title", "")[:50]

        # Find destination product ID
        dest_id = product_map.get(source_id)
        if not dest_id:
            # Fallback: match by SKU
            for v in product.get("variants", []):
                sku = v.get("sku", "")
                if sku and sku in sku_to_dest:
                    dest_id = sku_to_dest[sku]
                    break
        if not dest_id:
            skipped += 1
            continue

        # Verify dest product exists
        if str(dest_id) not in dest_product_ids:
            skipped += 1
            continue

        # Collect text metafields to set
        metafields_to_set = []
        metafields = product.get("metafields", [])

        for mf in metafields:
            ns = mf.get("namespace", "")
            key = mf.get("key", "")
            mf_type = mf.get("type", "")
            value = mf.get("value", "")
            ns_key = f"{ns}.{key}"

            # Only set text-based metafields (skip references — handled separately)
            if ns_key not in TEXT_METAFIELD_KEYS:
                continue

            if not value:
                continue

            # Check if already set on dest
            if args.only_empty:
                filled = existing_metafields.get(str(dest_id), set())
                if ns_key in filled:
                    continue

            # Sanitize rich text
            if "rich_text" in mf_type:
                value = sanitize_rich_text_json(value)

            metafields_to_set.append({
                "ownerId": f"gid://shopify/Product/{dest_id}",
                "namespace": ns,
                "key": key,
                "value": value,
                "type": mf_type,
            })

        if not metafields_to_set:
            skipped += 1
            continue

        if args.dry_run:
            field_names = [f"{m['namespace']}.{m['key']}" for m in metafields_to_set]
            print(f"  WOULD SET on {title}: {', '.join(field_names)}")
            updated += 1
            continue

        try:
            # metafieldsSet supports up to 25 metafields per call
            for i in range(0, len(metafields_to_set), 25):
                batch = metafields_to_set[i:i+25]
                client.set_metafields(batch)
            field_count = len(metafields_to_set)
            print(f"  SET {field_count} metafields on: {title}")
            updated += 1
        except Exception as e:
            print(f"  ERROR on {title}: {e}")
            errors += 1

    print(f"\nDone! Updated: {updated}, Skipped: {skipped}, Errors: {errors}")


if __name__ == "__main__":
    main()
