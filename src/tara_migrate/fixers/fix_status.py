#!/usr/bin/env python3
"""Fix unlisted products and remove duplicates on the Saudi Shopify store.

Addresses:
  - Unlisted products → publishes to sales channels (preserves Draft/Active status)
  - Duplicate products (same title) → keeps the one with more data, deletes the other

Usage:
    python fix_status.py --dry-run      # Preview changes
    python fix_status.py                # Apply fixes
"""

import argparse
import os
import time

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import load_json, save_json


def main():
    parser = argparse.ArgumentParser(description="Fix product statuses and remove duplicates")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without modifying")
    parser.add_argument("--skip-duplicates", action="store_true", help="Skip duplicate removal")
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ["SAUDI_SHOP_URL"]
    access_token = os.environ["SAUDI_ACCESS_TOKEN"]
    client = ShopifyClient(shop_url, access_token)

    print("Fetching all products from Shopify...")
    products = client.get_products()
    print(f"  Found {len(products)} products\n")

    # =============================================
    # Phase 1: Report statuses and fix Unlisted
    # =============================================
    print("=" * 60)
    print("PHASE 1: FIX UNLISTED PRODUCTS (publish to sales channels)")
    print("=" * 60)

    by_status = {}
    for p in products:
        s = p.get("status", "unknown")
        by_status.setdefault(s, []).append(p)
    for s, prods in sorted(by_status.items()):
        print(f"  {s}: {len(prods)} products")

    # Fix unlisted: publish to sales channels (doesn't change draft/active status)
    print("\nPublishing unpublished products to sales channels...")
    unpublished = 0
    for p in products:
        pid = p["id"]
        # Check published_at — if None, it's unpublished/unlisted
        if p.get("published_at") is None:
            title = p.get("title", "")[:50]
            if args.dry_run:
                print(f"  {title} — would publish")
                unpublished += 1
                continue
            try:
                client.update_product(pid, {"published": True, "published_scope": "global"})
                print(f"  {title} — published")
                unpublished += 1
            except Exception as e:
                print(f"  {title} — ERROR: {e}")
            time.sleep(0.3)

    print(f"  Published: {unpublished}")

    # =============================================
    # Phase 2: Find and remove duplicates
    # =============================================
    if args.skip_duplicates:
        print("\n  Skipping duplicate check (--skip-duplicates)")
        return

    print(f"\n{'=' * 60}")
    print("PHASE 2: FIND DUPLICATE PRODUCTS")
    print("=" * 60)

    # Group by title
    by_title = {}
    for p in products:
        title = p.get("title", "")
        if title not in by_title:
            by_title[title] = []
        by_title[title].append(p)

    duplicates = {title: prods for title, prods in by_title.items() if len(prods) > 1}

    if not duplicates:
        print("  No duplicate products found")
        return

    print(f"  Found {len(duplicates)} titles with duplicates:\n")

    id_map_file = "data/id_map.json"
    id_map = load_json(id_map_file)
    product_map = id_map.get("products", {})
    # Reverse map: dest_id → source_id
    dest_to_source = {str(v): str(k) for k, v in product_map.items()}

    deleted = 0
    for title, prods in duplicates.items():
        print(f"  '{title}' — {len(prods)} copies:")
        for p in prods:
            pid = p["id"]
            status = p.get("status", "?")
            variants = p.get("variants", [])
            sku = variants[0].get("sku", "") if variants else ""
            handle = p.get("handle", "")
            channels = len(p.get("published_scope", "")) > 0
            print(f"    id={pid} status={status} handle={handle} sku={sku}")

        # Keep the one with more variants/collections, or the Active one, or the first one
        # Prefer: active > draft, more variants > fewer, has SKU > no SKU
        def score(p):
            s = 0
            if p.get("status") == "active":
                s += 100
            s += len(p.get("variants", []))
            if p.get("variants") and p["variants"][0].get("sku"):
                s += 10
            if p.get("images"):
                s += len(p["images"])
            return s

        prods_sorted = sorted(prods, key=score, reverse=True)
        keep = prods_sorted[0]
        remove = prods_sorted[1:]

        print(f"    KEEP: id={keep['id']} (score={score(keep)})")
        for p in remove:
            pid = p["id"]
            print(f"    DELETE: id={pid} (score={score(p)})")

            if args.dry_run:
                deleted += 1
                continue

            try:
                client._request("DELETE", f"products/{pid}.json")
                print("      — deleted")
                deleted += 1

                # Remove from id_map
                source_id = dest_to_source.get(str(pid))
                if source_id and source_id in product_map:
                    # Re-map to the kept product
                    product_map[source_id] = keep["id"]
                    save_json(id_map, id_map_file)
            except Exception as e:
                print(f"      — ERROR: {e}")

            time.sleep(0.3)

    print("\n--- Summary ---")
    print(f"  Published:         {unpublished}")
    print(f"  Duplicates removed: {deleted}")


if __name__ == "__main__":
    main()
