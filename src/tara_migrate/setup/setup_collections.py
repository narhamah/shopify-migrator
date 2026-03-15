#!/usr/bin/env python3
"""Create collections in Saudi Shopify store and link products to them.

Fetches categories from the Saudi Magento store (taraformula.com/sa-en),
creates matching custom collections in Shopify, then links products by SKU.

Usage:
    # Dry run — show what would be created
    python setup_collections.py --dry-run

    # Create collections and link products
    python setup_collections.py

    # Only link products to existing collections (skip creation)
    python setup_collections.py --link-only
"""

import argparse
import os
import time

import requests as http_requests
from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import MAGENTO_HEADERS as HEADERS
from tara_migrate.core import config, load_json, save_json

REQUEST_DELAY = 2.0


def _flatten_categories(items, parent_path=""):
    """Recursively flatten nested Magento category tree."""
    flat = []
    for item in items:
        if item.get("product_count", 0) == 0 and not item.get("children"):
            continue
        flat.append(item)
        children = item.pop("children", []) or []
        if children:
            flat.extend(_flatten_categories(children))
    return flat


def fetch_categories(site_url, store_code):
    """Fetch all categories from Magento store."""
    graphql_url = f"{site_url}/graphql"
    headers = {**HEADERS, "Store": store_code}

    query = """
    {
        categories(filters: {}) {
            items {
                id
                name
                url_key
                url_path
                description
                image
                meta_title
                meta_description
                product_count
                children {
                    id name url_key url_path description image product_count
                    children {
                        id name url_key url_path description image product_count
                    }
                }
            }
        }
    }
    """

    print(f"Fetching categories from {site_url} (store: {store_code})...")
    try:
        resp = http_requests.post(graphql_url, json={"query": query}, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  Error fetching categories: {e}")
        return []

    if "errors" in data:
        print(f"  GraphQL errors: {data['errors']}")
        return []

    items = data.get("data", {}).get("categories", {}).get("items", [])
    flat = _flatten_categories(items)
    print(f"  Found {len(flat)} categories")
    return flat


def fetch_products_with_categories(site_url, store_code):
    """Fetch all products with their category associations."""
    graphql_url = f"{site_url}/graphql"
    headers = {**HEADERS, "Store": store_code}
    all_items = []
    current_page = 1
    page_size = 50

    print(f"Fetching products with categories from {site_url} (store: {store_code})...")

    while True:
        query = f"""
        {{
            products(search: "", pageSize: {page_size}, currentPage: {current_page}) {{
                total_count
                items {{
                    sku
                    name
                    categories {{
                        id
                        name
                        url_key
                    }}
                }}
                page_info {{
                    total_pages
                    current_page
                }}
            }}
        }}
        """

        try:
            resp = http_requests.post(graphql_url, json={"query": query}, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  Error on page {current_page}: {e}")
            break

        if "errors" in data:
            print(f"  GraphQL errors: {data['errors']}")
            break

        products_data = data.get("data", {}).get("products", {})
        items = products_data.get("items", [])
        all_items.extend(items)

        page_info = products_data.get("page_info", {})
        total_pages = page_info.get("total_pages", 1)
        print(f"  Page {current_page}/{total_pages}: {len(items)} products")

        if current_page >= total_pages:
            break
        current_page += 1
        time.sleep(REQUEST_DELAY)

    print(f"  Total: {len(all_items)} products fetched")
    return all_items


def build_category_product_map(products):
    """Build category_url_key → set of SKUs mapping."""
    cat_skus = {}  # url_key → set of SKUs
    for p in products:
        sku = p.get("sku", "")
        if not sku:
            continue
        for cat in p.get("categories", []):
            url_key = cat.get("url_key", "")
            if url_key:
                if url_key not in cat_skus:
                    cat_skus[url_key] = set()
                cat_skus[url_key].add(sku)
    return cat_skus


# Categories to skip (root-level or system categories)
SKIP_CATEGORIES = {"default-category", "root", "root-catalog"}


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Create collections and link products")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--link-only", action="store_true", help="Skip collection creation, only link products")
    parser.add_argument("--site", default="https://taraformula.com", help="Magento site URL")
    parser.add_argument("--store", default="sa-en", help="Store code (default: sa-en)")
    args = parser.parse_args()

    shop_url = config.get_dest_shop_url()
    access_token = config.get_dest_access_token()
    if not shop_url or not access_token:
        print("ERROR: DEST_SHOP_URL and DEST_ACCESS_TOKEN must be set in .env")
        return

    client = ShopifyClient(shop_url, access_token)
    progress_file = "data/collection_progress.json"
    progress = load_json(progress_file) if os.path.exists(progress_file) else {}
    if not isinstance(progress, dict):
        progress = {}

    # =========================================
    # Phase 1: Fetch data from Magento
    # =========================================
    print("\n=== Phase 1: Fetch Magento Data ===")

    categories = fetch_categories(args.site, args.store)
    products = fetch_products_with_categories(args.site, args.store)
    cat_skus = build_category_product_map(products)

    # Save for reference
    save_json(categories, "data/magento_categories.json")
    save_json({k: list(v) for k, v in cat_skus.items()}, "data/category_product_skus.json")

    # =========================================
    # Phase 2: Create collections in Shopify
    # =========================================
    print("\n=== Phase 2: Create Collections ===")

    collection_map = progress.get("collections", {})  # url_key → shopify_collection_id

    if args.link_only:
        print("  --link-only: skipping collection creation")
    else:
        # Get existing collections to avoid duplicates
        existing = client.get_collections()
        existing_by_handle = {c["handle"]: c for c in existing}
        print(f"  {len(existing)} collections already exist in Shopify")

        for cat in categories:
            url_key = cat.get("url_key", "")
            name = cat.get("name", "")
            if not url_key or url_key in SKIP_CATEGORIES:
                continue

            # Already created?
            if url_key in collection_map:
                print(f"  SKIP (already created): {name} [{url_key}]")
                continue

            # Already exists by handle?
            if url_key in existing_by_handle:
                coll = existing_by_handle[url_key]
                collection_map[url_key] = coll["id"]
                print(f"  EXISTS: {name} [{url_key}] → {coll['id']}")
                continue

            collection_data = {
                "title": name,
                "handle": url_key,
                "body_html": cat.get("description", "") or "",
                "published": True,
            }

            if cat.get("image"):
                collection_data["image"] = {"src": cat["image"]}

            if cat.get("meta_title"):
                collection_data["metafields"] = collection_data.get("metafields", [])
                collection_data["metafields"].append({
                    "namespace": "global",
                    "key": "title_tag",
                    "value": cat["meta_title"],
                    "type": "single_line_text_field",
                })
            if cat.get("meta_description"):
                collection_data["metafields"] = collection_data.get("metafields", [])
                collection_data["metafields"].append({
                    "namespace": "global",
                    "key": "description_tag",
                    "value": cat["meta_description"],
                    "type": "single_line_text_field",
                })

            if args.dry_run:
                sku_count = len(cat_skus.get(url_key, set()))
                print(f"  WOULD CREATE: {name} [{url_key}] ({sku_count} products)")
                continue

            try:
                created = client.create_custom_collection(collection_data)
                coll_id = created.get("id")
                collection_map[url_key] = coll_id
                print(f"  CREATED: {name} [{url_key}] → {coll_id}")
                progress["collections"] = collection_map
                save_json(progress, progress_file)
                time.sleep(0.5)
            except Exception as e:
                print(f"  ERROR creating {name}: {e}")

    # =========================================
    # Phase 3: Link products to collections
    # =========================================
    print("\n=== Phase 3: Link Products to Collections ===")

    # Build SKU → Shopify product ID map from existing products
    shopify_products = client.get_products()
    sku_to_product_id = {}
    for p in shopify_products:
        for v in p.get("variants", []):
            sku = v.get("sku", "")
            if sku:
                sku_to_product_id[sku] = p["id"]

    print(f"  {len(sku_to_product_id)} SKUs mapped to Shopify products")
    print(f"  {len(collection_map)} collections available")

    linked = progress.get("linked", {})  # "product_id_collection_id" → True
    created_count = 0
    skipped_count = 0
    error_count = 0

    for url_key, skus in cat_skus.items():
        coll_id = collection_map.get(url_key)
        if not coll_id:
            continue

        for sku in skus:
            product_id = sku_to_product_id.get(sku)
            if not product_id:
                continue

            link_key = f"{product_id}_{coll_id}"
            if link_key in linked:
                skipped_count += 1
                continue

            if args.dry_run:
                print(f"  WOULD LINK: {sku} (product {product_id}) → collection {coll_id} [{url_key}]")
                created_count += 1
                continue

            try:
                client.create_collect(product_id, coll_id)
                linked[link_key] = True
                created_count += 1
                if created_count % 20 == 0:
                    progress["linked"] = linked
                    save_json(progress, progress_file)
                    print(f"    ... linked {created_count} so far")
            except Exception as e:
                err_msg = str(e)
                if "already" in err_msg.lower() or "422" in err_msg:
                    linked[link_key] = True
                    skipped_count += 1
                else:
                    print(f"  ERROR linking {sku} → {url_key}: {e}")
                    error_count += 1

    progress["linked"] = linked
    save_json(progress, progress_file)

    print(f"\n  Linked: {created_count}, Skipped: {skipped_count}, Errors: {error_count}")
    print("Done!")


if __name__ == "__main__":
    main()
