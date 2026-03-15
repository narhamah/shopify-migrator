#!/usr/bin/env python3
"""Replace Spanish product images with English/Arabic images from Magento.

Fetches product media_gallery from Magento GraphQL for both English and Arabic
store views, then updates already-imported Shopify products with the correct
image URLs and alt text.

Usage:
    # Discover available store codes on both sites
    python fix_images.py --discover

    # Preview what would change (dry run)
    python fix_images.py --dry-run

    # Update images on Shopify products
    python fix_images.py

    # Use different Magento sites/store codes
    python fix_images.py --en-site https://taraformula.com --en-store sa-en --ar-site https://taraformula.ae --ar-store sa-ar

    # Only update local data files (don't touch Shopify)
    python fix_images.py --local-only
"""

import argparse
import json
import os
import time

import requests as http_requests
from dotenv import load_dotenv

from tara_migrate.core import AR_DIR, EN_DIR, REQUEST_DELAY, load_json, save_json
from tara_migrate.core import MAGENTO_HEADERS as HEADERS
from tara_migrate.core import config


def magento_gql(session, site_url, query, store_code, retries=3):
    """Execute a Magento GraphQL query with retry."""
    url = f"{site_url}/graphql"
    headers = {}
    if store_code:
        headers["Store"] = store_code

    for attempt in range(retries):
        time.sleep(REQUEST_DELAY)
        try:
            resp = session.post(url, json={"query": query}, headers=headers, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                if "errors" in data:
                    print(f"    GraphQL errors: {data['errors'][0].get('message', '')}")
                return data
            elif resp.status_code == 503:
                wait = REQUEST_DELAY * (attempt + 2)
                print(f"    503 rate limited, waiting {wait}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
                continue
            else:
                print(f"    GraphQL HTTP {resp.status_code}")
                return None
        except Exception as e:
            print(f"    GraphQL error: {e}")
            if attempt < retries - 1:
                time.sleep(REQUEST_DELAY * 2)
    return None


def fetch_all_product_images(session, site_url, store_code):
    """Fetch SKU → images mapping from Magento."""
    all_images = {}  # sku → [{"url": ..., "label": ..., "position": ...}]
    current_page = 1
    page_size = 50

    print(f"Fetching images from {site_url} (store: {store_code})...")

    while True:
        query = f"""
        {{
            products(search: "", pageSize: {page_size}, currentPage: {current_page}) {{
                total_count
                items {{
                    sku
                    name
                    media_gallery {{
                        url
                        label
                        position
                    }}
                }}
                page_info {{
                    total_pages
                    current_page
                }}
            }}
        }}
        """
        result = magento_gql(session, site_url, query, store_code)
        if not result or "data" not in result:
            print(f"    Failed on page {current_page}, stopping")
            break

        products = result["data"].get("products", {})
        items = products.get("items", [])
        page_info = products.get("page_info", {})
        total_pages = page_info.get("total_pages", 1)
        total_count = products.get("total_count", 0)

        for item in items:
            sku = item.get("sku", "")
            if not sku:
                continue
            media = item.get("media_gallery", [])
            if media:
                media_sorted = sorted(media, key=lambda m: m.get("position", 0))
                all_images[sku] = {
                    "name": item.get("name", ""),
                    "images": [
                        {"url": m["url"], "label": m.get("label", ""), "position": m.get("position", 0)}
                        for m in media_sorted if m.get("url")
                    ],
                }

        print(f"  Page {current_page}/{total_pages}: {len(items)} products (total: {total_count})")

        if current_page >= total_pages:
            break
        current_page += 1

    print(f"  Fetched images for {len(all_images)} SKUs")
    return all_images


def update_local_product_files(en_images, ar_images):
    """Update image URLs in local product JSON files."""
    updated = 0
    for label, directory, images in [("English", EN_DIR, en_images), ("Arabic", AR_DIR, ar_images)]:
        products_file = os.path.join(directory, "products.json")
        products = load_json(products_file)
        if not products:
            print(f"  {label}: no products in {products_file}")
            continue

        dir_updated = 0
        for product in products:
            sku = None
            for v in product.get("variants", []):
                if v.get("sku"):
                    sku = v["sku"]
                    break
            if not sku or sku not in images:
                continue

            img_data = images[sku]
            new_images = [
                {"src": img["url"], "alt": img.get("label", "")}
                for img in img_data["images"]
            ]
            if new_images and new_images != product.get("images", []):
                product["images"] = new_images
                dir_updated += 1

        if dir_updated > 0:
            save_json(products, products_file)
            print(f"  {label}: updated images for {dir_updated} products")
            updated += dir_updated
        else:
            print(f"  {label}: no image changes needed")

    return updated


def _get_shopify_client_and_maps():
    """Shared setup: Shopify client, id_map, SKU→source mapping."""
    from tara_migrate.client import ShopifyClient

    shop_url = config.get_dest_shop_url()
    access_token = config.get_dest_access_token()
    if not shop_url or not access_token:
        print("ERROR: DEST_SHOP_URL and DEST_ACCESS_TOKEN must be set in .env")
        return None, None, None

    client = ShopifyClient(shop_url, access_token)
    id_map = load_json("data/id_map.json", default={})
    product_map = id_map.get("products", {})

    if not product_map:
        print("No product mappings in id_map.json — run import_english.py first")
        return None, None, None

    # Build SKU → source_id from English products
    en_products = load_json(os.path.join(EN_DIR, "products.json"))
    sku_to_source = {}
    for p in en_products:
        source_id = str(p.get("id", ""))
        for v in p.get("variants", []):
            if v.get("sku"):
                sku_to_source[v["sku"]] = source_id

    # If no local products, try to build mapping from id_map + Shopify
    if not sku_to_source:
        print("  No local product data — fetching SKUs from Shopify...")
        all_products = client.get_products()
        dest_to_source = {str(v): str(k) for k, v in product_map.items()}
        for sp in all_products:
            dest_id = str(sp.get("id", ""))
            source_id = dest_to_source.get(dest_id)
            if source_id:
                for v in sp.get("variants", []):
                    if v.get("sku"):
                        sku_to_source[v["sku"]] = source_id

    return client, product_map, sku_to_source


def update_shopify_images(en_images, ar_images, dry_run=False):
    """Replace product images on Shopify with Magento English images,
    then store Arabic image URLs in a product metafield.

    Since Shopify doesn't support per-locale product images, we:
    1. Set English images as the default product images
    2. Store Arabic image URLs in custom.arabic_images metafield (JSON)
       so the theme can swap them when locale is Arabic
    """
    client, product_map, sku_to_source = _get_shopify_client_and_maps()
    if not client:
        return

    print("\nUpdating Shopify product images...")
    print(f"  SKU→source mappings: {len(sku_to_source)}")
    print(f"  English images: {len(en_images)} SKUs")
    print(f"  Arabic images: {len(ar_images)} SKUs")

    updated = 0
    errors = 0

    for sku, img_data in en_images.items():
        source_id = sku_to_source.get(sku)
        if not source_id:
            continue
        dest_id = product_map.get(source_id)
        if not dest_id:
            continue

        name = img_data["name"]
        new_images = [
            {"src": img["url"], "alt": img.get("label", "")}
            for img in img_data["images"]
        ]

        if not new_images:
            continue

        label = f"  {name[:40]} (sku: {sku})"

        if dry_run:
            ar_count = len(ar_images.get(sku, {}).get("images", []))
            print(f"{label} — would set {len(new_images)} EN images"
                  + (f" + {ar_count} AR images" if ar_count else ""))
            updated += 1
            continue

        try:
            # Get current images via REST
            resp = client._request("GET", f"products/{dest_id}.json", params={"fields": "id,images"})
            current_images = resp.json().get("product", {}).get("images", [])

            # Delete existing images
            for img in current_images:
                try:
                    client._request("DELETE", f"products/{dest_id}/images/{img['id']}.json")
                except Exception:
                    pass

            # Add new English images from Magento
            for i, img in enumerate(new_images):
                img_payload = {
                    "image": {
                        "src": img["src"],
                        "alt": img.get("alt", ""),
                        "position": i + 1,
                    }
                }
                client._request("POST", f"products/{dest_id}/images.json", json=img_payload)
                time.sleep(0.3)

            msg = f"{label} — set {len(new_images)} EN images"

            # Store Arabic images in a metafield
            ar_img_data = ar_images.get(sku)
            if ar_img_data and ar_img_data.get("images"):
                ar_urls = [
                    {"src": img["url"], "alt": img.get("label", "")}
                    for img in ar_img_data["images"]
                ]
                try:
                    client.set_metafields([{
                        "ownerId": f"gid://shopify/Product/{dest_id}",
                        "namespace": "custom",
                        "key": "arabic_images",
                        "value": json.dumps(ar_urls, ensure_ascii=False),
                        "type": "json",
                    }])
                    msg += f" + {len(ar_urls)} AR images (metafield)"
                except Exception as e:
                    msg += f" (AR metafield error: {e})"

            print(msg)
            updated += 1

        except Exception as e:
            err_msg = str(e)
            if hasattr(e, 'response') and e.response is not None:
                try:
                    err_msg = json.dumps(e.response.json(), indent=2)
                except Exception:
                    pass
            print(f"{label} — ERROR: {err_msg}")
            errors += 1

        time.sleep(0.5)

    print("\n--- Image Update Summary ---")
    print(f"  Updated: {updated}")
    print(f"  Errors:  {errors}")
    if ar_images:
        print("  Arabic images stored in metafield: custom.arabic_images")
        print("  Theme should check locale and use metafield URLs for Arabic")


def discover_store_codes(session, site_url):
    """List all available store views on the Magento site."""
    print(f"Discovering store views on {site_url}...")
    query = "{ availableStores { store_code store_name locale default_display_currency_code } }"
    result = magento_gql(session, site_url, query, store_code=None)
    if result and "data" in result:
        stores = result["data"].get("availableStores", [])
        print(f"\nAvailable store views ({len(stores)}):")
        for s in stores:
            print(f"  {s.get('store_code'):15s} {s.get('store_name'):30s} "
                  f"locale={s.get('locale'):8s} currency={s.get('default_display_currency_code')}")
        return stores
    else:
        print("  Failed to fetch store views")
        return []


def compare_images(session, site_url, en_store, ar_store):
    """Fetch a few products from both stores and compare image URLs."""
    query = """
    {
        products(search: "", pageSize: 5, currentPage: 1) {
            items {
                sku
                name
                media_gallery { url label position }
            }
        }
    }
    """
    print(f"\n--- Comparing images: {en_store} vs {ar_store} ---")
    en_result = magento_gql(session, site_url, query, en_store)
    ar_result = magento_gql(session, site_url, query, ar_store)

    en_items = en_result.get("data", {}).get("products", {}).get("items", []) if en_result else []
    ar_items = ar_result.get("data", {}).get("products", {}).get("items", []) if ar_result else []

    # Index by SKU
    en_by_sku = {item["sku"]: item for item in en_items if item.get("sku")}
    ar_by_sku = {item["sku"]: item for item in ar_items if item.get("sku")}

    for sku in en_by_sku:
        en = en_by_sku[sku]
        ar = ar_by_sku.get(sku)
        print(f"\n  SKU: {sku}")
        print(f"    EN name: {en.get('name', '')}")
        en_urls = [m.get("url", "") for m in en.get("media_gallery", [])]
        print(f"    EN images ({len(en_urls)}):")
        for u in en_urls:
            print(f"      {u}")
        if ar:
            print(f"    AR name: {ar.get('name', '')}")
            ar_urls = [m.get("url", "") for m in ar.get("media_gallery", [])]
            print(f"    AR images ({len(ar_urls)}):")
            for u in ar_urls:
                print(f"      {u}")
            if en_urls == ar_urls:
                print("    >>> SAME image URLs")
            else:
                print("    >>> DIFFERENT image URLs")
        else:
            print("    AR: not found")


def main():
    parser = argparse.ArgumentParser(description="Replace source images with Magento EN/AR images")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without modifying Shopify")
    parser.add_argument("--local-only", action="store_true", help="Only update local data files")
    parser.add_argument("--discover", action="store_true", help="List available Magento store views and compare images")
    parser.add_argument("--en-site", default=None,
                        help="English Magento site URL (default: MAGENTO_SITE_URL env)")
    parser.add_argument("--ar-site", default=None,
                        help="Arabic Magento site URL (default: MAGENTO_AR_SITE_URL env or taraformula.ae)")
    parser.add_argument("--en-store", default=None,
                        help="English store code (default: MAGENTO_STORE_CODE env)")
    parser.add_argument("--ar-store", default="sa-ar",
                        help="Arabic store code (default: sa-ar)")
    args = parser.parse_args()

    # Resolve Magento settings from config
    if not args.en_site:
        args.en_site = config.get_magento_site_url()
    if not args.en_store:
        args.en_store = config.get_magento_store_code()

    load_dotenv()
    session = http_requests.Session()
    session.headers.update(HEADERS)

    if args.discover:
        print("=== English site ===")
        discover_store_codes(session, args.en_site)
        if args.ar_site != args.en_site:
            print("\n=== Arabic site ===")
            discover_store_codes(session, args.ar_site)
        compare_images(session, args.en_site, args.en_store, args.ar_store)
        return

    # Fetch English images
    en_images = fetch_all_product_images(session, args.en_site, args.en_store)
    save_json(en_images, "data/en_images.json")
    print("Saved English images to data/en_images.json")

    # Fetch Arabic images
    ar_images = fetch_all_product_images(session, args.ar_site, args.ar_store)
    save_json(ar_images, "data/ar_images.json")
    print("Saved Arabic images to data/ar_images.json")

    # Show sample + comparison
    print("\nSample images (EN vs AR):")
    for sku in list(en_images.keys())[:3]:
        en = en_images[sku]
        ar = ar_images.get(sku, {})
        print(f"  {sku}: {en['name']}")
        en_urls = [img["url"] for img in en["images"]]
        ar_urls = [img["url"] for img in ar.get("images", [])]
        print(f"    EN: {len(en_urls)} images")
        print(f"    AR: {len(ar_urls)} images")
        if en_urls == ar_urls:
            print("    (same URLs)")
        else:
            print("    (DIFFERENT URLs)")

    # Update local product files
    print("\nUpdating local product files...")
    update_local_product_files(en_images, ar_images)

    # Update Shopify
    if not args.local_only:
        print(f"\n{'DRY RUN: ' if args.dry_run else ''}Updating Shopify products...")
        update_shopify_images(en_images, ar_images, dry_run=args.dry_run)
    else:
        print("\n--local-only: skipping Shopify update")

    print("\nDone!")


if __name__ == "__main__":
    main()
