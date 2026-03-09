#!/usr/bin/env python3
"""Fetch SAR prices from the Saudi store view and update product data + Shopify.

Fetches prices from taraformula.com with store code sa-en (Saudi Arabia),
updates the local product JSON files, and optionally updates already-imported
Shopify products.

Usage:
    # Fetch SAR prices and update local data files
    python fix_prices.py

    # Also update already-imported Shopify products
    python fix_prices.py --update-shopify

    # Use a different store code
    python fix_prices.py --store sa-en --site https://taraformula.com
"""

import argparse
import os
import time

import requests as http_requests
from dotenv import load_dotenv

from tara_migrate.core import AR_DIR, EN_DIR, REQUEST_DELAY, load_json, save_json
from tara_migrate.core import MAGENTO_HEADERS as HEADERS


def fetch_sar_prices(site_url, store_code, delay=REQUEST_DELAY):
    """Fetch all product prices from the Saudi Magento store view."""
    graphql_url = f"{site_url}/graphql"
    all_prices = {}  # sku → {final_price, regular_price, currency}
    current_page = 1
    page_size = 50

    print(f"Fetching prices from {site_url} (store: {store_code})...")

    while True:
        query = f"""
        {{
            products(search: "", pageSize: {page_size}, currentPage: {current_page}) {{
                total_count
                items {{
                    sku
                    name
                    price_range {{
                        minimum_price {{
                            regular_price {{ value currency }}
                            final_price {{ value currency }}
                        }}
                    }}
                }}
                page_info {{ current_page total_pages }}
            }}
        }}
        """

        headers = {**HEADERS, "Store": store_code}

        try:
            resp = http_requests.post(
                graphql_url,
                json={"query": query},
                headers=headers,
                timeout=30,
            )
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
        page_info = products_data.get("page_info", {})
        total_pages = page_info.get("total_pages", 1)
        total_count = products_data.get("total_count", 0)

        for item in items:
            sku = item.get("sku", "")
            if not sku:
                continue
            price_range = item.get("price_range", {})
            min_price = price_range.get("minimum_price", {})
            final = min_price.get("final_price", {})
            regular = min_price.get("regular_price", {})

            final_val = final.get("value")
            regular_val = regular.get("value")

            # Round to whole number (Magento returns conversion artifacts like 149.01)
            if final_val is not None:
                final_val = round(final_val)
            if regular_val is not None:
                regular_val = round(regular_val)

            all_prices[sku] = {
                "name": item.get("name", ""),
                "final_price": final_val,
                "regular_price": regular_val,
                "currency": final.get("currency", "SAR"),
            }

        print(f"  Page {current_page}/{total_pages}: {len(items)} products (total: {total_count})")

        if current_page >= total_pages:
            break
        current_page += 1
        time.sleep(delay)

    print(f"  Fetched prices for {len(all_prices)} SKUs")
    return all_prices


def update_product_files(prices, directories):
    """Update price fields in local product JSON files."""
    updated_count = 0

    for directory in directories:
        products_file = os.path.join(directory, "products.json")
        products = load_json(products_file)
        if not products:
            continue

        dir_updated = 0
        for product in products:
            for variant in product.get("variants", []):
                sku = variant.get("sku", "")
                if sku and sku in prices:
                    p = prices[sku]
                    old_price = variant.get("price", "0")
                    new_price = str(p["final_price"]) if p["final_price"] is not None else old_price

                    if old_price != new_price:
                        variant["price"] = new_price
                        if p["regular_price"] and p["final_price"] and p["regular_price"] != p["final_price"]:
                            variant["compare_at_price"] = str(p["regular_price"])
                        dir_updated += 1
                        print(f"  {product.get('title', '')[:40]}: {old_price} → {new_price} {p['currency']}")

        if dir_updated > 0:
            save_json(products, products_file)
            print(f"  Updated {dir_updated} prices in {products_file}")
            updated_count += dir_updated

    return updated_count


def update_shopify_products(prices):
    """Update prices on already-imported Shopify products."""
    from tara_migrate.client import ShopifyClient

    shop_url = os.environ.get("SAUDI_SHOP_URL")
    access_token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not access_token:
        print("ERROR: SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN must be set in .env")
        return

    client = ShopifyClient(shop_url, access_token)
    id_map = load_json("data/id_map.json", default={})
    product_map = id_map.get("products", {})

    if not product_map:
        print("No id_map.json found — run import_english.py first")
        return

    # Load English products to get SKU→source_id mapping
    en_products = load_json(os.path.join(EN_DIR, "products.json"))
    sku_to_source = {}
    for p in en_products:
        source_id = str(p.get("id", ""))
        for v in p.get("variants", []):
            if v.get("sku"):
                sku_to_source[v["sku"]] = source_id

    updated = 0
    for sku, price_data in prices.items():
        source_id = sku_to_source.get(sku)
        if not source_id:
            continue
        dest_id = product_map.get(source_id)
        if not dest_id:
            continue

        new_price = str(price_data["final_price"]) if price_data["final_price"] is not None else None
        if not new_price:
            continue

        compare_price = None
        if price_data["regular_price"] and price_data["final_price"] and price_data["regular_price"] != price_data["final_price"]:
            compare_price = str(price_data["regular_price"])

        try:
            # Fetch the Shopify product to get variant IDs
            product = client._get_json(f"products/{dest_id}.json")[0].get("product", {})
            variants = product.get("variants", [])
            for v in variants:
                update_data = {"price": new_price}
                if compare_price:
                    update_data["compare_at_price"] = compare_price
                client._request("PUT", f"variants/{v['id']}.json", json={"variant": update_data})
                print(f"  {price_data['name'][:40]}: → {new_price} {price_data['currency']} (variant {v['id']})")
                updated += 1
        except Exception as e:
            print(f"  Error updating {price_data['name'][:30]}: {e}")

    print(f"\nUpdated {updated} Shopify variant prices")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Fetch SAR prices and update products")
    parser.add_argument("--site", default="https://taraformula.com",
                        help="Magento site URL (default: https://taraformula.com)")
    parser.add_argument("--store", default="sa-en",
                        help="Store code for SAR prices (default: sa-en)")
    parser.add_argument("--update-shopify", action="store_true",
                        help="Also update already-imported Shopify products")
    parser.add_argument("--delay", type=float, default=REQUEST_DELAY,
                        help=f"Delay between requests (default: {REQUEST_DELAY})")
    args = parser.parse_args()

    # 1. Fetch SAR prices
    prices = fetch_sar_prices(args.site, args.store, args.delay)

    if not prices:
        print("No prices fetched. Check site URL and store code.")
        return

    # Save prices for reference
    save_json(prices, "data/sar_prices.json")
    print("\nSaved prices to data/sar_prices.json")

    # Show sample
    print("\nSample prices:")
    for sku, p in list(prices.items())[:5]:
        print(f"  {sku}: {p['final_price']} {p['currency']} ({p['name'][:40]})")

    # 2. Update local product JSON files
    print("\nUpdating local product files...")
    updated = update_product_files(prices, [EN_DIR, AR_DIR])
    print(f"Total local updates: {updated}")

    # 3. Optionally update Shopify
    if args.update_shopify:
        print("\nUpdating Shopify products...")
        update_shopify_products(prices)

    print("\nDone!")


if __name__ == "__main__":
    main()
