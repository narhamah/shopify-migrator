#!/usr/bin/env python3
"""Fetch storefront prices from a Magento store view and update local data + Shopify.

Usage:
    # Fetch prices and update destination-scoped local product JSON files
    python fix_prices.py --site https://taraformula.com.kw --store kw-en

    # Also update already-imported Shopify products on the destination store
    python fix_prices.py --site https://taraformula.com.kw --store kw-en --update-shopify
"""

from __future__ import annotations

import argparse
import os
import time
from decimal import Decimal, InvalidOperation

import requests as http_requests
from dotenv import load_dotenv

from tara_migrate.core import MAGENTO_HEADERS as HEADERS
from tara_migrate.core import REQUEST_DELAY, config, load_json, save_json

STORE_MANUAL_PRICE_OVERRIDES: dict[str, dict[str, str]] = {
    # Keep store-specific override support, but do not apply anything implicitly.
    "sa-en": {},
    "kw-en": {},
    "kw-ar": {},
}


def _normalize_decimal(value) -> Decimal | None:
    """Convert numeric values from Magento into a sane Decimal."""
    if value in (None, ""):
        return None
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None

    if decimal_value == decimal_value.to_integral():
        return decimal_value.quantize(Decimal("1"))

    # Shopify price fields support decimals; keep up to 3 places for Gulf currencies.
    return decimal_value.quantize(Decimal("0.001")).normalize()


def _decimal_to_price_string(value: Decimal | None) -> str | None:
    if value is None:
        return None

    scale = max(0, -value.as_tuple().exponent)
    if scale == 0:
        return format(value.quantize(Decimal("1")), "f")
    if scale <= 2:
        return format(value.quantize(Decimal("0.01")), "f")
    return format(value.quantize(Decimal("0.001")), "f").rstrip("0").rstrip(".")


def fetch_magento_prices(site_url, store_code, delay=REQUEST_DELAY, page_size=50):
    """Fetch all product prices from a Magento store view."""
    graphql_url = f"{site_url.rstrip('/')}/graphql"
    all_prices: dict[str, dict[str, str | None]] = {}
    current_page = 1

    print(f"Fetching prices from {site_url} (store: {store_code})...")

    while True:
        query = f"""
        {{
            products(filter: {{}}, pageSize: {page_size}, currentPage: {current_page}) {{
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
        except Exception as exc:
            print(f"  Error on page {current_page}: {exc}")
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
            sku = (item.get("sku") or "").strip()
            if not sku:
                continue

            price_range = item.get("price_range", {})
            min_price = price_range.get("minimum_price", {})
            final = min_price.get("final_price", {})
            regular = min_price.get("regular_price", {})

            final_decimal = _normalize_decimal(final.get("value"))
            regular_decimal = _normalize_decimal(regular.get("value"))

            all_prices[sku] = {
                "name": item.get("name", ""),
                "final_price": _decimal_to_price_string(final_decimal),
                "regular_price": _decimal_to_price_string(regular_decimal),
                "currency": final.get("currency") or regular.get("currency") or "",
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
                sku = (variant.get("sku") or "").strip()
                if not sku or sku not in prices:
                    continue

                price_data = prices[sku]
                new_price = price_data.get("final_price")
                regular_price = price_data.get("regular_price")
                old_price = str(variant.get("price", "0"))
                old_compare = variant.get("compare_at_price")
                old_compare = str(old_compare) if old_compare not in (None, "") else None

                if new_price and old_price != new_price:
                    variant["price"] = new_price
                    dir_updated += 1
                    print(f"  {product.get('title', '')[:40]}: {old_price} -> {new_price} {price_data.get('currency', '')}")

                if regular_price and new_price and regular_price != new_price:
                    if old_compare != regular_price:
                        variant["compare_at_price"] = regular_price
                        if old_price == new_price:
                            dir_updated += 1
                elif old_compare is not None:
                    variant["compare_at_price"] = None
                    if old_price == new_price:
                        dir_updated += 1

        if dir_updated > 0:
            save_json(products, products_file)
            print(f"  Updated {dir_updated} prices in {products_file}")
            updated_count += dir_updated

    return updated_count


def update_shopify_products(prices):
    """Update destination Shopify variant prices by SKU."""
    from tara_migrate.client import ShopifyClient

    shop_url = config.get_dest_shop_url()
    access_token = config.get_dest_access_token()
    if not shop_url or not access_token:
        print("ERROR: DEST_SHOP_URL and DEST_ACCESS_TOKEN must be set in .env")
        return 0

    client = ShopifyClient(shop_url, access_token)
    id_map = load_json(config.get_id_map_file(), default={})
    product_map = id_map.get("products", {})

    if not product_map:
        print("No id_map.json found - run import_english.py first")
        return 0

    updated = 0
    for source_id, dest_id in product_map.items():
        try:
            product = client._get_json(f"products/{dest_id}.json")[0].get("product", {})
        except Exception as exc:
            print(f"  Error loading destination product {dest_id}: {exc}")
            continue

        product_updated = 0
        for variant in product.get("variants", []):
            sku = (variant.get("sku") or "").strip()
            if not sku or sku not in prices:
                continue

            price_data = prices[sku]
            new_price = price_data.get("final_price")
            regular_price = price_data.get("regular_price")
            if not new_price:
                continue

            current_price = str(variant.get("price", ""))
            current_compare = variant.get("compare_at_price")
            current_compare = str(current_compare) if current_compare not in (None, "") else None

            update_data = {}
            if current_price != new_price:
                update_data["price"] = new_price

            if regular_price and regular_price != new_price:
                if current_compare != regular_price:
                    update_data["compare_at_price"] = regular_price
            elif current_compare is not None:
                update_data["compare_at_price"] = None

            if not update_data:
                continue

            try:
                client._request("PUT", f"variants/{variant['id']}.json", json={"variant": update_data})
                product_updated += 1
                updated += 1
                print(
                    f"  {price_data.get('name', '')[:40]}: -> {new_price} "
                    f"{price_data.get('currency', '')} (variant {variant['id']})"
                )
            except Exception as exc:
                print(f"  Error updating variant {variant.get('id')}: {exc}")

        if product_updated == 0 and source_id not in product_map:
            print(f"  No matched variants updated for product {dest_id}")

    print(f"\nUpdated {updated} Shopify variant prices")
    return updated


def update_shopify_by_handle(manual_prices, currency_hint=""):
    """Update destination Shopify product prices by handle for manual overrides."""
    from tara_migrate.client import ShopifyClient

    shop_url = config.get_dest_shop_url()
    access_token = config.get_dest_access_token()
    if not shop_url or not access_token:
        print("ERROR: DEST_SHOP_URL and DEST_ACCESS_TOKEN must be set in .env")
        return 0

    client = ShopifyClient(shop_url, access_token)
    updated = 0

    for handle, price in manual_prices.items():
        try:
            products = client._get_json(f"products.json?handle={handle}")[0].get("products", [])
            if not products:
                print(f"  {handle}: not found on Shopify - skipping")
                continue
            product = products[0]
            for variant in product.get("variants", []):
                client._request("PUT", f"variants/{variant['id']}.json", json={"variant": {"price": str(price)}})
                print(f"  {product['title'][:40]}: -> {price} {currency_hint} (variant {variant['id']})")
                updated += 1
        except Exception as exc:
            print(f"  Error updating {handle}: {exc}")

    return updated


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Fetch Magento prices and update products")
    parser.add_argument(
        "--site",
        default=None,
        help="Magento site URL (default: MAGENTO_SITE_URL env or https://taraformula.com)",
    )
    parser.add_argument(
        "--store",
        default=None,
        help="Magento store code (default: MAGENTO_STORE_CODE env or us-en)",
    )
    parser.add_argument(
        "--update-shopify",
        action="store_true",
        help="Also update already-imported Shopify products",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=REQUEST_DELAY,
        help=f"Delay between requests (default: {REQUEST_DELAY})",
    )
    args = parser.parse_args()

    site = args.site or config.get_magento_site_url()
    store = args.store or config.get_magento_store_code()

    prices = fetch_magento_prices(site, store, args.delay)
    if not prices:
        print("No prices fetched. Check site URL and store code.")
        return

    prices_file = config.get_progress_file("magento_prices.json")
    save_json(prices, prices_file)
    print(f"\nSaved prices to {prices_file}")

    print("\nSample prices:")
    for sku, price_data in list(prices.items())[:5]:
        print(f"  {sku}: {price_data['final_price']} {price_data['currency']} ({price_data['name'][:40]})")

    print("\nUpdating local product files...")
    updated = update_product_files(prices, [config.get_en_dir(), config.get_ar_dir()])
    print(f"Total local updates: {updated}")

    if args.update_shopify:
        print("\nUpdating Shopify products (from storefront source)...")
        update_shopify_products(prices)

        manual_prices = STORE_MANUAL_PRICE_OVERRIDES.get(store, {})
        if manual_prices:
            print(f"\nUpdating {len(manual_prices)} products with manual overrides...")
            manual_updated = update_shopify_by_handle(manual_prices)
            print(f"Updated {manual_updated} manual-price variants")

    print("\nDone!")


if __name__ == "__main__":
    main()
