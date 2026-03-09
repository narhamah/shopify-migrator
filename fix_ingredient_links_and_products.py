#!/usr/bin/env python3
"""Diagnose and fix two issues on the Saudi store:

1. Ingredient cards on /pages/ingredients are not clickable (no links)
2. Homepage "Our Best Products" section is empty (no collection assigned)

Usage:
    python fix_ingredient_links_and_products.py --inspect     # Diagnose only
    python fix_ingredient_links_and_products.py --fix         # Apply fixes
"""

import argparse
import json
import os
import time

from dotenv import load_dotenv
from shopify_client import ShopifyClient


def inspect_ingredients_page(client, theme_id):
    """Read and display the ingredients page template structure."""
    print("\n" + "=" * 60)
    print("INGREDIENTS PAGE TEMPLATE")
    print("=" * 60)

    try:
        asset = client.get_asset(theme_id, "templates/page.ingredients.json")
        template = json.loads(asset.get("value", "{}"))
        print(json.dumps(template, indent=2, ensure_ascii=False))
        return template
    except Exception as e:
        print(f"  ERROR: Could not read templates/page.ingredients.json: {e}")
        return None


def inspect_ingredient_sections(client, theme_id, template):
    """Read sections used in the ingredients page to understand card rendering."""
    if not template:
        return

    sections = template.get("sections", {})
    for section_id, section in sections.items():
        section_type = section.get("type", "unknown")
        print(f"\n--- Section: {section_id} (type: {section_type}) ---")
        print(f"  Settings: {json.dumps(section.get('settings', {}), indent=4, ensure_ascii=False)[:500]}")

        # Try to read the section's Liquid source
        try:
            section_asset = client.get_asset(theme_id, f"sections/{section_type}.liquid")
            liquid_source = section_asset.get("value", "")
            print(f"  Liquid source length: {len(liquid_source)} chars")

            # Check if the section has links for metaobjects
            if "url" in liquid_source.lower() or "href" in liquid_source.lower():
                # Find lines with url/href
                for i, line in enumerate(liquid_source.split("\n")):
                    if "url" in line.lower() or "href" in line.lower():
                        print(f"    Line {i+1}: {line.strip()[:120]}")
            else:
                print("  WARNING: No URL/href found in section source!")

            # Check for metaobject references
            if "metaobject" in liquid_source.lower() or "ingredient" in liquid_source.lower():
                for i, line in enumerate(liquid_source.split("\n")):
                    if "metaobject" in line.lower() or "ingredient" in line.lower():
                        print(f"    Line {i+1}: {line.strip()[:120]}")

        except Exception as e:
            print(f"  Could not read section source: {e}")


def inspect_metaobject_template(client, theme_id):
    """Check the metaobject/ingredient template."""
    print("\n" + "=" * 60)
    print("METAOBJECT/INGREDIENT TEMPLATE")
    print("=" * 60)

    try:
        asset = client.get_asset(theme_id, "templates/metaobject/ingredient.json")
        template = json.loads(asset.get("value", "{}"))
        print(json.dumps(template, indent=2, ensure_ascii=False))
        return template
    except Exception as e:
        print(f"  Could not read: {e}")
        return None


def inspect_homepage_products(client, theme_id):
    """Read homepage template and find the products section."""
    print("\n" + "=" * 60)
    print("HOMEPAGE TEMPLATE - PRODUCT SECTIONS")
    print("=" * 60)

    try:
        asset = client.get_asset(theme_id, "templates/index.json")
        template = json.loads(asset.get("value", "{}"))
    except Exception as e:
        print(f"  ERROR: Could not read templates/index.json: {e}")
        return None, None

    sections = template.get("sections", {})
    order = template.get("order", [])

    print(f"  Section order: {order}")

    product_sections = {}
    for section_id in order:
        section = sections.get(section_id, {})
        section_type = section.get("type", "unknown")
        settings = section.get("settings", {})

        # Check for product/collection related sections
        is_product = any(kw in section_type.lower() for kw in
                        ["product", "collection", "featured", "best"])
        has_collection = any(kw in str(settings).lower() for kw in
                           ["collection", "product"])

        if is_product or has_collection:
            product_sections[section_id] = section
            print(f"\n  Section: {section_id}")
            print(f"    Type: {section_type}")
            print(f"    Settings: {json.dumps(settings, indent=6, ensure_ascii=False)[:500]}")

        # Also check for heading text that mentions products
        heading = settings.get("heading", "") or settings.get("title", "") or ""
        if "product" in heading.lower() or "best" in heading.lower():
            if section_id not in product_sections:
                product_sections[section_id] = section
                print(f"\n  Section: {section_id} (matched by heading)")
                print(f"    Type: {section_type}")
                print(f"    Settings: {json.dumps(settings, indent=6, ensure_ascii=False)[:500]}")

    if not product_sections:
        print("\n  No product-related sections found. Dumping all sections:")
        for section_id in order:
            section = sections.get(section_id, {})
            print(f"\n  {section_id}: type={section.get('type')}")
            settings = section.get('settings', {})
            if settings:
                heading = settings.get("heading", "") or settings.get("title", "")
                if heading:
                    print(f"    heading: {heading}")
                collection = settings.get("collection", "")
                if collection:
                    print(f"    collection: {collection}")

    return template, product_sections


def list_collections(client):
    """List all collections on the store."""
    print("\n--- Available Collections ---")
    collections = client.get_collections()
    for c in collections:
        print(f"  {c['handle']}: {c['title']} (id: {c['id']})")
    return collections


def check_ingredient_urls(client):
    """Check if ingredients have working URLs."""
    print("\n--- Checking ingredient metaobject URLs ---")
    ingredients = client.get_metaobjects("ingredient")
    print(f"  Found {len(ingredients)} ingredients")

    # Show a few sample URLs
    for ing in ingredients[:3]:
        handle = ing.get("handle", "")
        name_field = next((f for f in ing.get("fields", []) if f["key"] == "name"), None)
        name = name_field["value"] if name_field else handle
        print(f"  {name}: /pages/ingredient/{handle}")
        # Check if it has a displayPageUrl
        if "displayPageUrl" in ing:
            print(f"    displayPageUrl: {ing['displayPageUrl']}")


def list_all_theme_snippets(client, theme_id):
    """List all snippets that mention 'ingredient'."""
    print("\n--- Theme snippets mentioning 'ingredient' ---")
    assets = client.list_assets(theme_id)
    ingredient_assets = []
    for a in assets:
        key = a.get("key", "")
        if "ingredient" in key.lower():
            ingredient_assets.append(key)
            print(f"  {key}")

    # Also check snippet/card related files
    card_assets = []
    for a in assets:
        key = a.get("key", "")
        if "card" in key.lower() or "metaobject" in key.lower():
            card_assets.append(key)
            print(f"  {key}")

    return ingredient_assets, card_assets


def read_asset_source(client, theme_id, key):
    """Read and print a theme asset source."""
    try:
        asset = client.get_asset(theme_id, key)
        value = asset.get("value", "")
        print(f"\n--- {key} ({len(value)} chars) ---")
        print(value[:3000])
        if len(value) > 3000:
            print(f"  ... ({len(value) - 3000} more chars)")
        return value
    except Exception as e:
        print(f"  Could not read {key}: {e}")
        return None


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Fix ingredient links and homepage products")
    parser.add_argument("--inspect", action="store_true", help="Inspect and diagnose issues")
    parser.add_argument("--fix", action="store_true", help="Apply fixes")
    parser.add_argument("--list-collections", action="store_true", help="List all collections")
    args = parser.parse_args()

    shop_url = os.environ.get("SAUDI_SHOP_URL")
    access_token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not access_token:
        print("ERROR: SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN must be set in .env")
        return

    client = ShopifyClient(shop_url, access_token)
    theme_id = client.get_main_theme_id()
    if not theme_id:
        print("ERROR: No main theme found")
        return

    print(f"Store: {shop_url}")
    print(f"Theme ID: {theme_id}")

    if args.list_collections:
        list_collections(client)
        return

    if args.inspect or not args.fix:
        # Diagnose ingredient cards
        template = inspect_ingredients_page(client, theme_id)
        inspect_ingredient_sections(client, theme_id, template)
        inspect_metaobject_template(client, theme_id)
        check_ingredient_urls(client)

        # Check for ingredient-related theme assets
        ing_assets, card_assets = list_all_theme_snippets(client, theme_id)
        for key in ing_assets + card_assets:
            read_asset_source(client, theme_id, key)

        # Diagnose homepage products
        homepage_template, product_sections = inspect_homepage_products(client, theme_id)

        if args.inspect:
            list_collections(client)

    if args.fix:
        print("\n  Fix mode: Review --inspect output first to determine the right fix.")
        print("  Fixes will be implemented based on diagnostic findings.")


if __name__ == "__main__":
    main()
