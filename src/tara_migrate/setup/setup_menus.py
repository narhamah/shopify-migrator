#!/usr/bin/env python3
"""Create navigation menus in the Saudi Shopify store.

Builds the main menu and footer menu by reading the Magento category tree
(for hierarchy) and matching to existing Shopify collections. Also adds
pages to the footer menu.

Usage:
    # Dry run — show what menus would look like
    python setup_menus.py --dry-run

    # Create/replace menus
    python setup_menus.py

    # Use a custom menu config instead of auto-detecting from Magento
    python setup_menus.py --config menus.json
"""

import argparse
import os
import time

import requests as http_requests
from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import MAGENTO_HEADERS as HEADERS
from tara_migrate.core import load_json, save_json

# Categories to skip in menu (root/system categories)
SKIP_CATEGORIES = {"default-category", "root", "root-catalog"}


def fetch_category_tree(site_url, store_code):
    """Fetch the full category tree from Magento (preserving hierarchy)."""
    graphql_url = f"{site_url}/graphql"
    headers = {**HEADERS, "Store": store_code}

    query = """
    {
        categories(filters: {}) {
            items {
                id
                name
                url_key
                product_count
                children {
                    id name url_key product_count
                    children {
                        id name url_key product_count
                    }
                }
            }
        }
    }
    """

    print(f"Fetching category tree from {site_url} (store: {store_code})...")
    try:
        resp = http_requests.post(graphql_url, json={"query": query}, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  Error: {e}")
        return []

    if "errors" in data:
        print(f"  GraphQL errors: {data['errors']}")
        return []

    items = data.get("data", {}).get("categories", {}).get("items", [])
    return items


def build_menu_from_categories(categories, collection_lookup):
    """Build a nested menu structure from Magento category tree.

    Returns list of menu items with nested children.
    """
    menu_items = []

    for cat in categories:
        url_key = cat.get("url_key", "")
        name = cat.get("name", "")
        children = cat.get("children") or []

        if not url_key or url_key in SKIP_CATEGORIES or not name:
            # If this is a root node, process its children as top-level
            if children:
                menu_items.extend(build_menu_from_categories(children, collection_lookup))
            continue

        # Look up Shopify collection
        coll = collection_lookup.get(url_key)
        if not coll and cat.get("product_count", 0) == 0 and not children:
            continue

        item = {"title": name}
        if coll:
            item["resourceId"] = f"gid://shopify/Collection/{coll['id']}"
        else:
            # No matching collection — link to /collections/handle
            item["url"] = f"/collections/{url_key}"

        # Process children
        if children:
            child_items = build_menu_from_categories(children, collection_lookup)
            if child_items:
                item["items"] = child_items

        menu_items.append(item)

    return menu_items


def print_menu_tree(items, indent=0):
    """Pretty-print a menu tree."""
    prefix = "  " * indent
    for item in items:
        target = item.get("resourceId", item.get("url", "(no link)"))
        print(f"{prefix}  - {item['title']}  →  {target}")
        if item.get("items"):
            print_menu_tree(item["items"], indent + 1)


def apply_config(client, config_file, dry_run=False):
    """Create menus from a JSON config file.

    Config format:
    {
        "main-menu": {
            "title": "Main Menu",
            "items": [
                {"title": "Shop All", "url": "/collections/all"},
                {"title": "Hair Care", "resourceId": "gid://shopify/Collection/123",
                 "items": [
                    {"title": "Shampoos", "resourceId": "gid://shopify/Collection/456"}
                 ]
                }
            ]
        },
        "footer": {
            "title": "Footer Menu",
            "items": [
                {"title": "About Us", "url": "/pages/about"},
                {"title": "Contact", "url": "/pages/contact"}
            ]
        }
    }
    """
    config = load_json(config_file)
    if not config:
        print(f"ERROR: Empty or missing config: {config_file}")
        return

    for handle, menu_data in config.items():
        title = menu_data.get("title", handle)
        items = menu_data.get("items", [])
        print(f"\nMenu: {title} ({handle}) — {len(items)} items")
        print_menu_tree(items)

        if dry_run:
            continue

        _create_or_replace_menu(client, title, handle, items)


def _create_or_replace_menu(client, title, handle, items):
    """Create a menu, replacing it if it already exists."""
    # Check if menu already exists
    try:
        existing_menus = client.get_menus()
    except Exception as e:
        if "ACCESS_DENIED" in str(e):
            print("  NOTE: Cannot read existing menus (missing read_online_store_navigation scope)")
            print("  Attempting to create menu directly...")
            existing_menus = []
        else:
            raise
    for menu in existing_menus:
        if menu["handle"] == handle:
            print(f"  Deleting existing menu: {menu['title']} ({menu['id']})")
            try:
                client.delete_menu(menu["id"])
                time.sleep(0.5)
            except Exception as e:
                print(f"  Error deleting menu: {e}")
                return

    try:
        result = client.create_menu(title, handle, items)
        if result:
            print(f"  Created: {title} ({result['id']})")
        else:
            print(f"  Menu '{handle}' already exists")
    except Exception as e:
        print(f"  Error creating menu: {e}")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Create navigation menus in Shopify")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created")
    parser.add_argument("--config", type=str, help="JSON config file for menu structure")
    parser.add_argument("--site", default="https://taraformula.com", help="Magento site URL")
    parser.add_argument("--store", default="sa-en", help="Store code (default: sa-en)")
    parser.add_argument("--show", action="store_true", help="Show existing menus in Shopify")
    args = parser.parse_args()

    shop_url = os.environ.get("SAUDI_SHOP_URL")
    access_token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not access_token:
        print("ERROR: SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN must be set in .env")
        return

    client = ShopifyClient(shop_url, access_token)

    # Show existing menus
    if args.show:
        try:
            menus = client.get_menus()
        except Exception as e:
            if "ACCESS_DENIED" in str(e):
                print("ERROR: Missing 'read_online_store_navigation' scope. Cannot read menus.")
                print("  Add this scope in your Shopify app settings, or use --dry-run to preview.")
                return
            raise
        print(f"\nExisting menus ({len(menus)}):")
        for menu in menus:
            print(f"\n  {menu['title']} (handle: {menu['handle']}, id: {menu['id']})")
            print_menu_tree(menu.get("items", []))
        return

    # Use config file if provided
    if args.config:
        apply_config(client, args.config, dry_run=args.dry_run)
        return

    # Auto-build menus from Magento categories + Shopify collections
    print("=== Building Menus from Magento Categories ===\n")

    # 1. Fetch category tree from Magento
    category_tree = fetch_category_tree(args.site, args.store)
    if not category_tree:
        print("No categories found. Use --config to supply menu structure manually.")
        return

    # 2. Get existing Shopify collections for linking
    print("\nFetching Shopify collections...")
    collections = client.get_collections()
    collection_lookup = {c["handle"]: c for c in collections}
    print(f"  Found {len(collections)} collections")

    # 3. Build main menu from category tree
    main_items = build_menu_from_categories(category_tree, collection_lookup)

    # Add "Home" as first item
    main_items.insert(0, {"title": "Home", "url": "/"})

    print(f"\n--- Main Menu ({len(main_items)} items) ---")
    print_menu_tree(main_items)

    # 4. Build footer menu from pages
    print("\nFetching Shopify pages...")
    pages = client.get_pages()
    print(f"  Found {len(pages)} pages")

    footer_items = []
    for page in pages:
        if page.get("published_at"):
            footer_items.append({
                "title": page.get("title", ""),
                "url": f"/pages/{page.get('handle', '')}",
            })

    # Add standard footer links
    footer_items.append({"title": "Contact", "url": "/pages/contact"})

    print(f"\n--- Footer Menu ({len(footer_items)} items) ---")
    print_menu_tree(footer_items)

    # 5. Save menu config for reference
    menu_config = {
        "main-menu": {"title": "Main Menu", "items": main_items},
        "footer": {"title": "Footer Menu", "items": footer_items},
    }
    save_json(menu_config, "data/menu_config.json")
    print("\nSaved menu config to data/menu_config.json")

    if args.dry_run:
        print("\nDRY RUN — no menus created. Review data/menu_config.json and run without --dry-run.")
        return

    # 6. Create menus
    print("\n=== Creating Menus ===")
    _create_or_replace_menu(client, "Main Menu", "main-menu", main_items)
    _create_or_replace_menu(client, "Footer Menu", "footer", footer_items)

    print("\nDone! Check your store's navigation in Shopify admin → Online Store → Navigation")


if __name__ == "__main__":
    main()
