#!/usr/bin/env python3
"""Verify destination store data completeness and correctness.

Checks all migrated data via the Shopify API and reports issues.

Usage:
    python verify_saudi.py
"""

import json
import os

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, load_json


def check_products(saudi, id_map):
    """Check all products have images, variants, prices, and metafields."""
    print("\n=== PRODUCTS ===")
    product_map = id_map.get("products", {})
    print(f"  ID map has {len(product_map)} product mappings")

    products = saudi.get_products()
    print(f"  destination store has {len(products)} products")

    issues = []
    for p in products:
        pid = p["id"]
        title = p.get("title", "")[:50]
        handle = p.get("handle", "")
        status = p.get("status", "unknown")
        images = p.get("images", [])
        variants = p.get("variants", [])

        if status != "active":
            issues.append(f"  NOT ACTIVE: '{title}' (status={status})")

        if not images:
            issues.append(f"  NO IMAGES: '{title}'")

        for v in variants:
            price = v.get("price", "0.00")
            if float(price) == 0:
                issues.append(f"  ZERO PRICE: '{title}' variant '{v.get('title', '')}'")
            if v.get("inventory_management") and v.get("inventory_quantity", 0) <= 0:
                issues.append(f"  NO INVENTORY: '{title}' variant '{v.get('title', '')}' (qty={v.get('inventory_quantity', 0)})")

        # Check product has English handle (not Spanish)
        spanish_indicators = ["champu", "acondicionador", "mascarilla", "serum-", "rutina-"]
        if any(ind in handle for ind in spanish_indicators):
            issues.append(f"  SPANISH HANDLE: '{title}' → /{handle}")

    if issues:
        print(f"  Found {len(issues)} product issues:")
        for issue in issues:
            print(issue)
    else:
        print("  All products OK")

    return products


def check_collections(saudi, id_map):
    """Check collections exist and have products."""
    print("\n=== COLLECTIONS ===")
    collection_map = id_map.get("collections", {})
    print(f"  ID map has {len(collection_map)} collection mappings")

    collections = saudi.get_collections()
    print(f"  destination store has {len(collections)} collections")

    issues = []
    for c in collections:
        title = c.get("title", "")[:50]
        handle = c.get("handle", "")
        # Check for image
        if not c.get("image"):
            issues.append(f"  NO IMAGE: collection '{title}'")

    if issues:
        print(f"  Found {len(issues)} collection issues:")
        for issue in issues:
            print(issue)
    else:
        if collections:
            print("  All collections OK")
        else:
            print("  WARNING: No collections found on destination store!")

    return collections


def check_pages(saudi):
    """Check pages exist."""
    print("\n=== PAGES ===")
    pages = saudi.get_pages()
    print(f"  destination store has {len(pages)} pages")
    for p in pages:
        print(f"    - {p.get('title', '')} (/{p.get('handle', '')})")
    if not pages:
        print("  WARNING: No pages found on destination store!")
    return pages


def check_blogs_articles(saudi):
    """Check blogs and articles."""
    print("\n=== BLOGS & ARTICLES ===")
    blogs = saudi.get_blogs()
    print(f"  destination store has {len(blogs)} blogs")
    total_articles = 0
    for b in blogs:
        articles = saudi.get_articles(b["id"])
        total_articles += len(articles)
        print(f"    - {b.get('title', '')} (/{b.get('handle', '')}): {len(articles)} articles")
    if not blogs:
        print("  WARNING: No blogs found on destination store!")
    return blogs, total_articles


def check_menus(saudi):
    """Check navigation menus."""
    print("\n=== NAVIGATION MENUS ===")
    try:
        menus = saudi.get_menus()
        print(f"  destination store has {len(menus)} menus")
        for m in menus:
            items = m.get("items", [])
            print(f"    - {m.get('title', '')} ({m.get('handle', '')}): {len(items)} items")
            for item in items:
                sub = item.get("items", [])
                suffix = f" ({len(sub)} sub-items)" if sub else ""
                print(f"      • {item.get('title', '')}{suffix}")
        if not menus:
            print("  WARNING: No navigation menus found!")
    except Exception as e:
        print(f"  Error fetching menus: {e}")


def check_metaobjects(saudi, id_map):
    """Check metaobject definitions and entries."""
    print("\n=== METAOBJECTS ===")

    defs = saudi.get_metaobject_definitions()
    print(f"  {len(defs)} definitions on destination store")

    issues = []
    for defn in defs:
        mo_type = defn.get("type", "")
        field_defs = defn.get("fieldDefinitions", [])

        entries = saudi.get_metaobjects(mo_type)
        file_fields = []
        for fd in field_defs:
            ft = fd.get("type", {})
            ft_name = ft.get("name", "") if isinstance(ft, dict) else str(ft)
            if "file_reference" in ft_name:
                file_fields.append(fd["key"])

        # Count file fields that are empty
        empty_files = 0
        total_files = 0
        for entry in entries:
            for f in entry.get("fields", []):
                if f["key"] in file_fields:
                    total_files += 1
                    if not f.get("value"):
                        empty_files += 1

        map_key = f"metaobjects_{mo_type}"
        mapped = len(id_map.get(map_key, {}))

        status = f"  {mo_type}: {len(entries)} entries, {mapped} mapped"
        if file_fields:
            status += f", files: {total_files - empty_files}/{total_files} populated"
        if empty_files > 0:
            issues.append(f"  MISSING FILES: {mo_type} has {empty_files}/{total_files} empty file fields")
        print(status)

    if issues:
        print("\n  File issues:")
        for issue in issues:
            print(issue)


def check_theme(saudi):
    """Check theme is installed and homepage has content."""
    print("\n=== THEME & HOMEPAGE ===")
    try:
        themes = saudi.get_themes()
        for t in themes:
            role = t.get("role", "")
            name = t.get("name", "")
            print(f"  Theme: {name} (role={role})")

        main_theme_id = saudi.get_main_theme_id()
        if not main_theme_id:
            print("  WARNING: No main theme found!")
            return

        # Check homepage template
        try:
            asset = saudi.get_asset(main_theme_id, "templates/index.json")
            template = json.loads(asset.get("value", "{}"))
            sections = template.get("sections", {})
            print(f"  Homepage sections: {len(sections)}")

            image_count = 0
            empty_image_count = 0
            for sid, section in sections.items():
                stype = section.get("type", "unknown")
                settings = section.get("settings", {})
                blocks = section.get("blocks", {})

                # Count images in settings
                for k, v in settings.items():
                    if isinstance(v, str) and v.startswith("shopify://shop_images/"):
                        image_count += 1

                # Count images in blocks
                for bid, block in blocks.items():
                    for k, v in block.get("settings", {}).items():
                        if isinstance(v, str) and v.startswith("shopify://shop_images/"):
                            image_count += 1

                print(f"    [{sid[:30]}] type={stype}, blocks={len(blocks)}")

            print(f"  Homepage images set: {image_count}")

        except Exception as e:
            print(f"  Error reading homepage template: {e}")
    except Exception as e:
        print(f"  Error fetching themes: {e}")


def check_locales(saudi):
    """Check locale configuration."""
    print("\n=== LOCALES ===")
    try:
        locales = saudi.get_locales()
        for loc in locales:
            primary = " (PRIMARY)" if loc.get("primary") else ""
            published = " [published]" if loc.get("published") else " [unpublished]"
            print(f"  {loc['locale']}{primary}{published}")
    except Exception as e:
        print(f"  Error: {e}")


def check_redirects(saudi):
    """Check URL redirects."""
    print("\n=== URL REDIRECTS ===")
    try:
        redirects = saudi.get_redirects()
        print(f"  {len(redirects)} redirects configured")
    except Exception as e:
        print(f"  Error: {e}")


def check_translations(saudi, id_map):
    """Spot-check Arabic translations on a few products."""
    print("\n=== ARABIC TRANSLATIONS (spot check) ===")
    product_map = id_map.get("products", {})

    # Check first 3 products
    checked = 0
    for source_gid, dest_id in list(product_map.items())[:3]:
        dest_gid = f"gid://shopify/Product/{dest_id}"
        try:
            resource = saudi.get_translatable_resource(dest_gid)
            if not resource:
                print(f"  Product {dest_id}: no translatable content found")
                continue

            content = resource.get("translatableContent", [])
            title_field = None
            for c in content:
                if c["key"] == "title":
                    title_field = c
                    break

            if title_field:
                print(f"  Product {dest_id}: title='{title_field.get('value', '')[:50]}'")
            checked += 1
        except Exception as e:
            print(f"  Error checking product {dest_id}: {e}")

    if checked == 0:
        print("  No products checked (no mappings)")


def check_seo(saudi, id_map):
    """Check SEO metafields on products."""
    print("\n=== SEO META TAGS ===")
    product_map = id_map.get("products", {})
    has_seo = 0
    missing_seo = 0

    for source_id, dest_id in product_map.items():
        try:
            mfs = saudi.get_metafields("products", dest_id)
            seo_fields = [mf for mf in mfs if mf.get("namespace") == "global" and mf.get("key") in ("title_tag", "description_tag")]
            if seo_fields:
                has_seo += 1
            else:
                missing_seo += 1
        except Exception:
            pass

    print(f"  Products with SEO tags: {has_seo}")
    print(f"  Products without SEO tags: {missing_seo}")


def check_publications(saudi):
    """Check sales channel publishing."""
    print("\n=== SALES CHANNELS ===")
    try:
        pubs = saudi.get_publications()
        print(f"  {len(pubs)} publications (sales channels)")
        for p in pubs:
            print(f"    - {p.get('name', 'Unknown')}")
    except Exception as e:
        print(f"  Error (likely missing read_publications scope): {e}")


def main():
    load_dotenv()
    dest_url = config.get_dest_shop_url()
    dest_token = config.get_dest_access_token()

    if not dest_url or not saudi_token:
        print("ERROR: Set DEST_SHOP_URL and DEST_ACCESS_TOKEN in .env")
        return

    saudi = ShopifyClient(dest_url, dest_token)
    id_map = load_json("data/id_map.json") if os.path.exists("data/id_map.json") else {}

    print("=" * 60)
    print("SAUDI STORE VERIFICATION REPORT")
    print("=" * 60)
    print(f"  Store: {saudi_url}")
    print(f"  ID map sections: {list(id_map.keys())}")

    check_products(saudi, id_map)
    check_collections(saudi, id_map)
    check_pages(saudi)
    check_blogs_articles(saudi)
    check_menus(saudi)
    check_metaobjects(saudi, id_map)
    check_theme(saudi)
    check_locales(saudi)
    check_redirects(saudi)
    check_translations(saudi, id_map)
    check_seo(saudi, id_map)
    check_publications(saudi)

    print("\n" + "=" * 60)
    print("VERIFICATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
