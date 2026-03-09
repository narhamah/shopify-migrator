#!/usr/bin/env python3
"""Post-migration setup for Saudi Shopify store.

Run AFTER import_english.py, import_arabic.py, and migrate_all_images.py.

Handles:
  Step 1:  Enable Arabic locale
  Step 2:  Link products to collections (collects)
  Step 3:  Build navigation menus
  Step 4:  Set SEO meta tags
  Step 5:  Create URL redirects
  Step 6:  Set inventory quantities
  Step 7:  Publish products/collections to sales channels
  Step 8:  Migrate discount codes / price rules
  Step 9:  Activate products (draft → active)
  Step 10: Create store policies
  Step 11: Update handles (Spanish → English)

Usage:
    python post_migration.py                    # Run all steps
    python post_migration.py --step 2           # Run only step 2
    python post_migration.py --step 2 --step 3  # Run steps 2 and 3
    python post_migration.py --dry-run          # Show what would be done
"""

import argparse
import json
import os

from dotenv import load_dotenv

from shopify_client import ShopifyClient


from utils import load_json, save_json


# =============================================
# Step 1: Enable Arabic locale
# =============================================

def step_enable_arabic(client, dry_run=False):
    """Enable Arabic (ar) locale on the Saudi store."""
    print("\n=== Step 1: Enable Arabic Locale ===")

    if dry_run:
        print("  Would enable 'ar' locale")
        return

    # Check current locales
    locales = client.get_locales()
    locale_codes = [loc["locale"] for loc in locales]
    print(f"  Current locales: {locale_codes}")

    if "ar" in locale_codes:
        print("  Arabic (ar) already enabled — skipping")
        return

    try:
        result = client.enable_locale("ar")
        print(f"  Enabled Arabic locale: {result}")
    except Exception as e:
        print(f"  Error enabling Arabic locale: {e}")


# =============================================
# Step 2: Link products to collections
# =============================================

def step_link_products_to_collections(client, dry_run=False):
    """Create product-collection associations from exported collects."""
    print("\n=== Step 2: Link Products to Collections ===")

    id_map = load_json("data/id_map.json", default={})
    product_map = id_map.get("products", {})
    collection_map = id_map.get("collections", {})
    progress = load_json("data/collects_progress.json", default={})

    # Try exported collects first
    collects = load_json("data/spain_export/collects.json")
    if not collects:
        # Fallback: try to infer from english data
        collects = load_json("data/english/collects.json")
    if not collects:
        # Last fallback: try spanish export
        collects = load_json("data/collects.json")

    if not collects:
        print("  No collects data found. Export collects first (re-run export_spain.py)")
        print("  Or manually assign products to collections in Shopify admin.")
        return

    print(f"  Found {len(collects)} product-collection links to create")

    created = 0
    skipped = 0
    errors = 0

    for i, collect in enumerate(collects):
        source_product_id = str(collect.get("product_id", ""))
        source_collection_id = str(collect.get("collection_id", ""))

        key = f"{source_product_id}_{source_collection_id}"
        if key in progress:
            skipped += 1
            continue

        dest_product_id = product_map.get(source_product_id)
        dest_collection_id = collection_map.get(source_collection_id)

        if not dest_product_id or not dest_collection_id:
            skipped += 1
            continue

        if dry_run:
            print(f"  Would link product {dest_product_id} → collection {dest_collection_id}")
            created += 1
            continue

        try:
            client.create_collect(dest_product_id, dest_collection_id)
            created += 1
            progress[key] = True
            if created % 10 == 0:
                save_json(progress, "data/collects_progress.json")
        except Exception as e:
            err_msg = str(e)
            if "already" in err_msg.lower() or "422" in err_msg:
                skipped += 1
                progress[key] = True
            else:
                print(f"  Error linking product {dest_product_id} → {dest_collection_id}: {e}")
                errors += 1

    save_json(progress, "data/collects_progress.json")
    print(f"  Created: {created}, Skipped: {skipped}, Errors: {errors}")


# =============================================
# Step 3: Build navigation menus
# =============================================

def step_build_navigation(client, dry_run=False):
    """Build main menu and footer menu from collections and pages."""
    print("\n=== Step 3: Build Navigation Menus ===")

    id_map = load_json("data/id_map.json", default={})
    collections = load_json("data/english/collections.json")
    pages = load_json("data/english/pages.json")

    collection_map = id_map.get("collections", {})
    page_map = id_map.get("pages", {})

    # Build main menu from collections
    main_items = []
    for coll in collections:
        source_id = str(coll["id"])
        dest_id = collection_map.get(source_id)
        if dest_id:
            main_items.append({
                "title": coll.get("title", ""),
                "resourceId": f"gid://shopify/Collection/{dest_id}",
            })

    if not main_items:
        print("  No collections found for main menu")
    else:
        print(f"  Main menu: {len(main_items)} items")
        if dry_run:
            for item in main_items:
                print(f"    - {item['title']}")
        else:
            try:
                result = client.create_menu("Main Menu", "main-menu", main_items)
                if result:
                    print(f"  Created main menu (id: {result['id']})")
                else:
                    print("  Main menu already exists")
            except Exception as e:
                print(f"  Error creating main menu: {e}")

    # Build footer menu from pages
    footer_items = []
    for page in pages:
        source_id = str(page["id"])
        dest_id = page_map.get(source_id)
        if dest_id:
            footer_items.append({
                "title": page.get("title", ""),
                "resourceId": f"gid://shopify/OnlineStorePage/{dest_id}",
            })

    if not footer_items:
        print("  No pages found for footer menu")
    else:
        print(f"  Footer menu: {len(footer_items)} items")
        if dry_run:
            for item in footer_items:
                print(f"    - {item['title']}")
        else:
            try:
                result = client.create_menu("Footer Menu", "footer", footer_items)
                if result:
                    print(f"  Created footer menu (id: {result['id']})")
                else:
                    print("  Footer menu already exists")
            except Exception as e:
                print(f"  Error creating footer menu: {e}")


# =============================================
# Step 4: Set SEO meta tags
# =============================================

def step_set_seo_tags(client, dry_run=False):
    """Set SEO meta titles and descriptions on products, collections, and pages."""
    print("\n=== Step 4: Set SEO Meta Tags ===")

    id_map = load_json("data/id_map.json", default={})
    product_map = id_map.get("products", {})
    progress = load_json("data/seo_progress.json", default={})

    # Products — check for global.title_tag and global.description_tag metafields
    products = load_json("data/english/products.json")
    updated = 0
    for product in products:
        source_id = str(product["id"])
        dest_id = product_map.get(source_id)
        if not dest_id or f"product_{source_id}" in progress:
            continue

        title_tag = None
        desc_tag = None
        for mf in product.get("metafields", []):
            if mf.get("namespace") == "global" and mf.get("key") == "title_tag":
                title_tag = mf.get("value")
            elif mf.get("namespace") == "global" and mf.get("key") == "description_tag":
                desc_tag = mf.get("value")

        if not title_tag and not desc_tag:
            progress[f"product_{source_id}"] = True
            continue

        if dry_run:
            print(f"  Product '{product.get('title', '')[:40]}': title='{(title_tag or '')[:30]}' desc='{(desc_tag or '')[:30]}'")
            updated += 1
        else:
            try:
                client.update_product_seo(dest_id, title_tag, desc_tag)
                updated += 1
                progress[f"product_{source_id}"] = True
            except Exception as e:
                print(f"  Product '{product.get('title', '')[:40]}': error: {e}")

    save_json(progress, "data/seo_progress.json")

    # Collections and pages — check their metafields too
    collections = load_json("data/english/collections.json")
    collection_map = id_map.get("collections", {})
    for coll in collections:
        source_id = str(coll["id"])
        dest_id = collection_map.get(source_id)
        if not dest_id or f"collection_{source_id}" in progress:
            continue

        title_tag = None
        desc_tag = None
        for mf in coll.get("metafields", []):
            if mf.get("namespace") == "global" and mf.get("key") == "title_tag":
                title_tag = mf.get("value")
            elif mf.get("namespace") == "global" and mf.get("key") == "description_tag":
                desc_tag = mf.get("value")

        if not title_tag and not desc_tag:
            progress[f"collection_{source_id}"] = True
            continue

        if dry_run:
            print(f"  Collection '{coll.get('title', '')[:40]}': SEO tags found")
            updated += 1
        else:
            try:
                metafields = []
                if title_tag:
                    metafields.append({
                        "ownerId": f"gid://shopify/Collection/{dest_id}",
                        "namespace": "global", "key": "title_tag",
                        "value": title_tag, "type": "single_line_text_field",
                    })
                if desc_tag:
                    metafields.append({
                        "ownerId": f"gid://shopify/Collection/{dest_id}",
                        "namespace": "global", "key": "description_tag",
                        "value": desc_tag, "type": "single_line_text_field",
                    })
                client.set_metafields(metafields)
                updated += 1
                progress[f"collection_{source_id}"] = True
            except Exception as e:
                print(f"  Collection '{coll.get('title', '')[:40]}': error: {e}")

    pages = load_json("data/english/pages.json")
    page_map = id_map.get("pages", {})
    for page in pages:
        source_id = str(page["id"])
        dest_id = page_map.get(source_id)
        if not dest_id or f"page_{source_id}" in progress:
            continue

        title_tag = None
        desc_tag = None
        for mf in page.get("metafields", []):
            if mf.get("namespace") == "global" and mf.get("key") == "title_tag":
                title_tag = mf.get("value")
            elif mf.get("namespace") == "global" and mf.get("key") == "description_tag":
                desc_tag = mf.get("value")

        if not title_tag and not desc_tag:
            progress[f"page_{source_id}"] = True
            continue

        if dry_run:
            print(f"  Page '{page.get('title', '')[:40]}': SEO tags found")
            updated += 1
        else:
            try:
                metafields = []
                if title_tag:
                    metafields.append({
                        "ownerId": f"gid://shopify/OnlineStorePage/{dest_id}",
                        "namespace": "global", "key": "title_tag",
                        "value": title_tag, "type": "single_line_text_field",
                    })
                if desc_tag:
                    metafields.append({
                        "ownerId": f"gid://shopify/OnlineStorePage/{dest_id}",
                        "namespace": "global", "key": "description_tag",
                        "value": desc_tag, "type": "single_line_text_field",
                    })
                client.set_metafields(metafields)
                updated += 1
                progress[f"page_{source_id}"] = True
            except Exception as e:
                print(f"  Page '{page.get('title', '')[:40]}': error: {e}")

    save_json(progress, "data/seo_progress.json")
    print(f"  Updated SEO tags on {updated} resources")


# =============================================
# Step 5: Create URL redirects
# =============================================

def _build_handle_remap():
    """Build old_handle → new_handle maps from Spain export vs English data.

    Returns a dict mapping old URL paths (e.g. /products/old-handle) to new
    paths (e.g. /products/new-handle) for products, collections, pages, blogs,
    and articles.
    """
    import re

    remap = {}  # "/products/old-handle" → "/products/new-handle"

    # Products
    spain_products = load_json("data/spain_export/products.json")
    english_products = load_json("data/english/products.json")
    spain_prod_by_id = {str(p["id"]): p.get("handle", "") for p in (spain_products if isinstance(spain_products, list) else [])}
    eng_prod_by_id = {str(p["id"]): p.get("handle", "") for p in (english_products if isinstance(english_products, list) else [])}
    for src_id, old_handle in spain_prod_by_id.items():
        new_handle = eng_prod_by_id.get(src_id, "")
        if old_handle and new_handle and old_handle != new_handle:
            remap[f"/products/{old_handle}"] = f"/products/{new_handle}"

    # Collections
    spain_collections = load_json("data/spain_export/collections.json")
    english_collections = load_json("data/english/collections.json")
    spain_coll_by_id = {str(c["id"]): c.get("handle", "") for c in (spain_collections if isinstance(spain_collections, list) else [])}
    eng_coll_by_id = {str(c["id"]): c.get("handle", "") for c in (english_collections if isinstance(english_collections, list) else [])}
    for src_id, old_handle in spain_coll_by_id.items():
        new_handle = eng_coll_by_id.get(src_id, "")
        if old_handle and new_handle and old_handle != new_handle:
            remap[f"/collections/{old_handle}"] = f"/collections/{new_handle}"

    # Pages
    spain_pages = load_json("data/spain_export/pages.json")
    english_pages = load_json("data/english/pages.json")
    spain_page_by_id = {str(p["id"]): p.get("handle", "") for p in (spain_pages if isinstance(spain_pages, list) else [])}
    eng_page_by_id = {str(p["id"]): p.get("handle", "") for p in (english_pages if isinstance(english_pages, list) else [])}
    for src_id, old_handle in spain_page_by_id.items():
        new_handle = eng_page_by_id.get(src_id, "")
        if old_handle and new_handle and old_handle != new_handle:
            remap[f"/pages/{old_handle}"] = f"/pages/{new_handle}"

    # Blogs
    spain_blogs = load_json("data/spain_export/blogs.json")
    english_blogs = load_json("data/english/blogs.json")
    spain_blog_by_id = {str(b["id"]): b.get("handle", "") for b in (spain_blogs if isinstance(spain_blogs, list) else [])}
    eng_blog_by_id = {str(b["id"]): b.get("handle", "") for b in (english_blogs if isinstance(english_blogs, list) else [])}
    blog_handle_map = {}  # old_blog_handle → new_blog_handle
    for src_id, old_handle in spain_blog_by_id.items():
        new_handle = eng_blog_by_id.get(src_id, "")
        if old_handle and new_handle and old_handle != new_handle:
            remap[f"/blogs/{old_handle}"] = f"/blogs/{new_handle}"
            blog_handle_map[old_handle] = new_handle

    # Articles (need blog handle context)
    spain_articles = load_json("data/spain_export/articles.json")
    english_articles = load_json("data/english/articles.json")
    if isinstance(spain_articles, list) and isinstance(english_articles, list):
        spain_art_by_id = {}
        for a in spain_articles:
            blog_id = str(a.get("blog_id", ""))
            old_blog_handle = spain_blog_by_id.get(blog_id, "")
            spain_art_by_id[str(a["id"])] = (old_blog_handle, a.get("handle", ""))
        eng_art_by_id = {}
        for a in english_articles:
            blog_id = str(a.get("blog_id", ""))
            new_blog_handle = eng_blog_by_id.get(blog_id, "")
            eng_art_by_id[str(a["id"])] = (new_blog_handle, a.get("handle", ""))
        for src_id, (old_bh, old_ah) in spain_art_by_id.items():
            new_bh, new_ah = eng_art_by_id.get(src_id, ("", ""))
            if old_bh and old_ah and new_bh and new_ah:
                old_path = f"/blogs/{old_bh}/{old_ah}"
                new_path = f"/blogs/{new_bh}/{new_ah}"
                if old_path != new_path:
                    remap[old_path] = new_path

    return remap


def _remap_redirect_target(target, remap):
    """Remap a redirect target using the handle remap table.

    Handles both path-only targets (/products/handle) and full URL targets
    (https://store.myshopify.com/products/handle).
    """
    import urllib.parse

    # Parse full URLs
    parsed = urllib.parse.urlparse(target)
    path = parsed.path if parsed.scheme else target

    # Normalize: strip trailing slash for matching
    path_normalized = path.rstrip("/")

    # Direct match
    if path_normalized in remap:
        new_path = remap[path_normalized]
        if parsed.scheme:
            return urllib.parse.urlunparse(parsed._replace(path=new_path))
        return new_path

    # Check if path starts with a known old prefix (e.g. /collections/old/products/x)
    for old_path, new_path in remap.items():
        if path_normalized.startswith(old_path + "/"):
            suffix = path_normalized[len(old_path):]
            remapped = new_path + suffix
            if parsed.scheme:
                return urllib.parse.urlunparse(parsed._replace(path=remapped))
            return remapped

    return target


def step_create_redirects(client, dry_run=False):
    """Create URL redirects from exported redirect data.

    Remaps redirect targets to account for handle changes (Spanish → English)
    that occurred during import.
    """
    print("\n=== Step 5: Create URL Redirects ===")

    redirects = load_json("data/spain_export/redirects.json")
    if not redirects:
        print("  No redirects found in export data")
        return

    # Build handle remap table (old Spanish handles → new English handles)
    remap = _build_handle_remap()
    if remap:
        print(f"  Built handle remap table with {len(remap)} entries")

    progress = load_json("data/redirects_progress.json", default={})
    created = 0
    remapped = 0

    for redir in redirects:
        path = redir.get("path", "")
        target = redir.get("target", "")

        if not path or not target:
            continue
        if path in progress:
            continue

        # Remap target to use new handles
        new_target = _remap_redirect_target(target, remap)
        if new_target != target:
            remapped += 1

        if dry_run:
            if new_target != target:
                print(f"  Would create redirect: {path} → {new_target}  (was: {target})")
            else:
                print(f"  Would create redirect: {path} → {new_target}")
            created += 1
            continue

        try:
            client.create_redirect(path, new_target)
            created += 1
            progress[path] = True
        except Exception as e:
            err_msg = str(e)
            if "422" in err_msg:
                progress[path] = True  # Already exists
            else:
                print(f"  Error creating redirect {path}: {e}")

    save_json(progress, "data/redirects_progress.json")
    print(f"  Created {created} redirects ({remapped} targets remapped)")


# =============================================
# Step 6: Set inventory quantities
# =============================================

def step_set_inventory(client, default_quantity=100, dry_run=False):
    """Set initial inventory quantities for all product variants."""
    print("\n=== Step 6: Set Inventory Quantities ===")

    id_map = load_json("data/id_map.json", default={})
    product_map = id_map.get("products", {})
    progress = load_json("data/inventory_progress.json", default={})

    if dry_run:
        print(f"  Would set inventory to {default_quantity} for all variants")
        return

    # Get the primary location
    try:
        locations = client.get_locations()
    except Exception as e:
        if "403" in str(e):
            print("  ERROR: Missing 'read_locations' scope on access token.")
            print("  Add the 'read_locations' and 'write_inventory' scopes in your Shopify app settings.")
            return
        raise
    if not locations:
        print("  Error: No locations found in store")
        return
    location = locations[0]
    location_gid = f"gid://shopify/Location/{location['id']}"
    print(f"  Using location: {location.get('name', '')} ({location_gid})")

    # Get all products in destination store
    products = client.get_products()
    updated = 0

    for product in products:
        product_id = str(product["id"])
        if product_id in progress:
            continue

        for variant in product.get("variants", []):
            variant_id = variant["id"]
            if not variant.get("inventory_management"):
                continue

            try:
                inv_item_id = client.get_inventory_item_id(variant_id)
                if inv_item_id:
                    client.set_inventory_quantity(inv_item_id, location_gid, default_quantity)
                    updated += 1
            except Exception as e:
                print(f"  Error setting inventory for variant {variant_id}: {e}")

        progress[product_id] = True
        if updated % 20 == 0:
            save_json(progress, "data/inventory_progress.json")

    save_json(progress, "data/inventory_progress.json")
    print(f"  Updated inventory for {updated} variants (qty: {default_quantity})")


# =============================================
# Step 7: Publish to sales channels
# =============================================

def step_publish_resources(client, dry_run=False):
    """Publish all products and collections to all sales channels."""
    print("\n=== Step 7: Publish Resources to Sales Channels ===")

    if dry_run:
        print("  Would publish all products and collections to all sales channels")
        return

    try:
        publications = client.get_publications()
    except Exception as e:
        print(f"  Error fetching publications: {e}")
        return

    if not publications:
        print("  No publications (sales channels) found")
        return

    pub_ids = [p["id"] for p in publications]
    pub_names = [p.get("name", "Unknown") for p in publications]
    print(f"  Sales channels: {', '.join(pub_names)}")

    id_map = load_json("data/id_map.json", default={})
    progress = load_json("data/publish_progress.json", default={})

    # Publish products
    product_map = id_map.get("products", {})
    published = 0
    for source_id, dest_id in product_map.items():
        key = f"product_{dest_id}"
        if key in progress:
            continue
        try:
            gid = f"gid://shopify/Product/{dest_id}"
            client.publish_resource(gid, pub_ids)
            published += 1
            progress[key] = True
        except Exception as e:
            err = str(e)
            if "already" in err.lower():
                progress[key] = True
            else:
                print(f"  Error publishing product {dest_id}: {e}")

    # Publish collections
    collection_map = id_map.get("collections", {})
    for source_id, dest_id in collection_map.items():
        key = f"collection_{dest_id}"
        if key in progress:
            continue
        try:
            gid = f"gid://shopify/Collection/{dest_id}"
            client.publish_resource(gid, pub_ids)
            published += 1
            progress[key] = True
        except Exception as e:
            err = str(e)
            if "already" in err.lower():
                progress[key] = True
            else:
                print(f"  Error publishing collection {dest_id}: {e}")

    save_json(progress, "data/publish_progress.json")
    print(f"  Published {published} resources to {len(pub_ids)} channels")


# =============================================
# Step 8: Migrate discount codes
# =============================================

def step_migrate_discounts(client, dry_run=False):
    """Migrate price rules and discount codes from exported data."""
    print("\n=== Step 8: Migrate Discount Codes ===")

    price_rules = load_json("data/spain_export/price_rules.json")
    if not price_rules:
        print("  No price rules found in export data")
        return

    progress = load_json("data/discounts_progress.json", default={})
    created_rules = 0
    created_codes = 0

    for rule in price_rules:
        source_id = str(rule.get("id", ""))
        if source_id in progress:
            continue

        # Build price rule data (strip source-specific IDs)
        rule_data = {
            "title": rule.get("title", ""),
            "target_type": rule.get("target_type", "line_item"),
            "target_selection": rule.get("target_selection", "all"),
            "allocation_method": rule.get("allocation_method", "across"),
            "value_type": rule.get("value_type", "percentage"),
            "value": rule.get("value", "0"),
            "customer_selection": rule.get("customer_selection", "all"),
            "starts_at": rule.get("starts_at"),
        }
        if rule.get("ends_at"):
            rule_data["ends_at"] = rule["ends_at"]
        if rule.get("usage_limit"):
            rule_data["usage_limit"] = rule["usage_limit"]
        if rule.get("once_per_customer") is not None:
            rule_data["once_per_customer"] = rule["once_per_customer"]

        if dry_run:
            print(f"  Would create price rule: {rule_data['title']} ({rule_data['value_type']}: {rule_data['value']})")
            codes = rule.get("discount_codes", [])
            for code in codes:
                print(f"    Would create code: {code.get('code', '')}")
            created_rules += 1
            created_codes += len(codes)
            continue

        try:
            created = client.create_price_rule(rule_data)
            dest_rule_id = created.get("id")
            if not dest_rule_id:
                print(f"  Failed to create price rule: {rule_data['title']}")
                continue
            created_rules += 1
            progress[source_id] = str(dest_rule_id)

            # Create associated discount codes
            for code_data in rule.get("discount_codes", []):
                code = code_data.get("code", "")
                if code:
                    try:
                        client.create_discount_code(dest_rule_id, code)
                        created_codes += 1
                    except Exception as e:
                        print(f"  Error creating code '{code}': {e}")
        except Exception as e:
            err_msg = str(e)
            if "422" in err_msg:
                progress[source_id] = "exists"
            elif "403" in err_msg:
                print(f"  ERROR: Missing 'write_price_rules' scope. Add it in your Shopify app settings.")
                break
            else:
                print(f"  Error creating price rule '{rule_data['title']}': {e}")

    save_json(progress, "data/discounts_progress.json")
    print(f"  Created {created_rules} price rules, {created_codes} discount codes")


# =============================================
# Step 9: Activate products
# =============================================

def step_activate_products(client, dry_run=False):
    """Set all draft products to active status."""
    print("\n=== Step 9: Activate Products ===")

    if dry_run:
        print("  Would activate all draft products")
        return

    id_map = load_json("data/id_map.json", default={})
    product_map = id_map.get("products", {})
    progress = load_json("data/activate_progress.json", default={})

    activated = 0
    for source_id, dest_id in product_map.items():
        if str(dest_id) in progress:
            continue
        try:
            client.update_product(dest_id, {"status": "active"})
            activated += 1
            progress[str(dest_id)] = True
        except Exception as e:
            print(f"  Error activating product {dest_id}: {e}")

    save_json(progress, "data/activate_progress.json")
    print(f"  Activated {activated} products")


# =============================================
# Step 10: Create store policies
# =============================================

def step_create_policies(client, dry_run=False):
    """Create store policies from exported policy data or defaults."""
    print("\n=== Step 10: Store Policies ===")

    policies = load_json("data/spain_export/policies.json")
    english_policies = load_json("data/english/policies.json")

    source = english_policies if english_policies else policies

    if not source:
        print("  No policy data found. Policies must be created manually.")
        print("  Go to: Saudi Shopify Admin → Settings → Policies")
        print("  Required: Privacy Policy, Terms of Service, Refund Policy, Shipping Policy")
        return

    print("  Policies found in export data:")
    for policy in source:
        title = policy.get("title", "Unknown")
        body_len = len(policy.get("body", ""))
        print(f"    - {title} ({body_len} chars)")

    print("\n  NOTE: Store policies must be set manually in Shopify admin.")
    print("  The exported policy text has been saved to data/spain_export/policies.json")
    print("  Copy the content to: Settings → Policies in the Saudi store admin.")


# =============================================
# Step 11: Update handles (Spanish → English)
# =============================================

def step_update_handles(client, dry_run=False):
    """Update product/collection/page handles from Spanish to English."""
    print("\n=== Step 11: Update Handles (Spanish → English) ===")

    id_map = load_json("data/id_map.json", default={})
    progress = load_json("data/handle_progress.json", default={})

    # Products
    products = load_json("data/english/products.json")
    spain_products = load_json("data/spain_export/products.json")
    spain_handles = {str(p["id"]): p.get("handle", "") for p in spain_products}
    product_map = id_map.get("products", {})

    updated = 0
    for product in products:
        source_id = str(product["id"])
        dest_id = product_map.get(source_id)
        if not dest_id or f"product_{source_id}" in progress:
            continue

        new_handle = product.get("handle", "")
        old_handle = spain_handles.get(source_id, "")

        # Only update if handle actually changed (was translated)
        if not new_handle or new_handle == old_handle:
            continue

        if dry_run:
            print(f"  Would update product handle: {old_handle} → {new_handle}")
        else:
            try:
                client.update_product(dest_id, {"handle": new_handle})
                print(f"  Updated: {old_handle} → {new_handle}")
                updated += 1
            except Exception as e:
                print(f"  Error updating {old_handle}: {e}")

        progress[f"product_{source_id}"] = True

    if not dry_run:
        save_json(progress, "data/handle_progress.json")

    # Collections
    collections = load_json("data/english/collections.json")
    spain_collections = load_json("data/spain_export/collections.json")
    spain_coll_handles = {str(c["id"]): c.get("handle", "") for c in spain_collections}
    collection_map = id_map.get("collections", {})

    for coll in collections:
        source_id = str(coll["id"])
        dest_id = collection_map.get(source_id)
        if not dest_id or f"collection_{source_id}" in progress:
            continue

        new_handle = coll.get("handle", "")
        old_handle = spain_coll_handles.get(source_id, "")

        if not new_handle or new_handle == old_handle:
            continue

        if dry_run:
            print(f"  Would update collection handle: {old_handle} → {new_handle}")
        else:
            try:
                client._request("PUT", f"custom_collections/{dest_id}.json",
                                json={"custom_collection": {"handle": new_handle}})
                print(f"  Updated collection: {old_handle} → {new_handle}")
                updated += 1
            except Exception as e:
                # Try smart collection
                try:
                    client._request("PUT", f"smart_collections/{dest_id}.json",
                                    json={"smart_collection": {"handle": new_handle}})
                    print(f"  Updated smart collection: {old_handle} → {new_handle}")
                    updated += 1
                except Exception:
                    print(f"  Error updating collection {old_handle}: {e}")

        progress[f"collection_{source_id}"] = True

    if not dry_run:
        save_json(progress, "data/handle_progress.json")

    print(f"  Updated {updated} handles")


# =============================================
# Main
# =============================================

def main():
    parser = argparse.ArgumentParser(description="Post-migration setup for Saudi store")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--step", type=int, action="append", help="Run specific step(s) only")
    parser.add_argument("--inventory-qty", type=int, default=100, help="Default inventory quantity (default: 100)")
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ["SAUDI_SHOP_URL"]
    access_token = os.environ["SAUDI_ACCESS_TOKEN"]
    client = ShopifyClient(shop_url, access_token)

    steps = args.step or [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

    if 1 in steps:
        step_enable_arabic(client, dry_run=args.dry_run)
    if 2 in steps:
        step_link_products_to_collections(client, dry_run=args.dry_run)
    if 3 in steps:
        step_build_navigation(client, dry_run=args.dry_run)
    if 4 in steps:
        step_set_seo_tags(client, dry_run=args.dry_run)
    if 5 in steps:
        step_create_redirects(client, dry_run=args.dry_run)
    if 6 in steps:
        step_set_inventory(client, default_quantity=args.inventory_qty, dry_run=args.dry_run)
    if 7 in steps:
        step_publish_resources(client, dry_run=args.dry_run)
    if 8 in steps:
        step_migrate_discounts(client, dry_run=args.dry_run)
    if 9 in steps:
        step_activate_products(client, dry_run=args.dry_run)
    if 10 in steps:
        step_create_policies(client, dry_run=args.dry_run)
    if 11 in steps:
        step_update_handles(client, dry_run=args.dry_run)

    print("\n=== Post-Migration Complete ===")
    print("\nRemaining MANUAL steps:")
    print("  1. Configure payment gateways (Settings → Payments)")
    print("     → Tap, Mada, Apple Pay, or other KSA providers")
    print("  2. Configure Saudi VAT 15% (Settings → Taxes and duties)")
    print("  3. Set up shipping zones/rates (Settings → Shipping and delivery)")
    print("  4. Set up domain and DNS (Settings → Domains)")
    print("  5. Install and configure theme (Online Store → Themes)")
    print("  6. Set up email notifications (Settings → Notifications)")
    print("  7. Install third-party apps (Klaviyo, reviews, etc.)")
    print("  8. Recreate Shopify Flows (export .flow from Spain, import to Saudi)")
    print("  9. Test checkout flow end-to-end")


if __name__ == "__main__":
    main()
