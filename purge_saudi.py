#!/usr/bin/env python3
"""Purge ALL migrated content from the Saudi Shopify store.

WARNING: This is DESTRUCTIVE and IRREVERSIBLE. It deletes:
  - All products
  - All custom & smart collections
  - All pages
  - All blogs & articles
  - All metaobjects & metaobject definitions
  - All URL redirects
  - All price rules & discount codes
  - All navigation menus
  - All uploaded files
  - All local progress/mapping files

Usage:
    python purge_saudi.py --dry-run       # Show what would be deleted
    python purge_saudi.py                 # Actually delete (requires confirmation)
    python purge_saudi.py --yes           # Skip confirmation prompt
    python purge_saudi.py --only products,collections  # Purge specific resources
"""

import argparse
import glob
import os
import time

from dotenv import load_dotenv

from shopify_client import ShopifyClient


RESOURCE_TYPES = [
    "menus",
    "redirects",
    "price_rules",
    "metaobjects",
    "metaobject_definitions",
    "articles",
    "blogs",
    "pages",
    "collections",
    "products",
    "files",
    "local_data",
]


def purge_products(client, dry_run=False):
    print("\n--- Products ---")
    products = client.get_products()
    print(f"  Found {len(products)} products")
    if dry_run or not products:
        return len(products)
    deleted = 0
    for p in products:
        try:
            client.delete_product(p["id"])
            deleted += 1
            if deleted % 10 == 0:
                print(f"  Deleted {deleted}/{len(products)}...")
                time.sleep(0.5)
        except Exception as e:
            print(f"  Error deleting product {p['id']} ({p.get('title', '')[:30]}): {e}")
    print(f"  Deleted {deleted} products")
    return deleted


def purge_collections(client, dry_run=False):
    print("\n--- Collections ---")
    collections = client.get_collections()
    print(f"  Found {len(collections)} collections")
    if dry_run or not collections:
        return len(collections)
    deleted = 0
    for c in collections:
        try:
            try:
                client.delete_custom_collection(c["id"])
            except Exception:
                client.delete_smart_collection(c["id"])
            deleted += 1
        except Exception as e:
            print(f"  Error deleting collection {c['id']} ({c.get('title', '')[:30]}): {e}")
    print(f"  Deleted {deleted} collections")
    return deleted


def purge_pages(client, dry_run=False):
    print("\n--- Pages ---")
    pages = client.get_pages()
    print(f"  Found {len(pages)} pages")
    if dry_run or not pages:
        return len(pages)
    deleted = 0
    for p in pages:
        try:
            client.delete_page(p["id"])
            deleted += 1
        except Exception as e:
            print(f"  Error deleting page {p['id']} ({p.get('title', '')[:30]}): {e}")
    print(f"  Deleted {deleted} pages")
    return deleted


def purge_blogs(client, dry_run=False):
    print("\n--- Blogs ---")
    blogs = client.get_blogs()
    print(f"  Found {len(blogs)} blogs")
    if dry_run or not blogs:
        return len(blogs)
    deleted = 0
    for b in blogs:
        try:
            client.delete_blog(b["id"])
            deleted += 1
        except Exception as e:
            print(f"  Error deleting blog {b['id']} ({b.get('title', '')[:30]}): {e}")
    print(f"  Deleted {deleted} blogs")
    return deleted


def purge_articles(client, dry_run=False):
    print("\n--- Articles ---")
    blogs = client.get_blogs()
    total = 0
    deleted = 0
    for b in blogs:
        articles = client.get_articles(b["id"])
        total += len(articles)
        if dry_run:
            print(f"  Blog '{b.get('title', '')}': {len(articles)} articles")
            continue
        for a in articles:
            try:
                client.delete_article(b["id"], a["id"])
                deleted += 1
            except Exception as e:
                print(f"  Error deleting article {a['id']}: {e}")
    if not dry_run:
        print(f"  Deleted {deleted} articles across {len(blogs)} blogs")
    else:
        print(f"  Found {total} articles total")
    return total


def purge_metaobjects(client, dry_run=False):
    print("\n--- Metaobjects ---")
    definitions = client.get_metaobject_definitions()
    total = 0
    for defn in definitions:
        obj_type = defn["type"]
        objects = client.get_metaobjects(obj_type)
        total += len(objects)
        print(f"  Type '{obj_type}': {len(objects)} entries")
        if dry_run:
            continue
        for obj in objects:
            try:
                client.delete_metaobject(obj["id"])
            except Exception as e:
                print(f"    Error deleting {obj['id']}: {e}")
        time.sleep(0.5)
    if not dry_run:
        print(f"  Deleted {total} metaobjects")
    return total


def purge_metaobject_definitions(client, dry_run=False):
    print("\n--- Metaobject Definitions ---")
    definitions = client.get_metaobject_definitions()
    print(f"  Found {len(definitions)} definitions")
    if dry_run or not definitions:
        return len(definitions)
    deleted = 0
    for defn in definitions:
        try:
            client.delete_metaobject_definition(defn["id"])
            deleted += 1
            print(f"  Deleted definition: {defn['type']}")
        except Exception as e:
            print(f"  Error deleting definition {defn['type']}: {e}")
    print(f"  Deleted {deleted} definitions")
    return deleted


def purge_redirects(client, dry_run=False):
    print("\n--- Redirects ---")
    redirects = client.get_redirects()
    print(f"  Found {len(redirects)} redirects")
    if dry_run or not redirects:
        return len(redirects)
    deleted = 0
    for r in redirects:
        try:
            client.delete_redirect(r["id"])
            deleted += 1
            if deleted % 50 == 0:
                print(f"  Deleted {deleted}/{len(redirects)}...")
                time.sleep(0.5)
        except Exception as e:
            print(f"  Error deleting redirect {r['id']}: {e}")
    print(f"  Deleted {deleted} redirects")
    return deleted


def purge_price_rules(client, dry_run=False):
    print("\n--- Price Rules & Discount Codes ---")
    rules = client.get_price_rules()
    print(f"  Found {len(rules)} price rules")
    if dry_run or not rules:
        return len(rules)
    deleted = 0
    for rule in rules:
        try:
            client.delete_price_rule(rule["id"])
            deleted += 1
        except Exception as e:
            print(f"  Error deleting price rule {rule['id']} ({rule.get('title', '')[:30]}): {e}")
    print(f"  Deleted {deleted} price rules (codes deleted automatically)")
    return deleted


def purge_menus(client, dry_run=False):
    print("\n--- Navigation Menus ---")
    menus = client.get_menus()
    print(f"  Found {len(menus)} menus")
    if dry_run or not menus:
        return len(menus)
    deleted = 0
    for menu in menus:
        try:
            client.delete_menu(menu["id"])
            deleted += 1
            print(f"  Deleted menu: {menu.get('title', '')}")
        except Exception as e:
            print(f"  Error deleting menu {menu['id']}: {e}")
    print(f"  Deleted {deleted} menus")
    return deleted


def purge_files(client, dry_run=False):
    print("\n--- Uploaded Files ---")
    files = client.get_files()
    print(f"  Found {len(files)} files")
    if dry_run or not files:
        return len(files)
    deleted = 0
    for f in files:
        fid = f.get("id")
        if not fid:
            continue
        try:
            client.delete_file(fid)
            deleted += 1
            if deleted % 20 == 0:
                print(f"  Deleted {deleted}/{len(files)}...")
                time.sleep(0.5)
        except Exception as e:
            print(f"  Error deleting file {fid}: {e}")
    print(f"  Deleted {deleted} files")
    return deleted


def purge_local_data(dry_run=False):
    print("\n--- Local Progress & Mapping Files ---")
    patterns = [
        "data/id_map.json",
        "data/file_map.json",
        "data/*_progress.json",
        "data/image_migration_report.json",
        "data/metaobject_diffs.json",
        "data/file_upload_cache.json",
    ]
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))
    files = sorted(set(files))
    print(f"  Found {len(files)} local tracking files")
    for f in files:
        print(f"    {f}")
    if dry_run or not files:
        return len(files)
    for f in files:
        os.remove(f)
    print(f"  Deleted {len(files)} local files")
    return len(files)


def main():
    parser = argparse.ArgumentParser(description="Purge all migrated content from Saudi store")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    parser.add_argument("--only", type=str, help="Comma-separated resource types to purge")
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ["SAUDI_SHOP_URL"]
    access_token = os.environ["SAUDI_ACCESS_TOKEN"]

    if args.only:
        selected = [s.strip() for s in args.only.split(",")]
        invalid = [s for s in selected if s not in RESOURCE_TYPES]
        if invalid:
            print(f"Error: Unknown resource types: {invalid}")
            print(f"Valid types: {', '.join(RESOURCE_TYPES)}")
            return
    else:
        selected = RESOURCE_TYPES

    if args.dry_run:
        print(f"=== DRY RUN — Scanning {shop_url} ===")
    else:
        print(f"\n{'='*60}")
        print(f"  WARNING: This will DELETE all data from {shop_url}")
        print(f"  Resources to purge: {', '.join(selected)}")
        print(f"{'='*60}\n")

        if not args.yes:
            confirm = input("Type 'DELETE' to confirm: ")
            if confirm != "DELETE":
                print("Aborted.")
                return

    client = ShopifyClient(shop_url, access_token)

    dispatch = {
        "menus": lambda: purge_menus(client, args.dry_run),
        "redirects": lambda: purge_redirects(client, args.dry_run),
        "price_rules": lambda: purge_price_rules(client, args.dry_run),
        "metaobjects": lambda: purge_metaobjects(client, args.dry_run),
        "metaobject_definitions": lambda: purge_metaobject_definitions(client, args.dry_run),
        "articles": lambda: purge_articles(client, args.dry_run),
        "blogs": lambda: purge_blogs(client, args.dry_run),
        "pages": lambda: purge_pages(client, args.dry_run),
        "collections": lambda: purge_collections(client, args.dry_run),
        "products": lambda: purge_products(client, args.dry_run),
        "files": lambda: purge_files(client, args.dry_run),
        "local_data": lambda: purge_local_data(args.dry_run),
    }

    totals = {}
    for resource in selected:
        if resource in dispatch:
            try:
                totals[resource] = dispatch[resource]()
            except Exception as e:
                err = str(e)
                if "ACCESS_DENIED" in err or "access denied" in err.lower() or "403" in err:
                    print(f"  SKIPPED — missing API scope for {resource}")
                    totals[resource] = "skipped (no scope)"
                else:
                    print(f"  ERROR purging {resource}: {e}")
                    totals[resource] = f"error: {e}"

    print(f"\n{'='*40}")
    if args.dry_run:
        print("DRY RUN SUMMARY — would delete:")
    else:
        print("PURGE COMPLETE:")
    for resource, count in totals.items():
        print(f"  {resource}: {count}")
    print(f"{'='*40}")


if __name__ == "__main__":
    main()
