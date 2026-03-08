#!/usr/bin/env python3
"""Step 3: Import English-translated content into the Saudi Shopify store."""

import argparse
import json
import os

from dotenv import load_dotenv

from shopify_client import ShopifyClient


def load_json(filepath):
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def convert_price(price, exchange_rate):
    if price is None:
        return None
    try:
        return str(round(float(price) * exchange_rate, 2))
    except (ValueError, TypeError):
        return price


def prepare_product_for_import(product, exchange_rate):
    """Strip source-specific fields and prepare product for creation."""
    p = {
        "title": product.get("title", ""),
        "body_html": product.get("body_html", ""),
        "vendor": product.get("vendor", ""),
        "product_type": product.get("product_type", ""),
        "tags": product.get("tags", ""),
        "handle": product.get("handle", ""),
        "status": product.get("status", "draft"),
    }

    # Keep images via src URL
    if product.get("images"):
        p["images"] = [{"src": img["src"]} for img in product["images"] if img.get("src")]

    # Variants with price conversion
    if product.get("variants"):
        p["variants"] = []
        for v in product["variants"]:
            variant = {
                "title": v.get("title", ""),
                "price": convert_price(v.get("price"), exchange_rate),
                "compare_at_price": convert_price(v.get("compare_at_price"), exchange_rate),
                "sku": v.get("sku", ""),
                "barcode": v.get("barcode", ""),
                "weight": v.get("weight"),
                "weight_unit": v.get("weight_unit", "kg"),
                "inventory_management": v.get("inventory_management"),
                "option1": v.get("option1"),
                "option2": v.get("option2"),
                "option3": v.get("option3"),
                "requires_shipping": v.get("requires_shipping", True),
                "taxable": v.get("taxable", True),
            }
            p["variants"].append(variant)

    # Options
    if product.get("options"):
        p["options"] = []
        for opt in product["options"]:
            p["options"].append({
                "name": opt.get("name", ""),
                "values": opt.get("values", []),
            })

    return p


def main():
    parser = argparse.ArgumentParser(description="Import English content into Saudi Shopify store")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created without making API calls")
    parser.add_argument("--exchange-rate", type=float, default=1.0, help="EUR to SAR exchange rate (default: 1.0)")
    args = parser.parse_args()

    load_dotenv()
    input_dir = "data/english"
    id_map_file = "data/id_map.json"
    id_map = load_json(id_map_file) if os.path.exists(id_map_file) else {}

    if args.dry_run:
        print("=== DRY RUN MODE — no API calls will be made ===\n")
        client = None
    else:
        shop_url = os.environ["SAUDI_SHOP_URL"]
        access_token = os.environ["SAUDI_ACCESS_TOKEN"]
        client = ShopifyClient(shop_url, access_token)

    exchange_rate = args.exchange_rate
    print(f"Exchange rate (EUR→SAR): {exchange_rate}")

    # Import products
    products = load_json(os.path.join(input_dir, "products.json"))
    print(f"\nImporting {len(products)} products...")
    for i, product in enumerate(products):
        source_id = str(product["id"])
        handle = product.get("handle", "")
        label = f"[{i+1}/{len(products)}] {product.get('title', '')[:50]}"

        if source_id in id_map.get("products", {}):
            print(f"  {label} — already imported, skipping")
            continue

        if args.dry_run:
            print(f"  {label} — would create (handle: {handle})")
            continue

        # Check if exists by handle
        existing = client.get_products_by_handle(handle)
        if existing:
            dest_id = existing[0]["id"]
            print(f"  {label} — already exists (id: {dest_id}), mapping")
            id_map.setdefault("products", {})[source_id] = dest_id
            save_json(id_map, id_map_file)
            continue

        product_data = prepare_product_for_import(product, exchange_rate)
        created = client.create_product(product_data)
        dest_id = created.get("id")
        print(f"  {label} — created (id: {dest_id})")
        id_map.setdefault("products", {})[source_id] = dest_id
        save_json(id_map, id_map_file)

    # Import collections
    collections = load_json(os.path.join(input_dir, "collections.json"))
    print(f"\nImporting {len(collections)} collections...")
    for i, collection in enumerate(collections):
        source_id = str(collection["id"])
        handle = collection.get("handle", "")
        label = f"[{i+1}/{len(collections)}] {collection.get('title', '')[:50]}"

        if source_id in id_map.get("collections", {}):
            print(f"  {label} — already imported, skipping")
            continue

        if args.dry_run:
            print(f"  {label} — would create (handle: {handle})")
            continue

        existing = client.get_collections_by_handle(handle)
        if existing:
            dest_id = existing[0]["id"]
            print(f"  {label} — already exists (id: {dest_id}), mapping")
            id_map.setdefault("collections", {})[source_id] = dest_id
            save_json(id_map, id_map_file)
            continue

        # Only custom collections can be created via REST
        coll_data = {
            "title": collection.get("title", ""),
            "body_html": collection.get("body_html", ""),
            "handle": handle,
        }
        if collection.get("image", {}).get("src"):
            coll_data["image"] = {"src": collection["image"]["src"]}

        created = client.create_custom_collection(coll_data)
        dest_id = created.get("id")
        print(f"  {label} — created (id: {dest_id})")
        id_map.setdefault("collections", {})[source_id] = dest_id
        save_json(id_map, id_map_file)

    # Import pages
    pages = load_json(os.path.join(input_dir, "pages.json"))
    print(f"\nImporting {len(pages)} pages...")
    for i, page in enumerate(pages):
        source_id = str(page["id"])
        handle = page.get("handle", "")
        label = f"[{i+1}/{len(pages)}] {page.get('title', '')[:50]}"

        if source_id in id_map.get("pages", {}):
            print(f"  {label} — already imported, skipping")
            continue

        if args.dry_run:
            print(f"  {label} — would create (handle: {handle})")
            continue

        existing = client.get_pages_by_handle(handle)
        if existing:
            dest_id = existing[0]["id"]
            print(f"  {label} — already exists (id: {dest_id}), mapping")
            id_map.setdefault("pages", {})[source_id] = dest_id
            save_json(id_map, id_map_file)
            continue

        page_data = {
            "title": page.get("title", ""),
            "body_html": page.get("body_html", ""),
            "handle": handle,
            "published": page.get("published_at") is not None,
        }
        created = client.create_page(page_data)
        dest_id = created.get("id")
        print(f"  {label} — created (id: {dest_id})")
        id_map.setdefault("pages", {})[source_id] = dest_id
        save_json(id_map, id_map_file)

    # Import blogs + articles
    blogs = load_json(os.path.join(input_dir, "blogs.json"))
    articles = load_json(os.path.join(input_dir, "articles.json"))
    print(f"\nImporting {len(blogs)} blogs...")
    for i, blog in enumerate(blogs):
        source_id = str(blog["id"])
        handle = blog.get("handle", "")
        label = f"[{i+1}/{len(blogs)}] {blog.get('title', '')[:50]}"

        if source_id in id_map.get("blogs", {}):
            print(f"  {label} — already imported, skipping")
            dest_blog_id = id_map["blogs"][source_id]
        elif args.dry_run:
            print(f"  {label} — would create (handle: {handle})")
            dest_blog_id = None
        else:
            existing = client.get_blogs_by_handle(handle)
            if existing:
                dest_blog_id = existing[0]["id"]
                print(f"  {label} — already exists (id: {dest_blog_id}), mapping")
            else:
                blog_data = {"title": blog.get("title", ""), "handle": handle}
                created = client.create_blog(blog_data)
                dest_blog_id = created.get("id")
                print(f"  {label} — created (id: {dest_blog_id})")
            id_map.setdefault("blogs", {})[source_id] = dest_blog_id
            save_json(id_map, id_map_file)

        # Import articles for this blog
        blog_articles = [a for a in articles if str(a.get("_blog_id")) == str(blog["id"])]
        print(f"    Importing {len(blog_articles)} articles...")
        for j, article in enumerate(blog_articles):
            art_source_id = str(article["id"])
            art_label = f"    [{j+1}/{len(blog_articles)}] {article.get('title', '')[:50]}"

            if art_source_id in id_map.get("articles", {}):
                print(f"  {art_label} — already imported, skipping")
                continue

            if args.dry_run or dest_blog_id is None:
                print(f"  {art_label} — would create")
                continue

            art_data = {
                "title": article.get("title", ""),
                "body_html": article.get("body_html", ""),
                "summary_html": article.get("summary_html", ""),
                "tags": article.get("tags", ""),
                "published": article.get("published_at") is not None,
                "author": article.get("author", ""),
            }
            if article.get("image", {}).get("src"):
                art_data["image"] = {"src": article["image"]["src"]}

            created = client.create_article(dest_blog_id, art_data)
            dest_art_id = created.get("id")
            print(f"  {art_label} — created (id: {dest_art_id})")
            id_map.setdefault("articles", {})[art_source_id] = dest_art_id
            save_json(id_map, id_map_file)

    if not args.dry_run:
        save_json(id_map, id_map_file)

    print("\n--- Import Summary ---")
    print(f"  Products:    {len(id_map.get('products', {}))}")
    print(f"  Collections: {len(id_map.get('collections', {}))}")
    print(f"  Pages:       {len(id_map.get('pages', {}))}")
    print(f"  Blogs:       {len(id_map.get('blogs', {}))}")
    print(f"  Articles:    {len(id_map.get('articles', {}))}")
    if args.dry_run:
        print("  (dry run — nothing was created)")


if __name__ == "__main__":
    main()
