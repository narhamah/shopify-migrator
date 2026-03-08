#!/usr/bin/env python3
"""Step 1: Export all content from the Spain Shopify store.

Exports products (with metafields), collections, pages, blogs, articles
(with metafields), and metaobjects (definitions + entries).
"""

import json
import os

from dotenv import load_dotenv

from shopify_client import ShopifyClient


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def save_json(data, filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    load_dotenv()
    shop_url = os.environ["SPAIN_SHOP_URL"]
    access_token = os.environ["SPAIN_ACCESS_TOKEN"]

    client = ShopifyClient(shop_url, access_token)
    output_dir = "data/spain_export"
    ensure_dir(output_dir)

    # Shop info
    print("Fetching shop info...")
    shop = client.get_shop()
    save_json(shop, os.path.join(output_dir, "shop.json"))
    print(f"  Shop: {shop.get('name', 'N/A')}")

    # Products + metafields
    print("Fetching products...")
    products = client.get_products()
    for i, product in enumerate(products):
        print(f"  Fetching metafields for product {i+1}/{len(products)}: {product.get('title', '')[:50]}")
        metafields = client.get_metafields("products", product["id"])
        product["metafields"] = metafields
    save_json(products, os.path.join(output_dir, "products.json"))
    print(f"  Exported {len(products)} products")

    # Collections
    print("Fetching collections...")
    collections = client.get_collections()
    save_json(collections, os.path.join(output_dir, "collections.json"))
    print(f"  Exported {len(collections)} collections")

    # Pages
    print("Fetching pages...")
    pages = client.get_pages()
    save_json(pages, os.path.join(output_dir, "pages.json"))
    print(f"  Exported {len(pages)} pages")

    # Blogs + articles (with metafields)
    print("Fetching blogs...")
    blogs = client.get_blogs()
    save_json(blogs, os.path.join(output_dir, "blogs.json"))
    print(f"  Exported {len(blogs)} blogs")

    print("Fetching articles...")
    all_articles = []
    for blog in blogs:
        articles = client.get_articles(blog["id"])
        for j, article in enumerate(articles):
            article["_blog_id"] = blog["id"]
            article["_blog_handle"] = blog.get("handle", "")
            print(f"  Fetching metafields for article {j+1}/{len(articles)}: {article.get('title', '')[:50]}")
            metafields = client.get_metafields("articles", article["id"])
            article["metafields"] = metafields
        all_articles.extend(articles)
        print(f"  Blog '{blog.get('title', '')}': {len(articles)} articles")
    save_json(all_articles, os.path.join(output_dir, "articles.json"))
    print(f"  Exported {len(all_articles)} articles total")

    # Metaobject definitions + entries (via GraphQL)
    print("Fetching metaobject definitions...")
    definitions = client.get_metaobject_definitions()
    save_json(definitions, os.path.join(output_dir, "metaobject_definitions.json"))
    print(f"  Found {len(definitions)} metaobject types")

    print("Fetching metaobjects...")
    all_metaobjects = {}
    total_count = 0
    for defn in definitions:
        mo_type = defn["type"]
        objects = client.get_metaobjects(mo_type)
        all_metaobjects[mo_type] = {
            "definition": defn,
            "objects": objects,
        }
        total_count += len(objects)
        print(f"  Type '{mo_type}': {len(objects)} objects")
    save_json(all_metaobjects, os.path.join(output_dir, "metaobjects.json"))
    print(f"  Exported {total_count} metaobjects across {len(definitions)} types")

    # Collection membership (which products belong to which collections)
    print("Fetching collection membership (collects)...")
    all_collects = []
    for collection in collections:
        collects = client.get_collects(collection_id=collection["id"])
        all_collects.extend(collects)
    save_json(all_collects, os.path.join(output_dir, "collects.json"))
    print(f"  Exported {len(all_collects)} product-collection links")

    # URL Redirects
    print("Fetching redirects...")
    redirects = client.get_redirects()
    save_json(redirects, os.path.join(output_dir, "redirects.json"))
    print(f"  Exported {len(redirects)} redirects")

    # Store policies
    print("Fetching policies...")
    policies = client.get_policies()
    save_json(policies, os.path.join(output_dir, "policies.json"))
    print(f"  Exported {len(policies)} policies")

    # Price rules and discount codes
    print("Fetching price rules and discount codes...")
    price_rules = client.get_price_rules()
    for rule in price_rules:
        rule["discount_codes"] = client.get_discount_codes(rule["id"])
    save_json(price_rules, os.path.join(output_dir, "price_rules.json"))
    print(f"  Exported {len(price_rules)} price rules")

    # SEO metafields for collections and pages
    print("Fetching SEO metafields for collections...")
    for i, collection in enumerate(collections):
        metafields = client.get_metafields("collections", collection["id"])
        collection["metafields"] = metafields
    save_json(collections, os.path.join(output_dir, "collections.json"))

    print("Fetching SEO metafields for pages...")
    for i, page in enumerate(pages):
        metafields = client.get_metafields("pages", page["id"])
        page["metafields"] = metafields
    save_json(pages, os.path.join(output_dir, "pages.json"))

    # Summary
    print("\n--- Export Summary ---")
    print(f"  Products:       {len(products)}")
    print(f"  Collections:    {len(collections)}")
    print(f"  Pages:          {len(pages)}")
    print(f"  Blogs:          {len(blogs)}")
    print(f"  Articles:       {len(all_articles)}")
    print(f"  Metaobj types:  {len(definitions)}")
    print(f"  Metaobjects:    {total_count}")
    print(f"  Collects:       {len(all_collects)}")
    print(f"  Redirects:      {len(redirects)}")
    print(f"  Price rules:    {len(price_rules)}")
    print(f"  Policies:       {len(policies)}")
    print(f"  Output:         {output_dir}/")


if __name__ == "__main__":
    main()
