#!/usr/bin/env python3
"""Step 1: Export all content from the Spain Shopify store."""

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

    # Blogs + articles
    print("Fetching blogs...")
    blogs = client.get_blogs()
    save_json(blogs, os.path.join(output_dir, "blogs.json"))
    print(f"  Exported {len(blogs)} blogs")

    print("Fetching articles...")
    all_articles = []
    for blog in blogs:
        articles = client.get_articles(blog["id"])
        for article in articles:
            article["_blog_id"] = blog["id"]
            article["_blog_handle"] = blog.get("handle", "")
        all_articles.extend(articles)
        print(f"  Blog '{blog.get('title', '')}': {len(articles)} articles")
    save_json(all_articles, os.path.join(output_dir, "articles.json"))
    print(f"  Exported {len(all_articles)} articles total")

    # Summary
    print("\n--- Export Summary ---")
    print(f"  Products:    {len(products)}")
    print(f"  Collections: {len(collections)}")
    print(f"  Pages:       {len(pages)}")
    print(f"  Blogs:       {len(blogs)}")
    print(f"  Articles:    {len(all_articles)}")
    print(f"  Output:      {output_dir}/")


if __name__ == "__main__":
    main()
