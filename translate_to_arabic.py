#!/usr/bin/env python3
"""Step 4: Translate English content to Arabic for the Saudi store."""

import json
import os

from dotenv import load_dotenv

from translator import Translator


def load_json(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data, filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_or_init(filepath):
    if os.path.exists(filepath):
        return load_json(filepath)
    return []


def main():
    load_dotenv()
    api_key = os.environ["ANTHROPIC_API_KEY"]

    translator = Translator(api_key)
    input_dir = "data/english"
    output_dir = "data/arabic"
    os.makedirs(output_dir, exist_ok=True)

    # Translate products
    products_file = os.path.join(output_dir, "products.json")
    products = load_json(os.path.join(input_dir, "products.json"))
    translated_products = load_or_init(products_file)
    existing_ids = {p["id"] for p in translated_products}

    print(f"Translating products (EN → AR)... ({len(existing_ids)} already done)")
    for i, product in enumerate(products):
        if product["id"] in existing_ids:
            print(f"  [{i+1}/{len(products)}] Skipping (already translated): {product.get('title', '')[:50]}")
            continue
        print(f"  [{i+1}/{len(products)}] Translating: {product.get('title', '')[:50]}")
        translated = translator.translate_product(product, "English", "Arabic")
        translated_products.append(translated)
        save_json(translated_products, products_file)
    print(f"  Done: {len(translated_products)} products")

    # Translate collections
    collections_file = os.path.join(output_dir, "collections.json")
    collections = load_json(os.path.join(input_dir, "collections.json"))
    translated_collections = load_or_init(collections_file)
    existing_ids = {c["id"] for c in translated_collections}

    print(f"Translating collections (EN → AR)... ({len(existing_ids)} already done)")
    for i, collection in enumerate(collections):
        if collection["id"] in existing_ids:
            print(f"  [{i+1}/{len(collections)}] Skipping: {collection.get('title', '')[:50]}")
            continue
        print(f"  [{i+1}/{len(collections)}] Translating: {collection.get('title', '')[:50]}")
        translated = translator.translate_collection(collection, "English", "Arabic")
        translated_collections.append(translated)
        save_json(translated_collections, collections_file)
    print(f"  Done: {len(translated_collections)} collections")

    # Translate pages
    pages_file = os.path.join(output_dir, "pages.json")
    pages = load_json(os.path.join(input_dir, "pages.json"))
    translated_pages = load_or_init(pages_file)
    existing_ids = {p["id"] for p in translated_pages}

    print(f"Translating pages (EN → AR)... ({len(existing_ids)} already done)")
    for i, page in enumerate(pages):
        if page["id"] in existing_ids:
            print(f"  [{i+1}/{len(pages)}] Skipping: {page.get('title', '')[:50]}")
            continue
        print(f"  [{i+1}/{len(pages)}] Translating: {page.get('title', '')[:50]}")
        translated = translator.translate_page(page, "English", "Arabic")
        translated_pages.append(translated)
        save_json(translated_pages, pages_file)
    print(f"  Done: {len(translated_pages)} pages")

    # Translate articles
    articles_file = os.path.join(output_dir, "articles.json")
    articles = load_json(os.path.join(input_dir, "articles.json"))
    translated_articles = load_or_init(articles_file)
    existing_ids = {a["id"] for a in translated_articles}

    print(f"Translating articles (EN → AR)... ({len(existing_ids)} already done)")
    for i, article in enumerate(articles):
        if article["id"] in existing_ids:
            print(f"  [{i+1}/{len(articles)}] Skipping: {article.get('title', '')[:50]}")
            continue
        print(f"  [{i+1}/{len(articles)}] Translating: {article.get('title', '')[:50]}")
        translated = translator.translate_article(article, "English", "Arabic")
        translated_articles.append(translated)
        save_json(translated_articles, articles_file)
    print(f"  Done: {len(translated_articles)} articles")

    # Copy blogs metadata
    blogs = load_json(os.path.join(input_dir, "blogs.json"))
    save_json(blogs, os.path.join(output_dir, "blogs.json"))

    print("\n--- Translation Summary (EN → AR) ---")
    print(f"  Products:    {len(translated_products)}")
    print(f"  Collections: {len(translated_collections)}")
    print(f"  Pages:       {len(translated_pages)}")
    print(f"  Articles:    {len(translated_articles)}")
    print(f"  Output:      {output_dir}/")
    print("\nNote: Use these Arabic translation files with Shopify's translation API")
    print("or a translation app (Langify, Transcy, etc.) to add Arabic locale content.")


if __name__ == "__main__":
    main()
