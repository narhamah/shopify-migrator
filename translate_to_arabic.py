#!/usr/bin/env python3
"""Step 4: Translate English content to Arabic.

Translates products (with metafields), collections, pages, articles
(with metafields), and metaobjects from English to Arabic.
Saves progress after each item for resumability.
"""

import json
import os

from dotenv import load_dotenv

from translator import Translator


from utils import load_json, save_json


def load_or_init(filepath):
    if os.path.exists(filepath):
        return load_json(filepath)
    return []


def main():
    load_dotenv()
    api_key = os.environ["OPENAI_API_KEY"]

    translator = Translator(api_key)
    input_dir = "data/english"
    output_dir = "data/arabic"
    os.makedirs(output_dir, exist_ok=True)

    # --- Products ---
    products_file = os.path.join(output_dir, "products.json")
    products = load_json(os.path.join(input_dir, "products.json"))
    translated_products = load_or_init(products_file)
    existing_ids = {p["id"] for p in translated_products}

    print(f"Translating products (EN → AR)... ({len(existing_ids)} already done)")
    for i, product in enumerate(products):
        if product["id"] in existing_ids:
            print(f"  [{i+1}/{len(products)}] Skipping: {product.get('title', '')[:50]}")
            continue
        print(f"  [{i+1}/{len(products)}] Translating: {product.get('title', '')[:50]}")
        translated = translator.translate_product(product, "English", "Arabic")
        translated_products.append(translated)
        save_json(translated_products, products_file)
    print(f"  Done: {len(translated_products)} products")

    # --- Collections ---
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

    # --- Pages ---
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

    # --- Articles ---
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

    # --- Metaobjects ---
    metaobjects_input = os.path.join(input_dir, "metaobjects.json")
    mo_count = 0
    if os.path.exists(metaobjects_input):
        metaobjects_file = os.path.join(output_dir, "metaobjects.json")
        all_metaobjects = load_json(metaobjects_input)

        translated_metaobjects = {}
        if os.path.exists(metaobjects_file):
            translated_metaobjects = load_json(metaobjects_file)

        for mo_type, type_data in all_metaobjects.items():
            objects = type_data.get("objects", [])
            if not objects:
                continue

            if mo_type not in translated_metaobjects:
                translated_metaobjects[mo_type] = {
                    "definition": type_data["definition"],
                    "objects": [],
                }

            existing_handles = {
                o.get("handle") for o in translated_metaobjects[mo_type].get("objects", [])
            }

            print(f"Translating metaobjects '{mo_type}' (EN → AR)... ({len(existing_handles)} already done)")
            for j, obj in enumerate(objects):
                handle = obj.get("handle", "")
                if handle in existing_handles:
                    print(f"  [{j+1}/{len(objects)}] Skipping: {handle}")
                    continue
                print(f"  [{j+1}/{len(objects)}] Translating: {handle}")
                translated = translator.translate_metaobject(obj, "English", "Arabic")
                translated_metaobjects[mo_type]["objects"].append(translated)
                save_json(translated_metaobjects, metaobjects_file)
                mo_count += 1

        # Copy definitions
        defs_input = os.path.join(input_dir, "metaobject_definitions.json")
        if os.path.exists(defs_input):
            save_json(load_json(defs_input), os.path.join(output_dir, "metaobject_definitions.json"))

    print("\n--- Translation Summary (EN → AR) ---")
    print(f"  Products:    {len(translated_products)}")
    print(f"  Collections: {len(translated_collections)}")
    print(f"  Pages:       {len(translated_pages)}")
    print(f"  Articles:    {len(translated_articles)}")
    if mo_count or os.path.exists(metaobjects_input):
        print(f"  Metaobjects: {mo_count} newly translated")
    print(f"  Output:      {output_dir}/")


if __name__ == "__main__":
    main()
