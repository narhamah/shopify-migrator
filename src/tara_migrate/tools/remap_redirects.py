#!/usr/bin/env python3
"""Remap URL redirects from source store to destination store.

The source store has redirects from old Magento URLs to Spanish Shopify URLs.
This script:
  1. Builds a map of Spanish handles → English handles from translated data
  2. For each redirect, keeps the source path (old Magento URL) as-is
  3. Looks up the Spanish Shopify target to find the corresponding English URL
  4. Outputs remapped redirects ready for import into the destination store

Usage:
    python remap_redirects.py
"""

import os

from tara_migrate.core import load_json, save_json


def build_handle_map(spain_dir, english_dir):
    """Build a map of Spanish Shopify URLs → English Shopify URLs.

    Returns a dict like:
        "/products/champu-densificante" → "/products/densifying-shampoo"
        "/collections/accesorios" → "/collections/accessories"
    """
    handle_map = {}

    resource_files = [
        ("products.json", "products", "handle"),
        ("collections.json", "collections", "handle"),
        ("pages.json", "pages", "handle"),
    ]

    for filename, resource_type, handle_key in resource_files:
        spain_path = os.path.join(spain_dir, filename)
        english_path = os.path.join(english_dir, filename)

        if not os.path.exists(spain_path) or not os.path.exists(english_path):
            continue

        source_items = load_json(spain_path)
        english_items = load_json(english_path)

        # Build ID → English handle lookup
        en_by_id = {item["id"]: item.get(handle_key, "") for item in english_items}

        for item in source_items:
            es_handle = item.get(handle_key, "")
            en_handle = en_by_id.get(item["id"], "")
            if es_handle and en_handle:
                es_url = f"/{resource_type}/{es_handle}"
                en_url = f"/{resource_type}/{en_handle}"
                handle_map[es_url] = en_url

    # Blogs and articles
    source_blogs_path = os.path.join(spain_dir, "blogs.json")
    english_blogs_path = os.path.join(english_dir, "blogs.json")
    if os.path.exists(source_blogs_path) and os.path.exists(english_blogs_path):
        source_blogs = load_json(source_blogs_path)
        english_blogs = load_json(english_blogs_path)
        en_blogs_by_id = {b["id"]: b.get("handle", "") for b in english_blogs}
        for blog in source_blogs:
            es_handle = blog.get("handle", "")
            en_handle = en_blogs_by_id.get(blog["id"], "")
            if es_handle and en_handle:
                handle_map[f"/blogs/{es_handle}"] = f"/blogs/{en_handle}"

    source_articles_path = os.path.join(spain_dir, "articles.json")
    english_articles_path = os.path.join(english_dir, "articles.json")
    if os.path.exists(source_articles_path) and os.path.exists(english_articles_path):
        source_articles = load_json(source_articles_path)
        english_articles = load_json(english_articles_path)
        en_articles_by_id = {a["id"]: a for a in english_articles}
        # Build blog ID → English blog handle map
        en_blog_handles = {}
        if os.path.exists(english_blogs_path):
            for b in load_json(english_blogs_path):
                en_blog_handles[b["id"]] = b.get("handle", "")

        for article in source_articles:
            es_handle = article.get("handle", "")
            en_article = en_articles_by_id.get(article["id"])
            if es_handle and en_article:
                en_handle = en_article.get("handle", "")
                blog_id = article.get("_blog_id")
                es_blog_handle = article.get("_blog_handle", "")
                en_blog_handle = en_blog_handles.get(blog_id, es_blog_handle)
                if en_handle:
                    handle_map[f"/blogs/{es_blog_handle}/{es_handle}"] = f"/blogs/{en_blog_handle}/{en_handle}"

    return handle_map


def remap_target(target, handle_map):
    """Look up a Spanish Shopify target URL in the handle map.

    Returns the English URL if found, or None if no match.
    """
    # Normalize: strip trailing slashes, lowercase
    normalized = target.rstrip("/")

    # Direct match
    if normalized in handle_map:
        return handle_map[normalized]

    # Try without query string
    base = normalized.split("?")[0]
    if base in handle_map:
        return handle_map[base]

    return None


def main():
    spain_dir = "data/source_export"
    english_dir = "data/english"
    output_dir = "data/english"

    redirects_path = os.path.join(spain_dir, "redirects.json")
    if not os.path.exists(redirects_path):
        print("No redirects.json found in Spain export.")
        return

    redirects = load_json(redirects_path)
    print(f"Found {len(redirects)} redirects from source store")

    # Build the Spanish → English handle map
    handle_map = build_handle_map(spain_dir, english_dir)
    print(f"Built handle map with {len(handle_map)} entries")

    remapped = []
    unmatched = []

    for redirect in redirects:
        path = redirect.get("path", "")
        target = redirect.get("target", "")

        new_target = remap_target(target, handle_map)

        if new_target:
            remapped.append({
                "path": path,
                "target": new_target,
            })
        else:
            unmatched.append({
                "path": path,
                "target": target,
                "reason": "no matching English handle found",
            })

    # Save remapped redirects
    output_path = os.path.join(output_dir, "redirects.json")
    save_json(remapped, output_path)
    print(f"\nRemapped: {len(remapped)} redirects → {output_path}")

    if unmatched:
        unmatched_path = os.path.join(output_dir, "redirects_unmatched.json")
        save_json(unmatched, unmatched_path)
        print(f"Unmatched: {len(unmatched)} redirects → {unmatched_path}")
        print("\nUnmatched targets (review manually):")
        for u in unmatched[:20]:
            print(f"  {u['path']} → {u['target']}")
        if len(unmatched) > 20:
            print(f"  ... and {len(unmatched) - 20} more")


if __name__ == "__main__":
    main()
