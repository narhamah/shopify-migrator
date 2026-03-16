#!/usr/bin/env python3
"""Step 1: Export all content from the source Shopify store.

Exports products (with metafields), collections, pages, blogs, articles
(with metafields), and metaobjects (definitions + entries).
Also builds a consolidated relations map (relations.json) capturing all
cross-references between resources.
"""

import json
import os
import re

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, save_json


REFERENCE_TYPES = {
    "metaobject_reference",
    "list.metaobject_reference",
    "product_reference",
    "list.product_reference",
    "collection_reference",
    "list.collection_reference",
    "page_reference",
    "list.page_reference",
    "article_reference",
    "list.article_reference",
}

GID_PATTERN = re.compile(r"gid://shopify/(\w+)/(\d+)")


def _parse_gid(gid_str):
    """Extract (type, id) from a Shopify GID string."""
    m = GID_PATTERN.match(str(gid_str))
    return (m.group(1), m.group(2)) if m else None


def _extract_gids(value):
    """Extract GID strings from a metafield/metaobject field value."""
    if not value:
        return []
    value = str(value).strip()
    if value.startswith("["):
        try:
            return [g for g in json.loads(value) if isinstance(g, str) and g.startswith("gid://")]
        except (json.JSONDecodeError, TypeError):
            return []
    if value.startswith("gid://"):
        return [value]
    return []


def build_relations(products, articles, all_metaobjects, collects):
    """Build a consolidated map of all cross-references between resources.

    Returns a list of relation dicts:
      {from_type, from_id, from_handle, field, to_type, to_id}
    """
    relations = []

    # Product → metaobject/product/collection refs (via metafields)
    for product in products:
        pid = product.get("id")
        handle = product.get("handle", "")
        for mf in product.get("metafields", []):
            mf_type = mf.get("type", "")
            if mf_type not in REFERENCE_TYPES:
                continue
            field_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
            for gid in _extract_gids(mf.get("value")):
                parsed = _parse_gid(gid)
                if parsed:
                    relations.append({
                        "from_type": "Product",
                        "from_id": pid,
                        "from_handle": handle,
                        "field": field_key,
                        "to_type": parsed[0],
                        "to_id": gid,
                    })

    # Article → metaobject/product/article refs (via metafields)
    for article in articles:
        aid = article.get("id")
        handle = article.get("handle", "")
        for mf in article.get("metafields", []):
            mf_type = mf.get("type", "")
            if mf_type not in REFERENCE_TYPES:
                continue
            field_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
            for gid in _extract_gids(mf.get("value")):
                parsed = _parse_gid(gid)
                if parsed:
                    relations.append({
                        "from_type": "Article",
                        "from_id": aid,
                        "from_handle": handle,
                        "field": field_key,
                        "to_type": parsed[0],
                        "to_id": gid,
                    })

    # Metaobject → metaobject/product/collection refs (via fields)
    for mo_type, type_data in all_metaobjects.items():
        defn = type_data.get("definition", {})
        # Build field type lookup from definition
        field_types = {}
        for fd in defn.get("fieldDefinitions", []):
            field_types[fd["key"]] = fd.get("type", {}).get("name", "")

        for obj in type_data.get("objects", []):
            obj_id = obj.get("id", "")
            obj_handle = obj.get("handle", "")
            for field in obj.get("fields", []):
                fkey = field.get("key", "")
                ftype = field.get("type", field_types.get(fkey, ""))
                if ftype not in REFERENCE_TYPES:
                    continue
                for gid in _extract_gids(field.get("value")):
                    parsed = _parse_gid(gid)
                    if parsed:
                        relations.append({
                            "from_type": f"Metaobject:{mo_type}",
                            "from_id": obj_id,
                            "from_handle": obj_handle,
                            "field": fkey,
                            "to_type": parsed[0],
                            "to_id": gid,
                        })

    # Collection membership (product ↔ collection)
    for collect in collects:
        relations.append({
            "from_type": "Product",
            "from_id": collect["product_id"],
            "from_handle": "",
            "field": "_collection_membership",
            "to_type": "Collection",
            "to_id": collect["collection_id"],
        })

    return relations


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def main():
    load_dotenv()
    shop_url = config.get_source_shop_url()
    access_token = config.get_source_access_token()

    client = ShopifyClient(shop_url, access_token)
    output_dir = "data/source_export"
    ensure_dir(output_dir)

    # Shop info (optional — may fail if read_shop scope is missing)
    print("Fetching shop info...")
    try:
        shop = client.get_shop()
        save_json(shop, os.path.join(output_dir, "shop.json"))
        print(f"  Shop: {shop.get('name', 'N/A')}")
    except Exception as e:
        print(f"  Skipped (scope missing): {e}")

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

    # Product metafield definitions (needed for smart collection rule remapping)
    print("Fetching product metafield definitions...")
    try:
        product_mf_defs = client.get_metafield_definitions("PRODUCT")
        save_json(product_mf_defs, os.path.join(output_dir, "product_metafield_definitions.json"))
        print(f"  Exported {len(product_mf_defs)} product metafield definitions")
    except Exception as e:
        print(f"  Skipped: {e}")

    # Collection membership (which products belong to which collections)
    print("Fetching collection membership...")
    all_collects = []
    for i, collection in enumerate(collections):
        cid = collection["id"]
        print(f"  Collection {i+1}/{len(collections)}: {collection.get('title', '')[:50]}")
        try:
            product_ids = client.get_collection_product_ids(cid)
            for pid in product_ids:
                all_collects.append({"collection_id": cid, "product_id": pid})
        except Exception as e:
            print(f"    Skipped: {e}")
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

    # Build consolidated relations map
    print("Building relations map...")
    relations = build_relations(products, all_articles, all_metaobjects, all_collects)
    save_json(relations, os.path.join(output_dir, "relations.json"))

    # Summarize relations by type
    rel_summary = {}
    for r in relations:
        key = f"{r['from_type']} → {r['to_type']} ({r['field']})"
        rel_summary[key] = rel_summary.get(key, 0) + 1
    if rel_summary:
        print(f"  {len(relations)} relations found:")
        for key, count in sorted(rel_summary.items(), key=lambda x: -x[1]):
            print(f"    {key}: {count}")

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
    print(f"  Relations:      {len(relations)}")
    print(f"  Redirects:      {len(redirects)}")
    print(f"  Price rules:    {len(price_rules)}")
    print(f"  Policies:       {len(policies)}")
    print(f"  Output:         {output_dir}/")


if __name__ == "__main__":
    main()
