#!/usr/bin/env python3
"""Comprehensive audit of all 7 bundle products — check every attribute,
metafield, image, collection membership, and linkage is intact."""
import os
import json
import requests
from dotenv import load_dotenv
load_dotenv()

shop_url = os.environ["SAUDI_SHOP_URL"]
access_token = os.environ["SAUDI_ACCESS_TOKEN"]
api_url = f"https://{shop_url}/admin/api/2024-10/graphql.json"
rest_url = f"https://{shop_url}/admin/api/2024-10"
headers = {
    "X-Shopify-Access-Token": access_token,
    "Content-Type": "application/json",
}

def gql(query, variables=None):
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = requests.post(api_url, json=payload, headers=headers)
    return resp.json()

def rest_get(endpoint, params=None):
    resp = requests.get(f"{rest_url}/{endpoint}", headers=headers, params=params or {})
    return resp.json()

SYSTEM_IDS = [
    ("Hair Strength System", 9218580021481),
    ("Hair Wellness System", 9218580381929),
    ("Nurture System", 9218582610153),
    ("Scalp + Hair Revival System", 9218583560425),
    ("Hair Density System", 9218583527657),
    ("Hair Stimulation System", 9218584051945),
    ("Age-Well System", 9218583494889),
]

print("=" * 80)
print("COMPREHENSIVE BUNDLE PRODUCT AUDIT")
print("=" * 80)

for name, pid in SYSTEM_IDS:
    print(f"\n{'─' * 80}")
    print(f"  {name} (ID: {pid})")
    print(f"{'─' * 80}")

    # 1. REST product data (full)
    product = rest_get(f"products/{pid}.json").get("product", {})

    print(f"\n  BASIC ATTRIBUTES:")
    print(f"    Title:        {product.get('title')}")
    print(f"    Handle:       {product.get('handle')}")
    print(f"    Status:       {product.get('status')}")
    print(f"    Product Type: {product.get('product_type') or '(empty)'}")
    print(f"    Vendor:       {product.get('vendor')}")
    print(f"    Tags:         {product.get('tags') or '(none)'}")
    print(f"    Created:      {product.get('created_at')}")
    print(f"    Updated:      {product.get('updated_at')}")
    print(f"    Published:    {product.get('published_at') or '(unpublished)'}")

    body = product.get("body_html") or ""
    print(f"    Body HTML:    {len(body)} chars {'✓' if body else '✗ EMPTY'}")
    if body:
        print(f"                  (preview: {body[:100].replace(chr(10), ' ')}...)")

    # 2. Variants
    variants = product.get("variants", [])
    print(f"\n  VARIANTS ({len(variants)}):")
    for v in variants:
        print(f"    [{v['id']}] {v['title']} — price: {v.get('price')} — compare_at: {v.get('compare_at_price')} — sku: {v.get('sku')} — inventory: {v.get('inventory_quantity')}")

    # 3. Options
    options = product.get("options", [])
    print(f"\n  OPTIONS ({len(options)}):")
    for o in options:
        print(f"    {o['name']}: {o.get('values', [])}")

    # 4. Images
    images = product.get("images", [])
    print(f"\n  IMAGES ({len(images)}):")
    for img in images:
        print(f"    [{img['id']}] {img.get('alt') or '(no alt)'} — {img['src'][:80]}...")

    # 5. Metafields via REST
    mf_data = rest_get(f"products/{pid}/metafields.json")
    metafields = mf_data.get("metafields", [])
    print(f"\n  METAFIELDS ({len(metafields)}):")
    for mf in metafields:
        val_preview = str(mf.get("value", ""))[:100]
        print(f"    {mf['namespace']}.{mf['key']} ({mf['type']}): {val_preview}")

    # 6. Bundle components via GraphQL
    gql_result = gql(f"""
    {{
      product(id: "gid://shopify/Product/{pid}") {{
        bundleComponents(first: 10) {{
          edges {{
            node {{
              componentProduct {{ id title }}
              quantity
            }}
          }}
        }}
        hasVariantsThatRequiresComponents
      }}
    }}
    """)
    gql_product = gql_result.get("data", {}).get("product", {})
    components = gql_product.get("bundleComponents", {}).get("edges", [])
    has_components = gql_product.get("hasVariantsThatRequiresComponents", False)
    print(f"\n  BUNDLE COMPONENTS ({len(components)}) — hasVariantsThatRequiresComponents: {has_components}")
    for edge in components:
        node = edge["node"]
        cp = node["componentProduct"]
        print(f"    - {cp['title']} (qty: {node['quantity']})")

    if not components:
        print(f"    ✗ WARNING: NO BUNDLE COMPONENTS!")

    # 7. Collection memberships
    collects = rest_get("collects.json", {"product_id": pid}).get("collects", [])
    print(f"\n  COLLECTION MEMBERSHIPS ({len(collects)}):")
    for c in collects:
        # Get collection title
        coll = rest_get(f"collections/{c['collection_id']}.json").get("collection", {})
        print(f"    - {coll.get('title', '?')} (collection_id: {c['collection_id']})")

    # 8. SEO info via GraphQL
    seo_result = gql(f"""
    {{
      product(id: "gid://shopify/Product/{pid}") {{
        seo {{
          title
          description
        }}
        onlineStoreUrl
      }}
    }}
    """)
    seo_product = seo_result.get("data", {}).get("product", {})
    seo = seo_product.get("seo", {})
    print(f"\n  SEO:")
    print(f"    Title:       {seo.get('title') or '(default)'}")
    print(f"    Description: {(seo.get('description') or '(default)')[:100]}")
    print(f"    URL:         {seo_product.get('onlineStoreUrl') or '(not published)'}")

print(f"\n{'=' * 80}")
print("AUDIT COMPLETE")
print("=" * 80)
