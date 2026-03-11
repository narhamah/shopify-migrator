#!/usr/bin/env python3
"""Investigate Arabic translation state on the live Shopify store via GraphQL.

Fetches translatable resources and checks which fields have Arabic translations,
which are missing, and which have misaligned content.

Usage:
    python investigate_translations.py
    python investigate_translations.py --type PRODUCT
    python investigate_translations.py --type METAFIELD --limit 5
    python investigate_translations.py --product-id 9218579857641
"""

import argparse
import json
import os
import re
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from tara_migrate.client.shopify_client import ShopifyClient


LOCALE = "ar"

# GraphQL queries
TRANSLATABLE_RESOURCES_QUERY = """
query($resourceType: TranslatableResourceType!, $first: Int!, $after: String) {
  translatableResources(resourceType: $resourceType, first: $first, after: $after) {
    edges {
      cursor
      node {
        resourceId
        translatableContent {
          key
          value
          digest
          locale
        }
        translations(locale: "ar") {
          key
          value
          outdated
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

TRANSLATABLE_BY_IDS_QUERY = """
query($resourceIds: [ID!]!, $first: Int!) {
  translatableResourcesByIds(resourceIds: $resourceIds, first: $first) {
    edges {
      node {
        resourceId
        translatableContent {
          key
          value
          digest
          locale
        }
        translations(locale: "ar") {
          key
          value
          outdated
        }
      }
    }
  }
}
"""


def has_arabic(text):
    if not text:
        return False
    arabic = len(re.findall(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", text))
    return arabic > 0


def extract_rich_text(text):
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None
    parts = []
    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "text" and "value" in node:
                parts.append(node["value"])
            for child in node.get("children", []):
                walk(child)
        elif isinstance(node, list):
            for item in node:
                walk(item)
    walk(data)
    return " ".join(parts) if parts else None


def analyze_resource(node):
    """Analyze a single translatable resource."""
    resource_id = node["resourceId"]
    content = node["translatableContent"]
    translations = {t["key"]: t for t in node["translations"]}

    fields = []
    for field in content:
        key = field["key"]
        value = field["value"] or ""
        digest = field["digest"]
        trans = translations.get(key)

        # Extract text from rich_text JSON for display
        display_value = value[:80]
        if value.startswith("{") and '"type"' in value:
            extracted = extract_rich_text(value)
            if extracted:
                display_value = f"[rich_text] {extracted[:70]}"

        if trans:
            trans_value = trans["value"] or ""
            trans_display = trans_value[:80]
            if trans_value.startswith("{") and '"type"' in trans_value:
                extracted = extract_rich_text(trans_value)
                if extracted:
                    trans_display = f"[rich_text] {extracted[:70]}"

            ar = has_arabic(trans_value) or (trans_value.startswith("{") and has_arabic(extract_rich_text(trans_value) or ""))
            outdated = trans.get("outdated", False)
            status = "OK" if ar else "NOT_ARABIC"
            if outdated:
                status += " [OUTDATED]"
            if trans_value == value and not ar:
                status = "IDENTICAL"
        else:
            trans_display = "(missing)"
            status = "MISSING"

        fields.append({
            "key": key,
            "value": display_value,
            "digest": digest,
            "translation": trans_display,
            "status": status,
        })

    return resource_id, fields


def main():
    parser = argparse.ArgumentParser(description="Investigate Arabic translations on Shopify")
    parser.add_argument("--type", default=None,
                        help="Resource type: PRODUCT, COLLECTION, METAFIELD, METAOBJECT, ONLINE_STORE_THEME")
    parser.add_argument("--product-id", default=None, help="Specific product GID number")
    parser.add_argument("--resource-ids", nargs="+", default=None,
                        help="Specific resource GIDs (e.g. gid://shopify/Product/123)")
    parser.add_argument("--limit", type=int, default=10, help="Resources per page (default: 10)")
    parser.add_argument("--all-pages", action="store_true", help="Fetch all pages")
    parser.add_argument("--json-out", default=None, help="Save results to JSON file")
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ.get("SAUDI_SHOP_URL")
    token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not token:
        print("ERROR: Set SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN in .env")
        sys.exit(1)

    client = ShopifyClient(shop_url, token)

    if args.product_id:
        gid = f"gid://shopify/Product/{args.product_id}"
        print(f"Fetching product {gid} and its metafields...\n")

        # Fetch the product
        data = client._graphql(TRANSLATABLE_BY_IDS_QUERY, {
            "resourceIds": [gid],
            "first": 1,
        })
        edges = data["translatableResourcesByIds"]["edges"]
        if not edges:
            print(f"Product {gid} not found")
            return

        resource_id, fields = analyze_resource(edges[0]["node"])
        print(f"=== {resource_id} ===")
        for f in fields:
            icon = {"OK": "✓", "MISSING": "✗", "IDENTICAL": "⚠", "NOT_ARABIC": "⚠"}.get(
                f["status"].split()[0], "?"
            )
            print(f"  {icon} [{f['status']:12s}] {f['key']:20s} | EN: {f['value'][:50]}")
            if f["translation"] != "(missing)":
                print(f"    {'':35s} | AR: {f['translation'][:50]}")

        # Also fetch metafields for this product
        print(f"\nFetching metafields for product {args.product_id}...")
        try:
            metafields = client.get_metafields("products", args.product_id)
            if metafields:
                mf_gids = [f"gid://shopify/Metafield/{mf['id']}" for mf in metafields]
                print(f"Found {len(metafields)} metafields, checking translations...\n")

                # Batch fetch in groups of 10
                for i in range(0, len(mf_gids), 10):
                    batch = mf_gids[i:i+10]
                    data = client._graphql(TRANSLATABLE_BY_IDS_QUERY, {
                        "resourceIds": batch,
                        "first": len(batch),
                    })
                    for edge in data["translatableResourcesByIds"]["edges"]:
                        rid, mf_fields = analyze_resource(edge["node"])
                        # Find metafield namespace/key
                        mf_match = next((m for m in metafields if f"gid://shopify/Metafield/{m['id']}" == rid), None)
                        ns_key = f"{mf_match['namespace']}.{mf_match['key']}" if mf_match else rid
                        for f in mf_fields:
                            icon = {"OK": "✓", "MISSING": "✗", "IDENTICAL": "⚠", "NOT_ARABIC": "⚠"}.get(
                                f["status"].split()[0], "?"
                            )
                            print(f"  {icon} [{f['status']:12s}] {ns_key:40s} | EN: {f['value'][:50]}")
                            if f["translation"] != "(missing)":
                                print(f"    {'':55s} | AR: {f['translation'][:50]}")
            else:
                print("  No metafields found via REST (may need GraphQL)")
        except Exception as e:
            print(f"  Error fetching metafields: {e}")

        return

    if args.resource_ids:
        print(f"Fetching {len(args.resource_ids)} specific resources...\n")
        data = client._graphql(TRANSLATABLE_BY_IDS_QUERY, {
            "resourceIds": args.resource_ids,
            "first": len(args.resource_ids),
        })
        for edge in data["translatableResourcesByIds"]["edges"]:
            resource_id, fields = analyze_resource(edge["node"])
            print(f"=== {resource_id} ===")
            for f in fields:
                icon = {"OK": "✓", "MISSING": "✗", "IDENTICAL": "⚠", "NOT_ARABIC": "⚠"}.get(
                    f["status"].split()[0], "?"
                )
                print(f"  {icon} [{f['status']:12s}] {f['key']:20s} | EN: {f['value'][:50]}")
                if f["translation"] != "(missing)":
                    print(f"    {'':35s} | AR: {f['translation'][:50]}")
        return

    if not args.type:
        # Summary mode: check all types
        resource_types = ["PRODUCT", "COLLECTION", "METAFIELD", "METAOBJECT",
                          "ONLINE_STORE_THEME", "PAGE"]
        print("=== TRANSLATION COVERAGE SUMMARY ===\n")

        for rtype in resource_types:
            try:
                data = client._graphql(TRANSLATABLE_RESOURCES_QUERY, {
                    "resourceType": rtype,
                    "first": 50,
                })
                edges = data["translatableResources"]["edges"]
                has_more = data["translatableResources"]["pageInfo"]["hasNextPage"]

                total_fields = 0
                translated_ok = 0
                missing = 0
                identical = 0
                not_arabic = 0
                outdated = 0

                for edge in edges:
                    _, fields = analyze_resource(edge["node"])
                    for f in fields:
                        if not f["value"] or f["value"] == "(empty)":
                            continue
                        total_fields += 1
                        s = f["status"]
                        if s.startswith("OK"):
                            translated_ok += 1
                        elif s == "MISSING":
                            missing += 1
                        elif s == "IDENTICAL":
                            identical += 1
                        elif s.startswith("NOT_ARABIC"):
                            not_arabic += 1
                        if "OUTDATED" in s:
                            outdated += 1

                pct = (translated_ok / total_fields * 100) if total_fields else 0
                more = "+" if has_more else ""
                print(f"  {rtype:25s} | {len(edges)}{more:2s} resources | "
                      f"{translated_ok}/{total_fields} fields OK ({pct:.0f}%) | "
                      f"missing={missing} identical={identical} not_arabic={not_arabic} "
                      f"outdated={outdated}")
            except Exception as e:
                print(f"  {rtype:25s} | ERROR: {e}")

        print("\nRun with --type <TYPE> for details, or --product-id <ID> for a specific product.")
        return

    # Detailed mode for specific type
    print(f"Fetching {args.type} translations...\n")
    cursor = None
    page = 0
    all_results = []

    while True:
        data = client._graphql(TRANSLATABLE_RESOURCES_QUERY, {
            "resourceType": args.type,
            "first": args.limit,
            "after": cursor,
        })
        edges = data["translatableResources"]["edges"]
        page_info = data["translatableResources"]["pageInfo"]

        for edge in edges:
            resource_id, fields = analyze_resource(edge["node"])
            all_results.append({"resourceId": resource_id, "fields": fields})

            print(f"=== {resource_id} ===")
            for f in fields:
                if not f["value"]:
                    continue
                icon = {"OK": "✓", "MISSING": "✗", "IDENTICAL": "⚠", "NOT_ARABIC": "⚠"}.get(
                    f["status"].split()[0], "?"
                )
                print(f"  {icon} [{f['status']:12s}] {f['key']:20s} | {f['value'][:60]}")
                if f["translation"] != "(missing)" and f["status"] != "OK":
                    print(f"    {'':35s} | AR: {f['translation'][:60]}")
            print()

        page += 1
        if not page_info["hasNextPage"] or not args.all_pages:
            break
        cursor = page_info["endCursor"]

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)
        print(f"\nSaved to {args.json_out}")


if __name__ == "__main__":
    main()
