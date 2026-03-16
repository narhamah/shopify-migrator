#!/usr/bin/env python3
"""Convert 7 system products into Shopify bundles using productBundleUpdate.

Each system product becomes a bundle of its 3 component products.
Uses the Shopify Admin GraphQL API (2024-10).
"""
import os
import sys
import json
import time
import requests
from dotenv import load_dotenv

load_dotenv()

shop_url = os.environ["SAUDI_SHOP_URL"]
access_token = os.environ["SAUDI_ACCESS_TOKEN"]
api_url = f"https://{shop_url}/admin/api/2024-10/graphql.json"
headers = {
    "X-Shopify-Access-Token": access_token,
    "Content-Type": "application/json",
}

# Bundle definitions: system handle -> list of component handles
BUNDLE_MAP = {
    "hair-strength-system": [
        "invigorating-shampoo",
        "repairing-hair-mask",
        "strengthening-scalp-serum",
    ],
    "hair-wellness-system": [
        "nourishing-shampoo",
        "hydrating-conditioner",
        "rejuvenating-scalp-serum",
    ],
    "nurture-system": [
        "nurture-shampoo",
        "nurture-conditioner",
        "nurture-leave-in-conditioner",
    ],
    "scalp-hair-revival-system": [
        "charcoal-salicylic-exfoliating-shampoo",
        "ghassoul-avocado-smoothing-conditioner",
        "cactus-red-seaweed-scalp-serum",
    ],
    "hair-density-system": [
        "scalp-prep-shampoo",
        "strand-thicken-conditioner",
        "follicle-boost-serum",
    ],
    "hair-stimulation-system": [
        "volumizing-shampoo",
        "thickening-conditioner",
        "follicle-stimulating-scalp-serum",
    ],
    "age-well-system": [
        "revitalizing-shampoo",
        "replenishing-conditioner",
        "scalp-support-serum",
    ],
}


def gql(query, variables=None):
    """Execute GraphQL query with retry on rate-limit."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    for attempt in range(4):
        resp = requests.post(api_url, json=payload, headers=headers)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 2 ** (attempt + 1)))
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data
    raise RuntimeError("Max retries exceeded")


def get_all_products():
    """Fetch all products with options via GraphQL pagination."""
    products = []
    cursor = None
    while True:
        after = f', after: "{cursor}"' if cursor else ""
        query = f"""
        {{
          products(first: 50{after}) {{
            edges {{
              cursor
              node {{
                id
                title
                handle
                options {{
                  id
                  name
                  values
                }}
              }}
            }}
            pageInfo {{
              hasNextPage
            }}
          }}
        }}
        """
        result = gql(query)
        edges = result["data"]["products"]["edges"]
        for edge in edges:
            products.append(edge["node"])
            cursor = edge["cursor"]
        if not result["data"]["products"]["pageInfo"]["hasNextPage"]:
            break
    return products


def create_bundle(system_product, component_products):
    """Convert a system product into a bundle using productBundleUpdate."""
    components = []
    for comp in component_products:
        option = comp["options"][0]  # All have single "Title" option
        components.append({
            "quantity": 1,
            "productId": comp["id"],
            "optionSelections": [
                {
                    "componentOptionId": option["id"],
                    "name": option["name"],
                    "values": option["values"],
                }
            ],
        })

    mutation = """
    mutation productBundleUpdate($input: ProductBundleUpdateInput!) {
      productBundleUpdate(input: $input) {
        productBundleOperation {
          id
          status
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    variables = {
        "input": {
            "productId": system_product["id"],
            "components": components,
        }
    }

    result = gql(mutation, variables)
    payload = result["data"]["productBundleUpdate"]
    if payload["userErrors"]:
        return None, payload["userErrors"]
    return payload["productBundleOperation"], None


def main():
    dry_run = "--dry-run" in sys.argv
    skip = set()
    # Hair Strength System already done in test
    if "--skip-first" in sys.argv:
        skip.add("hair-strength-system")

    print("Fetching all products from Saudi store...")
    products = get_all_products()
    handle_map = {p["handle"]: p for p in products}
    print(f"Found {len(products)} products.\n")

    results = []
    for system_handle, component_handles in BUNDLE_MAP.items():
        system = handle_map.get(system_handle)
        if not system:
            print(f"SKIP: System product '{system_handle}' not found")
            continue

        if system_handle in skip:
            print(f"SKIP: '{system_handle}' (already done)")
            continue

        components = []
        missing = False
        for ch in component_handles:
            comp = handle_map.get(ch)
            if not comp:
                print(f"ERROR: Component '{ch}' not found for system '{system_handle}'")
                missing = True
                break
            components.append(comp)

        if missing:
            continue

        comp_names = [c["title"] for c in components]
        print(f"{'[DRY RUN] ' if dry_run else ''}Converting: {system['title']}")
        print(f"  Components: {', '.join(comp_names)}")

        if dry_run:
            results.append({"system": system_handle, "status": "dry_run"})
            continue

        operation, errors = create_bundle(system, components)
        if errors:
            print(f"  ERRORS: {errors}")
            results.append({"system": system_handle, "status": "error", "errors": errors})
        else:
            print(f"  SUCCESS: Operation {operation['id']} — Status: {operation['status']}")
            results.append({
                "system": system_handle,
                "status": "success",
                "operation_id": operation["id"],
                "operation_status": operation["status"],
            })

        # Small delay between mutations to avoid rate limits
        time.sleep(0.5)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for r in results:
        status = r["status"]
        if status == "success":
            print(f"  OK  {r['system']} — {r['operation_id']}")
        elif status == "error":
            print(f"  ERR {r['system']} — {r['errors']}")
        else:
            print(f"  DRY {r['system']}")

    return results


if __name__ == "__main__":
    main()
