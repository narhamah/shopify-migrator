#!/usr/bin/env python3
"""Fetch the store-specific IDs needed to recreate the Ingredient→Collection Flow.

Prints:
  1. The MetafieldDefinition GID for custom.ingredients on PRODUCT
  2. All Publication GIDs (sales channels) to use in publishablePublish

Run this against the SAUDI store.
"""

import os

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config


def main():
    load_dotenv()
    shop_url = config.get_dest_shop_url()
    access_token = config.get_dest_access_token()
    client = ShopifyClient(shop_url, access_token)

    # 1. Find the MetafieldDefinition ID for custom.ingredients on PRODUCT
    print("=== MetafieldDefinition: custom.ingredients (PRODUCT) ===\n")
    defs = client.get_metafield_definitions("PRODUCT")
    ingredients_def = None
    for d in defs:
        if d["namespace"] == "custom" and d["key"] == "ingredients":
            ingredients_def = d
            break

    if ingredients_def:
        print(f"  Found: {ingredients_def['id']}")
        print(f"  Name:  {ingredients_def['name']}")
        print(f"  Type:  {ingredients_def['type']['name']}")
    else:
        print("  NOT FOUND — make sure setup_store.py has been run first")
        print(f"  Available definitions: {[(d['namespace'], d['key']) for d in defs]}")

    # 2. Get all Publications (sales channels)
    print("\n=== Publications (Sales Channels) ===\n")
    query = """
    {
      publications(first: 50) {
        edges {
          node {
            id
            name
          }
        }
      }
    }
    """
    data = client._graphql(query)
    publications = [edge["node"] for edge in data["publications"]["edges"]]

    for pub in publications:
        print(f"  {pub['id']}  — {pub['name']}")

    # 3. Print the ready-to-use JSON for each Flow action
    if ingredients_def and publications:
        print("\n" + "=" * 60)
        print("READY-TO-USE FLOW CONFIGURATIONS")
        print("=" * 60)

        print("\n--- Action 1: collectionCreate ---")
        print("""{
  "input": {
    "title": "{{metaobject.name}}",
    "handle": "{{metaobject.system.handle}}",
    "ruleSet": {
      "appliedDisjunctively": false,
      "rules": [
        {
          "column": "PRODUCT_METAFIELD_DEFINITION",
          "relation": "EQUALS",
          "condition": "{{metaobject.system.id}}",
          "conditionObjectId": "%s"
        }
      ]
    }
  }
}""" % ingredients_def['id'])

        print("\n--- Action 2: publishablePublish ---")
        pub_entries = ",\n    ".join(
            '{\n      "publicationId": "%s"\n    }' % pub["id"]
            for pub in publications
        )
        print("""{
  "id": "{{sendAdminApiRequest.collection.id}}",
  "input": [
    %s
  ]
}""" % pub_entries)

        print("\n--- Action 3: metaobjectUpdate ---")
        print("""{
  "id": "{{metaobject.system.id}}",
  "metaobject": {
    "fields": [
      {
        "key": "collection",
        "value": "{{sendAdminApiRequest.collection.id}}"
      }
    ]
  }
}""")


if __name__ == "__main__":
    main()
