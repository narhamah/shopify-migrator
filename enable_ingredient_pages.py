#!/usr/bin/env python3
"""Enable ingredient metaobject pages on the Saudi store.

In Shopify, metaobjects can have their own web pages when the definition
has the 'renderable' capability enabled. This gives each ingredient a URL
like /pages/ingredient/{handle}.

This script:
  1. Enables 'renderable' capability on the 'ingredient' metaobject definition
  2. Publishes all ingredient metaobjects to the Online Store channel

Usage:
    python enable_ingredient_pages.py --dry-run    # Preview
    python enable_ingredient_pages.py              # Run live
"""

import argparse
import os
import time

from dotenv import load_dotenv

from shopify_client import ShopifyClient


def enable_renderable(client, dry_run=False):
    """Enable renderable capability on the ingredient metaobject definition."""
    print("\n--- Step 1: Enable renderable capability ---")

    # Find the ingredient definition
    defs = client.get_metaobject_definitions()
    ingredient_def = None
    for d in defs:
        if d["type"] == "ingredient":
            ingredient_def = d
            break

    if not ingredient_def:
        print("  ERROR: No 'ingredient' metaobject definition found on Saudi store")
        return None

    def_id = ingredient_def["id"]
    print(f"  Found ingredient definition: {def_id}")

    if dry_run:
        print("  Would enable renderable capability")
        return def_id

    # Update definition to enable renderable capability
    # renderable needs a metaobjectThumbnailField set for SEO
    update_data = {
        "capabilities": {
            "renderable": {"enabled": True},
            "publishable": {"enabled": True},
        },
    }

    try:
        result = client.update_metaobject_definition(def_id, update_data)
        print(f"  Enabled renderable capability on '{result['type']}'")
    except Exception as e:
        err_msg = str(e)
        if "already" in err_msg.lower():
            print("  Renderable already enabled")
        else:
            print(f"  Error enabling renderable: {e}")
            # Try without renderable — it may already be enabled
            # and just needs publishing

    return def_id


def publish_metaobjects(client, dry_run=False):
    """Publish all ingredient metaobjects by setting publishable status to ACTIVE.

    Uses metaobjectUpdate to set capabilities.publishable.status = ACTIVE,
    which does NOT require 'read_publications' scope.
    """
    print("\n--- Step 2: Publish ingredient metaobjects ---")

    # Get all ingredient metaobjects
    ingredients = client.get_metaobjects("ingredient")
    print(f"  Found {len(ingredients)} ingredient metaobjects")

    if dry_run:
        for ing in ingredients:
            name_field = next((f for f in ing.get("fields", []) if f["key"] == "name"), None)
            name = name_field["value"] if name_field else ing["handle"]
            print(f"    Would publish: {name} ({ing['handle']})")
        return

    query = """
    mutation metaobjectUpdate($id: ID!, $metaobject: MetaobjectUpdateInput!) {
      metaobjectUpdate(id: $id, metaobject: $metaobject) {
        metaobject {
          id
          handle
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    published = 0
    errors = 0
    for ing in ingredients:
        name_field = next((f for f in ing.get("fields", []) if f["key"] == "name"), None)
        name = name_field["value"] if name_field else ing["handle"]

        try:
            data = client._graphql(query, {
                "id": ing["id"],
                "metaobject": {
                    "capabilities": {
                        "publishable": {
                            "status": "ACTIVE"
                        }
                    }
                }
            })
            result = data["metaobjectUpdate"]
            if result["userErrors"]:
                err_msgs = [e["message"] for e in result["userErrors"]]
                print(f"  Error publishing '{name}': {err_msgs}")
                errors += 1
            else:
                published += 1
                if published % 10 == 0:
                    print(f"  Published {published}/{len(ingredients)}...")
            time.sleep(0.2)
        except Exception as e:
            print(f"  Error publishing '{name}': {e}")
            errors += 1

    print(f"  Published: {published}, Errors: {errors}")


def check_theme_template(client):
    """Check if theme has a metaobject/ingredient template."""
    print("\n--- Step 3: Check theme template ---")

    try:
        theme_id = client.get_main_theme_id()
        if not theme_id:
            print("  WARNING: No main theme found")
            return

        # Check for metaobject ingredient template
        template_key = "templates/metaobject/ingredient.json"
        try:
            asset = client.get_asset(theme_id, template_key)
            if asset:
                print(f"  Template exists: {template_key}")
                return
        except Exception:
            pass

        # Also check .liquid variant
        template_key_liquid = "templates/metaobject.ingredient.liquid"
        try:
            asset = client.get_asset(theme_id, template_key_liquid)
            if asset:
                print(f"  Template exists: {template_key_liquid}")
                return
        except Exception:
            pass

        print(f"  WARNING: No metaobject/ingredient template found in theme!")
        print(f"  The theme needs a template at '{template_key}' to render ingredient pages.")
        print(f"  You can create this in the Shopify Theme Editor:")
        print(f"    1. Go to Online Store → Themes → Customize")
        print(f"    2. In the template dropdown, create a new template for 'metaobject/ingredient'")
        print(f"    3. Design the ingredient page layout with sections")

    except Exception as e:
        print(f"  Error checking theme: {e}")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Enable ingredient metaobject pages on Saudi store")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without making changes")
    args = parser.parse_args()

    saudi_url = os.environ.get("SAUDI_SHOP_URL")
    saudi_token = os.environ.get("SAUDI_ACCESS_TOKEN")

    if not saudi_url or not saudi_token:
        print("ERROR: Set SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN in .env")
        return

    saudi = ShopifyClient(saudi_url, saudi_token)

    print("=" * 60)
    print("ENABLE INGREDIENT PAGES")
    print("=" * 60)
    print(f"  Store: {saudi_url}")
    print(f"  Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    enable_renderable(saudi, args.dry_run)
    publish_metaobjects(saudi, args.dry_run)
    check_theme_template(saudi)

    print("\n" + "=" * 60)
    print("DONE")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Ensure the theme has a 'metaobject/ingredient' template")
    print("  2. Ingredient pages will be at /pages/ingredient/{handle}")
    print("  3. The ingredient cards should link to these URLs")


if __name__ == "__main__":
    main()
