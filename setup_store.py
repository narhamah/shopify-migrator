#!/usr/bin/env python3
"""Step 0: Set up the destination store schema.

Examines the Saudi store and creates any missing:
  - Metaobject definitions (benefit, faq_entry, blog_author, ingredient)
  - Product metafield definitions (19 fields)
  - Article metafield definitions (12 fields)

Run this BEFORE import_english.py. It is safe to re-run — it skips
anything that already exists.
"""

import argparse
import os

from dotenv import load_dotenv

from shopify_client import ShopifyClient


# =====================================================================
# Metaobject definitions — ordered by dependency
# =====================================================================
METAOBJECT_DEFINITIONS = [
    {
        "type": "benefit",
        "name": "Benefit",
        "access": {"storefront": "PUBLIC_READ"},
        "fieldDefinitions": [
            {"key": "title", "name": "Title", "type": "single_line_text_field",
             "validations": [{"name": "min", "value": "1"}]},
            {"key": "description", "name": "Description", "type": "single_line_text_field"},
            {"key": "category", "name": "Category", "type": "single_line_text_field"},
            {"key": "icon_label", "name": "Icon Label", "type": "single_line_text_field"},
        ],
    },
    {
        "type": "faq_entry",
        "name": "FAQ Entry",
        "access": {"storefront": "PUBLIC_READ"},
        "fieldDefinitions": [
            {"key": "question", "name": "Question", "type": "single_line_text_field",
             "validations": [{"name": "min", "value": "1"}]},
            {"key": "answer", "name": "Answer", "type": "rich_text_field"},
        ],
    },
    {
        "type": "blog_author",
        "name": "Blog Author",
        "access": {"storefront": "PUBLIC_READ"},
        "fieldDefinitions": [
            {"key": "name", "name": "Name", "type": "single_line_text_field",
             "validations": [{"name": "min", "value": "1"}]},
            {"key": "bio", "name": "Bio", "type": "single_line_text_field"},
            {"key": "avatar", "name": "Avatar", "type": "file_reference"},
        ],
    },
    {
        "type": "ingredient",
        "name": "Ingredient",
        "access": {"storefront": "PUBLIC_READ"},
        "displayNameKey": "name",
        "fieldDefinitions": [
            {"key": "name", "name": "Name", "type": "single_line_text_field",
             "validations": [{"name": "min", "value": "1"}]},
            {"key": "inci_name", "name": "INCI Name", "type": "single_line_text_field"},
            {"key": "benefits", "name": "Benefits", "type": "list.metaobject_reference",
             "validations": [{"name": "metaobject_definition_id", "value": "RESOLVE:benefit"}]},
            {"key": "one_line_benefit", "name": "One-Line Benefit", "type": "single_line_text_field"},
            {"key": "description", "name": "Description", "type": "rich_text_field"},
            {"key": "source", "name": "Source", "type": "single_line_text_field"},
            {"key": "origin", "name": "Origin", "type": "single_line_text_field"},
            {"key": "category", "name": "Category", "type": "single_line_text_field"},
            {"key": "concern", "name": "Concern", "type": "single_line_text_field"},
            {"key": "image", "name": "Image", "type": "file_reference"},
            {"key": "icon", "name": "Icon", "type": "file_reference"},
            {"key": "science_images", "name": "Science Images", "type": "list.file_reference"},
            {"key": "is_hero", "name": "Is Hero", "type": "boolean"},
            {"key": "sort_order", "name": "Sort Order", "type": "number_integer"},
            {"key": "collection", "name": "Collection", "type": "collection_reference"},
        ],
    },
]

# =====================================================================
# Product metafield definitions (19 fields)
# =====================================================================
PRODUCT_METAFIELD_DEFINITIONS = [
    # Core
    {"namespace": "custom", "key": "tagline", "name": "Tagline", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "short_description", "name": "Short Description", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "size_ml", "name": "Size (ml)", "type": "single_line_text_field"},
    # Accordion sections (heading + content pairs)
    {"namespace": "custom", "key": "key_benefits_heading", "name": "Key Benefits Heading", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "key_benefits_content", "name": "Key Benefits Content", "type": "rich_text_field"},
    {"namespace": "custom", "key": "clinical_results_heading", "name": "Clinical Results Heading", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "clinical_results_content", "name": "Clinical Results Content", "type": "rich_text_field"},
    {"namespace": "custom", "key": "how_to_use_heading", "name": "How to Use Heading", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "how_to_use_content", "name": "How to Use Content", "type": "rich_text_field"},
    {"namespace": "custom", "key": "whats_inside_heading", "name": "What's Inside Heading", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "whats_inside_content", "name": "What's Inside Content", "type": "rich_text_field"},
    {"namespace": "custom", "key": "free_of_heading", "name": "Free Of Heading", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "free_of_content", "name": "Free Of Content", "type": "rich_text_field"},
    {"namespace": "custom", "key": "awards_heading", "name": "Awards Heading", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "awards_content", "name": "Awards Content", "type": "rich_text_field"},
    {"namespace": "custom", "key": "fragrance_heading", "name": "Fragrance Heading", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "fragrance_content", "name": "Fragrance Content", "type": "rich_text_field"},
    # Reference fields
    {"namespace": "custom", "key": "ingredients", "name": "Ingredients", "type": "list.metaobject_reference",
     "validations": [{"name": "metaobject_definition_id", "value": "RESOLVE:ingredient"}]},
    {"namespace": "custom", "key": "faqs", "name": "FAQs", "type": "list.metaobject_reference",
     "validations": [{"name": "metaobject_definition_id", "value": "RESOLVE:faq_entry"}]},
]

# =====================================================================
# Article metafield definitions (12 fields)
# =====================================================================
ARTICLE_METAFIELD_DEFINITIONS = [
    {"namespace": "custom", "key": "featured", "name": "Featured", "type": "boolean"},
    {"namespace": "custom", "key": "is_hero", "name": "Is Hero", "type": "boolean"},
    {"namespace": "custom", "key": "blog_summary", "name": "Blog Summary", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "read_time_override", "name": "Read Time Override", "type": "number_integer"},
    {"namespace": "custom", "key": "hero_caption", "name": "Hero Caption", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "short_title", "name": "Short Title", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "related_articles", "name": "Related Articles", "type": "list.article_reference"},
    {"namespace": "custom", "key": "related_products", "name": "Related Products", "type": "list.product_reference"},
    {"namespace": "custom", "key": "author", "name": "Author", "type": "metaobject_reference",
     "validations": [{"name": "metaobject_definition_id", "value": "RESOLVE:blog_author"}]},
    {"namespace": "custom", "key": "ingredients", "name": "Ingredients", "type": "list.metaobject_reference",
     "validations": [{"name": "metaobject_definition_id", "value": "RESOLVE:ingredient"}]},
    {"namespace": "custom", "key": "listing_image", "name": "Listing Image", "type": "file_reference"},
    {"namespace": "custom", "key": "hero_image", "name": "Hero Image", "type": "file_reference"},
]


def resolve_metaobject_definition_ids(definitions_list, existing_defs):
    """Replace RESOLVE:type_handle placeholders with actual definition GIDs."""
    for defn in definitions_list:
        if defn.get("validations"):
            for v in defn["validations"]:
                if v["value"].startswith("RESOLVE:"):
                    mo_type = v["value"].split(":", 1)[1]
                    if mo_type in existing_defs:
                        v["value"] = existing_defs[mo_type]["id"]
                    else:
                        print(f"  WARNING: Cannot resolve metaobject definition '{mo_type}' — not found")
                        v["value"] = ""


def main():
    parser = argparse.ArgumentParser(description="Set up destination store schema")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created without making API calls")
    args = parser.parse_args()

    load_dotenv()

    if args.dry_run:
        print("=== DRY RUN MODE ===\n")
        client = None
    else:
        shop_url = os.environ["SAUDI_SHOP_URL"]
        access_token = os.environ["SAUDI_ACCESS_TOKEN"]
        client = ShopifyClient(shop_url, access_token)

    # ==========================================================
    # 1. Metaobject definitions
    # ==========================================================
    print("=== Metaobject Definitions ===")
    existing_mo_defs = {}
    if not args.dry_run:
        print("Fetching existing metaobject definitions...")
        existing = client.get_metaobject_definitions()
        existing_mo_defs = {d["type"]: d for d in existing}
        print(f"  Found: {list(existing_mo_defs.keys()) or '(none)'}")

    for defn in METAOBJECT_DEFINITIONS:
        mo_type = defn["type"]
        label = f"  {defn['name']} ({mo_type})"

        if mo_type in existing_mo_defs:
            print(f"{label} — already exists, skipping")
            continue

        if args.dry_run:
            print(f"{label} — would create ({len(defn['fieldDefinitions'])} fields)")
            continue

        # Resolve RESOLVE: placeholders in field validations
        for fd in defn.get("fieldDefinitions", []):
            if fd.get("validations"):
                for v in fd["validations"]:
                    if isinstance(v.get("value"), str) and v["value"].startswith("RESOLVE:"):
                        ref_type = v["value"].split(":", 1)[1]
                        if ref_type in existing_mo_defs:
                            v["value"] = existing_mo_defs[ref_type]["id"]
                        else:
                            print(f"    WARNING: Cannot resolve '{ref_type}' — create it first")

        try:
            result = client.create_metaobject_definition(defn)
            if result:
                print(f"{label} — created (id: {result['id']})")
                existing_mo_defs[mo_type] = result
            else:
                print(f"{label} — already exists (via API)")
                # Refetch to get the ID
                refreshed = client.get_metaobject_definitions()
                for d in refreshed:
                    if d["type"] == mo_type:
                        existing_mo_defs[mo_type] = d
                        break
        except Exception as e:
            print(f"{label} — error: {e}")

    # ==========================================================
    # 2. Product metafield definitions
    # ==========================================================
    print("\n=== Product Metafield Definitions ===")
    existing_product_mfs = set()
    if not args.dry_run:
        print("Fetching existing product metafield definitions...")
        existing = client.get_metafield_definitions("PRODUCT")
        existing_product_mfs = {f"{d['namespace']}.{d['key']}" for d in existing}
        print(f"  Found: {len(existing_product_mfs)} definitions")

    for mf_def in PRODUCT_METAFIELD_DEFINITIONS:
        ns_key = f"{mf_def['namespace']}.{mf_def['key']}"
        label = f"  {mf_def['name']} ({ns_key})"

        if ns_key in existing_product_mfs:
            print(f"{label} — already exists")
            continue

        if args.dry_run:
            print(f"{label} — would create (type: {mf_def['type']})")
            continue

        definition = {
            "name": mf_def["name"],
            "namespace": mf_def["namespace"],
            "key": mf_def["key"],
            "type": mf_def["type"],
            "ownerType": "PRODUCT",
        }

        # Resolve RESOLVE: validation placeholders
        if mf_def.get("validations"):
            validations = []
            for v in mf_def["validations"]:
                val = v.copy()
                if isinstance(val.get("value"), str) and val["value"].startswith("RESOLVE:"):
                    ref_type = val["value"].split(":", 1)[1]
                    if ref_type in existing_mo_defs:
                        val["value"] = existing_mo_defs[ref_type]["id"]
                    else:
                        print(f"    WARNING: Cannot resolve '{ref_type}'")
                        continue
                validations.append(val)
            definition["validations"] = validations

        try:
            result = client.create_metafield_definition(definition)
            if result:
                print(f"{label} — created")
            else:
                print(f"{label} — already exists (via API)")
        except Exception as e:
            print(f"{label} — error: {e}")

    # ==========================================================
    # 3. Article metafield definitions
    # ==========================================================
    print("\n=== Article Metafield Definitions ===")
    existing_article_mfs = set()
    if not args.dry_run:
        print("Fetching existing article metafield definitions...")
        existing = client.get_metafield_definitions("ARTICLE")
        existing_article_mfs = {f"{d['namespace']}.{d['key']}" for d in existing}
        print(f"  Found: {len(existing_article_mfs)} definitions")

    for mf_def in ARTICLE_METAFIELD_DEFINITIONS:
        ns_key = f"{mf_def['namespace']}.{mf_def['key']}"
        label = f"  {mf_def['name']} ({ns_key})"

        if ns_key in existing_article_mfs:
            print(f"{label} — already exists")
            continue

        if args.dry_run:
            print(f"{label} — would create (type: {mf_def['type']})")
            continue

        definition = {
            "name": mf_def["name"],
            "namespace": mf_def["namespace"],
            "key": mf_def["key"],
            "type": mf_def["type"],
            "ownerType": "ARTICLE",
        }

        if mf_def.get("validations"):
            validations = []
            for v in mf_def["validations"]:
                val = v.copy()
                if isinstance(val.get("value"), str) and val["value"].startswith("RESOLVE:"):
                    ref_type = val["value"].split(":", 1)[1]
                    if ref_type in existing_mo_defs:
                        val["value"] = existing_mo_defs[ref_type]["id"]
                    else:
                        print(f"    WARNING: Cannot resolve '{ref_type}'")
                        continue
                validations.append(val)
            definition["validations"] = validations

        try:
            result = client.create_metafield_definition(definition)
            if result:
                print(f"{label} — created")
            else:
                print(f"{label} — already exists (via API)")
        except Exception as e:
            print(f"{label} — error: {e}")

    # ==========================================================
    # Summary
    # ==========================================================
    print("\n=== Setup Complete ===")
    print(f"  Metaobject types:         {len(existing_mo_defs)}/4")
    if not args.dry_run:
        product_count = len(client.get_metafield_definitions("PRODUCT"))
        article_count = len(client.get_metafield_definitions("ARTICLE"))
        print(f"  Product metafields:       {product_count}/19")
        print(f"  Article metafields:       {article_count}/12")
    print("\nThe destination store schema is ready. You can now run import_english.py.")


if __name__ == "__main__":
    main()
