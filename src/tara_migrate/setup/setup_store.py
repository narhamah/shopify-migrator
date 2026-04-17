#!/usr/bin/env python3
"""Set up the destination store schema.

Uses the live source store as the schema authority when credentials are
available, with local defaults only as a fallback. This keeps destination
stores aligned with the current Saudi data model, including image metafields,
reference validations, and smart-collection capabilities.
"""

import argparse

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import DEFINITION_ORDER, config

# ---------------------------------------------------------------------
# Fallback schema when live source access is unavailable
# ---------------------------------------------------------------------

DEFAULT_METAOBJECT_DEFINITIONS = [
    {
        "type": "benefit",
        "name": "Benefit",
        "access": {"storefront": "PUBLIC_READ"},
        "capabilities": {"translatable": {"enabled": True}},
        "displayNameKey": "title",
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
        "capabilities": {"translatable": {"enabled": True}},
        "displayNameKey": "question",
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
        "capabilities": {"translatable": {"enabled": True}},
        "displayNameKey": "name",
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
        "capabilities": {
            "publishable": {"enabled": True},
            "renderable": {
                "enabled": True,
                "data": {"metaTitleKey": "name", "metaDescriptionKey": "description"},
            },
            "translatable": {"enabled": True},
            "onlineStore": {"enabled": True, "data": {"urlHandle": "ingredient"}},
        },
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

DEFAULT_PRODUCT_METAFIELD_DEFINITIONS = [
    {"namespace": "custom", "key": "tagline", "name": "Tagline", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "short_description", "name": "Short Description", "type": "single_line_text_field"},
    {"namespace": "custom", "key": "size_ml", "name": "Size (ml)", "type": "single_line_text_field"},
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
    {"namespace": "custom", "key": "ingredients", "name": "Ingredients", "type": "list.metaobject_reference",
     "validations": [{"name": "metaobject_definition_id", "value": "RESOLVE:ingredient"}],
     "capabilities": {"smartCollectionCondition": {"enabled": True}}},
    {"namespace": "custom", "key": "faqs", "name": "FAQs", "type": "list.metaobject_reference",
     "validations": [{"name": "metaobject_definition_id", "value": "RESOLVE:faq_entry"}]},
]

DEFAULT_ARTICLE_METAFIELD_DEFINITIONS = [
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

DISPLAY_NAME_KEYS = {
    "benefit": "title",
    "faq_entry": "question",
    "blog_author": "name",
    "ingredient": "name",
    "store_location": "name",
}


def _definition_sort_key(definition):
    mo_type = definition.get("type", "")
    if mo_type in DEFINITION_ORDER:
        return (0, DEFINITION_ORDER.index(mo_type), mo_type)
    return (1, 999, mo_type)


def _maybe_get_source_client():
    try:
        return ShopifyClient(config.get_source_shop_url(), config.get_source_access_token())
    except Exception:
        return None


def _load_live_source_schema(source_client):
    if not source_client:
        return None, None, None
    try:
        metaobject_defs = source_client.get_metaobject_definitions()
        product_defs = source_client.get_metafield_definitions("PRODUCT")
        article_defs = source_client.get_metafield_definitions("ARTICLE")
        print(f"Loaded live source schema: {len(metaobject_defs)} metaobject defs, "
              f"{len(product_defs)} product metafield defs, {len(article_defs)} article metafield defs")
        return metaobject_defs, product_defs, article_defs
    except Exception as e:
        print(f"WARNING: Could not load live source schema; using defaults ({e})")
        return None, None, None


def _normalize_metaobject_field_definition(field_definition, source_mo_def_id_to_type, dest_mo_defs):
    normalized = {
        "key": field_definition["key"],
        "name": field_definition.get("name", field_definition["key"]),
        "type": field_definition["type"]["name"] if isinstance(field_definition.get("type"), dict) else field_definition["type"],
    }

    validations = []
    for validation in field_definition.get("validations", []):
        item = dict(validation)
        value = item.get("value")
        if isinstance(value, str) and value.startswith("RESOLVE:"):
            ref_type = value.split(":", 1)[1]
            if ref_type not in dest_mo_defs:
                continue
            item["value"] = dest_mo_defs[ref_type]["id"]
        elif item.get("name") == "metaobject_definition_id" and value in source_mo_def_id_to_type:
            ref_type = source_mo_def_id_to_type[value]
            if ref_type not in dest_mo_defs:
                continue
            item["value"] = dest_mo_defs[ref_type]["id"]
        validations.append(item)

    if validations:
        normalized["validations"] = validations
    return normalized


def _normalize_metafield_definition(definition, owner_type, source_mo_def_id_to_type, dest_mo_defs):
    normalized = {
        "name": definition["name"],
        "namespace": definition["namespace"],
        "key": definition["key"],
        "type": definition["type"]["name"] if isinstance(definition.get("type"), dict) else definition["type"],
        "ownerType": owner_type,
    }

    validations = []
    for validation in definition.get("validations", []):
        item = dict(validation)
        value = item.get("value")
        if isinstance(value, str) and value.startswith("RESOLVE:"):
            ref_type = value.split(":", 1)[1]
            if ref_type not in dest_mo_defs:
                continue
            item["value"] = dest_mo_defs[ref_type]["id"]
        elif item.get("name") == "metaobject_definition_id" and value in source_mo_def_id_to_type:
            ref_type = source_mo_def_id_to_type[value]
            if ref_type not in dest_mo_defs:
                continue
            item["value"] = dest_mo_defs[ref_type]["id"]
        validations.append(item)

    if validations:
        normalized["validations"] = validations

    if definition.get("capabilities"):
        normalized["capabilities"] = definition["capabilities"]

    return normalized


def _normalize_metaobject_capabilities(definition):
    capabilities = definition.get("capabilities") or {}
    normalized = {}

    publishable = capabilities.get("publishable")
    if publishable:
        normalized["publishable"] = {"enabled": bool(publishable.get("enabled"))}

    translatable = capabilities.get("translatable")
    if translatable:
        normalized["translatable"] = {"enabled": bool(translatable.get("enabled"))}

    renderable = capabilities.get("renderable")
    if renderable:
        item = {"enabled": bool(renderable.get("enabled"))}
        data = renderable.get("data") or {}
        if item["enabled"] and data:
            item["data"] = {key: value for key, value in data.items() if value}
        normalized["renderable"] = item

    online_store = capabilities.get("onlineStore")
    if online_store:
        item = {"enabled": bool(online_store.get("enabled"))}
        data = online_store.get("data") or {}
        if item["enabled"] and data:
            item["data"] = {key: value for key, value in data.items() if value}
        normalized["onlineStore"] = item

    return normalized


def _normalize_metaobject_access(definition):
    access = dict(definition.get("access") or {"storefront": "PUBLIC_READ"})
    mo_type = str(definition.get("type", ""))
    admin = access.get("admin")
    if admin == "PUBLIC_READ_WRITE":
        access["admin"] = "MERCHANT_READ_WRITE"
    elif admin == "PUBLIC_READ":
        access["admin"] = "MERCHANT_READ"
    if not mo_type.startswith("$app:"):
        access.pop("admin", None)
    return access


def main():
    parser = argparse.ArgumentParser(description="Set up destination store schema")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created without making API calls")
    args = parser.parse_args()

    load_dotenv()

    if args.dry_run:
        print("=== DRY RUN MODE ===\n")
        client = None
    else:
        client = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())

    source_client = _maybe_get_source_client()
    source_metaobject_defs, source_product_defs, source_article_defs = _load_live_source_schema(source_client)

    dest_name = (config.get_dest_name() or "").lower()
    if dest_name == "usa":
        # USA should not inherit Saudi's Spanish custom schema choices/labels.
        merged_defs = {definition["type"]: definition for definition in DEFAULT_METAOBJECT_DEFINITIONS}
        for definition in (source_metaobject_defs or []):
            if definition.get("type", "").startswith("shopify--"):
                merged_defs[definition["type"]] = definition
        metaobject_definitions = sorted(merged_defs.values(), key=_definition_sort_key)
        product_metafield_definitions = DEFAULT_PRODUCT_METAFIELD_DEFINITIONS
        article_metafield_definitions = DEFAULT_ARTICLE_METAFIELD_DEFINITIONS
    else:
        metaobject_definitions = sorted(source_metaobject_defs or DEFAULT_METAOBJECT_DEFINITIONS, key=_definition_sort_key)
        product_metafield_definitions = source_product_defs or DEFAULT_PRODUCT_METAFIELD_DEFINITIONS
        article_metafield_definitions = source_article_defs or DEFAULT_ARTICLE_METAFIELD_DEFINITIONS
    source_mo_def_id_to_type = {
        definition["id"]: definition["type"]
        for definition in (source_metaobject_defs or [])
        if definition.get("id") and definition.get("type")
    }

    # ----------------------------------------------------------
    # Metaobject definitions
    # ----------------------------------------------------------
    print("=== Metaobject Definitions ===")
    existing_mo_defs = {}
    if not args.dry_run:
        print("Fetching existing metaobject definitions...")
        existing = client.get_metaobject_definitions()
        existing_mo_defs = {definition["type"]: definition for definition in existing}
        print(f"  Found: {list(existing_mo_defs.keys()) or '(none)'}")

    for definition in metaobject_definitions:
        mo_type = definition["type"]
        label = f"  {definition.get('name', mo_type)} ({mo_type})"

        if mo_type in existing_mo_defs:
            print(f"{label} - already exists, skipping")
            continue

        if args.dry_run:
            print(f"{label} - would create ({len(definition.get('fieldDefinitions', []))} fields)")
            continue

        try:
            if mo_type.startswith("shopify--"):
                result = client.enable_standard_metaobject_definition(mo_type)
            else:
                normalized_fields = [
                    _normalize_metaobject_field_definition(field_definition, source_mo_def_id_to_type, existing_mo_defs)
                    for field_definition in definition.get("fieldDefinitions", [])
                ]
                payload = {
                    "type": mo_type,
                    "name": definition.get("name", mo_type),
                    "fieldDefinitions": normalized_fields,
                    "access": _normalize_metaobject_access(definition),
                }
                display_name_key = definition.get("displayNameKey") or DISPLAY_NAME_KEYS.get(mo_type)
                if display_name_key:
                    payload["displayNameKey"] = display_name_key
                capabilities = _normalize_metaobject_capabilities(definition)
                if capabilities:
                    payload["capabilities"] = capabilities
                result = client.create_metaobject_definition(payload)
            if result:
                print(f"{label} - created (id: {result['id']})")
                existing_mo_defs[mo_type] = result
            else:
                print(f"{label} - already exists (via API)")
                refreshed = client.get_metaobject_definitions()
                for item in refreshed:
                    if item["type"] == mo_type:
                        existing_mo_defs[mo_type] = item
                        break
        except Exception as e:
            print(f"{label} - error: {e}")

    if not args.dry_run:
        refreshed_defs = client.get_metaobject_definitions()
        existing_mo_defs = {definition["type"]: definition for definition in refreshed_defs}
        for definition in metaobject_definitions:
            mo_type = definition["type"]
            existing = existing_mo_defs.get(mo_type)
            if not existing:
                continue

            update_data = {}
            display_key = definition.get("displayNameKey") or DISPLAY_NAME_KEYS.get(mo_type)
            if display_key and existing.get("displayNameKey") != display_key:
                update_data["displayNameKey"] = display_key

            desired_caps = _normalize_metaobject_capabilities(definition)
            existing_caps = _normalize_metaobject_capabilities(existing)
            if desired_caps and existing_caps != desired_caps:
                update_data["capabilities"] = desired_caps

            if not update_data:
                continue

            try:
                client.update_metaobject_definition(existing["id"], update_data)
                updates = ", ".join(sorted(update_data.keys()))
                print(f"  {mo_type} - updated {updates} to match source schema")
            except Exception as e:
                print(f"  {mo_type} - schema update failed: {e}")

    # ----------------------------------------------------------
    # Product metafield definitions
    # ----------------------------------------------------------
    print("\n=== Product Metafield Definitions ===")
    existing_product_defs = {}
    if not args.dry_run:
        print("Fetching existing product metafield definitions...")
        existing = client.get_metafield_definitions("PRODUCT")
        existing_product_defs = {f"{item['namespace']}.{item['key']}": item for item in existing}
        print(f"  Found: {len(existing_product_defs)} definitions")

    for definition in product_metafield_definitions:
        ns_key = f"{definition['namespace']}.{definition['key']}"
        label = f"  {definition['name']} ({ns_key})"

        desired = _normalize_metafield_definition(
            definition, "PRODUCT", source_mo_def_id_to_type, existing_mo_defs
        )

        if ns_key in existing_product_defs:
            print(f"{label} - already exists")
            if not args.dry_run:
                existing = existing_product_defs[ns_key]
                existing_caps = existing.get("capabilities", {})
                desired_caps = desired.get("capabilities", {})
                existing_validations = existing.get("validations", [])
                desired_validations = desired.get("validations", [])
                if existing_caps != desired_caps or existing_validations != desired_validations:
                    update_input = {
                        "namespace": existing["namespace"],
                        "key": existing["key"],
                        "ownerType": "PRODUCT",
                    }
                    if desired_caps:
                        update_input["capabilities"] = desired_caps
                    if desired_validations:
                        update_input["validations"] = desired_validations
                    try:
                        client.update_metafield_definition(update_input)
                        print("    updated capabilities/validations to match source schema")
                    except Exception as e:
                        print(f"    update failed: {e}")
            continue

        if args.dry_run:
            print(f"{label} - would create (type: {desired['type']})")
            continue

        try:
            result = client.create_metafield_definition(desired)
            if result:
                print(f"{label} - created")
            else:
                print(f"{label} - already exists (via API)")
        except Exception as e:
            print(f"{label} - error: {e}")

    # ----------------------------------------------------------
    # Article metafield definitions
    # ----------------------------------------------------------
    print("\n=== Article Metafield Definitions ===")
    existing_article_defs = {}
    if not args.dry_run:
        print("Fetching existing article metafield definitions...")
        existing = client.get_metafield_definitions("ARTICLE")
        existing_article_defs = {f"{item['namespace']}.{item['key']}": item for item in existing}
        print(f"  Found: {len(existing_article_defs)} definitions")

    for definition in article_metafield_definitions:
        ns_key = f"{definition['namespace']}.{definition['key']}"
        label = f"  {definition['name']} ({ns_key})"
        desired = _normalize_metafield_definition(
            definition, "ARTICLE", source_mo_def_id_to_type, existing_mo_defs
        )

        if ns_key in existing_article_defs:
            print(f"{label} - already exists")
            continue

        if args.dry_run:
            print(f"{label} - would create (type: {desired['type']})")
            continue

        try:
            result = client.create_metafield_definition(desired)
            if result:
                print(f"{label} - created")
            else:
                print(f"{label} - already exists (via API)")
        except Exception as e:
            print(f"{label} - error: {e}")

    # ----------------------------------------------------------
    # Summary
    # ----------------------------------------------------------
    print("\n=== Setup Complete ===")
    print(f"  Metaobject types:         {len(existing_mo_defs)}/{len(metaobject_definitions)}")
    if not args.dry_run:
        product_count = len(client.get_metafield_definitions("PRODUCT"))
        article_count = len(client.get_metafield_definitions("ARTICLE"))
        print(f"  Product metafields:       {product_count}/{len(product_metafield_definitions)}")
        print(f"  Article metafields:       {article_count}/{len(article_metafield_definitions)}")
    print("\nThe destination store schema is ready. You can now run import_english.py.")


if __name__ == "__main__":
    main()
