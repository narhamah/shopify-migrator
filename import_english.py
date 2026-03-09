#!/usr/bin/env python3
"""Step 3: Import English-translated content into the Saudi Shopify store.

Prerequisites: Run setup_store.py first to create metaobject/metafield definitions.

Phases:
  0. Examine destination store (existing definitions)
  1. Create metaobject entries (benefit → faq_entry → blog_author → ingredient)
  2. Create products (with text metafields, prices converted)
  3. Create collections
  4. Create pages
  5. Create blogs + articles
  6. Remap reference fields (ingredient→benefit, product→ingredient, etc.)
"""

import argparse
import json
import os

from dotenv import load_dotenv

from shopify_client import ShopifyClient
from utils import load_json, save_json, sanitize_rich_text_json


def prepare_product_for_import(product, sar_prices=None):
    """Strip source-specific fields and prepare product for creation.

    Args:
        product: product dict from the translated JSON
        sar_prices: dict of SKU → {final_price, regular_price, currency}
                    loaded from data/sar_prices.json (fetched by fix_prices.py)
    """
    status = product.get("status", "draft")
    p = {
        "title": product.get("title", ""),
        "body_html": product.get("body_html", ""),
        "vendor": product.get("vendor", ""),
        "product_type": product.get("product_type", ""),
        "tags": product.get("tags", ""),
        "handle": product.get("handle", ""),
        "status": status,
        "published": status == "active",  # Publish to sales channels if active
    }

    if product.get("images"):
        p["images"] = []
        for img in product["images"]:
            if img.get("src"):
                img_data = {"src": img["src"]}
                if img.get("alt"):
                    img_data["alt"] = img["alt"]
                p["images"].append(img_data)

    if product.get("variants"):
        p["variants"] = []
        for v in product["variants"]:
            sku = v.get("sku", "")
            # Use SAR price from Saudi Magento store if available
            price = v.get("price", "0")
            compare_at_price = v.get("compare_at_price")
            if sar_prices and sku and sku in sar_prices:
                sp = sar_prices[sku]
                if sp.get("final_price") is not None:
                    price = str(sp["final_price"])
                if sp.get("regular_price") and sp.get("final_price") and sp["regular_price"] != sp["final_price"]:
                    compare_at_price = str(sp["regular_price"])
                elif sp.get("final_price") is not None:
                    compare_at_price = None

            variant = {
                "title": v.get("title", ""),
                "price": price,
                "compare_at_price": compare_at_price,
                "sku": sku,
                "barcode": v.get("barcode", ""),
                "weight": v.get("weight"),
                "weight_unit": v.get("weight_unit", "kg"),
                "inventory_management": v.get("inventory_management"),
                "option1": v.get("option1"),
                "option2": v.get("option2"),
                "option3": v.get("option3"),
                "requires_shipping": v.get("requires_shipping", True),
                "taxable": v.get("taxable", True),
            }
            p["variants"].append(variant)

    if product.get("options"):
        p["options"] = []
        for opt in product["options"]:
            p["options"].append({
                "name": opt.get("name", ""),
                "values": opt.get("values", []),
            })

    # Include metafields (excluding reference types that need ID remapping)
    if product.get("metafields"):
        p["metafields"] = []
        for mf in product["metafields"]:
            mf_type = mf.get("type", "")
            # Skip reference fields — they point to source store IDs
            if "reference" in mf_type:
                continue
            value = mf["value"]
            # Sanitize rich_text JSON to fix control characters
            if "rich_text" in mf_type or (isinstance(value, str) and value.strip().startswith('{"type":"root"')):
                value = sanitize_rich_text_json(value)
            p["metafields"].append({
                "namespace": mf["namespace"],
                "key": mf["key"],
                "value": value,
                "type": mf_type,
            })

    return p


def main():
    parser = argparse.ArgumentParser(description="Import English content into Saudi Shopify store")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created without making API calls")
    parser.add_argument("--reset", action="store_true", help="Clear id_map and progress files for a fresh import")
    args = parser.parse_args()

    load_dotenv()
    input_dir = "data/english"
    id_map_file = "data/id_map.json"
    file_map_file = "data/file_map.json"
    arabic_progress_file = "data/arabic_import_progress.json"

    if args.reset:
        for f in [id_map_file, file_map_file, arabic_progress_file]:
            if os.path.exists(f):
                os.remove(f)
                print(f"  Cleared {f}")
        print("  Reset complete — starting fresh import\n")

    id_map = load_json(id_map_file) if os.path.exists(id_map_file) else {}

    if args.dry_run:
        print("=== DRY RUN MODE — no API calls will be made ===\n")
        client = None
    else:
        shop_url = os.environ["SAUDI_SHOP_URL"]
        access_token = os.environ["SAUDI_ACCESS_TOKEN"]
        client = ShopifyClient(shop_url, access_token)

    # Fetch SAR prices from Saudi Magento store
    sar_prices_file = "data/sar_prices.json"
    sar_prices = {}
    try:
        from fix_prices import fetch_sar_prices
        sar_prices = fetch_sar_prices("https://taraformula.com", "sa-en")
        if sar_prices:
            save_json(sar_prices, sar_prices_file)
            print(f"Fetched SAR prices for {len(sar_prices)} SKUs")
        else:
            print("WARNING: Could not fetch SAR prices — products will use source data prices")
    except Exception as e:
        # Fall back to cached prices
        if os.path.exists(sar_prices_file):
            sar_prices = load_json(sar_prices_file)
            print(f"Using cached SAR prices for {len(sar_prices)} SKUs ({e})")
        else:
            print(f"WARNING: Could not fetch SAR prices ({e}) — products will use source data prices")

    # =============================================
    # Phase 0: Examine destination store
    # =============================================
    existing_defs = {}
    if not args.dry_run:
        print("\nExamining destination store...")
        try:
            dest_definitions = client.get_metaobject_definitions()
            existing_defs = {d["type"]: d for d in dest_definitions}
            print(f"  Found {len(existing_defs)} existing metaobject definitions: {list(existing_defs.keys())}")
        except Exception as e:
            print(f"  Could not fetch existing definitions: {e}")
            existing_defs = {}

    # =============================================
    # Phase 1: Metaobject definitions + entries
    # =============================================
    metaobjects_file = os.path.join(input_dir, "metaobjects.json")
    if os.path.exists(metaobjects_file):
        all_metaobjects = load_json(metaobjects_file)
        definitions_file = os.path.join(input_dir, "metaobject_definitions.json")
        definitions = load_json(definitions_file) if os.path.exists(definitions_file) else []

        # Create metaobject definitions first (order matters: benefit before ingredient)
        # Sort so that types without references come first
        DEFINITION_ORDER = ["benefit", "faq_entry", "blog_author", "ingredient"]
        sorted_defs = sorted(definitions, key=lambda d: (
            DEFINITION_ORDER.index(d["type"]) if d["type"] in DEFINITION_ORDER else 999
        ))

        print(f"\nChecking/creating {len(sorted_defs)} metaobject definitions...")
        for defn in sorted_defs:
            mo_type = defn["type"]
            label = f"  {defn.get('name', mo_type)} ({mo_type})"

            if mo_type in existing_defs:
                print(f"{label} — already exists in destination store, skipping creation")
                continue

            if args.dry_run:
                print(f"{label} — would create definition")
                continue

            # Build definition input
            field_defs = []
            for fd in defn.get("fieldDefinitions", []):
                field_def = {
                    "key": fd["key"],
                    "name": fd.get("name", fd["key"]),
                    "type": fd["type"]["name"],
                }
                if fd.get("validations"):
                    field_def["validations"] = fd["validations"]
                field_defs.append(field_def)

            def_input = {
                "type": mo_type,
                "name": defn.get("name", mo_type),
                "fieldDefinitions": field_defs,
                "access": {"storefront": "PUBLIC_READ"},
            }

            try:
                result = client.create_metaobject_definition(def_input)
                if result:
                    print(f"{label} — created (id: {result['id']})")
                    existing_defs[mo_type] = result
                else:
                    print(f"{label} — already exists")
            except Exception as e:
                print(f"{label} — error: {e}")

        # Create metaobject entries
        for mo_type, type_data in all_metaobjects.items():
            objects = type_data.get("objects", [])
            if not objects:
                continue

            print(f"\nImporting {len(objects)} '{mo_type}' metaobjects...")
            for j, obj in enumerate(objects):
                handle = obj.get("handle", "")
                source_id = obj.get("id", "")
                label = f"  [{j+1}/{len(objects)}] {handle}"
                map_key = f"metaobjects_{mo_type}"

                if source_id in id_map.get(map_key, {}):
                    print(f"{label} — already imported, skipping")
                    continue

                if args.dry_run:
                    print(f"{label} — would create")
                    continue

                # Check if exists
                existing = client.get_metaobjects_by_handle(mo_type, handle)
                if existing:
                    dest_id = existing["id"]
                    print(f"{label} — already exists (id: {dest_id}), mapping")
                    id_map.setdefault(map_key, {})[source_id] = dest_id
                    save_json(id_map, id_map_file)
                    continue

                # Build fields (skip file references and metaobject references for now)
                fields = []
                for field in obj.get("fields", []):
                    field_type = field.get("type", "")
                    if "file_reference" in field_type or "metaobject_reference" in field_type:
                        continue
                    if "collection_reference" in field_type:
                        continue
                    if field.get("value"):
                        val = field["value"]
                        # Sanitize rich_text JSON to fix translation corruption
                        if "rich_text" in field_type or (isinstance(val, str) and val.strip().startswith('{"type":"root"')):
                            val = sanitize_rich_text_json(val)
                        fields.append({
                            "key": field["key"],
                            "value": val,
                        })

                mo_input = {
                    "type": mo_type,
                    "handle": handle,
                    "fields": fields,
                }

                try:
                    created = client.create_metaobject(mo_input)
                    if created:
                        dest_id = created["id"]
                        print(f"{label} — created (id: {dest_id})")
                        id_map.setdefault(map_key, {})[source_id] = dest_id
                    else:
                        print(f"{label} — already exists (handle collision)")
                except Exception as e:
                    print(f"{label} — error: {e}")
                save_json(id_map, id_map_file)

    # =============================================
    # Phase 2: Products
    # =============================================
    products = load_json(os.path.join(input_dir, "products.json"))
    print(f"\nImporting {len(products)} products...")
    for i, product in enumerate(products):
        source_id = str(product["id"])
        handle = product.get("handle", "")
        label = f"[{i+1}/{len(products)}] {product.get('title', '')[:50]}"

        if source_id in id_map.get("products", {}):
            print(f"  {label} — already imported, skipping")
            continue

        if args.dry_run:
            print(f"  {label} — would create (handle: {handle})")
            continue

        existing = client.get_products_by_handle(handle)
        if existing:
            dest_id = existing[0]["id"]
            print(f"  {label} — already exists (id: {dest_id}), mapping")
            id_map.setdefault("products", {})[source_id] = dest_id
            save_json(id_map, id_map_file)
            continue

        product_data = prepare_product_for_import(product, sar_prices)
        try:
            created = client.create_product(product_data)
            dest_id = created.get("id")
            print(f"  {label} — created (id: {dest_id})")
            id_map.setdefault("products", {})[source_id] = dest_id
            save_json(id_map, id_map_file)
        except Exception as e:
            err_msg = str(e)
            # Try to extract response body for 422 errors
            if hasattr(e, 'response') and e.response is not None:
                try:
                    err_body = e.response.json()
                    err_msg = json.dumps(err_body, indent=2)
                except Exception:
                    err_msg = e.response.text[:500]
            print(f"  {label} — ERROR: {err_msg}")

    # =============================================
    # Phase 3: Collections
    # =============================================
    collections = load_json(os.path.join(input_dir, "collections.json"))

    # Build metafield definition GID remapping for smart collection rules.
    # Smart collection rules reference MetafieldDefinition GIDs from the Spain
    # store — we need to remap them to the destination store's GIDs.
    spain_def_gid_to_dest = {}  # Spain MetafieldDefinition GID → dest GID
    if not args.dry_run:
        try:
            # Load Spain definitions (exported by export_spain.py)
            spain_defs_file = os.path.join("data", "spain_export", "product_metafield_definitions.json")
            spain_defs = load_json(spain_defs_file) if os.path.exists(spain_defs_file) else []

            # Fetch destination definitions
            dest_product_defs = client.get_metafield_definitions("PRODUCT")
            dest_mf_def_map = {}
            for d in dest_product_defs:
                dest_mf_def_map[(d["namespace"], d["key"])] = d["id"]

            # Map Spain GID → dest GID by matching namespace.key
            for sd in spain_defs:
                nk = (sd["namespace"], sd["key"])
                if nk in dest_mf_def_map:
                    spain_def_gid_to_dest[sd["id"]] = dest_mf_def_map[nk]

            if spain_def_gid_to_dest:
                print(f"  Mapped {len(spain_def_gid_to_dest)} metafield definition GIDs for smart collection rules")
        except Exception as e:
            print(f"  Warning: Could not build metafield definition mapping: {e}")

    # Build reverse map: metaobject source GID → dest GID (across all types)
    all_metaobject_id_map = {}
    for map_key, mapping in id_map.items():
        if map_key.startswith("metaobjects_"):
            all_metaobject_id_map.update(mapping)

    print(f"\nImporting {len(collections)} collections...")
    for i, collection in enumerate(collections):
        source_id = str(collection["id"])
        handle = collection.get("handle", "")
        label = f"[{i+1}/{len(collections)}] {collection.get('title', '')[:50]}"

        if source_id in id_map.get("collections", {}):
            print(f"  {label} — already imported, skipping")
            continue

        if args.dry_run:
            print(f"  {label} — would create (handle: {handle})")
            continue

        existing = client.get_collections_by_handle(handle)
        if existing:
            dest_id = existing[0]["id"]
            print(f"  {label} — already exists (id: {dest_id}), mapping")
            id_map.setdefault("collections", {})[source_id] = dest_id
            save_json(id_map, id_map_file)
            continue

        # Determine if smart or custom collection
        is_smart = collection.get("rules") is not None

        try:
            if is_smart:
                # Remap smart collection rule GIDs from Spain → destination
                remapped_rules = []
                skip_collection = False
                for rule in collection.get("rules", []):
                    rule = dict(rule)  # shallow copy
                    # Remap condition_object_id (MetafieldDefinition GID)
                    coid = rule.get("condition_object_id")
                    if coid and coid.startswith("gid://"):
                        dest_coid = spain_def_gid_to_dest.get(coid)
                        if dest_coid:
                            rule["condition_object_id"] = dest_coid
                        else:
                            print(f"  {label} — SKIPPED: no matching metafield definition in dest store")
                            skip_collection = True
                            break
                    # Remap condition (metaobject GID)
                    cond = rule.get("condition", "")
                    if cond.startswith("gid://shopify/Metaobject/"):
                        dest_cond = all_metaobject_id_map.get(cond)
                        if dest_cond:
                            rule["condition"] = dest_cond
                        # If not mapped, keep original — might still work
                    remapped_rules.append(rule)

                if skip_collection:
                    continue

                coll_data = {
                    "title": collection.get("title", ""),
                    "body_html": collection.get("body_html", ""),
                    "handle": handle,
                    "rules": remapped_rules,
                    "disjunctive": collection.get("disjunctive", False),
                }
                if collection.get("image", {}).get("src"):
                    coll_data["image"] = {"src": collection["image"]["src"]}
                if collection.get("sort_order"):
                    coll_data["sort_order"] = collection["sort_order"]
                created = client.create_smart_collection(coll_data)
            else:
                coll_data = {
                    "title": collection.get("title", ""),
                    "body_html": collection.get("body_html", ""),
                    "handle": handle,
                }
                if collection.get("image", {}).get("src"):
                    coll_data["image"] = {"src": collection["image"]["src"]}
                created = client.create_custom_collection(coll_data)
            dest_id = created.get("id")
            print(f"  {label} — created (id: {dest_id})")
            id_map.setdefault("collections", {})[source_id] = dest_id
            save_json(id_map, id_map_file)
        except Exception as e:
            err_msg = str(e)
            if hasattr(e, 'response') and e.response is not None:
                try:
                    err_msg = json.dumps(e.response.json(), indent=2)
                except Exception:
                    pass
            print(f"  {label} — ERROR: {err_msg}")

    # =============================================
    # Phase 4: Pages
    # =============================================
    pages = load_json(os.path.join(input_dir, "pages.json"))
    print(f"\nImporting {len(pages)} pages...")
    for i, page in enumerate(pages):
        source_id = str(page["id"])
        handle = page.get("handle", "")
        label = f"[{i+1}/{len(pages)}] {page.get('title', '')[:50]}"

        if source_id in id_map.get("pages", {}):
            print(f"  {label} — already imported, skipping")
            continue

        if args.dry_run:
            print(f"  {label} — would create (handle: {handle})")
            continue

        existing = client.get_pages_by_handle(handle)
        if existing:
            dest_id = existing[0]["id"]
            print(f"  {label} — already exists (id: {dest_id}), mapping")
            id_map.setdefault("pages", {})[source_id] = dest_id
            save_json(id_map, id_map_file)
            continue

        page_data = {
            "title": page.get("title", ""),
            "body_html": page.get("body_html", ""),
            "handle": handle,
            "published": page.get("published_at") is not None,
        }
        # Preserve template suffix for special pages
        if page.get("template_suffix"):
            page_data["template_suffix"] = page["template_suffix"]

        created = client.create_page(page_data)
        dest_id = created.get("id")
        print(f"  {label} — created (id: {dest_id})")
        id_map.setdefault("pages", {})[source_id] = dest_id
        save_json(id_map, id_map_file)

    # =============================================
    # Phase 5: Blogs + Articles
    # =============================================
    blogs = load_json(os.path.join(input_dir, "blogs.json"))
    articles = load_json(os.path.join(input_dir, "articles.json"))
    print(f"\nImporting {len(blogs)} blogs...")
    for i, blog in enumerate(blogs):
        source_id = str(blog["id"])
        handle = blog.get("handle", "")
        label = f"[{i+1}/{len(blogs)}] {blog.get('title', '')[:50]}"

        if source_id in id_map.get("blogs", {}):
            print(f"  {label} — already imported, skipping")
            dest_blog_id = id_map["blogs"][source_id]
        elif args.dry_run:
            print(f"  {label} — would create (handle: {handle})")
            dest_blog_id = None
        else:
            existing = client.get_blogs_by_handle(handle)
            if existing:
                dest_blog_id = existing[0]["id"]
                print(f"  {label} — already exists (id: {dest_blog_id}), mapping")
            else:
                blog_data = {"title": blog.get("title", ""), "handle": handle}
                created = client.create_blog(blog_data)
                dest_blog_id = created.get("id")
                print(f"  {label} — created (id: {dest_blog_id})")
            id_map.setdefault("blogs", {})[source_id] = dest_blog_id
            save_json(id_map, id_map_file)

        blog_articles = [a for a in articles if str(a.get("_blog_id")) == str(blog["id"])]
        print(f"    Importing {len(blog_articles)} articles...")
        for j, article in enumerate(blog_articles):
            art_source_id = str(article["id"])
            art_label = f"    [{j+1}/{len(blog_articles)}] {article.get('title', '')[:50]}"

            if art_source_id in id_map.get("articles", {}):
                print(f"  {art_label} — already imported, skipping")
                continue

            if args.dry_run or dest_blog_id is None:
                print(f"  {art_label} — would create")
                continue

            art_data = {
                "title": article.get("title", ""),
                "body_html": article.get("body_html", ""),
                "summary_html": article.get("summary_html", ""),
                "tags": article.get("tags", ""),
                "published": article.get("published_at") is not None,
                "author": article.get("author", ""),
            }
            if article.get("image", {}).get("src"):
                art_data["image"] = {"src": article["image"]["src"]}

            # Include non-reference metafields
            if article.get("metafields"):
                art_data["metafields"] = []
                for mf in article["metafields"]:
                    mf_type = mf.get("type", "")
                    if "reference" in mf_type:
                        continue
                    art_data["metafields"].append({
                        "namespace": mf["namespace"],
                        "key": mf["key"],
                        "value": mf["value"],
                        "type": mf_type,
                    })

            created = client.create_article(dest_blog_id, art_data)
            dest_art_id = created.get("id")
            print(f"  {art_label} — created (id: {dest_art_id})")
            id_map.setdefault("articles", {})[art_source_id] = dest_art_id
            save_json(id_map, id_map_file)

    if not args.dry_run:
        save_json(id_map, id_map_file)

    # =============================================
    # Phase 6: Remap reference fields
    # =============================================
    if not args.dry_run:
        print("\n--- Phase 6: Remapping reference fields ---")
        ref_progress = id_map.get("_ref_remapped", {})

        # 6a. Ingredient → benefit references
        metaobjects_data = load_json(metaobjects_file) if os.path.exists(metaobjects_file) else {}
        ingredients = metaobjects_data.get("ingredient", {}).get("objects", [])
        benefit_map = id_map.get("metaobjects_benefit", {})
        ingredient_map = id_map.get("metaobjects_ingredient", {})
        collection_map = id_map.get("collections", {})

        for obj in ingredients:
            source_id = obj.get("id", "")
            dest_id = ingredient_map.get(source_id)
            if not dest_id or f"ingredient_{source_id}" in ref_progress:
                continue

            fields_to_update = []
            for field in obj.get("fields", []):
                # Remap benefit references
                if field["key"] == "benefits" and field.get("value") and "metaobject_reference" in field.get("type", ""):
                    try:
                        source_refs = json.loads(field["value"])
                        dest_refs = [benefit_map.get(ref, ref) for ref in source_refs if benefit_map.get(ref)]
                        if dest_refs:
                            fields_to_update.append({"key": "benefits", "value": json.dumps(dest_refs)})
                    except (json.JSONDecodeError, TypeError):
                        pass
                # Remap collection reference
                if field["key"] == "collection" and field.get("value") and "collection_reference" in field.get("type", ""):
                    source_coll_gid = field["value"]
                    # Extract numeric ID from GID
                    source_coll_id = source_coll_gid.split("/")[-1] if "/" in source_coll_gid else source_coll_gid
                    dest_coll_id = collection_map.get(source_coll_id)
                    if dest_coll_id:
                        fields_to_update.append({"key": "collection", "value": f"gid://shopify/Collection/{dest_coll_id}"})

            if fields_to_update:
                try:
                    client.update_metaobject(dest_id, fields_to_update)
                    print(f"  Ingredient {obj.get('handle', '')} — remapped {len(fields_to_update)} reference fields")
                except Exception as e:
                    print(f"  Ingredient {obj.get('handle', '')} — error: {e}")

            ref_progress[f"ingredient_{source_id}"] = True

        # 6b. Product → ingredient/faq references via metafieldsSet
        products = load_json(os.path.join(input_dir, "products.json"))
        product_map = id_map.get("products", {})
        faq_map = id_map.get("metaobjects_faq_entry", {})

        for product in products:
            source_id = str(product["id"])
            dest_id = product_map.get(source_id)
            if not dest_id or f"product_{source_id}" in ref_progress:
                continue

            metafields_to_set = []
            for mf in product.get("metafields", []):
                mf_type = mf.get("type", "")
                if "reference" not in mf_type or not mf.get("value"):
                    continue

                ns = mf.get("namespace", "custom")
                key = mf.get("key", "")
                value = mf["value"]

                if key == "ingredients" and "metaobject_reference" in mf_type:
                    try:
                        source_refs = json.loads(value)
                        dest_refs = [ingredient_map.get(ref, ref) for ref in source_refs if ingredient_map.get(ref)]
                        if dest_refs:
                            metafields_to_set.append({
                                "ownerId": f"gid://shopify/Product/{dest_id}",
                                "namespace": ns, "key": key,
                                "value": json.dumps(dest_refs),
                                "type": mf_type,
                            })
                    except (json.JSONDecodeError, TypeError):
                        pass

                elif key == "faqs" and "metaobject_reference" in mf_type:
                    try:
                        source_refs = json.loads(value)
                        dest_refs = [faq_map.get(ref, ref) for ref in source_refs if faq_map.get(ref)]
                        if dest_refs:
                            metafields_to_set.append({
                                "ownerId": f"gid://shopify/Product/{dest_id}",
                                "namespace": ns, "key": key,
                                "value": json.dumps(dest_refs),
                                "type": mf_type,
                            })
                    except (json.JSONDecodeError, TypeError):
                        pass

            if metafields_to_set:
                try:
                    client.set_metafields(metafields_to_set)
                    print(f"  Product '{product.get('title', '')[:40]}' — set {len(metafields_to_set)} reference metafields")
                except Exception as e:
                    print(f"  Product '{product.get('title', '')[:40]}' — error: {e}")

            ref_progress[f"product_{source_id}"] = True

        # 6c. Article → blog_author/ingredient/related references
        articles = load_json(os.path.join(input_dir, "articles.json"))
        article_map = id_map.get("articles", {})
        blog_author_map = id_map.get("metaobjects_blog_author", {})

        for article in articles:
            source_id = str(article["id"])
            dest_id = article_map.get(source_id)
            if not dest_id or f"article_{source_id}" in ref_progress:
                continue

            metafields_to_set = []
            for mf in article.get("metafields", []):
                mf_type = mf.get("type", "")
                if "reference" not in mf_type or not mf.get("value"):
                    continue

                ns = mf.get("namespace", "custom")
                key = mf.get("key", "")
                value = mf["value"]

                if key == "author" and "metaobject_reference" in mf_type:
                    dest_ref = blog_author_map.get(value)
                    if dest_ref:
                        metafields_to_set.append({
                            "ownerId": f"gid://shopify/OnlineStoreArticle/{dest_id}",
                            "namespace": ns, "key": key,
                            "value": dest_ref,
                            "type": mf_type,
                        })

                elif key == "ingredients" and "metaobject_reference" in mf_type:
                    try:
                        source_refs = json.loads(value)
                        dest_refs = [ingredient_map.get(ref, ref) for ref in source_refs if ingredient_map.get(ref)]
                        if dest_refs:
                            metafields_to_set.append({
                                "ownerId": f"gid://shopify/OnlineStoreArticle/{dest_id}",
                                "namespace": ns, "key": key,
                                "value": json.dumps(dest_refs),
                                "type": mf_type,
                            })
                    except (json.JSONDecodeError, TypeError):
                        pass

                elif key == "related_products" and "product_reference" in mf_type:
                    try:
                        source_refs = json.loads(value)
                        dest_refs = []
                        for ref in source_refs:
                            ref_id = ref.split("/")[-1] if "/" in ref else ref
                            dest_prod_id = product_map.get(ref_id)
                            if dest_prod_id:
                                dest_refs.append(f"gid://shopify/Product/{dest_prod_id}")
                        if dest_refs:
                            metafields_to_set.append({
                                "ownerId": f"gid://shopify/OnlineStoreArticle/{dest_id}",
                                "namespace": ns, "key": key,
                                "value": json.dumps(dest_refs),
                                "type": mf_type,
                            })
                    except (json.JSONDecodeError, TypeError):
                        pass

                elif key == "related_articles" and "article_reference" in mf_type:
                    try:
                        source_refs = json.loads(value)
                        dest_refs = []
                        for ref in source_refs:
                            ref_id = ref.split("/")[-1] if "/" in ref else ref
                            dest_art_id = article_map.get(ref_id)
                            if dest_art_id:
                                dest_refs.append(f"gid://shopify/OnlineStoreArticle/{dest_art_id}")
                        if dest_refs:
                            metafields_to_set.append({
                                "ownerId": f"gid://shopify/OnlineStoreArticle/{dest_id}",
                                "namespace": ns, "key": key,
                                "value": json.dumps(dest_refs),
                                "type": mf_type,
                            })
                    except (json.JSONDecodeError, TypeError):
                        pass

            if metafields_to_set:
                try:
                    client.set_metafields(metafields_to_set)
                    print(f"  Article '{article.get('title', '')[:40]}' — set {len(metafields_to_set)} reference metafields")
                except Exception as e:
                    # OnlineStoreArticle GID may not work for metafieldsSet in
                    # some API versions — retry with Article GID format
                    if "invalid id" in str(e):
                        for mf in metafields_to_set:
                            mf["ownerId"] = mf["ownerId"].replace(
                                "OnlineStoreArticle", "Article"
                            )
                        try:
                            client.set_metafields(metafields_to_set)
                            print(f"  Article '{article.get('title', '')[:40]}' — set {len(metafields_to_set)} reference metafields (Article GID)")
                        except Exception as e2:
                            print(f"  Article '{article.get('title', '')[:40]}' — error: {e2}")
                    else:
                        print(f"  Article '{article.get('title', '')[:40]}' — error: {e}")

            ref_progress[f"article_{source_id}"] = True

        id_map["_ref_remapped"] = ref_progress
        save_json(id_map, id_map_file)
        print("  Reference remapping complete.")

    print("\n--- Import Summary ---")
    print(f"  Products:    {len(id_map.get('products', {}))}")
    print(f"  Collections: {len(id_map.get('collections', {}))}")
    print(f"  Pages:       {len(id_map.get('pages', {}))}")
    print(f"  Blogs:       {len(id_map.get('blogs', {}))}")
    print(f"  Articles:    {len(id_map.get('articles', {}))}")
    mo_keys = [k for k in id_map if k.startswith("metaobjects_")]
    for k in mo_keys:
        print(f"  {k}: {len(id_map[k])}")
    if args.dry_run:
        print("  (dry run — nothing was created)")


if __name__ == "__main__":
    main()
