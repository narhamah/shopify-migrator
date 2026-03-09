#!/usr/bin/env python3
"""Compare and resolve metaobject differences between Spain and Saudi stores.

Detects schema mismatches (missing definitions, field differences), entry
gaps (objects in Spain but not Saudi), and broken references. Then fixes them.

Usage:
    python resolve_metaobject_diffs.py --inspect       # Show differences
    python resolve_metaobject_diffs.py --dry-run       # Preview fixes
    python resolve_metaobject_diffs.py                  # Fix all differences
    python resolve_metaobject_diffs.py --type ingredient # Only check one type
"""

import argparse
import json
import os
import time

from dotenv import load_dotenv

from shopify_client import ShopifyClient
from utils import load_json, save_json, ID_MAP_FILE, DEFINITION_ORDER


# ---------------------------------------------------------------------------
# Step 1: Compare definitions (schema)
# ---------------------------------------------------------------------------

def compare_definitions(spain, saudi):
    """Compare metaobject definitions between stores.

    Returns:
        dict with keys:
          missing_in_saudi: list of types only in Spain
          field_diffs: dict of {type: {missing_fields: [...], type_mismatches: [...]}}
          matching: list of types that match perfectly
    """
    spain_defs = spain.get_metaobject_definitions()
    saudi_defs = saudi.get_metaobject_definitions()

    spain_by_type = {d["type"]: d for d in spain_defs}
    saudi_by_type = {d["type"]: d for d in saudi_defs}

    result = {
        "missing_in_saudi": [],
        "extra_in_saudi": [],
        "field_diffs": {},
        "matching": [],
    }

    for mo_type, spain_def in spain_by_type.items():
        if mo_type not in saudi_by_type:
            result["missing_in_saudi"].append(mo_type)
            continue

        saudi_def = saudi_by_type[mo_type]
        spain_fields = {f["key"]: f for f in spain_def.get("fieldDefinitions", [])}
        saudi_fields = {f["key"]: f for f in saudi_def.get("fieldDefinitions", [])}

        missing = []
        type_mismatches = []

        for key, spain_field in spain_fields.items():
            if key not in saudi_fields:
                missing.append({
                    "key": key,
                    "name": spain_field.get("name", key),
                    "type": spain_field["type"]["name"],
                })
            else:
                spain_type = spain_field["type"]["name"]
                saudi_type = saudi_fields[key]["type"]["name"]
                if spain_type != saudi_type:
                    type_mismatches.append({
                        "key": key,
                        "spain_type": spain_type,
                        "saudi_type": saudi_type,
                    })

        if missing or type_mismatches:
            result["field_diffs"][mo_type] = {
                "missing_fields": missing,
                "type_mismatches": type_mismatches,
            }
        else:
            result["matching"].append(mo_type)

    for mo_type in saudi_by_type:
        if mo_type not in spain_by_type:
            result["extra_in_saudi"].append(mo_type)

    return result, spain_by_type, saudi_by_type


# ---------------------------------------------------------------------------
# Step 2: Compare entries
# ---------------------------------------------------------------------------

def compare_entries(spain, saudi, mo_type, id_map):
    """Compare metaobject entries between stores for a given type.

    Returns:
        dict with keys:
          missing_in_saudi: list of {id, handle} in Spain but not Saudi
          mapped: count of entries with valid mappings
          unmapped: list of {id, handle} in Spain with no mapping
          orphaned_in_saudi: list of handles in Saudi not matching any Spain entry
    """
    spain_entries = spain.get_metaobjects(mo_type)
    saudi_entries = saudi.get_metaobjects(mo_type)

    spain_by_handle = {e["handle"]: e for e in spain_entries}
    saudi_by_handle = {e["handle"]: e for e in saudi_entries}

    map_key = f"metaobjects_{mo_type}"
    mo_map = id_map.get(map_key, {})

    result = {
        "spain_count": len(spain_entries),
        "saudi_count": len(saudi_entries),
        "missing_in_saudi": [],
        "mapped": 0,
        "unmapped": [],
        "orphaned_in_saudi": [],
    }

    for handle, entry in spain_by_handle.items():
        source_gid = entry["id"]

        if source_gid in mo_map:
            result["mapped"] += 1
        elif handle in saudi_by_handle:
            # Exists by handle but not in id_map — needs mapping
            result["unmapped"].append({
                "id": source_gid,
                "handle": handle,
                "saudi_id": saudi_by_handle[handle]["id"],
            })
        else:
            result["missing_in_saudi"].append({
                "id": source_gid,
                "handle": handle,
            })

    for handle in saudi_by_handle:
        if handle not in spain_by_handle:
            result["orphaned_in_saudi"].append(handle)

    return result


# ---------------------------------------------------------------------------
# Step 3: Compare references (links between metaobjects)
# ---------------------------------------------------------------------------

def check_references(saudi, id_map):
    """Check that all metaobject reference fields point to valid entries.

    Returns list of broken references.
    """
    broken = []

    # Build set of all known Saudi metaobject GIDs
    valid_gids = set()
    for map_key, mapping in id_map.items():
        if map_key.startswith("metaobjects_"):
            for dest_gid in mapping.values():
                valid_gids.add(dest_gid)

    saudi_defs = saudi.get_metaobject_definitions()
    for defn in saudi_defs:
        mo_type = defn.get("type", "")
        field_defs = defn.get("fieldDefinitions", [])

        ref_fields = []
        for fd in field_defs:
            ft = fd.get("type", {})
            ft_name = ft.get("name", "") if isinstance(ft, dict) else str(ft)
            if "metaobject_reference" in ft_name:
                ref_fields.append({
                    "key": fd["key"],
                    "is_list": "list." in ft_name,
                })

        if not ref_fields:
            continue

        entries = saudi.get_metaobjects(mo_type)
        for entry in entries:
            handle = entry.get("handle", "")
            for rf in ref_fields:
                field = None
                for f in entry.get("fields", []):
                    if f["key"] == rf["key"]:
                        field = f
                        break
                if not field or not field.get("value"):
                    continue

                if rf["is_list"]:
                    try:
                        refs = json.loads(field["value"])
                    except (json.JSONDecodeError, TypeError):
                        continue
                    for ref_gid in refs:
                        if ref_gid not in valid_gids:
                            # Verify via API
                            try:
                                result = saudi._graphql("""
                                    query checkNode($id: ID!) {
                                        node(id: $id) { id }
                                    }
                                """, {"id": ref_gid})
                                if not result.get("node"):
                                    broken.append({
                                        "type": mo_type,
                                        "handle": handle,
                                        "field": rf["key"],
                                        "broken_ref": ref_gid,
                                    })
                            except Exception:
                                broken.append({
                                    "type": mo_type,
                                    "handle": handle,
                                    "field": rf["key"],
                                    "broken_ref": ref_gid,
                                })
                else:
                    ref_gid = field["value"]
                    if ref_gid not in valid_gids:
                        try:
                            result = saudi._graphql("""
                                query checkNode($id: ID!) {
                                    node(id: $id) { id }
                                }
                            """, {"id": ref_gid})
                            if not result.get("node"):
                                broken.append({
                                    "type": mo_type,
                                    "handle": handle,
                                    "field": rf["key"],
                                    "broken_ref": ref_gid,
                                })
                        except Exception:
                            broken.append({
                                "type": mo_type,
                                "handle": handle,
                                "field": rf["key"],
                                "broken_ref": ref_gid,
                            })

    return broken


# ---------------------------------------------------------------------------
# Fixes
# ---------------------------------------------------------------------------

def fix_missing_definitions(spain, saudi, missing_types, spain_defs, saudi_defs, dry_run=False):
    """Create missing metaobject definitions in Saudi store."""
    # Dependency order: benefit/faq_entry before ingredient
    DEP_ORDER = DEFINITION_ORDER
    ordered = sorted(missing_types, key=lambda t: DEP_ORDER.index(t) if t in DEP_ORDER else 999)

    for mo_type in ordered:
        spain_def = spain_defs[mo_type]
        field_defs = []
        for f in spain_def.get("fieldDefinitions", []):
            field_def = {
                "key": f["key"],
                "name": f.get("name", f["key"]),
                "type": f["type"]["name"],
            }
            # Handle validations — remap metaobject_definition_id references
            if f.get("validations"):
                validations = []
                for v in f["validations"]:
                    if v["name"] == "metaobject_definition_id" and v.get("value"):
                        # Find the referenced type
                        ref_type = None
                        for sd in spain_defs.values():
                            if sd["id"] == v["value"]:
                                ref_type = sd["type"]
                                break
                        if ref_type and ref_type in saudi_defs:
                            validations.append({
                                "name": v["name"],
                                "value": saudi_defs[ref_type]["id"],
                            })
                        elif ref_type:
                            # Will be created in this run — skip validation for now
                            pass
                    else:
                        validations.append({"name": v["name"], "value": v["value"]})
                if validations:
                    field_def["validations"] = validations
            field_defs.append(field_def)

        display_key = None
        for f in spain_def.get("fieldDefinitions", []):
            if f["key"] in ("name", "title", "question"):
                display_key = f["key"]
                break
        if not display_key and spain_def.get("fieldDefinitions"):
            display_key = spain_def["fieldDefinitions"][0]["key"]

        def_input = {
            "type": mo_type,
            "name": spain_def.get("name", mo_type),
            "access": {"storefront": "PUBLIC_READ"},
            "capabilities": {"publishable": {"enabled": True}},
            "displayNameKey": display_key,
            "fieldDefinitions": field_defs,
        }

        if dry_run:
            print(f"  WOULD CREATE definition '{mo_type}' ({len(field_defs)} fields)")
            continue

        try:
            result = saudi.create_metaobject_definition(def_input)
            if result:
                print(f"  CREATED definition '{mo_type}' → {result['id']}")
                saudi_defs[mo_type] = result
            else:
                print(f"  '{mo_type}' already exists")
        except Exception as e:
            print(f"  ERROR creating '{mo_type}': {e}")


def fix_missing_fields(spain, saudi, field_diffs, spain_defs, saudi_defs, dry_run=False):
    """Add missing fields to existing Saudi definitions."""
    for mo_type, diff in field_diffs.items():
        missing = diff.get("missing_fields", [])
        if not missing:
            continue

        saudi_def_id = saudi_defs.get(mo_type, {}).get("id")
        if not saudi_def_id:
            print(f"  SKIP: no Saudi definition for '{mo_type}'")
            continue

        for field in missing:
            if dry_run:
                print(f"  WOULD ADD field '{field['key']}' ({field['type']}) to '{mo_type}'")
                continue

            try:
                saudi.update_metaobject_definition(saudi_def_id, {
                    "fieldDefinitions": [{
                        "create": {
                            "key": field["key"],
                            "name": field["name"],
                            "type": field["type"],
                        }
                    }],
                })
                print(f"  ADDED field '{field['key']}' to '{mo_type}'")
            except Exception as e:
                print(f"  ERROR adding field '{field['key']}' to '{mo_type}': {e}")


def fix_missing_entries(spain, saudi, mo_type, missing, id_map, dry_run=False):
    """Create missing metaobject entries in Saudi store."""
    id_map_file = "data/id_map.json"
    map_key = f"metaobjects_{mo_type}"

    for entry_info in missing:
        source_gid = entry_info["id"]
        handle = entry_info["handle"]

        # Fetch full entry from Spain
        try:
            source_obj = spain._graphql("""
                query getMetaobject($id: ID!) {
                    metaobject(id: $id) {
                        id handle type
                        fields { key value type }
                    }
                }
            """, {"id": source_gid})
            source_mo = source_obj.get("metaobject")
            if not source_mo:
                print(f"  SKIP {handle}: could not fetch from Spain")
                continue
        except Exception as e:
            print(f"  SKIP {handle}: {e}")
            continue

        # Build fields — skip file and metaobject references for first pass
        fields = []
        for field in source_mo.get("fields", []):
            ft = field.get("type", "")
            if "file_reference" in ft or "metaobject_reference" in ft or "collection_reference" in ft:
                continue
            if field.get("value"):
                fields.append({"key": field["key"], "value": field["value"]})

        if dry_run:
            print(f"  WOULD CREATE '{handle}' ({len(fields)} text fields)")
            continue

        try:
            created = saudi.create_metaobject({
                "type": mo_type,
                "handle": handle,
                "fields": fields,
            })
            if created:
                dest_id = created["id"]
                id_map.setdefault(map_key, {})[source_gid] = dest_id
                save_json(id_map, id_map_file)
                print(f"  CREATED '{handle}' → {dest_id}")
            else:
                # Already exists — look up and map
                existing = saudi.get_metaobjects_by_handle(mo_type, handle)
                if existing:
                    id_map.setdefault(map_key, {})[source_gid] = existing["id"]
                    save_json(id_map, id_map_file)
                    print(f"  EXISTS '{handle}' → {existing['id']} (mapped)")
        except Exception as e:
            print(f"  ERROR creating '{handle}': {e}")


def fix_unmapped_entries(unmapped, id_map, mo_type, dry_run=False):
    """Add id_map entries for metaobjects that exist in both stores by handle."""
    id_map_file = "data/id_map.json"
    map_key = f"metaobjects_{mo_type}"

    for entry_info in unmapped:
        source_gid = entry_info["id"]
        saudi_id = entry_info["saudi_id"]
        handle = entry_info["handle"]

        if dry_run:
            print(f"  WOULD MAP '{handle}': {source_gid} → {saudi_id}")
            continue

        id_map.setdefault(map_key, {})[source_gid] = saudi_id
        save_json(id_map, id_map_file)
        print(f"  MAPPED '{handle}': {source_gid} → {saudi_id}")


def fix_references(spain, saudi, id_map, dry_run=False):
    """Re-link metaobject reference fields using id_map.

    Handles:
      - ingredient → benefits (list.metaobject_reference → benefit)
      - ingredient → collection (collection_reference)
      - product → ingredients, faqs (list.metaobject_reference)
      - article → author, ingredients, related_products, related_articles
    """
    print("\n  Relinking metaobject references...")

    # Build a unified source→dest GID map across all metaobject types
    all_mo_map = {}
    for map_key, mapping in id_map.items():
        if map_key.startswith("metaobjects_"):
            all_mo_map.update(mapping)

    collection_map = id_map.get("collections", {})
    product_map = id_map.get("products", {})
    article_map = id_map.get("articles", {})

    updated = 0

    # 1. Ingredient → benefit references
    ingredient_map = id_map.get("metaobjects_ingredient", {})
    benefit_map = id_map.get("metaobjects_benefit", {})

    for source_gid, dest_gid in ingredient_map.items():
        try:
            source_obj = spain._graphql("""
                query getMetaobject($id: ID!) {
                    metaobject(id: $id) {
                        handle
                        fields { key value type }
                    }
                }
            """, {"id": source_gid})
            source_mo = source_obj.get("metaobject")
            if not source_mo:
                continue
        except Exception:
            continue

        fields_to_update = []
        for field in source_mo.get("fields", []):
            key = field["key"]
            ft = field.get("type", "")
            value = field.get("value", "")

            if key == "benefits" and "metaobject_reference" in ft and value:
                try:
                    source_refs = json.loads(value)
                    dest_refs = [benefit_map.get(ref) for ref in source_refs if benefit_map.get(ref)]
                    if dest_refs:
                        fields_to_update.append({"key": "benefits", "value": json.dumps(dest_refs)})
                except (json.JSONDecodeError, TypeError):
                    pass

            elif key == "collection" and "collection_reference" in ft and value:
                source_coll_id = value.split("/")[-1] if "/" in value else value
                dest_coll_id = collection_map.get(source_coll_id)
                if dest_coll_id:
                    fields_to_update.append({
                        "key": "collection",
                        "value": f"gid://shopify/Collection/{dest_coll_id}",
                    })

        if fields_to_update:
            handle = source_mo.get("handle", "")
            if dry_run:
                print(f"    WOULD update ingredient '{handle}': {[f['key'] for f in fields_to_update]}")
            else:
                try:
                    saudi.update_metaobject(dest_gid, fields_to_update)
                    print(f"    Updated ingredient '{handle}': {[f['key'] for f in fields_to_update]}")
                    updated += 1
                except Exception as e:
                    print(f"    ERROR updating ingredient '{handle}': {e}")

    # 2. Product → ingredient/faq references
    for source_id, dest_id in product_map.items():
        # Load source product metafields from exported data
        en_products = load_json("data/english/products.json")
        if not isinstance(en_products, list):
            break

        source_product = None
        for p in en_products:
            if str(p.get("id", "")) == source_id:
                source_product = p
                break
        if not source_product:
            continue

        metafields_to_set = []
        for mf in source_product.get("metafields", []):
            mf_type = mf.get("type", "")
            if "metaobject_reference" not in mf_type or not mf.get("value"):
                continue

            key = mf.get("key", "")
            value = mf["value"]

            if key in ("ingredients", "faqs"):
                try:
                    source_refs = json.loads(value)
                    dest_refs = [all_mo_map.get(ref) for ref in source_refs if all_mo_map.get(ref)]
                    if dest_refs:
                        metafields_to_set.append({
                            "ownerId": f"gid://shopify/Product/{dest_id}",
                            "namespace": mf.get("namespace", "custom"),
                            "key": key,
                            "value": json.dumps(dest_refs),
                            "type": mf_type,
                        })
                except (json.JSONDecodeError, TypeError):
                    pass

        if metafields_to_set:
            title = source_product.get("title", "")[:30]
            if dry_run:
                print(f"    WOULD set product '{title}' refs: {[m['key'] for m in metafields_to_set]}")
            else:
                try:
                    saudi.set_metafields(metafields_to_set)
                    print(f"    Set product '{title}' refs: {[m['key'] for m in metafields_to_set]}")
                    updated += 1
                except Exception as e:
                    print(f"    ERROR setting product '{title}' refs: {e}")

    print(f"\n  Relinked {updated} reference fields")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Compare and resolve metaobject differences between Spain and Saudi stores")
    parser.add_argument("--inspect", action="store_true",
                        help="Show differences without fixing")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be fixed")
    parser.add_argument("--type", type=str, default=None,
                        help="Only check a specific metaobject type")
    parser.add_argument("--skip-refs", action="store_true",
                        help="Skip reference relinking")
    args = parser.parse_args()

    spain_url = os.environ.get("SPAIN_SHOP_URL")
    spain_token = os.environ.get("SPAIN_ACCESS_TOKEN")
    saudi_url = os.environ.get("SAUDI_SHOP_URL")
    saudi_token = os.environ.get("SAUDI_ACCESS_TOKEN")

    if not all([spain_url, spain_token, saudi_url, saudi_token]):
        print("ERROR: Set SPAIN_SHOP_URL, SPAIN_ACCESS_TOKEN, SAUDI_SHOP_URL, SAUDI_ACCESS_TOKEN in .env")
        return

    spain = ShopifyClient(spain_url, spain_token)
    saudi = ShopifyClient(saudi_url, saudi_token)

    id_map = load_json("data/id_map.json") if os.path.exists("data/id_map.json") else {}
    dry_run = args.dry_run or args.inspect

    print("=" * 60)
    print("METAOBJECT DIFF & RESOLVE: Spain ↔ Saudi")
    print("=" * 60)

    # --- Step 1: Schema comparison ---
    print("\n--- Step 1: Schema Comparison ---")
    diffs, spain_defs, saudi_defs = compare_definitions(spain, saudi)

    if diffs["missing_in_saudi"]:
        print(f"\n  MISSING definitions in Saudi: {diffs['missing_in_saudi']}")
    if diffs["extra_in_saudi"]:
        print(f"  EXTRA definitions in Saudi (not in Spain): {diffs['extra_in_saudi']}")
    if diffs["field_diffs"]:
        for mo_type, diff in diffs["field_diffs"].items():
            if diff["missing_fields"]:
                print(f"  {mo_type}: missing fields: {[f['key'] for f in diff['missing_fields']]}")
            if diff["type_mismatches"]:
                for mm in diff["type_mismatches"]:
                    print(f"  {mo_type}.{mm['key']}: type mismatch — Spain={mm['spain_type']}, Saudi={mm['saudi_type']}")
    if diffs["matching"]:
        print(f"  Matching definitions: {diffs['matching']}")

    # Fix schema
    if diffs["missing_in_saudi"]:
        print("\n  Fixing missing definitions...")
        fix_missing_definitions(spain, saudi, diffs["missing_in_saudi"],
                                spain_defs, saudi_defs, dry_run=dry_run)

    if diffs["field_diffs"]:
        print("\n  Fixing missing fields...")
        fix_missing_fields(spain, saudi, diffs["field_diffs"],
                           spain_defs, saudi_defs, dry_run=dry_run)

    # --- Step 2: Entry comparison ---
    print("\n--- Step 2: Entry Comparison ---")

    types_to_check = [args.type] if args.type else list(spain_defs.keys())
    DEP_ORDER = DEFINITION_ORDER
    types_to_check = sorted(types_to_check,
                            key=lambda t: DEP_ORDER.index(t) if t in DEP_ORDER else 999)

    for mo_type in types_to_check:
        print(f"\n  --- {mo_type} ---")
        entry_diff = compare_entries(spain, saudi, mo_type, id_map)

        print(f"    Spain: {entry_diff['spain_count']} entries")
        print(f"    Saudi: {entry_diff['saudi_count']} entries")
        print(f"    Mapped: {entry_diff['mapped']}")
        if entry_diff["unmapped"]:
            print(f"    Unmapped (exist in both, need mapping): {len(entry_diff['unmapped'])}")
        if entry_diff["missing_in_saudi"]:
            print(f"    Missing in Saudi: {len(entry_diff['missing_in_saudi'])}")
        if entry_diff["orphaned_in_saudi"]:
            print(f"    Orphaned in Saudi (not in Spain): {entry_diff['orphaned_in_saudi']}")

        # Fix unmapped entries (just need id_map update)
        if entry_diff["unmapped"]:
            fix_unmapped_entries(entry_diff["unmapped"], id_map, mo_type, dry_run=dry_run)

        # Fix missing entries
        if entry_diff["missing_in_saudi"]:
            fix_missing_entries(spain, saudi, mo_type,
                                entry_diff["missing_in_saudi"], id_map, dry_run=dry_run)

    # --- Step 3: Reference check ---
    if not args.skip_refs:
        print("\n--- Step 3: Reference Validation ---")
        broken = check_references(saudi, id_map)
        if broken:
            print(f"\n  Found {len(broken)} broken references:")
            for b in broken[:10]:
                print(f"    {b['type']}.{b['handle']}.{b['field']} → {b['broken_ref'][:50]}")
            if len(broken) > 10:
                print(f"    ... and {len(broken) - 10} more")

            print("\n  Relinking references...")
            fix_references(spain, saudi, id_map, dry_run=dry_run)
        else:
            print("  All references are valid!")
    else:
        print("\n  Skipping reference validation (--skip-refs)")

    # Save final id_map
    if not dry_run:
        save_json(id_map, "data/id_map.json")

    # --- Summary ---
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    save_json(diffs, "data/metaobject_diffs.json")
    print(f"  Diff report saved to data/metaobject_diffs.json")
    for map_key in sorted(id_map.keys()):
        if map_key.startswith("metaobjects_"):
            print(f"  {map_key}: {len(id_map[map_key])} mappings")


if __name__ == "__main__":
    main()
