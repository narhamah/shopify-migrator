#!/usr/bin/env python3
"""Migrate metaobject entries from source Shopify store to destination.

Reads metaobject definitions and entries from the source store, creates
matching definitions in the destination store (with Online Store access
enabled for publishable types), then migrates all entries — including
downloading and re-uploading file references (images).

Usage:
    # Dry run — show what would be migrated
    python migrate_metaobjects.py --type ingredient --dry-run

    # Migrate all ingredient metaobjects
    python migrate_metaobjects.py --type ingredient

    # Migrate all metaobject types
    python migrate_metaobjects.py --all

    # List available types in source store
    python migrate_metaobjects.py --list
"""

import argparse
import json
import os
import time

from dotenv import load_dotenv
from shopify_client import ShopifyClient


def load_json(filepath):
    if not os.path.exists(filepath):
        return {}
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def list_types(source):
    """List all metaobject types in the source store."""
    defs = source.get_metaobject_definitions()
    print(f"\nMetaobject types in source store ({len(defs)}):")
    for d in defs:
        fields = d.get("fieldDefinitions", [])
        field_summary = ", ".join(f"{f['key']}:{f['type']['name']}" for f in fields[:5])
        if len(fields) > 5:
            field_summary += f", ... (+{len(fields)-5})"
        print(f"  {d['type']} ({d['name']}) — {len(fields)} fields: {field_summary}")

        # Count entries
        entries = source.get_metaobjects(d["type"])
        print(f"    Entries: {len(entries)}")


def ensure_definition(source, dest, mo_type, dry_run=False):
    """Ensure the metaobject definition exists in the destination store.

    Creates it with publishable capability enabled for Online Store access.
    Returns the destination definition ID, or None on error.
    """
    # Get source definition
    source_defs = source.get_metaobject_definitions()
    source_def = None
    for d in source_defs:
        if d["type"] == mo_type:
            source_def = d
            break
    if not source_def:
        print(f"ERROR: Type '{mo_type}' not found in source store")
        return None

    # Check if already exists in dest
    dest_defs = dest.get_metaobject_definitions()
    dest_def_map = {d["type"]: d for d in dest_defs}

    if mo_type in dest_def_map:
        print(f"  Definition '{mo_type}' already exists in destination")
        return dest_def_map[mo_type]["id"]

    if dry_run:
        print(f"  WOULD CREATE definition '{mo_type}' with {len(source_def['fieldDefinitions'])} fields")
        return "dry-run-id"

    # Build field definitions
    field_defs = []
    for f in source_def["fieldDefinitions"]:
        field_def = {
            "key": f["key"],
            "name": f["name"],
            "type": f["type"]["name"],
        }
        # Include validations if present
        if f.get("validations"):
            validations = []
            for v in f["validations"]:
                # Skip metaobject_definition_id validations — they reference source GIDs
                # We'll resolve them separately
                if v["name"] == "metaobject_definition_id" and v.get("value"):
                    # Try to find the referenced type in dest
                    ref_type = _resolve_definition_type(source_defs, v["value"])
                    if ref_type and ref_type in dest_def_map:
                        validations.append({
                            "name": v["name"],
                            "value": dest_def_map[ref_type]["id"]
                        })
                    else:
                        print(f"    WARNING: Cannot resolve reference type for {f['key']} — skipping validation")
                else:
                    validations.append({"name": v["name"], "value": v["value"]})
            if validations:
                field_def["validations"] = validations
        field_defs.append(field_def)

    # Determine displayNameKey
    display_key = None
    for f in source_def["fieldDefinitions"]:
        if f["key"] in ("name", "title", "question"):
            display_key = f["key"]
            break
    if not display_key and source_def["fieldDefinitions"]:
        display_key = source_def["fieldDefinitions"][0]["key"]

    definition_input = {
        "type": mo_type,
        "name": source_def["name"],
        "access": {"storefront": "PUBLIC_READ"},
        "capabilities": {
            "publishable": {"enabled": True},
        },
        "displayNameKey": display_key,
        "fieldDefinitions": field_defs,
    }

    try:
        result = dest.create_metaobject_definition(definition_input)
        if result:
            print(f"  CREATED definition '{mo_type}' → {result['id']}")
            return result["id"]
        else:
            # Already exists — refetch
            dest_defs = dest.get_metaobject_definitions()
            for d in dest_defs:
                if d["type"] == mo_type:
                    return d["id"]
            return None
    except Exception as e:
        print(f"  ERROR creating definition: {e}")
        return None


def _resolve_definition_type(all_defs, definition_gid):
    """Find the metaobject type for a given definition GID."""
    for d in all_defs:
        if d["id"] == definition_gid:
            return d["type"]
    return None


def migrate_entries(source, dest, mo_type, file_map, dry_run=False):
    """Migrate all metaobject entries of a given type.

    Returns dict mapping source GID → dest GID.
    """
    entries = source.get_metaobjects(mo_type)
    print(f"\n  Migrating {len(entries)} '{mo_type}' entries...")

    id_map = {}
    progress_file = f"data/metaobject_migrate_{mo_type}_progress.json"
    progress = load_json(progress_file) if os.path.exists(progress_file) else {}
    if not isinstance(progress, dict):
        progress = {}

    for i, entry in enumerate(entries):
        source_id = entry["id"]
        handle = entry["handle"]

        if source_id in progress:
            id_map[source_id] = progress[source_id]
            continue

        # Check if entry already exists in dest by handle
        existing = dest.get_metaobjects_by_handle(mo_type, handle)
        if existing:
            dest_id = existing["id"]
            id_map[source_id] = dest_id
            progress[source_id] = dest_id
            print(f"    EXISTS [{i+1}/{len(entries)}]: {handle} → {dest_id}")
            continue

        # Build fields, handling file references
        fields = []
        deferred_fields = []  # Fields to update later (metaobject references)
        for field in entry.get("fields", []):
            field_type = field.get("type", "")
            value = field.get("value")
            key = field.get("key")

            if not value:
                continue

            # File references — download from source, upload to dest
            if "file_reference" in field_type:
                if dry_run:
                    fields.append({"key": key, "value": value})
                    continue

                resolved = _resolve_file_reference(source, dest, value, file_map)
                if resolved:
                    fields.append({"key": key, "value": resolved})
                else:
                    print(f"      WARNING: Could not resolve file for {key}")
                continue

            # Metaobject references — defer until all entries are created
            if "metaobject_reference" in field_type:
                deferred_fields.append(field)
                continue

            fields.append({"key": key, "value": value})

        if dry_run:
            field_names = [f["key"] for f in fields]
            print(f"    WOULD CREATE [{i+1}/{len(entries)}]: {handle} (fields: {', '.join(field_names)})")
            continue

        mo_input = {
            "type": mo_type,
            "handle": handle,
            "fields": fields,
        }

        try:
            created = dest.create_metaobject(mo_input)
            if created:
                dest_id = created["id"]
                id_map[source_id] = dest_id
                progress[source_id] = dest_id
                print(f"    CREATED [{i+1}/{len(entries)}]: {handle} → {dest_id}")
            else:
                # Already exists
                existing = dest.get_metaobjects_by_handle(mo_type, handle)
                if existing:
                    dest_id = existing["id"]
                    id_map[source_id] = dest_id
                    progress[source_id] = dest_id
                    print(f"    EXISTS [{i+1}/{len(entries)}]: {handle} → {dest_id}")
        except Exception as e:
            print(f"    ERROR [{i+1}/{len(entries)}]: {handle} — {e}")

        if (i + 1) % 10 == 0:
            save_json(progress, progress_file)

    save_json(progress, progress_file)

    # Second pass: update deferred metaobject reference fields
    print(f"\n  Updating metaobject reference fields...")
    updated = 0
    for entry in entries:
        source_id = entry["id"]
        dest_id = id_map.get(source_id)
        if not dest_id:
            continue

        fields_to_update = []
        for field in entry.get("fields", []):
            field_type = field.get("type", "")
            value = field.get("value")
            if not value or "metaobject_reference" not in field_type:
                continue

            # Remap GIDs
            if field_type == "list.metaobject_reference":
                try:
                    source_gids = json.loads(value)
                    dest_gids = [id_map.get(gid, gid) for gid in source_gids]
                    fields_to_update.append({"key": field["key"], "value": json.dumps(dest_gids)})
                except json.JSONDecodeError:
                    pass
            else:
                dest_ref = id_map.get(value, value)
                fields_to_update.append({"key": field["key"], "value": dest_ref})

        if fields_to_update and not dry_run:
            try:
                dest.update_metaobject(dest_id, fields_to_update)
                updated += 1
            except Exception as e:
                print(f"    ERROR updating refs for {entry['handle']}: {e}")

    if updated:
        print(f"    Updated references on {updated} entries")

    return id_map


def _resolve_file_reference(source, dest, file_gid, file_map):
    """Download a file from source store and upload to dest, returning new GID."""
    if file_gid in file_map:
        return file_map[file_gid]

    # Handle list.file_reference (JSON array of GIDs)
    if file_gid.startswith("["):
        try:
            gids = json.loads(file_gid)
            resolved = []
            for g in gids:
                r = _resolve_single_file(source, dest, g, file_map)
                if r:
                    resolved.append(r)
            result = json.dumps(resolved)
            file_map[file_gid] = result
            return result
        except json.JSONDecodeError:
            pass

    return _resolve_single_file(source, dest, file_gid, file_map)


def _resolve_single_file(source, dest, file_gid, file_map):
    """Resolve a single file GID."""
    if file_gid in file_map:
        return file_map[file_gid]

    try:
        file_info = source.get_file_by_id(file_gid)
        if not file_info:
            print(f"      File not found in source: {file_gid}")
            return None

        # Get the public URL
        url = None
        if "image" in file_info and file_info["image"]:
            url = file_info["image"].get("url")
        elif "url" in file_info:
            url = file_info["url"]

        if not url:
            print(f"      No URL for file: {file_gid}")
            return None

        alt = file_info.get("alt", "")
        result = dest.upload_file_from_url(url, alt=alt, optimize=True)
        if result:
            new_gid = result.get("id") or result
            file_map[file_gid] = new_gid
            print(f"      Uploaded: {file_gid} → {new_gid}")
            return new_gid
    except Exception as e:
        print(f"      ERROR uploading file {file_gid}: {e}")
    return None


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Migrate metaobjects between Shopify stores")
    parser.add_argument("--type", type=str, help="Metaobject type to migrate (e.g. 'ingredient')")
    parser.add_argument("--all", action="store_true", help="Migrate all metaobject types")
    parser.add_argument("--list", action="store_true", help="List metaobject types in source store")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    if not args.type and not args.all and not args.list:
        parser.print_help()
        return

    # Source store (Spain)
    source_url = os.environ.get("SPAIN_SHOP_URL")
    source_token = os.environ.get("SPAIN_ACCESS_TOKEN")
    if not source_url or not source_token:
        print("ERROR: SPAIN_SHOP_URL and SPAIN_ACCESS_TOKEN must be set in .env")
        return

    source = ShopifyClient(source_url, source_token)

    if args.list:
        list_types(source)
        return

    # Destination store (Saudi)
    dest_url = os.environ.get("SAUDI_SHOP_URL")
    dest_token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not dest_url or not dest_token:
        print("ERROR: SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN must be set in .env")
        return

    dest = ShopifyClient(dest_url, dest_token)

    # Load file map for re-use across runs
    file_map_file = "data/metaobject_file_map.json"
    file_map = load_json(file_map_file) if os.path.exists(file_map_file) else {}
    if not isinstance(file_map, dict):
        file_map = {}

    # Determine types to migrate
    if args.all:
        source_defs = source.get_metaobject_definitions()
        types_to_migrate = [d["type"] for d in source_defs]
        print(f"Migrating all {len(types_to_migrate)} types: {types_to_migrate}")
    else:
        types_to_migrate = [args.type]

    all_id_maps = {}

    for mo_type in types_to_migrate:
        print(f"\n{'='*60}")
        print(f"MIGRATING: {mo_type}")
        print(f"{'='*60}")

        # 1. Ensure definition exists
        print("\n--- Ensuring definition ---")
        def_id = ensure_definition(source, dest, mo_type, dry_run=args.dry_run)
        if not def_id and not args.dry_run:
            print(f"  SKIP: Could not create definition for '{mo_type}'")
            continue

        # 2. Migrate entries
        print("\n--- Migrating entries ---")
        id_map = migrate_entries(source, dest, mo_type, file_map, dry_run=args.dry_run)
        all_id_maps[mo_type] = id_map
        print(f"\n  Migrated {len(id_map)} entries for '{mo_type}'")

    # Save file map
    save_json(file_map, file_map_file)

    # Save combined ID map
    save_json(all_id_maps, "data/metaobject_id_map.json")

    print(f"\n{'='*60}")
    print("MIGRATION COMPLETE")
    print(f"{'='*60}")
    for mo_type, id_map in all_id_maps.items():
        print(f"  {mo_type}: {len(id_map)} entries migrated")
    print(f"\n  File map saved to {file_map_file}")
    print(f"  ID map saved to data/metaobject_id_map.json")


if __name__ == "__main__":
    main()
