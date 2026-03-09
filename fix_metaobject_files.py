#!/usr/bin/env python3
"""DEPRECATED: Use migrate_all_images.py --phase 4 or build_site.py --phase 4 instead.

Backfill file_reference fields on metaobject entries.

During the initial import, file_reference fields (icons, images) were skipped
because they point to source-store file GIDs. This script:
  1. Reads source metaobject data to find file_reference fields with CDN URLs.
  2. Downloads and uploads each file to the Saudi store.
  3. Updates the metaobject entry with the new file GID.

Usage:
    python fix_metaobject_files.py --dry-run    # preview
    python fix_metaobject_files.py               # backfill all
"""

import argparse
import json
import os
import time

from dotenv import load_dotenv
from shopify_client import ShopifyClient
from utils import load_json, save_json


def extract_cdn_url_from_references(field):
    """Extract the CDN URL from a file_reference field's references."""
    refs = field.get("references", [])
    if isinstance(refs, list):
        for ref in refs:
            # MediaImage
            img = ref.get("image", {})
            if img and img.get("url"):
                return img["url"]
            # GenericFile
            url = ref.get("url")
            if url:
                return url
    elif isinstance(refs, dict):
        img = refs.get("image", {})
        if img and img.get("url"):
            return img["url"]
        url = refs.get("url")
        if url:
            return url
    # Fallback: value might contain a GID, which we can't use directly
    return None


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Backfill file_reference fields on metaobjects")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--source-dir", default="data/spain_export",
                        help="Directory containing source metaobjects.json")
    parser.add_argument("--type", default=None,
                        help="Only process a specific metaobject type")
    args = parser.parse_args()

    shop_url = os.environ.get("SAUDI_SHOP_URL")
    access_token = os.environ.get("SAUDI_ACCESS_TOKEN")

    # Also need Spain store for fetching file URLs if not in export
    spain_url = os.environ.get("SPAIN_SHOP_URL")
    spain_token = os.environ.get("SPAIN_ACCESS_TOKEN")

    if not shop_url or not access_token:
        print("ERROR: SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN must be set in .env")
        return

    saudi_client = ShopifyClient(shop_url, access_token)
    spain_client = None
    if spain_url and spain_token:
        spain_client = ShopifyClient(spain_url, spain_token)

    # Load source metaobject data
    source_file = os.path.join(args.source_dir, "metaobjects.json")
    all_metaobjects = load_json(source_file)
    if not all_metaobjects:
        # Try english dir
        source_file = "data/english/metaobjects.json"
        all_metaobjects = load_json(source_file)
    if not all_metaobjects:
        print("ERROR: No metaobject data found. Make sure data/spain_export/metaobjects.json exists.")
        return

    # Load ID map for metaobject GID mapping
    id_map = load_json("data/id_map.json", default={})

    # Track uploaded files to avoid re-uploading the same file
    file_upload_cache = {}  # source_file_gid → dest_file_gid
    cache_file = "data/file_upload_cache.json"
    if os.path.exists(cache_file):
        file_upload_cache = load_json(cache_file, default={})

    updated = 0
    skipped = 0
    errors = 0

    for mo_type, type_data in all_metaobjects.items():
        if args.type and mo_type != args.type:
            continue

        objects = type_data.get("objects", [])
        defn = type_data.get("definition", {})

        # Identify file_reference fields from definition
        file_field_keys = set()
        for fd in defn.get("fieldDefinitions", []):
            field_type = fd.get("type", {}).get("name", "")
            if "file_reference" in field_type:
                file_field_keys.add(fd["key"])

        if not file_field_keys:
            continue

        print(f"\n--- {mo_type} ({len(objects)} objects) ---")
        print(f"  File fields: {file_field_keys}")

        map_key = f"metaobjects_{mo_type}"

        for obj in objects:
            source_id = obj.get("id", "")
            handle = obj.get("handle", "")

            # Find dest metaobject ID
            dest_id = id_map.get(map_key, {}).get(source_id)
            if not dest_id:
                # Try to find by handle on dest store
                existing = saudi_client.get_metaobjects_by_handle(mo_type, handle)
                if existing:
                    dest_id = existing["id"]
                else:
                    print(f"  SKIP {handle}: no dest metaobject found")
                    skipped += 1
                    continue

            # Collect file_reference fields that need updating
            fields_to_update = []
            for field in obj.get("fields", []):
                if field["key"] not in file_field_keys:
                    continue

                field_type = field.get("type", "")
                source_value = field.get("value", "")

                if not source_value:
                    continue

                # Try to get CDN URL from references
                cdn_url = extract_cdn_url_from_references(field)

                if not cdn_url and spain_client and source_value.startswith("gid://"):
                    # Fetch the file info from Spain store
                    try:
                        file_info = spain_client.get_file_by_id(source_value)
                        if file_info:
                            img = file_info.get("image", {})
                            if img and img.get("url"):
                                cdn_url = img["url"]
                            elif file_info.get("url"):
                                cdn_url = file_info["url"]
                    except Exception as e:
                        print(f"  WARN: Could not fetch file {source_value}: {e}")

                if not cdn_url:
                    print(f"  SKIP {handle}.{field['key']}: no CDN URL available")
                    continue

                # Check cache
                cache_key = source_value or cdn_url
                if cache_key in file_upload_cache:
                    dest_file_gid = file_upload_cache[cache_key]
                    fields_to_update.append({
                        "key": field["key"],
                        "value": dest_file_gid,
                    })
                    continue

                if args.dry_run:
                    print(f"  WOULD upload {field['key']} for {handle}: {cdn_url[:80]}")
                    continue

                # Upload to Saudi store
                try:
                    dest_file_gid = saudi_client.upload_file_from_url(cdn_url)
                    if dest_file_gid:
                        file_upload_cache[cache_key] = dest_file_gid
                        save_json(file_upload_cache, cache_file)
                        fields_to_update.append({
                            "key": field["key"],
                            "value": dest_file_gid,
                        })
                        print(f"  Uploaded {field['key']} for {handle}")
                        time.sleep(0.5)  # Rate limit
                    else:
                        print(f"  ERROR: Upload returned None for {handle}.{field['key']}")
                        errors += 1
                except Exception as e:
                    print(f"  ERROR uploading {handle}.{field['key']}: {e}")
                    errors += 1

            if not fields_to_update:
                if not args.dry_run:
                    skipped += 1
                continue

            if args.dry_run:
                updated += 1
                continue

            # Update metaobject with file references
            try:
                saudi_client.update_metaobject(dest_id, fields_to_update)
                field_names = [f["key"] for f in fields_to_update]
                print(f"  UPDATED {handle}: {', '.join(field_names)}")
                updated += 1
            except Exception as e:
                print(f"  ERROR updating {handle}: {e}")
                errors += 1

    print(f"\nDone! Updated: {updated}, Skipped: {skipped}, Errors: {errors}")


if __name__ == "__main__":
    main()
