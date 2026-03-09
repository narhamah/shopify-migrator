#!/usr/bin/env python3
"""Migrate homepage section images from Spain store to Saudi store.

Reads the Spain store's homepage template (templates/index.json), finds all
section and block settings that contain image references (shopify://shop_images/...),
resolves them to CDN URLs, downloads and optimizes to WebP, uploads to the
Saudi store, and updates the Saudi store's homepage template.

Also handles metaobject file_reference fields for ALL metaobject types
(including icon_item icons that were previously skipped).

Usage:
    python migrate_homepage_images.py --inspect           # Preview what would be migrated
    python migrate_homepage_images.py --dry-run           # Show actions without executing
    python migrate_homepage_images.py                     # Run full migration
    python migrate_homepage_images.py --metaobjects-only  # Only fix metaobject file refs
"""

import argparse
import json
import os
import re
import time

from dotenv import load_dotenv

from optimize_images import download_and_optimize, optimize_image
from shopify_client import ShopifyClient
from utils import load_json, save_json, IMAGE_KEYWORDS, SECTION_PRESETS


def _is_image_setting(key):
    """Check if a setting key is an image field."""
    key_lower = key.lower()
    return any(kw in key_lower for kw in IMAGE_KEYWORDS)


def _is_shopify_image_ref(value):
    """Check if value is a Shopify image file reference."""
    return isinstance(value, str) and value.startswith("shopify://shop_images/")


def _guess_preset_for_section(section_type, setting_key):
    """Guess the best optimization preset for a section/setting combo."""
    st = section_type.lower()

    # Check direct section type mapping
    for pattern, preset in SECTION_PRESETS.items():
        if pattern in st:
            return preset

    # Guess from setting key
    key = setting_key.lower()
    if "icon" in key:
        return "icon"
    if "logo" in key:
        return "logo"
    if "hero" in key or "banner" in key:
        return "hero"
    if "thumbnail" in key:
        return "thumbnail"

    return "default"


def resolve_shopify_image_to_url(client, theme_id, image_ref):
    """Resolve a shopify://shop_images/filename reference to a CDN URL.

    Shopify stores theme images in the Files section. We need to find the
    actual CDN URL by searching for the file by filename.
    """
    if not _is_shopify_image_ref(image_ref):
        return None

    filename = image_ref.replace("shopify://shop_images/", "")

    # Query the Files API for this filename
    try:
        query = """
        query findFile($query: String!) {
          files(first: 1, query: $query) {
            nodes {
              ... on MediaImage {
                id
                image { url }
              }
              ... on GenericFile {
                id
                url
              }
            }
          }
        }
        """
        data = client._graphql(query, {"query": f"filename:{filename}"})
        nodes = data.get("files", {}).get("nodes", [])
        if nodes:
            node = nodes[0]
            img = node.get("image", {})
            if img and img.get("url"):
                return img["url"]
            if node.get("url"):
                return node["url"]
    except Exception as e:
        print(f"    Could not resolve {image_ref}: {e}")

    return None


def get_homepage_images(client, theme_id):
    """Read the homepage template and extract all image settings.

    Returns list of dicts:
        {section_id, section_type, block_id (or None), setting_key, value, preset}
    """
    try:
        asset = client.get_asset(theme_id, "templates/index.json")
    except Exception as e:
        print(f"ERROR: Could not read templates/index.json: {e}")
        return [], {}

    template = json.loads(asset.get("value", "{}"))
    sections = template.get("sections", {})
    images = []

    for section_id, section in sections.items():
        section_type = section.get("type", "unknown")
        settings = section.get("settings", {})
        blocks = section.get("blocks", {})

        # Section-level image settings
        for key, value in settings.items():
            if _is_image_setting(key) and value:
                preset = _guess_preset_for_section(section_type, key)
                images.append({
                    "section_id": section_id,
                    "section_type": section_type,
                    "block_id": None,
                    "setting_key": key,
                    "value": value,
                    "preset": preset,
                })

        # Block-level image settings
        for block_id, block in blocks.items():
            btype = block.get("type", "unknown")
            bsettings = block.get("settings", {})
            for key, value in bsettings.items():
                if _is_image_setting(key) and value:
                    preset = _guess_preset_for_section(section_type, key)
                    # Icons in block settings
                    if "icon" in btype.lower() or "icon" in key.lower():
                        preset = "icon"
                    images.append({
                        "section_id": section_id,
                        "section_type": section_type,
                        "block_id": block_id,
                        "block_type": btype,
                        "setting_key": key,
                        "value": value,
                        "preset": preset,
                    })

    return images, template


def migrate_homepage_images(spain_client, saudi_client, dry_run=False):
    """Migrate all homepage images from Spain store to Saudi store."""
    print("\n=== Migrating Homepage Section Images ===")

    # Get Spain store theme
    spain_theme_id = spain_client.get_main_theme_id()
    if not spain_theme_id:
        print("ERROR: No main theme on Spain store")
        return
    print(f"  Spain theme ID: {spain_theme_id}")

    # Get Saudi store theme
    saudi_theme_id = saudi_client.get_main_theme_id()
    if not saudi_theme_id:
        print("ERROR: No main theme on Saudi store")
        return
    print(f"  Saudi theme ID: {saudi_theme_id}")

    # Read Spain homepage images
    spain_images, spain_template = get_homepage_images(spain_client, spain_theme_id)
    print(f"  Found {len(spain_images)} image settings on Spain homepage")

    if not spain_images:
        print("  No images to migrate")
        return

    # Read Saudi homepage template
    try:
        saudi_asset = saudi_client.get_asset(saudi_theme_id, "templates/index.json")
        saudi_template = json.loads(saudi_asset.get("value", "{}"))
    except Exception as e:
        print(f"ERROR: Could not read Saudi homepage template: {e}")
        return

    # Load cache
    cache_file = "data/homepage_image_cache.json"
    cache = load_json(cache_file) if os.path.exists(cache_file) else {}

    updated = 0
    errors = 0

    for img_info in spain_images:
        section_id = img_info["section_id"]
        block_id = img_info.get("block_id")
        key = img_info["setting_key"]
        value = img_info["value"]
        preset = img_info["preset"]

        if block_id:
            label = f"{section_id}.blocks.{block_id}.{key}"
        else:
            label = f"{section_id}.settings.{key}"

        # Check if this section/block exists in Saudi template
        saudi_sections = saudi_template.get("sections", {})
        if section_id not in saudi_sections:
            print(f"  SKIP {label}: section not in Saudi template")
            continue

        # Check if already set in Saudi
        saudi_section = saudi_sections[section_id]
        if block_id:
            saudi_block = saudi_section.get("blocks", {}).get(block_id, {})
            existing = saudi_block.get("settings", {}).get(key)
        else:
            existing = saudi_section.get("settings", {}).get(key)

        if existing and _is_shopify_image_ref(existing):
            print(f"  SKIP {label}: already set ({existing})")
            continue

        # Resolve the Spain image ref to a CDN URL
        if _is_shopify_image_ref(value):
            cdn_url = resolve_shopify_image_to_url(spain_client, spain_theme_id, value)
            if not cdn_url:
                print(f"  SKIP {label}: could not resolve {value}")
                errors += 1
                continue
        elif value.startswith("http"):
            cdn_url = value
        else:
            print(f"  SKIP {label}: unrecognized value format: {value[:50]}")
            continue

        cache_key = f"{cdn_url}|{preset}"
        if cache_key in cache:
            shopify_ref = cache[cache_key]
            print(f"  CACHED {label}: {shopify_ref}")
        elif dry_run:
            print(f"  WOULD upload [{preset}]: {label} ← {cdn_url[:80]}")
            updated += 1
            continue
        else:
            # Download, optimize, upload
            try:
                opt_bytes, opt_filename, mime = download_and_optimize(cdn_url, preset=preset)
                dest_file_gid = saudi_client.upload_file_bytes(opt_bytes, opt_filename)
                if not dest_file_gid:
                    print(f"  ERROR {label}: upload returned None")
                    errors += 1
                    continue

                # Wait briefly for file processing, then get the filename
                time.sleep(1)
                file_info = saudi_client.get_file_by_id(dest_file_gid)
                if file_info:
                    img_data = file_info.get("image", {})
                    file_url = img_data.get("url", "") or file_info.get("url", "")
                    if file_url:
                        fname = file_url.split("/")[-1].split("?")[0]
                        shopify_ref = f"shopify://shop_images/{fname}"
                    else:
                        # Fallback: use the uploaded filename
                        shopify_ref = f"shopify://shop_images/{opt_filename}"
                else:
                    shopify_ref = f"shopify://shop_images/{opt_filename}"

                cache[cache_key] = shopify_ref
                save_json(cache, cache_file)
                print(f"  UPLOADED [{preset}] {label}: {shopify_ref}")
                time.sleep(0.5)
            except Exception as e:
                print(f"  ERROR {label}: {e}")
                errors += 1
                continue

        # Update Saudi template
        if block_id:
            saudi_sections[section_id].setdefault("blocks", {}).setdefault(block_id, {}).setdefault("settings", {})[key] = shopify_ref
        else:
            saudi_sections[section_id].setdefault("settings", {})[key] = shopify_ref
        updated += 1

    if updated > 0 and not dry_run:
        template_str = json.dumps(saudi_template, ensure_ascii=False, indent=2)
        saudi_client.put_asset(saudi_theme_id, "templates/index.json", template_str)
        print(f"\n  Updated Saudi homepage template with {updated} images")
    else:
        print(f"\n  {'Would update' if dry_run else 'Updated'} {updated} images, {errors} errors")


def migrate_metaobject_files(spain_client, saudi_client, dry_run=False):
    """Migrate ALL metaobject file_reference fields from Spain to Saudi.

    Reads metaobject definitions from Saudi store to find file_reference fields,
    fetches source data from Spain, downloads/optimizes/uploads each file.
    """
    print("\n=== Migrating Metaobject File References ===")

    id_map = load_json("data/id_map.json", default={})
    file_cache_file = "data/file_map.json"
    file_cache = load_json(file_cache_file) if os.path.exists(file_cache_file) else {}

    # Get all metaobject definitions from Saudi store
    saudi_defs = saudi_client.get_metaobject_definitions()
    print(f"  Found {len(saudi_defs)} metaobject definitions on Saudi store")

    updated_total = 0
    errors_total = 0

    for defn in saudi_defs:
        mo_type = defn.get("type", "")
        field_defs = defn.get("fieldDefinitions", [])

        # Find file_reference fields
        file_fields = {}
        for fd in field_defs:
            ft = fd.get("type", {}).get("name", "") if isinstance(fd.get("type"), dict) else fd.get("type", "")
            if "file_reference" in ft:
                is_list = "list." in ft
                file_fields[fd["key"]] = {"is_list": is_list, "type": ft}

        if not file_fields:
            continue

        print(f"\n  --- {mo_type} (file fields: {list(file_fields.keys())}) ---")

        map_key = f"metaobjects_{mo_type}"
        mo_id_map = id_map.get(map_key, {})

        if not mo_id_map:
            print(f"    No ID mapping found for {mo_type}, skipping")
            continue

        # For each mapped metaobject, check if file fields need populating
        for source_gid, dest_gid in mo_id_map.items():
            # Get current state of dest metaobject
            try:
                dest_obj = saudi_client._graphql("""
                    query getMetaobject($id: ID!) {
                        metaobject(id: $id) {
                            id
                            handle
                            fields { key value type }
                        }
                    }
                """, {"id": dest_gid})
                dest_mo = dest_obj.get("metaobject")
                if not dest_mo:
                    continue
            except Exception as e:
                print(f"    Could not fetch dest metaobject {dest_gid}: {e}")
                continue

            handle = dest_mo.get("handle", "")
            dest_fields = {f["key"]: f for f in dest_mo.get("fields", [])}

            # Check which file fields are empty
            fields_to_update = []
            for field_key, field_info in file_fields.items():
                dest_field = dest_fields.get(field_key, {})
                if dest_field.get("value"):
                    # Already has a value
                    continue

                # Need to get the source file URL from Spain store
                try:
                    source_obj = spain_client._graphql("""
                        query getMetaobject($id: ID!) {
                            metaobject(id: $id) {
                                id
                                handle
                                fields {
                                    key
                                    value
                                    type
                                    references(first: 5) {
                                        nodes {
                                            ... on MediaImage {
                                                id
                                                image { url }
                                            }
                                            ... on GenericFile {
                                                id
                                                url
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    """, {"id": source_gid})
                    source_mo = source_obj.get("metaobject")
                    if not source_mo:
                        continue
                except Exception as e:
                    print(f"    Could not fetch source metaobject {source_gid}: {e}")
                    continue

                source_field = None
                for sf in source_mo.get("fields", []):
                    if sf["key"] == field_key:
                        source_field = sf
                        break

                if not source_field or not source_field.get("value"):
                    continue

                # Determine preset based on field name
                preset = "icon" if "icon" in field_key.lower() else "default"
                if "avatar" in field_key.lower() or "image" in field_key.lower():
                    preset = "thumbnail"

                if field_info["is_list"]:
                    # List of file references
                    try:
                        source_gids = json.loads(source_field["value"])
                    except (json.JSONDecodeError, TypeError):
                        continue

                    dest_gids = []
                    for sgid in source_gids:
                        if sgid in file_cache:
                            dest_gids.append(file_cache[sgid])
                            continue

                        url = _get_file_url(spain_client, sgid, source_field)
                        if not url:
                            continue

                        if dry_run:
                            print(f"    WOULD upload [{preset}] {handle}.{field_key}")
                            continue

                        try:
                            dest_fid = _upload_optimized(saudi_client, url, f"{handle}_{field_key}", preset)
                            if dest_fid:
                                file_cache[sgid] = dest_fid
                                dest_gids.append(dest_fid)
                                save_json(file_cache, file_cache_file)
                                time.sleep(0.5)
                        except Exception as e:
                            print(f"    ERROR uploading {handle}.{field_key}: {e}")
                            errors_total += 1

                    if dest_gids:
                        fields_to_update.append({
                            "key": field_key,
                            "value": json.dumps(dest_gids),
                        })
                else:
                    # Single file reference
                    sgid = source_field["value"]
                    if sgid in file_cache:
                        fields_to_update.append({
                            "key": field_key,
                            "value": file_cache[sgid],
                        })
                        continue

                    url = _get_file_url(spain_client, sgid, source_field)
                    if not url:
                        print(f"    SKIP {handle}.{field_key}: could not get URL for {sgid[:40]}")
                        continue

                    if dry_run:
                        print(f"    WOULD upload [{preset}] {handle}.{field_key} ← {url[:60]}")
                        updated_total += 1
                        continue

                    try:
                        dest_fid = _upload_optimized(saudi_client, url, f"{handle}_{field_key}", preset)
                        if dest_fid:
                            file_cache[sgid] = dest_fid
                            save_json(file_cache, file_cache_file)
                            fields_to_update.append({
                                "key": field_key,
                                "value": dest_fid,
                            })
                            print(f"    UPLOADED [{preset}] {handle}.{field_key}")
                            time.sleep(0.5)
                        else:
                            errors_total += 1
                    except Exception as e:
                        print(f"    ERROR {handle}.{field_key}: {e}")
                        errors_total += 1

            if fields_to_update and not dry_run:
                try:
                    saudi_client.update_metaobject(dest_gid, fields_to_update)
                    field_names = [f["key"] for f in fields_to_update]
                    print(f"    UPDATED {handle}: {', '.join(field_names)}")
                    updated_total += len(fields_to_update)
                except Exception as e:
                    print(f"    ERROR updating {handle}: {e}")
                    errors_total += 1

    print(f"\n  Metaobjects: {'would update' if dry_run else 'updated'} {updated_total} fields, {errors_total} errors")


def _get_file_url(client, file_gid, source_field=None):
    """Get CDN URL for a file, trying references first then API lookup."""
    # Try references from the field
    if source_field:
        refs = source_field.get("references", {})
        nodes = refs.get("nodes", []) if isinstance(refs, dict) else []
        for node in nodes:
            img = node.get("image", {})
            if img and img.get("url"):
                return img["url"]
            if node.get("url"):
                return node["url"]

    # Fall back to API lookup
    if file_gid and file_gid.startswith("gid://"):
        try:
            node = client.get_file_by_id(file_gid)
            if node:
                img = node.get("image", {})
                if img and img.get("url"):
                    return img["url"]
                if node.get("url"):
                    return node["url"]
        except Exception:
            pass

    return None


def _upload_optimized(client, url, alt, preset):
    """Download, optimize to WebP with preset, upload to Shopify."""
    opt_bytes, opt_filename, mime = download_and_optimize(url, preset=preset)
    return client.upload_file_bytes(opt_bytes, opt_filename, alt=alt)


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Migrate homepage images and metaobject files")
    parser.add_argument("--inspect", action="store_true", help="Show Spain homepage image settings")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--homepage-only", action="store_true", help="Only migrate homepage images")
    parser.add_argument("--metaobjects-only", action="store_true", help="Only migrate metaobject files")
    args = parser.parse_args()

    spain_url = os.environ.get("SPAIN_SHOP_URL")
    spain_token = os.environ.get("SPAIN_ACCESS_TOKEN")
    saudi_url = os.environ.get("SAUDI_SHOP_URL")
    saudi_token = os.environ.get("SAUDI_ACCESS_TOKEN")

    if not spain_url or not spain_token:
        print("ERROR: SPAIN_SHOP_URL and SPAIN_ACCESS_TOKEN must be set in .env")
        return
    if not saudi_url or not saudi_token:
        print("ERROR: SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN must be set in .env")
        return

    spain_client = ShopifyClient(spain_url, spain_token)
    saudi_client = ShopifyClient(saudi_url, saudi_token)

    if args.inspect:
        theme_id = spain_client.get_main_theme_id()
        if not theme_id:
            print("ERROR: No main theme on Spain store")
            return
        images, template = get_homepage_images(spain_client, theme_id)
        print(f"\nSpain Homepage — {len(images)} image settings:\n")
        for img in images:
            block_label = f".blocks.{img.get('block_id', '')}" if img.get("block_id") else ""
            print(f"  [{img['preset']:10s}] {img['section_id']}{block_label}.{img['setting_key']}")
            print(f"             = {img['value'][:80]}")
        save_json(template, "data/spain_homepage_template.json")
        print(f"\n  Saved template to data/spain_homepage_template.json")
        return

    if not args.metaobjects_only:
        migrate_homepage_images(spain_client, saudi_client, dry_run=args.dry_run)

    if not args.homepage_only:
        migrate_metaobject_files(spain_client, saudi_client, dry_run=args.dry_run)

    print("\nDone!")


if __name__ == "__main__":
    main()
