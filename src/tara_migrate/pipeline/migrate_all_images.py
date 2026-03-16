#!/usr/bin/env python3
"""Unified image migration: source store → destination store.

Orchestrates ALL image migration in the correct order:

  Phase 1: Product images (from Magento EN/AR or source Shopify)
  Phase 2: Collection images (passed through via src URL)
  Phase 3: Homepage / theme section images (shopify://shop_images/...)
  Phase 4: Metaobject file_reference fields (avatar, image, icon, science_images)
  Phase 5: Article metafield file_reference fields (listing_image, hero_image)
  Phase 6: Verification report

Usage:
    python migrate_all_images.py --inspect         # Show what would be migrated
    python migrate_all_images.py --dry-run         # Preview without making changes
    python migrate_all_images.py                   # Run full migration
    python migrate_all_images.py --phase 4         # Run only phase 4
    python migrate_all_images.py --phase 4,5       # Run phases 4 and 5
"""

import argparse
import json
import os
import time

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import (
    ARTICLE_FILE_METAFIELDS,
    FILE_FIELD_PRESETS,
    FILE_MAP_FILE,
    load_json,
    save_json,
)
from tara_migrate.pipeline.image_helpers import (
    extract_template_images as _extract_template_images,
)
from tara_migrate.pipeline.image_helpers import (
    is_shopify_image_ref as _is_shopify_image_ref,
)
from tara_migrate.pipeline.image_helpers import (
    resolve_shopify_image_to_url as _resolve_shopify_image_to_url,
)
from tara_migrate.tools.optimize_images import download_and_optimize
from tara_migrate.core import config

# ---------------------------------------------------------------------------
# Phase 1: Product images
# ---------------------------------------------------------------------------

def phase1_product_images(spain, saudi, id_map, file_map, dry_run=False):
    """Ensure all products on destination store have images.

    Products created by import_english.py should already have images via src
    URL passthrough. This phase checks for products with missing images and
    re-uploads from source.
    """
    print("\n" + "=" * 60)
    print("PHASE 1: Product Images")
    print("=" * 60)

    product_map = id_map.get("products", {})
    if not product_map:
        print("  No product mappings found — run import_english.py first")
        return

    # Load source products for image data
    en_products = load_json(os.path.join(config.get_en_dir(), "products.json"))
    source_by_id = {}
    for p in en_products:
        source_by_id[str(p.get("id", ""))] = p

    fixed = 0
    checked = 0

    for source_id, dest_id in product_map.items():
        source_product = source_by_id.get(source_id, {})
        source_images = source_product.get("images", [])

        if not source_images:
            continue

        checked += 1

        # Check if dest product has images
        try:
            resp = saudi._request("GET", f"products/{dest_id}.json",
                                  params={"fields": "id,images,title"})
            dest_product = resp.json().get("product", {})
        except Exception as e:
            print(f"  ERROR checking product {dest_id}: {e}")
            continue

        dest_images = dest_product.get("images", [])
        title = dest_product.get("title", "")[:40]

        if dest_images:
            continue  # Already has images

        if dry_run:
            print(f"  WOULD re-upload {len(source_images)} images for '{title}'")
            fixed += 1
            continue

        # Re-upload from source URLs
        for i, img in enumerate(source_images):
            src_url = img.get("src")
            if not src_url:
                continue
            try:
                img_payload = {
                    "image": {
                        "src": src_url,
                        "alt": img.get("alt", ""),
                        "position": i + 1,
                    }
                }
                saudi._request("POST", f"products/{dest_id}/images.json",
                               json=img_payload)
                time.sleep(0.3)
            except Exception as e:
                print(f"  ERROR uploading image for '{title}': {e}")
                break

        print(f"  Re-uploaded {len(source_images)} images for '{title}'")
        fixed += 1

    print(f"\n  Checked {checked} products, fixed {fixed} with missing images")


# ---------------------------------------------------------------------------
# Phase 2: Collection images
# ---------------------------------------------------------------------------

def phase2_collection_images(spain, saudi, id_map, file_map, dry_run=False):
    """Ensure collections have images. Re-upload from source if missing."""
    print("\n" + "=" * 60)
    print("PHASE 2: Collection Images")
    print("=" * 60)

    collection_map = id_map.get("collections", {})
    if not collection_map:
        print("  No collection mappings found — run import_english.py first")
        return

    en_collections = load_json(os.path.join(config.get_en_dir(), "collections.json"))
    source_by_id = {}
    for c in en_collections:
        source_by_id[str(c.get("id", ""))] = c

    fixed = 0

    for source_id, dest_id in collection_map.items():
        source_coll = source_by_id.get(source_id, {})
        source_image = source_coll.get("image", {})

        if not source_image or not source_image.get("src"):
            continue

        # Check if dest collection has image — try custom first, then smart
        try:
            resp = saudi._request("GET", f"custom_collections/{dest_id}.json",
                                  params={"fields": "id,image,title"})
            dest_coll = resp.json().get("custom_collection", {})
        except Exception:
            try:
                resp = saudi._request("GET", f"smart_collections/{dest_id}.json",
                                      params={"fields": "id,image,title"})
                dest_coll = resp.json().get("smart_collection", {})
            except Exception:
                continue

        if dest_coll.get("image"):
            continue

        title = dest_coll.get("title", "")[:40]

        if dry_run:
            print(f"  WOULD set image for collection '{title}'")
            fixed += 1
            continue

        try:
            opt_bytes, opt_filename, mime = download_and_optimize(
                source_image["src"], preset="collection")
            file_gid = saudi.upload_file_bytes(opt_bytes, opt_filename,
                                               alt=f"collection_{source_coll.get('handle', '')}")
            if file_gid:
                print(f"  Set image for collection '{title}'")
                fixed += 1
                time.sleep(0.5)
        except Exception as e:
            print(f"  ERROR setting image for '{title}': {e}")

    print(f"\n  Fixed {fixed} collections with missing images")


# ---------------------------------------------------------------------------
# Phase 3: Homepage / theme section images
# ---------------------------------------------------------------------------


def phase3_homepage_images(spain, saudi, id_map, file_map, dry_run=False):
    """Migrate homepage section images from source theme to destination theme."""
    print("\n" + "=" * 60)
    print("PHASE 3: Homepage / Theme Section Images")
    print("=" * 60)

    source_theme_id = source.get_main_theme_id()
    dest_theme_id = saudi.get_main_theme_id()
    if not source_theme_id or not dest_theme_id:
        print("  ERROR: Could not find main theme on one or both stores")
        return

    # Read templates from both stores
    try:
        source_asset = source.get_asset(source_theme_id, "templates/index.json")
        source_template = json.loads(source_asset.get("value", "{}"))
    except Exception as e:
        print(f"  ERROR reading source homepage template: {e}")
        return

    try:
        saudi_asset = saudi.get_asset(dest_theme_id, "templates/index.json")
        dest_template = json.loads(saudi_asset.get("value", "{}"))
    except Exception as e:
        print(f"  ERROR reading Saudi homepage template: {e}")
        return

    spain_images = _extract_template_images(source_template)
    print(f"  Found {len(spain_images)} image settings on source homepage")

    if not spain_images:
        return

    cache_file = config.get_progress_file("homepage_image_cache.json")
    cache = load_json(cache_file) if os.path.exists(cache_file) and isinstance(load_json(cache_file), dict) else {}

    updated = 0
    errors = 0

    for img_info in spain_images:
        section_id = img_info["section_id"]
        block_id = img_info.get("block_id")
        key = img_info["setting_key"]
        value = img_info["value"]
        preset = img_info["preset"]

        label = f"{section_id}.blocks.{block_id}.{key}" if block_id else f"{section_id}.settings.{key}"

        # Check section exists in Saudi template
        dest_sections = dest_template.get("sections", {})
        if section_id not in dest_sections:
            print(f"  SKIP {label}: section not in Saudi template")
            continue

        # Check if already set
        dest_section = dest_sections[section_id]
        if block_id:
            existing = dest_section.get("blocks", {}).get(block_id, {}).get("settings", {}).get(key)
        else:
            existing = dest_section.get("settings", {}).get(key)

        if existing and _is_shopify_image_ref(existing):
            continue  # Already set

        # Resolve source image URL
        if _is_shopify_image_ref(value):
            cdn_url = _resolve_shopify_image_to_url(spain, value)
            if not cdn_url:
                errors += 1
                continue
        elif isinstance(value, str) and value.startswith("http"):
            cdn_url = value
        else:
            continue

        cache_key = f"{cdn_url}|{preset}"
        if cache_key in cache:
            shopify_ref = cache[cache_key]
        elif dry_run:
            print(f"  WOULD upload [{preset}]: {label}")
            updated += 1
            continue
        else:
            try:
                opt_bytes, opt_filename, mime = download_and_optimize(cdn_url, preset=preset)
                dest_file_gid = saudi.upload_file_bytes(opt_bytes, opt_filename)
                if not dest_file_gid:
                    errors += 1
                    continue

                time.sleep(1)
                file_info = saudi.get_file_by_id(dest_file_gid)
                if file_info:
                    img_data = file_info.get("image", {})
                    file_url = img_data.get("url", "") or file_info.get("url", "")
                    if file_url:
                        fname = file_url.split("/")[-1].split("?")[0]
                        shopify_ref = f"shopify://shop_images/{fname}"
                    else:
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
            dest_sections[section_id].setdefault("blocks", {}).setdefault(
                block_id, {}).setdefault("settings", {})[key] = shopify_ref
        else:
            dest_sections[section_id].setdefault("settings", {})[key] = shopify_ref
        updated += 1

    if updated > 0 and not dry_run:
        template_str = json.dumps(dest_template, ensure_ascii=False, indent=2)
        saudi.put_asset(dest_theme_id, "templates/index.json", template_str)
        print(f"\n  Updated Saudi homepage template with {updated} images")
    else:
        print(f"\n  {'Would update' if dry_run else 'Updated'} {updated} images, {errors} errors")


# ---------------------------------------------------------------------------
# Phase 4: Metaobject file_reference fields
# ---------------------------------------------------------------------------

def _get_file_url(client, file_gid, source_field=None):
    """Get CDN URL for a file, trying references first then API."""
    if source_field:
        refs = source_field.get("references", {})
        nodes = refs.get("nodes", []) if isinstance(refs, dict) else []
        for node in nodes:
            img = node.get("image", {})
            if img and img.get("url"):
                return img["url"]
            if node.get("url"):
                return node["url"]

    if file_gid and isinstance(file_gid, str) and file_gid.startswith("gid://"):
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
    """Download, optimize to WebP, upload to Shopify."""
    opt_bytes, opt_filename, mime = download_and_optimize(url, preset=preset)
    return client.upload_file_bytes(opt_bytes, opt_filename, alt=alt)


def phase4_metaobject_files(spain, saudi, id_map, file_map, dry_run=False):
    """Migrate file_reference fields for all metaobject types.

    Uses a two-strategy approach:
      1. Try exported data from data/english/metaobjects.json (fast)
      2. Fall back to live API queries from both stores (thorough)
    """
    print("\n" + "=" * 60)
    print("PHASE 4: Metaobject File References")
    print("=" * 60)

    file_map_file = FILE_MAP_FILE
    updated = 0
    errors = 0

    # Get all metaobject definitions from Saudi to discover file fields dynamically
    saudi_defs = saudi.get_metaobject_definitions()
    print(f"  Found {len(saudi_defs)} metaobject definitions on destination store")

    for defn in saudi_defs:
        mo_type = defn.get("type", "")
        field_defs = defn.get("fieldDefinitions", [])

        # Discover file_reference fields
        file_fields = {}
        for fd in field_defs:
            ft = fd.get("type", {})
            ft_name = ft.get("name", "") if isinstance(ft, dict) else str(ft)
            if "file_reference" in ft_name:
                file_fields[fd["key"]] = {
                    "is_list": "list." in ft_name,
                    "type": ft_name,
                }

        if not file_fields:
            continue

        map_key = f"metaobjects_{mo_type}"
        mo_id_map = id_map.get(map_key, {})

        if not mo_id_map:
            print(f"\n  --- {mo_type}: no ID mappings, skipping ---")
            continue

        print(f"\n  --- {mo_type} ({len(mo_id_map)} entries, file fields: {list(file_fields.keys())}) ---")

        for source_gid, dest_gid in mo_id_map.items():
            # Get dest metaobject to check which fields are empty
            try:
                dest_obj = saudi._graphql("""
                    query getMetaobject($id: ID!) {
                        metaobject(id: $id) {
                            id handle
                            fields { key value type }
                        }
                    }
                """, {"id": dest_gid})
                dest_mo = dest_obj.get("metaobject")
                if not dest_mo:
                    continue
            except Exception:
                continue

            handle = dest_mo.get("handle", "")
            dest_field_map = {f["key"]: f for f in dest_mo.get("fields", [])}

            # Check which file fields need populating
            needs_source = any(
                not dest_field_map.get(fk, {}).get("value")
                for fk in file_fields
            )

            source_mo = None
            if needs_source:
                try:
                    source_obj = spain._graphql("""
                        query getMetaobject($id: ID!) {
                            metaobject(id: $id) {
                                id handle
                                fields {
                                    key value type
                                    references(first: 10) {
                                        nodes {
                                            ... on MediaImage { id image { url } }
                                            ... on GenericFile { id url }
                                        }
                                    }
                                }
                            }
                        }
                    """, {"id": source_gid})
                    source_mo = source_obj.get("metaobject")
                except Exception:
                    pass

            if not source_mo and needs_source:
                continue

            source_field_map = {sf["key"]: sf for sf in (source_mo or {}).get("fields", [])}

            fields_to_update = []
            for field_key, field_info in file_fields.items():
                dest_field = dest_field_map.get(field_key, {})
                if dest_field.get("value"):
                    continue  # Already has value

                source_field = source_field_map.get(field_key)
                if not source_field or not source_field.get("value"):
                    continue

                preset = FILE_FIELD_PRESETS.get(field_key, "default")

                if field_info["is_list"]:
                    try:
                        source_gids = json.loads(source_field["value"])
                    except (json.JSONDecodeError, TypeError):
                        continue

                    dest_gids = []
                    for sgid in source_gids:
                        if sgid in file_map:
                            dest_gids.append(file_map[sgid])
                            continue

                        url = _get_file_url(spain, sgid, source_field)
                        if not url:
                            continue

                        if dry_run:
                            print(f"    WOULD upload [{preset}] {handle}.{field_key}")
                            continue

                        try:
                            dest_fid = _upload_optimized(saudi, url, f"{handle}_{field_key}", preset)
                            if dest_fid:
                                file_map[sgid] = dest_fid
                                dest_gids.append(dest_fid)
                                save_json(file_map, file_map_file)
                                time.sleep(0.5)
                        except Exception as e:
                            print(f"    ERROR {handle}.{field_key}: {e}")
                            errors += 1

                    if dest_gids:
                        fields_to_update.append({
                            "key": field_key,
                            "value": json.dumps(dest_gids),
                        })
                else:
                    sgid = source_field["value"]
                    if sgid in file_map:
                        fields_to_update.append({
                            "key": field_key,
                            "value": file_map[sgid],
                        })
                        continue

                    url = _get_file_url(spain, sgid, source_field)
                    if not url:
                        continue

                    if dry_run:
                        print(f"    WOULD upload [{preset}] {handle}.{field_key}")
                        updated += 1
                        continue

                    try:
                        dest_fid = _upload_optimized(saudi, url, f"{handle}_{field_key}", preset)
                        if dest_fid:
                            file_map[sgid] = dest_fid
                            save_json(file_map, file_map_file)
                            fields_to_update.append({
                                "key": field_key,
                                "value": dest_fid,
                            })
                            print(f"    UPLOADED [{preset}] {handle}.{field_key}")
                            time.sleep(0.5)
                        else:
                            errors += 1
                    except Exception as e:
                        print(f"    ERROR {handle}.{field_key}: {e}")
                        errors += 1

            if fields_to_update and not dry_run:
                try:
                    saudi.update_metaobject(dest_gid, fields_to_update)
                    field_names = [f["key"] for f in fields_to_update]
                    print(f"    UPDATED {handle}: {', '.join(field_names)}")
                    updated += len(fields_to_update)
                except Exception as e:
                    print(f"    ERROR updating {handle}: {e}")
                    errors += 1

    save_json(file_map, file_map_file)
    print(f"\n  {'Would update' if dry_run else 'Updated'} {updated} file fields, {errors} errors")


# ---------------------------------------------------------------------------
# Phase 5: Article metafield file references
# ---------------------------------------------------------------------------

def phase5_article_files(spain, saudi, id_map, file_map, dry_run=False):
    """Migrate article listing_image and hero_image file references."""
    print("\n" + "=" * 60)
    print("PHASE 5: Article File References")
    print("=" * 60)

    file_map_file = FILE_MAP_FILE
    articles = load_json(os.path.join(config.get_en_dir(), "articles.json"))
    if not isinstance(articles, list):
        articles = []
    article_map = id_map.get("articles", {})

    if not article_map:
        print("  No article mappings found — run import_english.py first")
        return

    print(f"  Processing {len(articles)} articles...")
    updated = 0
    errors = 0

    for i, article in enumerate(articles):
        source_id = str(article["id"])
        dest_id = article_map.get(source_id)
        if not dest_id:
            continue

        metafields_to_set = []
        for mf in article.get("metafields", []):
            ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
            mf_type = mf.get("type", "")

            if ns_key not in ARTICLE_FILE_METAFIELDS or not mf.get("value"):
                continue
            if "file_reference" not in mf_type:
                continue

            source_gid = mf["value"]

            if source_gid in file_map:
                dest_file_id = file_map[source_gid]
            else:
                url = _get_file_url(spain, source_gid)
                if not url:
                    continue

                preset = "hero" if "hero" in mf["key"] else "thumbnail"

                if dry_run:
                    print(f"  WOULD upload [{preset}] article '{article.get('title', '')[:30]}' {mf['key']}")
                    updated += 1
                    continue

                try:
                    dest_file_id = _upload_optimized(
                        saudi, url,
                        alt=f"article_{article.get('handle', '')}_{mf['key']}",
                        preset=preset,
                    )
                    if dest_file_id:
                        file_map[source_gid] = dest_file_id
                        save_json(file_map, file_map_file)
                        time.sleep(0.5)
                    else:
                        errors += 1
                        continue
                except Exception as e:
                    print(f"  ERROR: article '{article.get('title', '')[:30]}' {mf['key']}: {e}")
                    errors += 1
                    continue

            metafields_to_set.append({
                "ownerId": f"gid://shopify/OnlineStoreArticle/{dest_id}",
                "namespace": mf["namespace"],
                "key": mf["key"],
                "value": dest_file_id,
                "type": mf_type,
            })

        if metafields_to_set and not dry_run:
            try:
                saudi.set_metafields(metafields_to_set)
                print(f"  [{i+1}] '{article.get('title', '')[:40]}' — set {len(metafields_to_set)} file metafields")
                updated += len(metafields_to_set)
            except Exception as e:
                print(f"  [{i+1}] '{article.get('title', '')[:40]}' — error: {e}")
                errors += 1

    save_json(file_map, file_map_file)
    print(f"\n  {'Would update' if dry_run else 'Updated'} {updated} article file fields, {errors} errors")


# ---------------------------------------------------------------------------
# Phase 6: Verification report
# ---------------------------------------------------------------------------

def phase6_verify(spain, saudi, id_map, file_map, dry_run=False):
    """Generate a verification report of image migration status."""
    print("\n" + "=" * 60)
    print("PHASE 6: Verification Report")
    print("=" * 60)

    report = {
        "products_with_images": 0,
        "products_missing_images": 0,
        "metaobject_files_populated": 0,
        "metaobject_files_missing": 0,
        "homepage_images_set": 0,
        "file_map_entries": len(file_map),
    }

    # Check product images
    product_map = id_map.get("products", {})
    for source_id, dest_id in product_map.items():
        try:
            resp = saudi._request("GET", f"products/{dest_id}.json",
                                  params={"fields": "id,images"})
            product = resp.json().get("product", {})
            if product.get("images"):
                report["products_with_images"] += 1
            else:
                report["products_missing_images"] += 1
        except Exception:
            report["products_missing_images"] += 1

    # Check metaobject file fields
    saudi_defs = saudi.get_metaobject_definitions()
    for defn in saudi_defs:
        mo_type = defn.get("type", "")
        field_defs = defn.get("fieldDefinitions", [])
        file_field_keys = [
            fd["key"] for fd in field_defs
            if "file_reference" in (fd.get("type", {}).get("name", "") if isinstance(fd.get("type"), dict) else "")
        ]

        if not file_field_keys:
            continue

        map_key = f"metaobjects_{mo_type}"
        for source_gid, dest_gid in id_map.get(map_key, {}).items():
            try:
                dest_obj = saudi._graphql("""
                    query getMetaobject($id: ID!) {
                        metaobject(id: $id) {
                            fields { key value }
                        }
                    }
                """, {"id": dest_gid})
                dest_mo = dest_obj.get("metaobject", {})
                for f in dest_mo.get("fields", []):
                    if f["key"] in file_field_keys:
                        if f.get("value"):
                            report["metaobject_files_populated"] += 1
                        else:
                            report["metaobject_files_missing"] += 1
            except Exception:
                pass

    # Check homepage
    try:
        dest_theme_id = saudi.get_main_theme_id()
        if dest_theme_id:
            saudi_asset = saudi.get_asset(dest_theme_id, "templates/index.json")
            dest_template = json.loads(saudi_asset.get("value", "{}"))
            images = _extract_template_images(dest_template)
            for img in images:
                if _is_shopify_image_ref(img["value"]):
                    report["homepage_images_set"] += 1
    except Exception:
        pass

    print(f"\n  Products with images:        {report['products_with_images']}")
    print(f"  Products missing images:     {report['products_missing_images']}")
    print(f"  Metaobject files populated:  {report['metaobject_files_populated']}")
    print(f"  Metaobject files missing:    {report['metaobject_files_missing']}")
    print(f"  Homepage images set:         {report['homepage_images_set']}")
    print(f"  File map entries (cache):    {report['file_map_entries']}")

    save_json(report, config.get_progress_file("image_migration_report.json"))
    print("\n  Report saved to data/image_migration_report.json")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

PHASES = {
    1: ("Product Images", phase1_product_images),
    2: ("Collection Images", phase2_collection_images),
    3: ("Homepage / Theme Images", phase3_homepage_images),
    4: ("Metaobject File References", phase4_metaobject_files),
    5: ("Article File References", phase5_article_files),
    6: ("Verification Report", phase6_verify),
}


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Unified image migration: Source → Destination Shopify store")
    parser.add_argument("--inspect", action="store_true",
                        help="Show what would be migrated across all phases")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview all changes without executing")
    parser.add_argument("--phase", type=str, default=None,
                        help="Run specific phases only (e.g., '4' or '3,4,5')")
    args = parser.parse_args()

    source_url = config.get_source_shop_url()
    source_token = config.get_source_access_token()
    dest_url = config.get_dest_shop_url()
    dest_token = config.get_dest_access_token()

    if not all([source_url, source_token, dest_url, saudi_token]):
        print("ERROR: Set SOURCE_SHOP_URL, SOURCE_ACCESS_TOKEN, DEST_SHOP_URL, DEST_ACCESS_TOKEN in .env")
        return

    source = ShopifyClient(source_url, source_token)
    saudi = ShopifyClient(dest_url, dest_token)

    id_map = load_json(config.get_id_map_file()) if os.path.exists(config.get_id_map_file()) else {}
    file_map_file = FILE_MAP_FILE
    file_map = load_json(file_map_file) if os.path.exists(file_map_file) else {}
    if not isinstance(file_map, dict):
        file_map = {}

    # Determine which phases to run
    if args.phase:
        phases_to_run = [int(p.strip()) for p in args.phase.split(",")]
    else:
        phases_to_run = list(PHASES.keys())

    dry_run = args.dry_run or args.inspect

    print("=" * 60)
    print("UNIFIED IMAGE MIGRATION: Source → Destination")
    print("=" * 60)
    print(f"  Mode:   {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"  Phases: {phases_to_run}")
    print(f"  File map entries: {len(file_map)}")
    print(f"  ID map sections: {list(id_map.keys())}")

    for phase_num in phases_to_run:
        if phase_num not in PHASES:
            print(f"\n  Unknown phase {phase_num}, skipping")
            continue
        name, func = PHASES[phase_num]
        func(spain, saudi, id_map, file_map, dry_run=dry_run)

    # Save final file map
    save_json(file_map, file_map_file)

    print("\n" + "=" * 60)
    print("IMAGE MIGRATION COMPLETE")
    print("=" * 60)
    print(f"  Total file map entries: {len(file_map)}")


if __name__ == "__main__":
    main()
