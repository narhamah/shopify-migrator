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
import io
import json
import os
import time
import urllib.parse

from dotenv import load_dotenv
import requests
from PIL import Image

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


def _load_metaobject_file_fallbacks():
    """Load non-empty metaobject file fields from canonical/local English data.

    The destination-scoped English directory may be a fresh copy of the source
    export and can legitimately miss file_reference values that still exist in
    the canonical English dataset. We merge both so later phases can recover
    these fields by handle.
    """
    fallback_by_type = {}
    candidate_paths = [
        os.path.join("data", "english", "metaobjects.json"),
        os.path.join(config.get_en_dir(), "metaobjects.json"),
    ]

    for path in candidate_paths:
        if not os.path.exists(path):
            continue
        data = load_json(path)
        if not isinstance(data, dict):
            continue

        for mo_type, type_data in data.items():
            objects = type_data.get("objects", []) if isinstance(type_data, dict) else []
            if not isinstance(objects, list):
                continue

            type_bucket = fallback_by_type.setdefault(mo_type, {})
            for obj in objects:
                handle = obj.get("handle", "")
                if not handle:
                    continue
                handle_bucket = type_bucket.setdefault(handle, {})
                for field in obj.get("fields", []):
                    value = field.get("value")
                    if not value:
                        continue
                    handle_bucket[field.get("key", "")] = field

    return fallback_by_type


def _build_extra_source_clients(primary_client):
    """Build optional fallback source clients for canonical assets."""
    clients = []
    fallback_pairs = [
        (os.environ.get("SPAIN_SHOP_URL"), os.environ.get("SPAIN_ACCESS_TOKEN")),
    ]
    primary_shop = getattr(primary_client, "shop_url", None)
    seen = {primary_shop} if primary_shop else set()

    for shop_url, token in fallback_pairs:
        if not shop_url or not token or shop_url in seen:
            continue
        try:
            clients.append(ShopifyClient(shop_url, token))
            seen.add(shop_url)
        except Exception:
            continue

    return clients


def _get_file_url_from_clients(primary_client, extra_clients, file_gid, source_field=None):
    """Resolve a file GID against the primary source, then fallback sources."""
    url = _get_file_url(primary_client, file_gid, source_field)
    if url:
        return url
    for client in extra_clients:
        url = _get_file_url(client, file_gid)
        if url:
            return url
    return None


def _get_valid_mapped_dest_file_id(client, file_map, source_file_gid, require_media_image=False):
    """Return a cached destination file ID only if it still exists."""
    dest_file_gid = file_map.get(source_file_gid)
    if not dest_file_gid:
        return None
    try:
        node = client.get_file_by_id(dest_file_gid)
    except Exception:
        node = None
    if node and node.get("id") == dest_file_gid:
        if require_media_image and not dest_file_gid.startswith("gid://shopify/MediaImage/"):
            file_map.pop(source_file_gid, None)
            return None
        return dest_file_gid
    file_map.pop(source_file_gid, None)
    return None


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
        collection_resource = None
        try:
            resp = saudi._request("GET", f"custom_collections/{dest_id}.json",
                                  params={"fields": "id,image,title"})
            dest_coll = resp.json().get("custom_collection", {})
            collection_resource = "custom_collection"
        except Exception:
            try:
                resp = saudi._request("GET", f"smart_collections/{dest_id}.json",
                                      params={"fields": "id,image,title"})
                dest_coll = resp.json().get("smart_collection", {})
                collection_resource = "smart_collection"
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
            endpoint = "custom_collections" if collection_resource == "custom_collection" else "smart_collections"
            saudi._request(
                "PUT",
                f"{endpoint}/{dest_id}.json",
                json={collection_resource: {"id": int(dest_id), "image": {"src": source_image["src"]}}},
            )
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

    source_theme_id = spain.get_main_theme_id()
    dest_theme_id = saudi.get_main_theme_id()
    if not source_theme_id or not dest_theme_id:
        print("  ERROR: Could not find main theme on one or both stores")
        return

    # Read templates from both stores
    try:
        source_asset = spain.get_asset(source_theme_id, "templates/index.json")
        source_template = json.loads(source_asset.get("value", "{}"))
    except Exception as e:
        print(f"  ERROR reading source homepage template: {e}")
        return

    try:
        saudi_asset = saudi.get_asset(dest_theme_id, "templates/index.json")
        dest_template = json.loads(saudi_asset.get("value", "{}"))
    except Exception as e:
        print(f"  ERROR reading destination homepage template: {e}")
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


def _prepare_media_image_upload(url):
    """Download an image and ensure the filename/bytes produce a MediaImage."""
    parsed = urllib.parse.urlparse(url)
    filename = os.path.basename(parsed.path) or "image"
    filename = filename.split("?")[0]

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    content = resp.content

    ext = os.path.splitext(filename.lower())[1]
    if ext in {".jpg", ".jpeg", ".png", ".gif"}:
        return content, filename

    try:
        image = Image.open(io.BytesIO(content))
    except Exception:
        return content, filename

    base = os.path.splitext(filename)[0] or "image"
    if image.mode in ("RGBA", "LA") or (image.mode == "P" and "transparency" in image.info):
        converted = image.convert("RGBA")
        output = io.BytesIO()
        converted.save(output, format="PNG", optimize=True)
        return output.getvalue(), f"{base}.png"

    converted = image.convert("RGB")
    output = io.BytesIO()
    converted.save(output, format="JPEG", quality=90, optimize=True)
    return output.getvalue(), f"{base}.jpg"


def _upload_media_image_reference(client, url, alt):
    """Upload a storefront image as a Shopify MediaImage, not a GenericFile."""
    content, filename = _prepare_media_image_upload(url)
    dest_file_gid = client.upload_file_bytes(content, filename, alt=alt)
    if not dest_file_gid:
        return None
    try:
        node = client.get_file_by_id(dest_file_gid)
    except Exception:
        node = None
    if node and dest_file_gid.startswith("gid://shopify/MediaImage/"):
        return dest_file_gid
    return None


def _filename_from_url(url):
    if not url:
        return ""
    return url.split("/")[-1].split("?")[0].lower()


def _parse_file_reference_value(value, is_list):
    if not value:
        return []
    if not is_list:
        return [value]
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return []
    if isinstance(parsed, list):
        return parsed
    return []


def _dest_file_reference_is_valid(client, value, is_list=False, require_media_image=False):
    """Check that a destination file reference resolves to an existing valid file."""
    file_gids = _parse_file_reference_value(value, is_list)
    if not file_gids:
        return False
    for file_gid in file_gids:
        try:
            node = client.get_file_by_id(file_gid)
        except Exception:
            return False
        if not node or node.get("id") != file_gid:
            return False
        if require_media_image and not file_gid.startswith("gid://shopify/MediaImage/"):
            return False
    return True


def phase4_product_metafield_files(spain, saudi, id_map, file_map, dry_run=False):
    """Migrate product-level file_reference metafields such as PDP galleries."""
    print("\n" + "=" * 60)
    print("PHASE 4: Product File Reference Metafields")
    print("=" * 60)

    source_products = load_json(os.path.join(config.SOURCE_DIR, "products.json"))
    if not isinstance(source_products, list):
        source_products = []

    product_map = id_map.get("products", {})
    if not source_products or not product_map:
        print("  No source products or product mappings found")
        return

    print("  Fetching destination products and files...")
    dest_products = saudi.get_products()
    dest_files = saudi.get_files()
    dest_products_by_id = {str(product["id"]): product for product in dest_products}
    dest_files_by_name = {}
    for item in dest_files:
        url = item.get("image", {}).get("url", "") or item.get("url", "")
        name = _filename_from_url(url)
        if name and item.get("id"):
            dest_files_by_name[name] = item["id"]

    updated = 0
    skipped = 0
    errors = 0

    for source_product in source_products:
        source_id = str(source_product.get("id", ""))
        dest_id = product_map.get(source_id)
        if not dest_id:
            continue

        dest_product = dest_products_by_id.get(str(dest_id))
        if not dest_product:
            continue

        dest_image_refs_by_name = {}
        for image in dest_product.get("images", []):
            name = _filename_from_url(image.get("src", ""))
            gid = image.get("admin_graphql_api_id")
            if name and gid:
                dest_image_refs_by_name[name] = gid

        source_image_urls_by_gid = {}
        for image in source_product.get("images", []):
            gid = image.get("admin_graphql_api_id")
            url = image.get("src", "")
            if gid and url:
                source_image_urls_by_gid[gid] = url

        metafields_to_set = []
        handle = source_product.get("handle", "")
        for metafield in source_product.get("metafields", []):
            mf_type = metafield.get("type", "")
            if "file_reference" not in mf_type:
                continue

            is_list = mf_type.startswith("list.")
            source_refs = _parse_file_reference_value(metafield.get("value"), is_list)
            if not source_refs:
                continue

            dest_refs = []
            for source_ref in source_refs:
                if source_ref in file_map:
                    dest_refs.append(file_map[source_ref])
                    continue

                source_url = source_image_urls_by_gid.get(source_ref) or _get_file_url(spain, source_ref)
                if not source_url:
                    errors += 1
                    print(f"  ERROR {handle}.{metafield['namespace']}.{metafield['key']}: could not resolve {source_ref}")
                    continue

                filename = _filename_from_url(source_url)
                dest_ref = dest_image_refs_by_name.get(filename) or dest_files_by_name.get(filename)

                if not dest_ref and not dry_run:
                    try:
                        preset = "product"
                        dest_ref = _upload_optimized(
                            saudi,
                            source_url,
                            alt=f"{handle}_{metafield['key']}",
                            preset=preset,
                        )
                        if dest_ref:
                            dest_files_by_name[filename] = dest_ref
                    except Exception as e:
                        errors += 1
                        print(f"  ERROR uploading {filename} for {handle}: {e}")
                        continue

                if dest_ref:
                    file_map[source_ref] = dest_ref
                    dest_refs.append(dest_ref)
                    save_json(file_map, FILE_MAP_FILE)
                else:
                    errors += 1
                    print(f"  ERROR {handle}.{metafield['namespace']}.{metafield['key']}: no destination ref for {filename}")

            if not dest_refs:
                skipped += 1
                continue

            metafields_to_set.append({
                "ownerId": f"gid://shopify/Product/{dest_id}",
                "namespace": metafield["namespace"],
                "key": metafield["key"],
                "value": json.dumps(dest_refs) if is_list else dest_refs[0],
                "type": mf_type,
            })

        if metafields_to_set and not dry_run:
            try:
                saudi.set_metafields(metafields_to_set)
                print(f"  Updated {handle}: {len(metafields_to_set)} file metafields")
                updated += len(metafields_to_set)
            except Exception as e:
                errors += 1
                print(f"  ERROR updating {handle}: {e}")
        elif metafields_to_set:
            print(f"  WOULD update {handle}: {len(metafields_to_set)} file metafields")
            updated += len(metafields_to_set)

    print(f"\n  {'Would update' if dry_run else 'Updated'} {updated} product file metafields, {skipped} skipped, {errors} errors")


def phase5_metaobject_files(spain, saudi, id_map, file_map, dry_run=False):
    """Migrate file_reference fields for all metaobject types.

    Uses a two-strategy approach:
      1. Try live source metaobjects
      2. Fall back to canonical English metaobject exports by handle
      3. Resolve file IDs against optional fallback source stores (e.g. Spain)
    """
    print("\n" + "=" * 60)
    print("PHASE 5: Metaobject File References")
    print("=" * 60)

    file_map_file = FILE_MAP_FILE
    updated = 0
    errors = 0
    fallback_metaobjects = _load_metaobject_file_fallbacks()
    extra_source_clients = _build_extra_source_clients(spain)

    # Get all metaobject definitions from Saudi to discover file fields dynamically
    saudi_defs = saudi.get_metaobject_definitions()
    print(f"  Found {len(saudi_defs)} metaobject definitions on destination store")
    if extra_source_clients:
        fallback_names = ", ".join(client.shop_url for client in extra_source_clients)
        print(f"  Extra source fallbacks enabled: {fallback_names}")

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
        source_gid_by_dest_gid = {dest_gid: source_gid for source_gid, dest_gid in mo_id_map.items()}

        try:
            dest_metaobjects = saudi.get_metaobjects(mo_type)
        except Exception as e:
            print(f"\n  --- {mo_type}: error loading destination metaobjects: {e} ---")
            continue

        if not dest_metaobjects:
            print(f"\n  --- {mo_type}: no destination metaobjects found ---")
            continue

        mapped_live = sum(1 for mo in dest_metaobjects if mo.get("id") in source_gid_by_dest_gid)
        print(
            f"\n  --- {mo_type} ({len(dest_metaobjects)} live entries, "
            f"{mapped_live} mapped IDs, file fields: {list(file_fields.keys())}) ---"
        )

        source_metaobjects_by_handle = None

        for dest_mo in dest_metaobjects:
            dest_gid = dest_mo.get("id")
            handle = dest_mo.get("handle", "")
            dest_field_map = {f["key"]: f for f in dest_mo.get("fields", [])}

            # Check which file fields need populating
            needs_source = any(
                not _dest_file_reference_is_valid(
                    saudi,
                    dest_field_map.get(fk, {}).get("value"),
                    is_list=file_fields[fk]["is_list"],
                    require_media_image=True,
                )
                for fk in file_fields
            )
            if not needs_source:
                continue

            source_gid = source_gid_by_dest_gid.get(dest_gid)

            source_mo = None
            if source_gid:
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

            if not source_mo:
                if source_metaobjects_by_handle is None:
                    try:
                        source_metaobjects_by_handle = {
                            mo.get("handle", ""): mo
                            for mo in spain.get_metaobjects(mo_type)
                            if mo.get("handle")
                        }
                    except Exception:
                        source_metaobjects_by_handle = {}
                source_mo = source_metaobjects_by_handle.get(handle)

            source_field_map = {sf["key"]: sf for sf in (source_mo or {}).get("fields", [])}
            fallback_field_map = fallback_metaobjects.get(mo_type, {}).get(handle, {})

            fields_to_update = []
            for field_key, field_info in file_fields.items():
                dest_field = dest_field_map.get(field_key, {})
                if _dest_file_reference_is_valid(
                    saudi,
                    dest_field.get("value"),
                    is_list=field_info["is_list"],
                    require_media_image=True,
                ):
                    continue  # Already has value

                source_field = source_field_map.get(field_key)
                using_fallback = False
                if (not source_field or not source_field.get("value")) and fallback_field_map:
                    source_field = fallback_field_map.get(field_key)
                    using_fallback = bool(source_field and source_field.get("value"))
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
                        mapped_dest_gid = _get_valid_mapped_dest_file_id(
                            saudi, file_map, sgid, require_media_image=True
                        )
                        if mapped_dest_gid:
                            dest_gids.append(mapped_dest_gid)
                            continue

                        url = _get_file_url_from_clients(
                            spain,
                            extra_source_clients,
                            sgid,
                            source_field if not using_fallback else None,
                        )
                        if not url:
                            continue

                        if dry_run:
                            origin = "fallback" if using_fallback else "source"
                            print(f"    WOULD upload [{preset}] {handle}.{field_key} ({origin})")
                            continue

                        try:
                            dest_fid = _upload_media_image_reference(saudi, url, f"{handle}_{field_key}")
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
                    mapped_dest_gid = _get_valid_mapped_dest_file_id(
                        saudi, file_map, sgid, require_media_image=True
                    )
                    if mapped_dest_gid:
                        fields_to_update.append({
                            "key": field_key,
                            "value": mapped_dest_gid,
                        })
                        continue

                    url = _get_file_url_from_clients(
                        spain,
                        extra_source_clients,
                        sgid,
                        source_field if not using_fallback else None,
                    )
                    if not url:
                        continue

                    if dry_run:
                        origin = "fallback" if using_fallback else "source"
                        print(f"    WOULD upload [{preset}] {handle}.{field_key} ({origin})")
                        updated += 1
                        continue

                    try:
                        dest_fid = _upload_media_image_reference(saudi, url, f"{handle}_{field_key}")
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

            if fields_to_update:
                field_names = [f["key"] for f in fields_to_update]
                if dry_run:
                    print(f"    WOULD update {handle}: {', '.join(field_names)}")
                    updated += len(fields_to_update)
                else:
                    try:
                        saudi.update_metaobject(dest_gid, fields_to_update)
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

def phase6_article_files(spain, saudi, id_map, file_map, dry_run=False):
    """Migrate article listing_image and hero_image file references."""
    print("\n" + "=" * 60)
    print("PHASE 6: Article File References")
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

            dest_file_id = _get_valid_mapped_dest_file_id(
                saudi, file_map, source_gid, require_media_image=True
            )
            if dest_file_id:
                pass
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
                    dest_file_id = _upload_media_image_reference(
                        saudi,
                        url,
                        alt=f"article_{article.get('handle', '')}_{mf['key']}",
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

def phase7_verify(spain, saudi, id_map, file_map, dry_run=False):
    """Generate a verification report of image migration status."""
    print("\n" + "=" * 60)
    print("PHASE 7: Verification Report")
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
    4: ("Product File Reference Metafields", phase4_product_metafield_files),
    5: ("Metaobject File References", phase5_metaobject_files),
    6: ("Article File References", phase6_article_files),
    7: ("Verification Report", phase7_verify),
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

    if not all([source_url, source_token, dest_url, dest_token]):
        print("ERROR: Set SOURCE_SHOP_URL, SOURCE_ACCESS_TOKEN, DEST_SHOP_URL, DEST_ACCESS_TOKEN in .env")
        return

    spain = ShopifyClient(source_url, source_token)
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
