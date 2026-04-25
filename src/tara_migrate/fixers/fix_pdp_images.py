#!/usr/bin/env python3
"""Localize PDP product images: EN+neutral for English, AR+neutral for Arabic.

Creates a `custom.pdp_images` metafield (list.file_reference) on each product,
populates it with EN+neutral images as the default, and registers AR+neutral
images as the Arabic translation via the Translations API. The theme renders
from this metafield — Shopify auto-serves the locale-appropriate version.

Also removes Spanish and Arabic images from the main product gallery (they
now live in the metafield / translation), and deletes Spanish files entirely.

Arabic images are sourced from three places (in priority order):
1. Already in the Shopify product gallery (classified by filename)
2. Already in the Shopify Files library (matched by filename)
3. Local data/arabic/products.json (uploaded from Magento URLs)

Usage:
    python fix_pdp_images.py --dry-run          # Preview only
    python fix_pdp_images.py --probe-only       # Test translation capability
    python fix_pdp_images.py --product HANDLE   # Single product
    python fix_pdp_images.py --skip-delete      # Don't delete ES files
    python fix_pdp_images.py --skip-cleanup     # Don't remove images from gallery
    python fix_pdp_images.py                    # Full pipeline
"""

import argparse
import json
import os
import re
import time

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, load_json, save_json

# ── Language classification by filename ──────────────────────────────────────

LANG_PATTERNS = {
    "en": re.compile(r"[_-](en|eng)[_.\d]", re.IGNORECASE),
    "ar": re.compile(r"[_-]ar[_.\d]", re.IGNORECASE),
    "es": re.compile(r"[_-](es|esp|sp)[_.\d]", re.IGNORECASE),
}

PROGRESS_FILE = "data/pdp_images_progress.json"
AR_UPLOAD_CACHE_FILE = "data/ar_image_uploads.json"


def classify_image_lang(url: str) -> str:
    """Classify image language from filename in URL.

    Returns: 'en', 'ar', 'es', or 'neutral'.
    """
    filename = url.rsplit("/", 1)[-1].split("?")[0].lower()
    for lang, pattern in LANG_PATTERNS.items():
        if pattern.search(filename):
            return lang
    return "neutral"


def url_to_filename(url: str) -> str:
    """Extract filename from a CDN URL, stripping query params."""
    return url.rsplit("/", 1)[-1].split("?")[0]


# ── Load local Arabic image data ─────────────────────────────────────────────

def load_local_arabic_images(en_products_data):
    """Load Arabic product images from local data/arabic/products.json.

    Matches Arabic products to English products by image URL overlap, then
    extracts AR-tagged images from each Arabic product.

    Returns: {en_handle: [{"src": url, "alt": alt}, ...]}
    """
    ar_file = os.path.join("data", "arabic", "products.json")
    if not os.path.exists(ar_file):
        print("  WARNING: data/arabic/products.json not found")
        return {}

    ar_products = load_json(ar_file)
    if not ar_products:
        return {}

    print(f"  Loaded {len(ar_products)} products from data/arabic/products.json")

    # Build filename → EN handle lookup from EN products
    fname_to_en_handle = {}
    for ep in en_products_data:
        handle = ep.get("handle", "")
        for img in ep.get("images", []):
            fname = url_to_filename(img.get("src", "")).lower()
            fname_to_en_handle[fname] = handle

    # Match AR products to EN handles by image overlap
    result = {}
    for ap in ar_products:
        ar_fnames = {}
        for img in ap.get("images", []):
            fname = url_to_filename(img.get("src", "")).lower()
            ar_fnames[fname] = img

        # Find best EN handle match
        handle_votes = {}
        for fname in ar_fnames:
            en_handle = fname_to_en_handle.get(fname)
            if en_handle:
                handle_votes[en_handle] = handle_votes.get(en_handle, 0) + 1

        if not handle_votes:
            continue
        best_handle = max(handle_votes, key=handle_votes.get)

        # Extract AR-tagged images only
        ar_images = []
        for fname, img in ar_fnames.items():
            if classify_image_lang(fname) == "ar":
                ar_images.append({
                    "src": img.get("src", ""),
                    "alt": img.get("alt", ""),
                })

        if ar_images:
            # Merge with any existing (don't overwrite)
            existing = result.get(best_handle, [])
            existing_srcs = {url_to_filename(e["src"]).lower() for e in existing}
            for img in ar_images:
                if url_to_filename(img["src"]).lower() not in existing_srcs:
                    existing.append(img)
            result[best_handle] = existing

    matched = sum(1 for v in result.values() if v)
    total_imgs = sum(len(v) for v in result.values())
    print(f"  Matched {matched} products with {total_imgs} AR images from local data")
    return result


# ── Phase 0: Metafield definition ────────────────────────────────────────────

def ensure_pdp_images_metafield(client, dry_run):
    """Create custom.pdp_images metafield definition if it doesn't exist."""
    definition = {
        "name": "PDP Images",
        "namespace": "custom",
        "key": "pdp_images",
        "type": "list.file_reference",
        "ownerType": "PRODUCT",
    }
    if dry_run:
        print("  [DRY RUN] Would create metafield definition: custom.pdp_images (list.file_reference)")
        return
    try:
        result = client.create_metafield_definition(definition)
        if result:
            print(f"  Created metafield definition: {result['name']} ({result['id']})")
        else:
            print("  Metafield definition custom.pdp_images already exists")
    except Exception as e:
        if "in use" in str(e).lower() or "already exists" in str(e).lower():
            print("  Metafield definition custom.pdp_images already exists")
        else:
            raise


# ── Phase 1: Fetch product media via GraphQL ─────────────────────────────────

PRODUCT_MEDIA_QUERY = """
query GetProductMedia($first: Int!, $after: String) {
  products(first: $first, after: $after) {
    edges {
      cursor
      node {
        id
        title
        handle
        media(first: 100) {
          edges {
            node {
              ... on MediaImage {
                id
                image {
                  url
                  altText
                }
              }
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


def fetch_all_product_media(client):
    """Fetch all products with their media GIDs and URLs via GraphQL."""
    all_products = []
    cursor = None
    while True:
        variables = {"first": 50}
        if cursor:
            variables["after"] = cursor
        data = client._graphql(PRODUCT_MEDIA_QUERY, variables)
        products_data = data["products"]
        for edge in products_data["edges"]:
            node = edge["node"]
            media_items = []
            for media_edge in node.get("media", {}).get("edges", []):
                media_node = media_edge["node"]
                if media_node.get("id") and media_node.get("image"):
                    media_items.append({
                        "gid": media_node["id"],
                        "url": media_node["image"].get("url", ""),
                        "alt": media_node["image"].get("altText", ""),
                    })
            all_products.append({
                "gid": node["id"],
                "title": node["title"],
                "handle": node["handle"],
                "media": media_items,
            })
            cursor = edge["cursor"]
        if not products_data["pageInfo"]["hasNextPage"]:
            break
    return all_products


# ── Build URL-based mapping between REST IDs and GraphQL GIDs ────────────────

def build_rest_to_gql_map(rest_products, gql_products):
    """Map REST product image IDs to GraphQL media GIDs by matching URLs.

    Returns: {rest_product_id: {rest_image_id: graphql_gid}}
    """
    gql_by_handle = {p["handle"]: p for p in gql_products}

    mapping = {}
    for rp in rest_products:
        handle = rp.get("handle", "")
        gp = gql_by_handle.get(handle)
        if not gp:
            continue

        rest_id = rp["id"]
        mapping[rest_id] = {}

        gql_by_fname = {}
        for m in gp["media"]:
            fname = url_to_filename(m["url"])
            gql_by_fname[fname] = m["gid"]

        for img in rp.get("images", []):
            src = img.get("src", "")
            fname = url_to_filename(src)
            gql_gid = gql_by_fname.get(fname)
            if gql_gid:
                mapping[rest_id][img["id"]] = gql_gid

    return mapping


# ── Build Files library index ────────────────────────────────────────────────

def build_files_index(client):
    """Build {normalized_filename: file_gid} from Shopify Files library.

    Indexes ALL files (not just Arabic) so we can look up any image.
    """
    print("  Fetching Shopify Files library...")
    all_files = client.get_files()
    print(f"  Found {len(all_files)} files in library")

    index = {}
    ar_count = 0
    for f in all_files:
        url = f.get("image", {}).get("url", "") or f.get("url", "")
        if not url:
            continue
        fname = url_to_filename(url).lower()
        gid = f.get("id", "")
        if not gid:
            continue
        index[fname] = gid
        if classify_image_lang(url) == "ar":
            ar_count += 1

    print(f"  Indexed {len(index)} files ({ar_count} Arabic)")
    return index


# ── Upload missing AR images ─────────────────────────────────────────────────

def upload_missing_ar_images(client, handle, local_ar_images, files_index,
                             upload_cache, dry_run):
    """Upload AR images from Magento URLs if not already in Shopify Files.

    Returns list of File GIDs for all AR images (existing + newly uploaded).
    """
    ar_gids = []
    uploaded = 0

    for img in local_ar_images:
        src = img["src"]
        fname = url_to_filename(src).lower()

        # Already in Shopify Files?
        if fname in files_index:
            ar_gids.append(files_index[fname])
            continue

        # Already uploaded in a previous run?
        if src in upload_cache:
            ar_gids.append(upload_cache[src])
            continue

        if dry_run:
            ar_gids.append(f"PENDING_UPLOAD:{fname}")
            uploaded += 1
            continue

        # Upload from Magento URL
        try:
            alt = img.get("alt", "")
            gid = client.upload_file_from_url(src, filename=fname, alt=alt)
            if gid:
                ar_gids.append(gid)
                upload_cache[src] = gid
                files_index[fname] = gid
                uploaded += 1
                time.sleep(0.5)
            else:
                print(f"      WARNING: upload returned None for {fname}")
        except Exception as e:
            print(f"      ERROR uploading {fname}: {e}")

    if uploaded:
        print(f"    Uploaded {uploaded} AR images from Magento")

    return ar_gids


# ── Probe — verify file_reference translation works ──────────────────────────

def probe_file_reference_translation(client, gql_products):
    """Test that list.file_reference metafields can be translated.

    Sets a test metafield on the first product with images, checks if it
    appears in translatableContent, and returns the translation key name.
    Returns None if translation is not supported.
    """
    test_product = None
    test_gid = None
    for p in gql_products:
        if p["media"]:
            test_product = p
            test_gid = p["media"][0]["gid"]
            break
    if not test_product:
        print("  ERROR: No products with images found for probe")
        return None

    print(f"  Probing with product: {test_product['handle']}")
    print(f"    Setting test metafield with GID: {test_gid}")

    result = client.set_metafields([{
        "ownerId": test_product["gid"],
        "namespace": "custom",
        "key": "pdp_images",
        "value": json.dumps([test_gid]),
        "type": "list.file_reference",
    }])
    metafield_gid = result[0]["id"]
    print(f"    Metafield GID: {metafield_gid}")

    time.sleep(1)

    resource = client.get_translatable_resource(metafield_gid)
    if not resource:
        print("    FAIL: translatableResource returned nothing for metafield GID")
        return None

    content = resource.get("translatableContent", [])
    print(f"    Translatable content keys: {[tc['key'] for tc in content]}")

    for tc in content:
        if tc["key"] == "value":
            print(f"    SUCCESS: 'value' key found with digest: {tc['digest'][:20]}...")
            return "value"

    print("    FAIL: No 'value' key found in translatableContent")
    print("    Available keys:", [tc["key"] for tc in content])
    return None


# ── Process each product ─────────────────────────────────────────────────────

def process_product(client, gql_product, rest_product, rest_to_gql,
                    files_index, local_ar_map, upload_cache,
                    translation_key, dry_run, skip_cleanup, skip_delete):
    """Process a single product: set metafield, register translation, cleanup.

    Returns dict with counts for the summary report.
    """
    handle = gql_product["handle"]
    title = gql_product["title"]
    product_gid = gql_product["gid"]
    rest_id = rest_product["id"] if rest_product else None

    # Classify all media images in the product gallery
    en_gids = []
    ar_gallery_gids = []
    es_gids = []
    neutral_gids = []
    es_rest_ids = []
    ar_rest_ids = []

    gql_to_rest = {}
    if rest_id and rest_id in rest_to_gql:
        gql_to_rest = {v: k for k, v in rest_to_gql[rest_id].items()}

    for m in gql_product["media"]:
        lang = classify_image_lang(m["url"])
        gid = m["gid"]
        rest_img_id = gql_to_rest.get(gid)

        if lang == "en":
            en_gids.append(gid)
        elif lang == "ar":
            ar_gallery_gids.append(gid)
            if rest_img_id:
                ar_rest_ids.append(rest_img_id)
        elif lang == "es":
            es_gids.append(gid)
            if rest_img_id:
                es_rest_ids.append(rest_img_id)
        else:
            neutral_gids.append(gid)

    # Gather ALL Arabic GIDs from three sources:
    # 1. Already in product gallery
    ar_gids = list(ar_gallery_gids)
    ar_gid_set = set(ar_gids)

    # 2. In Shopify Files library (match by filename stem from EN images)
    for m in gql_product["media"]:
        if classify_image_lang(m["url"]) != "en":
            continue
        fname = url_to_filename(m["url"]).lower()
        ar_fname = re.sub(r"[_-](en|eng)([_.\d])", r"_ar\2", fname, flags=re.IGNORECASE)
        if ar_fname != fname and ar_fname in files_index:
            gid = files_index[ar_fname]
            if gid not in ar_gid_set:
                ar_gids.append(gid)
                ar_gid_set.add(gid)

    # 3. Local data/arabic/products.json → upload missing from Magento
    local_ar_images = local_ar_map.get(handle, [])
    if local_ar_images:
        # Filter to images not already found
        missing_local = []
        for img in local_ar_images:
            fname = url_to_filename(img["src"]).lower()
            if fname not in files_index:
                # Also check if already in gallery by GID set
                missing_local.append(img)
            elif files_index[fname] not in ar_gid_set:
                # In Files but not yet in our AR list
                ar_gids.append(files_index[fname])
                ar_gid_set.add(files_index[fname])

        if missing_local:
            uploaded_gids = upload_missing_ar_images(
                client, handle, missing_local, files_index, upload_cache, dry_run)
            for gid in uploaded_gids:
                if gid not in ar_gid_set:
                    ar_gids.append(gid)
                    ar_gid_set.add(gid)

    total = len(en_gids) + len(ar_gids) + len(es_gids) + len(neutral_gids)
    print(f"\n  {handle} ({title[:40]}): {total} images")
    print(f"    EN: {len(en_gids)}  AR: {len(ar_gids)}  "
          f"ES: {len(es_gids)}  neutral: {len(neutral_gids)}")
    sources = []
    if ar_gallery_gids:
        sources.append(f"{len(ar_gallery_gids)} gallery")
    files_ar = len(ar_gids) - len(ar_gallery_gids)
    if files_ar > 0:
        sources.append(f"{files_ar} files/uploaded")
    if sources:
        print(f"    AR sources: {', '.join(sources)}")

    stats = {"metafield": 0, "translation": 0, "gallery_removed": 0,
             "es_deleted": 0, "ar_uploaded": 0, "skipped": False}

    # Build default value (EN + neutral)
    default_gids = en_gids + neutral_gids
    if not default_gids:
        print(f"    WARNING: No EN or neutral images — skipping metafield")
        stats["skipped"] = True
        return stats

    default_value = json.dumps(default_gids)

    if dry_run:
        print(f"    [DRY RUN] Would set metafield: {len(default_gids)} images (EN+neutral)")
        if ar_gids:
            ar_value_gids = ar_gids + neutral_gids
            print(f"    [DRY RUN] Would register AR translation: "
                  f"{len(ar_value_gids)} images (AR+neutral)")
        else:
            print(f"    [DRY RUN] WARNING: No AR images found — Arabic will show EN+neutral")
        if es_rest_ids:
            print(f"    [DRY RUN] Would remove {len(es_rest_ids)} ES images from gallery")
        if ar_rest_ids:
            print(f"    [DRY RUN] Would remove {len(ar_rest_ids)} AR images from gallery")
        if es_gids and not skip_delete:
            print(f"    [DRY RUN] Would delete {len(es_gids)} ES files from Files library")
        stats["metafield"] = 1
        stats["translation"] = 1 if ar_gids else 0
        stats["gallery_removed"] = len(es_rest_ids) + len(ar_rest_ids)
        stats["es_deleted"] = len(es_gids) if not skip_delete else 0
        return stats

    # Step A: Set default metafield value
    try:
        result = client.set_metafields([{
            "ownerId": product_gid,
            "namespace": "custom",
            "key": "pdp_images",
            "value": default_value,
            "type": "list.file_reference",
        }])
        metafield_gid = result[0]["id"]
        print(f"    Set metafield ({len(default_gids)} images): {metafield_gid}")
        stats["metafield"] = 1
    except Exception as e:
        print(f"    ERROR setting metafield: {e}")
        return stats

    # Step B: Register Arabic translation (if AR images exist)
    if ar_gids:
        ar_value_gids = ar_gids + neutral_gids
        ar_value = json.dumps(ar_value_gids)

        try:
            time.sleep(0.3)
            resource = client.get_translatable_resource(metafield_gid)
            digest = None
            if resource:
                for tc in resource.get("translatableContent", []):
                    if tc["key"] == translation_key:
                        digest = tc["digest"]
                        break

            if not digest:
                print(f"    WARNING: No digest for {translation_key} — skipping AR translation")
            else:
                client.register_translations(metafield_gid, "ar", [{
                    "locale": "ar",
                    "key": translation_key,
                    "value": ar_value,
                    "translatableContentDigest": digest,
                }])
                print(f"    Registered AR translation ({len(ar_value_gids)} images)")
                stats["translation"] = 1
        except Exception as e:
            print(f"    ERROR registering AR translation: {e}")
    else:
        print(f"    No AR images — Arabic will show EN+neutral (fallback)")

    # Step C: Remove ES + AR images from product gallery
    if not skip_cleanup and rest_id:
        images_to_remove = es_rest_ids + ar_rest_ids
        for img_id in images_to_remove:
            try:
                client._request("DELETE", f"products/{rest_id}/images/{img_id}.json")
                stats["gallery_removed"] += 1
                time.sleep(0.2)
            except Exception as e:
                print(f"    ERROR removing image {img_id}: {e}")

        if images_to_remove:
            print(f"    Removed {stats['gallery_removed']} images from gallery "
                  f"(ES: {len(es_rest_ids)}, AR: {len(ar_rest_ids)})")

    # Step D: Delete ES files from Shopify Files
    if not skip_delete and es_gids:
        deleted = 0
        for gid in es_gids:
            try:
                client.delete_file(gid)
                deleted += 1
                time.sleep(0.2)
            except Exception as e:
                print(f"    ERROR deleting file {gid}: {e}")
        stats["es_deleted"] = deleted
        if deleted:
            print(f"    Deleted {deleted} ES files from Files library")

    time.sleep(0.3)
    return stats


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Localize PDP images: EN+neutral default, AR+neutral translation")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without modifying Shopify")
    parser.add_argument("--probe-only", action="store_true",
                        help="Test translation capability and exit")
    parser.add_argument("--product", type=str, default=None,
                        help="Process a single product by handle")
    parser.add_argument("--skip-delete", action="store_true",
                        help="Don't delete ES files from Files library")
    parser.add_argument("--skip-cleanup", action="store_true",
                        help="Don't remove images from product gallery")
    parser.add_argument("--reset", action="store_true",
                        help="Ignore progress file, reprocess all products")
    args = parser.parse_args()

    load_dotenv()
    shop_url = config.get_dest_shop_url()
    access_token = config.get_dest_access_token()
    client = ShopifyClient(shop_url, access_token)

    prefix = "DRY RUN: " if args.dry_run else ""
    print(f"\n{prefix}PDP Image Localization")
    print(f"  Store: {shop_url}")
    print()

    # Phase 0: Ensure metafield definition exists
    print("Phase 0: Metafield definition")
    ensure_pdp_images_metafield(client, args.dry_run)

    # Phase 1: Fetch product data from all sources
    print("\nPhase 1: Fetching product data")
    print("  Fetching product media via GraphQL...")
    gql_products = fetch_all_product_media(client)
    print(f"  Found {len(gql_products)} products via GraphQL")

    print("  Fetching products via REST...")
    rest_products = client.get_products()
    print(f"  Found {len(rest_products)} products via REST")

    # Build REST ↔ GraphQL mapping
    rest_to_gql = build_rest_to_gql_map(rest_products, gql_products)
    rest_by_handle = {p["handle"]: p for p in rest_products}

    # Build Files library index
    files_index = build_files_index(client)

    # Load local Arabic images from data/arabic/products.json
    print("\n  Loading local Arabic image data...")
    en_products_data = load_json(os.path.join("data", "english", "products.json"))
    local_ar_map = load_local_arabic_images(en_products_data or rest_products)

    # Load upload cache (for resuming uploads)
    upload_cache = load_json(AR_UPLOAD_CACHE_FILE, default={})

    # Phase 2: Probe
    print("\nPhase 2: Probe — verifying list.file_reference translation support")
    if args.dry_run:
        print("  [DRY RUN] Skipping probe — will assume 'value' key works")
        translation_key = "value"
    else:
        translation_key = probe_file_reference_translation(client, gql_products)
        if not translation_key:
            print("\n  ABORT: list.file_reference metafields are NOT translatable.")
            print("  The metafield approach won't work for per-locale images.")
            print("  Consider using a JSON metafield with theme-side locale switching instead.")
            return

    if args.probe_only:
        print("\n  Probe complete. Translation key:", translation_key)
        return

    # Phase 3: Process products
    print(f"\nPhase 3: Processing products")

    progress = load_json(PROGRESS_FILE, default={"processed": [], "errors": {}})
    if args.reset:
        progress = {"processed": [], "errors": {}}

    if args.product:
        gql_products = [p for p in gql_products if p["handle"] == args.product]
        if not gql_products:
            print(f"  ERROR: Product '{args.product}' not found")
            return

    totals = {"metafield": 0, "translation": 0, "gallery_removed": 0,
              "es_deleted": 0, "skipped": 0, "errors": 0, "no_ar": 0}

    for i, gp in enumerate(gql_products, 1):
        handle = gp["handle"]

        if handle in progress["processed"] and not args.reset and not args.dry_run:
            continue

        rest_product = rest_by_handle.get(handle)

        try:
            stats = process_product(
                client, gp, rest_product, rest_to_gql, files_index,
                local_ar_map, upload_cache, translation_key,
                args.dry_run, args.skip_cleanup, args.skip_delete,
            )
            totals["metafield"] += stats["metafield"]
            totals["translation"] += stats["translation"]
            totals["gallery_removed"] += stats["gallery_removed"]
            totals["es_deleted"] += stats["es_deleted"]
            if stats.get("skipped"):
                totals["skipped"] += 1
            if stats["metafield"] and not stats["translation"]:
                totals["no_ar"] += 1

            if not args.dry_run:
                progress["processed"].append(handle)
                save_json(progress, PROGRESS_FILE)
                # Save upload cache after each product
                if upload_cache:
                    save_json(upload_cache, AR_UPLOAD_CACHE_FILE)

        except Exception as e:
            print(f"    ERROR processing {handle}: {e}")
            totals["errors"] += 1
            if not args.dry_run:
                progress["errors"][handle] = str(e)
                save_json(progress, PROGRESS_FILE)

    # Summary
    print(f"\n{'='*50}")
    print(f"{'DRY RUN ' if args.dry_run else ''}Summary")
    print(f"  Products with metafield: {totals['metafield']}")
    print(f"  AR translations set:     {totals['translation']}")
    print(f"  No AR images (EN only):  {totals['no_ar']}")
    print(f"  Gallery images removed:  {totals['gallery_removed']}")
    print(f"  ES files deleted:        {totals['es_deleted']}")
    print(f"  Skipped (no images):     {totals['skipped']}")
    print(f"  Errors:                  {totals['errors']}")

    if not args.dry_run:
        print(f"\n  Progress saved to: {PROGRESS_FILE}")
        if upload_cache:
            print(f"  Upload cache saved to: {AR_UPLOAD_CACHE_FILE}")
        print("\n  NEXT STEP: Update your theme to render from")
        print("  product.metafields.custom.pdp_images instead of product.media")


if __name__ == "__main__":
    main()
