#!/usr/bin/env python3
"""Clean PDP images: remove duplicates and wrong-language images via OCR.

For each product:
1. Downloads all images from both pdp_images (EN) and pdp_images_ar metafields
2. OCR detects actual text language in each image
3. Perceptual hashing detects visual duplicates
4. Removes: wrong-language images, duplicates, Spanish images
5. Rebuilds metafield values with clean, deduped, correctly-ordered images
6. Deletes orphaned duplicate files from Shopify Files

Usage:
    python clean_pdp_images.py --dry-run          # Preview only
    python clean_pdp_images.py --product HANDLE   # Single product
    python clean_pdp_images.py                    # Full run
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from io import BytesIO

import imagehash
import pytesseract
import requests as http_requests
from PIL import Image
from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, load_json, save_json
from tara_migrate.tools.image_lang_detect import classify_image_language

# Perceptual hash similarity threshold (lower = more similar)
HASH_THRESHOLD = 8

PRODUCT_MF_QUERY = """
query($id: ID!) {
  product(id: $id) {
    mfEn: metafield(namespace: "custom", key: "pdp_images") { id value }
    mfAr: metafield(namespace: "custom", key: "pdp_images_ar") { id value }
  }
}
"""

FILE_QUERY = """
query($id: ID!) {
  node(id: $id) {
    ... on MediaImage {
      id
      alt
      image { url }
    }
  }
}
"""

CACHE_DIR = "data/pdp_image_cache"


def download_image(url, session):
    """Download image bytes from URL."""
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        return None


def get_file_url(client, gid):
    """Get CDN URL for a Shopify file GID."""
    data = client._graphql(FILE_QUERY, {"id": gid})
    node = data.get("node", {})
    if node:
        return node.get("image", {}).get("url", "")
    return ""


def ocr_classify(image_bytes):
    """Classify image language via OCR. Returns 'en', 'ar', 'es', or None."""
    try:
        return classify_image_language(image_bytes)
    except Exception:
        return None


def perceptual_hash(image_bytes):
    """Compute perceptual hash of an image."""
    try:
        img = Image.open(BytesIO(image_bytes))
        return imagehash.phash(img)
    except Exception:
        return None


def fname_from_url(url):
    return url.rsplit("/", 1)[-1].split("?")[0].lower()


def process_product(client, session, product, dry_run):
    """Process a single product: download, classify, dedup, rebuild."""
    handle = product["handle"]
    pgid = f"gid://shopify/Product/{product['id']}"

    # Get metafield values
    data = client._graphql(PRODUCT_MF_QUERY, {"id": pgid})
    prod_data = data.get("product", {})
    mf_en = prod_data.get("mfEn")
    mf_ar = prod_data.get("mfAr")

    en_gids = json.loads(mf_en["value"]) if mf_en and mf_en.get("value") else []
    ar_gids = json.loads(mf_ar["value"]) if mf_ar and mf_ar.get("value") else []

    if not en_gids and not ar_gids:
        return {"status": "skip", "reason": "no metafields"}

    # Collect all unique GIDs
    all_gids = list(dict.fromkeys(en_gids + ar_gids))

    # Download and analyze each image
    images = {}  # gid → {url, fname, bytes, ocr_lang, phash}
    print(f"    Downloading {len(all_gids)} images...")

    for gid in all_gids:
        url = get_file_url(client, gid)
        if not url:
            continue
        fname = fname_from_url(url)

        # Check cache
        cache_path = os.path.join(CACHE_DIR, hashlib.md5(gid.encode()).hexdigest())
        if os.path.exists(cache_path + ".json"):
            cached = json.load(open(cache_path + ".json"))
            images[gid] = cached
            images[gid]["from_cache"] = True
            continue

        img_bytes = download_image(url, session)
        if not img_bytes:
            continue

        ocr_lang = ocr_classify(img_bytes)
        phash = perceptual_hash(img_bytes)

        info = {
            "gid": gid,
            "url": url,
            "fname": fname,
            "ocr_lang": ocr_lang,
            "phash": str(phash) if phash else None,
            "size": len(img_bytes),
        }
        images[gid] = info

        # Cache
        json.dump(info, open(cache_path + ".json", "w"))

        time.sleep(0.1)

    print(f"    Analyzed {len(images)} images")

    # Detect duplicates via perceptual hashing
    hashes_seen = {}  # phash_str → first_gid
    duplicates = set()
    for gid in all_gids:
        if gid not in images or not images[gid].get("phash"):
            continue
        ph = images[gid]["phash"]
        # Check against all seen hashes
        is_dup = False
        for seen_ph, seen_gid in hashes_seen.items():
            try:
                dist = imagehash.hex_to_hash(ph) - imagehash.hex_to_hash(seen_ph)
                if dist <= HASH_THRESHOLD:
                    duplicates.add(gid)
                    is_dup = True
                    break
            except Exception:
                pass
        if not is_dup:
            hashes_seen[ph] = gid

    # Build clean EN list: keep EN + neutral, remove AR/ES, remove duplicates
    clean_en = []
    en_removed = {"wrong_lang": [], "duplicate": []}
    en_kept_hashes = {}

    for gid in en_gids:
        if gid not in images:
            clean_en.append(gid)  # Can't analyze, keep it
            continue

        info = images[gid]
        ocr = info.get("ocr_lang")
        ph = info.get("phash")

        # Remove Arabic or Spanish text images from EN
        if ocr == "ar":
            en_removed["wrong_lang"].append(info["fname"])
            continue
        if ocr == "es":
            en_removed["wrong_lang"].append(info["fname"])
            continue

        # Remove duplicates (keep first occurrence)
        if ph:
            is_dup = False
            for kept_ph in en_kept_hashes:
                try:
                    if imagehash.hex_to_hash(ph) - imagehash.hex_to_hash(kept_ph) <= HASH_THRESHOLD:
                        en_removed["duplicate"].append(info["fname"])
                        is_dup = True
                        break
                except Exception:
                    pass
            if is_dup:
                continue
            en_kept_hashes[ph] = gid

        clean_en.append(gid)

    # Build clean AR list: keep AR + neutral, remove EN/ES, remove duplicates
    clean_ar = []
    ar_removed = {"wrong_lang": [], "duplicate": []}
    ar_kept_hashes = {}

    for gid in ar_gids:
        if gid not in images:
            clean_ar.append(gid)
            continue

        info = images[gid]
        ocr = info.get("ocr_lang")
        ph = info.get("phash")

        # Remove English or Spanish text images from AR
        if ocr == "en":
            ar_removed["wrong_lang"].append(info["fname"])
            continue
        if ocr == "es":
            ar_removed["wrong_lang"].append(info["fname"])
            continue

        # Remove duplicates
        if ph:
            is_dup = False
            for kept_ph in ar_kept_hashes:
                try:
                    if imagehash.hex_to_hash(ph) - imagehash.hex_to_hash(kept_ph) <= HASH_THRESHOLD:
                        ar_removed["duplicate"].append(info["fname"])
                        is_dup = True
                        break
                except Exception:
                    pass
            if is_dup:
                continue
            ar_kept_hashes[ph] = gid

        clean_ar.append(gid)

    # Report
    en_diff = len(en_gids) - len(clean_en)
    ar_diff = len(ar_gids) - len(clean_ar)

    print(f"    EN: {len(en_gids)} → {len(clean_en)} "
          f"(removed {len(en_removed['wrong_lang'])} wrong-lang, "
          f"{len(en_removed['duplicate'])} dupes)")
    print(f"    AR: {len(ar_gids)} → {len(clean_ar)} "
          f"(removed {len(ar_removed['wrong_lang'])} wrong-lang, "
          f"{len(ar_removed['duplicate'])} dupes)")

    if en_removed["wrong_lang"]:
        for f in en_removed["wrong_lang"]:
            print(f"      EN removed (wrong lang): {f}")
    if en_removed["duplicate"]:
        for f in en_removed["duplicate"]:
            print(f"      EN removed (duplicate): {f}")
    if ar_removed["wrong_lang"]:
        for f in ar_removed["wrong_lang"]:
            print(f"      AR removed (wrong lang): {f}")
    if ar_removed["duplicate"]:
        for f in ar_removed["duplicate"]:
            print(f"      AR removed (duplicate): {f}")

    if dry_run:
        return {
            "status": "would_fix" if en_diff or ar_diff else "ok",
            "en_before": len(en_gids), "en_after": len(clean_en),
            "ar_before": len(ar_gids), "ar_after": len(clean_ar),
        }

    # Update metafields if changed
    changed = False
    if en_diff and clean_en:
        client.set_metafields([{
            "ownerId": pgid,
            "namespace": "custom",
            "key": "pdp_images",
            "value": json.dumps(clean_en),
            "type": "list.file_reference",
        }])
        changed = True

    if ar_diff and clean_ar:
        client.set_metafields([{
            "ownerId": pgid,
            "namespace": "custom",
            "key": "pdp_images_ar",
            "value": json.dumps(clean_ar),
            "type": "list.file_reference",
        }])
        changed = True

    if changed:
        print(f"    Updated metafields")

    # Collect GIDs to potentially delete (removed from both EN and AR)
    all_kept = set(clean_en + clean_ar)
    all_original = set(en_gids + ar_gids)
    orphaned = all_original - all_kept

    return {
        "status": "fixed" if changed else "ok",
        "en_before": len(en_gids), "en_after": len(clean_en),
        "ar_before": len(ar_gids), "ar_after": len(clean_ar),
        "orphaned_gids": list(orphaned),
    }


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    load_dotenv()

    parser = argparse.ArgumentParser(description="Clean PDP images: remove duplicates and wrong-language")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--product", type=str, default=None)
    parser.add_argument("--delete-orphans", action="store_true",
                        help="Delete orphaned files from Shopify Files library")
    args = parser.parse_args()

    client = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())
    session = http_requests.Session()

    os.makedirs(CACHE_DIR, exist_ok=True)

    products = client.get_products()
    if args.product:
        products = [p for p in products if p["handle"] == args.product]

    prefix = "DRY RUN: " if args.dry_run else ""
    print(f"\n{prefix}PDP Image Cleanup (OCR + dedup)")
    print(f"  {len(products)} products")
    print("=" * 70)

    all_orphans = []
    totals = {"fixed": 0, "ok": 0, "en_removed": 0, "ar_removed": 0}

    for p in products:
        handle = p["handle"]
        print(f"\n  {handle}")

        result = process_product(client, session, p, args.dry_run)

        if result["status"] in ("fixed", "would_fix"):
            totals["fixed"] += 1
            totals["en_removed"] += result["en_before"] - result["en_after"]
            totals["ar_removed"] += result["ar_before"] - result["ar_after"]
        else:
            totals["ok"] += 1

        if result.get("orphaned_gids"):
            all_orphans.extend(result["orphaned_gids"])

        time.sleep(0.2)

    print()
    print("=" * 70)
    print(f"Products cleaned: {totals['fixed']}")
    print(f"Products already clean: {totals['ok']}")
    print(f"EN images removed: {totals['en_removed']}")
    print(f"AR images removed: {totals['ar_removed']}")
    print(f"Orphaned files: {len(all_orphans)}")

    if all_orphans and args.delete_orphans and not args.dry_run:
        print(f"\nDeleting {len(all_orphans)} orphaned files...")
        deleted = 0
        for gid in all_orphans:
            try:
                client.delete_file(gid)
                deleted += 1
                time.sleep(0.2)
            except Exception:
                pass
        print(f"  Deleted: {deleted}")


if __name__ == "__main__":
    main()
