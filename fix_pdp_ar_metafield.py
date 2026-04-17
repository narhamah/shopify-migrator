#!/usr/bin/env python3
"""Create custom.pdp_images_ar metafield with Arabic images for each product.

Since Shopify doesn't auto-resolve list.file_reference translations on
collection pages, we store Arabic images in a separate metafield and
check request.locale in the theme.
"""

import json
import re
import sys
import time

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, load_json


def classify(fname):
    fname = fname.lower()
    if re.search(r"[_-](en|eng)[_.\d]", fname):
        return "en"
    if re.search(r"[_-]ar[_.\d]", fname):
        return "ar"
    if re.search(r"[_-](es|esp|sp)[_.\d]", fname):
        return "es"
    return "neutral"


def fname_from_url(url):
    return url.rsplit("/", 1)[-1].split("?")[0].lower()


def normalize_fname(fname):
    fname = fname.lower()
    name, _, ext = fname.rpartition(".")
    if name:
        name = name.rstrip("_")
        return f"{name}.{ext}"
    return fname


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    load_dotenv()

    client = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())

    # Create pdp_images_ar metafield definition
    print("Creating custom.pdp_images_ar metafield definition...")
    try:
        result = client.create_metafield_definition({
            "name": "PDP Images (Arabic)",
            "namespace": "custom",
            "key": "pdp_images_ar",
            "type": "list.file_reference",
            "ownerType": "PRODUCT",
        })
        if result:
            print(f"  Created: {result['id']}")
        else:
            print("  Already exists")
    except Exception as e:
        if "in use" in str(e).lower() or "already exists" in str(e).lower():
            print("  Already exists")
        else:
            raise

    # Load Magento data
    ar_items = load_json("data/magento_ar_images.json")
    en_items = load_json("data/magento_en_images.json")

    # Build Magento AR images by SKU (in position order)
    magento_ar = {}
    for item in ar_items:
        sku = item.get("sku", "")
        if not sku:
            continue
        media = sorted(item.get("media_gallery", []), key=lambda m: m.get("position", 0))
        ar_ordered = [m for m in media if classify(m["url"].split("/")[-1]) in ("ar", "neutral")]
        magento_ar[sku] = ar_ordered

    # Build Shopify Files index with fuzzy matching
    print("Fetching Shopify Files library...")
    all_files = client.get_files()
    files_idx = {}
    for f in all_files:
        url = f.get("image", {}).get("url", "") or f.get("url", "")
        if not url:
            continue
        gid = f.get("id", "")
        if not gid:
            continue
        fname = fname_from_url(url)
        if fname not in files_idx:
            files_idx[fname] = gid
        norm = normalize_fname(fname)
        if norm not in files_idx:
            files_idx[norm] = gid
        base = re.sub(r"_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.", ".", fname)
        if base not in files_idx:
            files_idx[base] = gid
    print(f"  {len(files_idx)} entries indexed")

    def resolve_gid(url):
        fname = fname_from_url(url)
        return files_idx.get(fname) or files_idx.get(normalize_fname(fname))

    # Process products
    products = client.get_products()
    print(f"  {len(products)} products")
    print("=" * 80)

    fixed = 0
    for p in products:
        handle = p["handle"]
        sku = next((v["sku"] for v in p.get("variants", []) if v.get("sku")), None)
        if not sku or sku not in magento_ar:
            continue

        ar_media = magento_ar[sku]
        pgid = f"gid://shopify/Product/{p['id']}"

        ar_gids = [g for g in (resolve_gid(m["url"]) for m in ar_media) if g]

        if not ar_gids:
            print(f"  {handle[:45]:45s} AR: 0 (skipped)")
            continue

        try:
            client.set_metafields([{
                "ownerId": pgid,
                "namespace": "custom",
                "key": "pdp_images_ar",
                "value": json.dumps(ar_gids),
                "type": "list.file_reference",
            }])
            print(f"  {handle[:45]:45s} AR: {len(ar_gids)}")
            fixed += 1
        except Exception as e:
            print(f"  ERROR {handle}: {e}")

        time.sleep(0.2)

    print()
    print("=" * 80)
    print(f"Set pdp_images_ar on {fixed} products")


if __name__ == "__main__":
    main()
