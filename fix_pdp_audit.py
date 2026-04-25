#!/usr/bin/env python3
"""Fix PDP image mismatches: upload missing files, rebuild metafields to match Magento.

Reads audit results from data/pdp_audit_results.json, uploads missing images
from Magento, and rebuilds custom.pdp_images metafield values + AR translations
to exactly match the Magento source of truth in order and quantity.
"""

import json
import re
import sys
import time

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, load_json, save_json

MAGENTO_BASE = "https://taraformula.ae"

PRODUCT_MF_QUERY = """
query($id: ID!) {
  product(id: $id) {
    metafield(namespace: "custom", key: "pdp_images") {
      id
      value
    }
  }
}
"""

TRANSLATABLE_QUERY = """
query($rid: ID!) {
  translatableResource(resourceId: $rid) {
    translatableContent {
      key
      value
      digest
    }
  }
}
"""


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


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    load_dotenv()

    client = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())

    # Load audit results and Magento data
    audit = load_json("data/pdp_audit_results.json")
    en_items = load_json("data/magento_en_images.json")
    ar_items = load_json("data/magento_ar_images.json")

    if not audit or not en_items or not ar_items:
        print("ERROR: Missing audit or Magento data files")
        return

    # Build Magento by SKU with full image data
    magento = {}
    for item in en_items:
        sku = item.get("sku", "")
        if not sku:
            continue
        media = sorted(item.get("media_gallery", []), key=lambda m: m.get("position", 0))
        en_imgs, neutral_imgs = [], []
        for m in media:
            lang = classify(m["url"].split("/")[-1])
            if lang == "en":
                en_imgs.append(m)
            elif lang == "neutral":
                neutral_imgs.append(m)
        magento[sku] = {"en": en_imgs, "neutral": neutral_imgs, "ar": []}

    for item in ar_items:
        sku = item.get("sku", "")
        if not sku or sku not in magento:
            continue
        media = sorted(item.get("media_gallery", []), key=lambda m: m.get("position", 0))
        for m in media:
            if classify(m["url"].split("/")[-1]) == "ar":
                magento[sku]["ar"].append(m)

    # Build Shopify Files index
    print("Fetching Shopify Files library...")
    all_files = client.get_files()
    files_idx = {}
    for f in all_files:
        url = f.get("image", {}).get("url", "") or f.get("url", "")
        if url:
            fname = fname_from_url(url)
            files_idx[fname] = f.get("id", "")
    print(f"  {len(files_idx)} files indexed")

    # Filter to mismatches only
    to_fix = [r for r in audit if r.get("status") == "MISMATCH"]
    print(f"  {len(to_fix)} products to fix")
    print("=" * 80)

    upload_count = 0
    fix_count = 0
    error_count = 0

    for r in to_fix:
        handle = r["handle"]
        sku = r.get("sku")
        product_gid = r.get("product_gid")

        if not sku or sku not in magento:
            print(f"\n  SKIP {handle} (no Magento data)")
            continue

        mag = magento[sku]
        print(f"\n  Fixing {handle}...")

        # Step 1: Upload missing files
        all_needed = mag["en"] + mag["neutral"] + mag["ar"]
        for m in all_needed:
            fname = fname_from_url(m["url"])
            if fname in files_idx:
                continue
            # Upload from Magento
            src_url = m["url"]
            if not src_url.startswith("http"):
                src_url = MAGENTO_BASE + src_url
            try:
                gid = client.upload_file_from_url(src_url, filename=fname, alt=m.get("label", ""))
                if gid:
                    files_idx[fname] = gid
                    upload_count += 1
                    if upload_count % 10 == 0:
                        print(f"    Uploaded {upload_count} files so far...")
                    time.sleep(0.5)
                else:
                    print(f"    WARNING: upload returned None for {fname}")
            except Exception as e:
                print(f"    ERROR uploading {fname}: {e}")
                error_count += 1

        # Step 2: Build EN value (EN images + neutral, in Magento position order)
        en_gids = []
        for m in mag["en"] + mag["neutral"]:
            fname = fname_from_url(m["url"])
            gid = files_idx.get(fname)
            if gid:
                en_gids.append(gid)
            else:
                print(f"    WARNING: still missing {fname}")

        # Step 3: Build AR value (AR images + neutral, in Magento position order)
        ar_gids = []
        for m in mag["ar"] + mag["neutral"]:
            fname = fname_from_url(m["url"])
            gid = files_idx.get(fname)
            if gid:
                ar_gids.append(gid)
            else:
                print(f"    WARNING: still missing AR {fname}")

        if not en_gids:
            print(f"    ERROR: No EN GIDs resolved — skipping")
            error_count += 1
            continue

        # Step 4: Set metafield
        try:
            result = client.set_metafields([{
                "ownerId": product_gid,
                "namespace": "custom",
                "key": "pdp_images",
                "value": json.dumps(en_gids),
                "type": "list.file_reference",
            }])
            mf_gid = result[0]["id"]
            print(f"    Set EN metafield: {len(en_gids)} images")
        except Exception as e:
            print(f"    ERROR setting metafield: {e}")
            error_count += 1
            continue

        # Step 5: Register AR translation
        if ar_gids:
            try:
                time.sleep(0.3)
                resource = client._graphql(TRANSLATABLE_QUERY, {"rid": mf_gid})
                tr = resource.get("translatableResource", {})
                digest = None
                for tc in tr.get("translatableContent", []):
                    if tc["key"] == "value":
                        digest = tc["digest"]
                        break

                if not digest:
                    print(f"    WARNING: No digest — skipping AR translation")
                else:
                    client.register_translations(mf_gid, "ar", [{
                        "locale": "ar",
                        "key": "value",
                        "value": json.dumps(ar_gids),
                        "translatableContentDigest": digest,
                    }])
                    print(f"    Set AR translation: {len(ar_gids)} images")
            except Exception as e:
                print(f"    ERROR registering AR: {e}")
                error_count += 1
        else:
            print(f"    No AR images (neutral-only product)")

        fix_count += 1
        time.sleep(0.3)

    print()
    print("=" * 80)
    print(f"Fixed:    {fix_count}")
    print(f"Uploaded: {upload_count} files")
    print(f"Errors:   {error_count}")


if __name__ == "__main__":
    main()
