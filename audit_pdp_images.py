#!/usr/bin/env python3
"""Audit PDP images: compare Shopify metafields vs Magento source of truth."""

import json
import re
import sys
import time

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, load_json, save_json

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

AR_TRANSLATION_QUERY = """
query($rid: ID!) {
  translatableResource(resourceId: $rid) {
    translations(locale: "ar") {
      key
      value
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


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    load_dotenv()

    client = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())

    # Load Magento data
    en_items = load_json("data/magento_en_images.json")
    ar_items = load_json("data/magento_ar_images.json")
    if not en_items or not ar_items:
        print("ERROR: Run the Magento fetch first (magento_en/ar_images.json)")
        return

    # Build Magento by SKU
    magento = {}
    for item in en_items:
        sku = item.get("sku", "")
        if not sku:
            continue
        media = sorted(item.get("media_gallery", []), key=lambda m: m.get("position", 0))
        en_imgs, neutral_imgs = [], []
        for m in media:
            fname = m["url"].split("/")[-1]
            lang = classify(fname)
            if lang == "en":
                en_imgs.append(m)
            elif lang == "neutral":
                neutral_imgs.append(m)
        magento[sku] = {
            "name": item["name"],
            "url_key": item.get("url_key", ""),
            "en": en_imgs,
            "neutral": neutral_imgs,
            "ar": [],
        }

    for item in ar_items:
        sku = item.get("sku", "")
        if not sku or sku not in magento:
            continue
        media = sorted(item.get("media_gallery", []), key=lambda m: m.get("position", 0))
        for m in media:
            if classify(m["url"].split("/")[-1]) == "ar":
                magento[sku]["ar"].append(m)

    # Shopify Files index
    print("Fetching Shopify Files library...")
    all_files = client.get_files()
    files_idx = {}
    for f in all_files:
        url = f.get("image", {}).get("url", "") or f.get("url", "")
        if url:
            fname = url.rsplit("/", 1)[-1].split("?")[0].lower()
            files_idx[fname] = f.get("id", "")
    print(f"  {len(files_idx)} files indexed")

    # Shopify products
    products = client.get_products()
    print(f"  {len(products)} products")
    print("=" * 85)

    results = []
    for p in products:
        handle = p["handle"]
        sku = next((v["sku"] for v in p.get("variants", []) if v.get("sku")), None)

        if not sku or sku not in magento:
            print(f"  SKIP {handle[:40]:40s} (no Magento match)")
            results.append({"handle": handle, "status": "SKIP"})
            continue

        mag = magento[sku]
        pid = p["id"]
        pgid = f"gid://shopify/Product/{pid}"

        # Get metafield
        data = client._graphql(PRODUCT_MF_QUERY, {"id": pgid})
        mf = data.get("product", {}).get("metafield")

        en_have = 0
        ar_have = 0
        mf_gid = None
        en_mf_gids = []
        ar_mf_gids = []

        if mf and mf.get("id"):
            mf_gid = mf["id"]
            en_mf_gids = json.loads(mf["value"])
            en_have = len(en_mf_gids)

            try:
                tdata = client._graphql(AR_TRANSLATION_QUERY, {"rid": mf_gid})
                for t in tdata.get("translatableResource", {}).get("translations", []):
                    if t["key"] == "value" and t.get("value"):
                        ar_mf_gids = json.loads(t["value"])
                        ar_have = len(ar_mf_gids)
            except Exception:
                pass

        # Expected
        en_need = len(mag["en"]) + len(mag["neutral"])
        ar_need = len(mag["ar"]) + len(mag["neutral"])

        # Check missing files
        en_missing = []
        for m in mag["en"] + mag["neutral"]:
            fname = m["url"].split("/")[-1].split("?")[0].lower()
            if fname not in files_idx:
                en_missing.append({"fname": fname, "url": m["url"]})

        ar_missing = []
        for m in mag["ar"]:
            fname = m["url"].split("/")[-1].split("?")[0].lower()
            if fname not in files_idx:
                ar_missing.append({"fname": fname, "url": m["url"]})

        en_ok = en_have == en_need
        ar_ok = ar_have == ar_need or (len(mag["ar"]) == 0 and ar_have == 0)
        status = "OK" if en_ok and ar_ok else "MISMATCH"

        marker = "  OK  " if status == "OK" else " MISS "
        line = f"{marker} {handle[:40]:40s} EN:{en_have:2d}/{en_need:2d}  AR:{ar_have:2d}/{ar_need:2d}"
        extras = []
        if not en_ok:
            extras.append(f"EN off by {en_have - en_need:+d}")
        if not ar_ok:
            extras.append(f"AR off by {ar_have - ar_need:+d}")
        if en_missing:
            extras.append(f"{len(en_missing)} EN files not in library")
        if ar_missing:
            extras.append(f"{len(ar_missing)} AR files not in library")
        if extras:
            line += "  << " + "; ".join(extras)
        print(line)

        results.append({
            "handle": handle, "sku": sku, "status": status,
            "en_have": en_have, "en_need": en_need,
            "ar_have": ar_have, "ar_need": ar_need,
            "en_missing_files": en_missing,
            "ar_missing_files": ar_missing,
            "mag_en": [m["url"].split("/")[-1] for m in mag["en"]],
            "mag_neutral": [m["url"].split("/")[-1] for m in mag["neutral"]],
            "mag_ar": [m["url"].split("/")[-1] for m in mag["ar"]],
            "mf_gid": mf_gid, "product_gid": pgid,
        })
        time.sleep(0.15)

    print()
    print("=" * 85)
    mismatches = [r for r in results if r.get("status") == "MISMATCH"]
    ok = [r for r in results if r.get("status") == "OK"]
    skipped = [r for r in results if r.get("status") == "SKIP"]
    print(f"OK: {len(ok)}  Mismatches: {len(mismatches)}  Skipped: {len(skipped)}")

    if mismatches:
        print()
        print("=== MISMATCHES TO FIX ===")
        for r in mismatches:
            print(f"  {r['handle']}: EN {r['en_have']}/{r['en_need']}, "
                  f"AR {r['ar_have']}/{r['ar_need']}")
            if r.get("en_missing_files"):
                for mf in r["en_missing_files"]:
                    print(f"    EN missing: {mf['fname']}")
            if r.get("ar_missing_files"):
                for mf in r["ar_missing_files"]:
                    print(f"    AR missing: {mf['fname']}")

    save_json(results, "data/pdp_audit_results.json")
    print(f"\nSaved to data/pdp_audit_results.json")


if __name__ == "__main__":
    main()
