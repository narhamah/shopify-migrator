#!/usr/bin/env python3
"""Fix PDP image ordering: maintain Magento position order instead of EN-first.

Rebuilds custom.pdp_images metafield values to match Magento carousel order
exactly. EN metafield = EN+neutral images in Magento position order.
AR metafield = AR+neutral images in Magento position order.
"""

import json
import os
import re
import sys
import time

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, load_json

# Arabic Magento storefront base (overridable; no longer hardcoded to one store).
MAGENTO_BASE = os.environ.get("MAGENTO_AR_SITE_URL", "https://taraformula.ae")

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


def normalize_fname(fname):
    """Normalize filename for fuzzy matching.

    Shopify strips trailing underscores before extensions:
      tara_pdp_sage_system_en_3_1_.jpg → tara_pdp_sage_system_en_3_1.jpg
    """
    fname = fname.lower()
    # Strip trailing underscores before extension
    name, _, ext = fname.rpartition(".")
    if name:
        name = name.rstrip("_")
        return f"{name}.{ext}"
    return fname


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    load_dotenv()

    client = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())

    en_items = load_json("data/magento_en_images.json")
    ar_items = load_json("data/magento_ar_images.json")

    # Build Magento data by SKU — keep position order
    magento = {}
    for item in en_items:
        sku = item.get("sku", "")
        if not sku:
            continue
        media = sorted(item.get("media_gallery", []), key=lambda m: m.get("position", 0))
        # EN page: keep EN + neutral in their original position order
        en_ordered = []
        for m in media:
            lang = classify(m["url"].split("/")[-1])
            if lang in ("en", "neutral"):
                en_ordered.append(m)
        magento[sku] = {"en_ordered": en_ordered, "ar_ordered": []}

    for item in ar_items:
        sku = item.get("sku", "")
        if not sku or sku not in magento:
            continue
        media = sorted(item.get("media_gallery", []), key=lambda m: m.get("position", 0))
        # AR page: keep AR + neutral in their original position order
        ar_ordered = []
        for m in media:
            lang = classify(m["url"].split("/")[-1])
            if lang in ("ar", "neutral"):
                ar_ordered.append(m)
        magento[sku]["ar_ordered"] = ar_ordered

    # Shopify Files index — use both exact and normalized filenames
    print("Fetching Shopify Files library...")
    all_files = client.get_files()
    files_idx = {}  # normalized_fname → GID (first match wins, no UUID duplicates)
    for f in all_files:
        url = f.get("image", {}).get("url", "") or f.get("url", "")
        if not url:
            continue
        gid = f.get("id", "")
        if not gid:
            continue
        fname = fname_from_url(url)
        # Index by exact filename
        if fname not in files_idx:
            files_idx[fname] = gid
        # Index by normalized filename (strip trailing underscores)
        norm = normalize_fname(fname)
        if norm not in files_idx:
            files_idx[norm] = gid
        # Also index by base name without UUID suffix
        # e.g., tara_pdp_sage_system_en_3_1_79ac65fc-....jpg → tara_pdp_sage_system_en_3_1.jpg
        base = re.sub(r"_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.", ".", fname)
        if base not in files_idx:
            files_idx[base] = gid
    print(f"  {len(files_idx)} filename entries indexed")

    products = client.get_products()
    print(f"  {len(products)} products")
    print("=" * 80)

    fixed = 0
    errors = 0

    for p in products:
        handle = p["handle"]
        sku = next((v["sku"] for v in p.get("variants", []) if v.get("sku")), None)
        if not sku or sku not in magento:
            continue

        mag = magento[sku]
        pgid = f"gid://shopify/Product/{p['id']}"

        # Resolve GIDs in Magento position order with fuzzy filename matching
        def resolve_gid(url):
            fname = fname_from_url(url)
            gid = files_idx.get(fname)
            if gid:
                return gid
            # Try normalized (strip trailing underscores)
            norm = normalize_fname(fname)
            gid = files_idx.get(norm)
            if gid:
                return gid
            return None

        en_gids = []
        en_missing = []
        for m in mag["en_ordered"]:
            gid = resolve_gid(m["url"])
            if gid:
                en_gids.append(gid)
            else:
                en_missing.append(fname_from_url(m["url"]))

        ar_gids = []
        ar_missing = []
        for m in mag["ar_ordered"]:
            gid = resolve_gid(m["url"])
            if gid:
                ar_gids.append(gid)
            else:
                ar_missing.append(fname_from_url(m["url"]))

        # Upload any missing files
        all_missing = []
        for m in mag["en_ordered"] + mag["ar_ordered"]:
            fname = fname_from_url(m["url"])
            if not resolve_gid(m["url"]):
                all_missing.append(m)

        if all_missing:
            for m in all_missing:
                fname = fname_from_url(m["url"])
                norm = normalize_fname(fname)
                src_url = m["url"]
                if not src_url.startswith("http"):
                    src_url = MAGENTO_BASE + src_url
                try:
                    gid = client.upload_file_from_url(src_url, filename=norm, alt=m.get("label", ""))
                    if gid:
                        files_idx[fname] = gid
                        files_idx[norm] = gid
                        lang = classify(fname)
                        if lang in ("en", "neutral"):
                            en_gids.append(gid)
                        elif lang == "ar":
                            ar_gids.append(gid)
                        time.sleep(0.5)
                except Exception as e:
                    print(f"    ERROR uploading {fname}: {e}")
            # Re-resolve in correct order after uploads
            en_gids = [g for g in (resolve_gid(m["url"]) for m in mag["en_ordered"]) if g]
            ar_gids = [g for g in (resolve_gid(m["url"]) for m in mag["ar_ordered"]) if g]

        if not en_gids:
            continue

        # Set metafield
        try:
            result = client.set_metafields([{
                "ownerId": pgid,
                "namespace": "custom",
                "key": "pdp_images",
                "value": json.dumps(en_gids),
                "type": "list.file_reference",
            }])
            mf_gid = result[0]["id"]
        except Exception as e:
            print(f"  ERROR {handle}: metafield: {e}")
            errors += 1
            continue

        # Register AR translation
        if ar_gids:
            try:
                time.sleep(0.3)
                resource = client._graphql(TRANSLATABLE_QUERY, {"rid": mf_gid})
                digest = None
                for tc in resource.get("translatableResource", {}).get("translatableContent", []):
                    if tc["key"] == "value":
                        digest = tc["digest"]
                        break

                if digest:
                    client.register_translations(mf_gid, "ar", [{
                        "locale": "ar",
                        "key": "value",
                        "value": json.dumps(ar_gids),
                        "translatableContentDigest": digest,
                    }])
            except Exception as e:
                print(f"  ERROR {handle}: AR translation: {e}")
                errors += 1

        extras = ""
        if en_missing:
            extras += f" ({len(en_missing)} EN still missing)"
        if ar_missing:
            extras += f" ({len(ar_missing)} AR still missing)"
        print(f"  {handle[:45]:45s} EN:{len(en_gids):2d}  AR:{len(ar_gids):2d}{extras}")
        fixed += 1
        time.sleep(0.2)

    print()
    print("=" * 80)
    print(f"Fixed ordering: {fixed}")
    print(f"Errors: {errors}")


if __name__ == "__main__":
    main()
