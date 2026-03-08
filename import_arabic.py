#!/usr/bin/env python3
"""Step 5: Import Arabic translations into the Saudi Shopify store.

Uses the Shopify Translations API to register Arabic translations for all
resources that were imported in Step 3 (import_english.py). Also updates
metaobject entries with their Arabic field values.

Prerequisites:
  - Step 3 (import_english.py) must have been run first
  - data/id_map.json must exist with source→destination ID mappings
  - data/arabic/ must contain the translated content
  - The Saudi store must have Arabic (ar) enabled as a locale
    (Settings > Languages > Add language > Arabic)
"""

import argparse
import json
import os

from dotenv import load_dotenv

from import_english import sanitize_rich_text_json
from shopify_client import ShopifyClient


ARABIC_LOCALE = "ar"


def load_json(filepath):
    if not os.path.exists(filepath):
        return [] if filepath.endswith(".json") else {}
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def build_translation_inputs(translatable_content, arabic_fields):
    """Match Arabic translations to their translatable content digests.

    Args:
        translatable_content: List from Shopify's translatableContent
            (each has: key, value, digest, locale)
        arabic_fields: Dict of {key: arabic_value} to register
    Returns:
        List of TranslationInput dicts ready for translationsRegister
    """
    translations = []
    for tc in translatable_content:
        key = tc["key"]
        if key in arabic_fields and arabic_fields[key]:
            translations.append({
                "key": key,
                "value": arabic_fields[key],
                "locale": ARABIC_LOCALE,
                "translatableContentDigest": tc["digest"],
            })
    return translations


def main():
    parser = argparse.ArgumentParser(description="Import Arabic translations into Saudi Shopify store")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be translated without making API calls")
    args = parser.parse_args()

    load_dotenv()
    arabic_dir = "data/arabic"
    english_dir = "data/english"
    id_map_file = "data/id_map.json"
    progress_file = "data/arabic_import_progress.json"

    id_map = load_json(id_map_file)
    progress = load_json(progress_file) if os.path.exists(progress_file) else {}

    if args.dry_run:
        print("=== DRY RUN MODE — no API calls will be made ===\n")
        client = None
    else:
        shop_url = os.environ["SAUDI_SHOP_URL"]
        access_token = os.environ["SAUDI_ACCESS_TOKEN"]
        client = ShopifyClient(shop_url, access_token)

    # =============================================
    # Products — register Arabic translations
    # =============================================
    en_products = load_json(os.path.join(english_dir, "products.json"))
    ar_products = load_json(os.path.join(arabic_dir, "products.json"))
    ar_products_by_id = {p["id"]: p for p in ar_products}

    print(f"\nRegistering Arabic translations for {len(en_products)} products...")
    for i, en_product in enumerate(en_products):
        source_id = str(en_product["id"])
        dest_id = id_map.get("products", {}).get(source_id)
        if not dest_id:
            print(f"  [{i+1}/{len(en_products)}] No dest ID for source {source_id}, skipping")
            continue

        if f"product_{dest_id}" in progress:
            print(f"  [{i+1}/{len(en_products)}] Already done: {en_product.get('title', '')[:50]}")
            continue

        ar_product = ar_products_by_id.get(en_product["id"])
        if not ar_product:
            print(f"  [{i+1}/{len(en_products)}] No Arabic translation found, skipping")
            continue

        # Build Arabic field map
        arabic_fields = {
            "title": ar_product.get("title", ""),
            "body_html": ar_product.get("body_html", ""),
            "handle": ar_product.get("handle", ""),
            "product_type": ar_product.get("product_type", ""),
        }

        # Add metafield translations
        if ar_product.get("metafields"):
            for mf in ar_product["metafields"]:
                mf_type = mf.get("type", "")
                if "reference" in mf_type:
                    continue
                key = f"custom.{mf['key']}" if mf.get("namespace") == "custom" else f"{mf.get('namespace', '')}.{mf['key']}"
                value = mf.get("value", "")
                if "rich_text" in mf_type or (isinstance(value, str) and value.strip().startswith('{"type":"root"')):
                    value = sanitize_rich_text_json(value)
                arabic_fields[key] = value

        # Collect Arabic image alt text for later
        ar_image_alts = []
        for img in ar_product.get("images", []):
            alt = img.get("alt", "")
            if alt:
                ar_image_alts.append(alt)

        label = f"  [{i+1}/{len(en_products)}] {en_product.get('title', '')[:50]}"

        if args.dry_run:
            img_note = f" + {len(ar_image_alts)} image alts" if ar_image_alts else ""
            print(f"{label} — would register {len(arabic_fields)} Arabic fields{img_note}")
            continue

        # Get translatable content with digests
        gid = f"gid://shopify/Product/{dest_id}"
        try:
            resource = client.get_translatable_resource(gid)
            if not resource:
                print(f"{label} — could not fetch translatable content")
                continue

            translations = build_translation_inputs(
                resource["translatableContent"], arabic_fields
            )
            if translations:
                client.register_translations(gid, ARABIC_LOCALE, translations)
                msg = f"{label} — registered {len(translations)} translations"
            else:
                msg = f"{label} — no matching translatable fields"

            # Register Arabic image alt text
            if ar_image_alts:
                try:
                    # Get product images from Shopify to get their GIDs
                    img_resp = client._request("GET", f"products/{dest_id}.json",
                                               params={"fields": "id,images"})
                    shopify_images = img_resp.json().get("product", {}).get("images", [])
                    img_translated = 0
                    for idx, shopify_img in enumerate(shopify_images):
                        if idx >= len(ar_image_alts):
                            break
                        img_gid = f"gid://shopify/ProductImage/{shopify_img['id']}"
                        img_resource = client.get_translatable_resource(img_gid)
                        if img_resource and img_resource.get("translatableContent"):
                            img_translations = build_translation_inputs(
                                img_resource["translatableContent"],
                                {"alt": ar_image_alts[idx]}
                            )
                            if img_translations:
                                client.register_translations(img_gid, ARABIC_LOCALE, img_translations)
                                img_translated += 1
                    if img_translated:
                        msg += f" + {img_translated} image alts"
                except Exception as img_err:
                    msg += f" (image alt error: {img_err})"

            print(msg)
            progress[f"product_{dest_id}"] = True
            save_json(progress, progress_file)
        except Exception as e:
            print(f"{label} — error: {e}")

    # =============================================
    # Collections — register Arabic translations
    # =============================================
    en_collections = load_json(os.path.join(english_dir, "collections.json"))
    ar_collections = load_json(os.path.join(arabic_dir, "collections.json"))
    ar_collections_by_id = {c["id"]: c for c in ar_collections}

    print(f"\nRegistering Arabic translations for {len(en_collections)} collections...")
    for i, en_coll in enumerate(en_collections):
        source_id = str(en_coll["id"])
        dest_id = id_map.get("collections", {}).get(source_id)
        if not dest_id:
            continue

        if f"collection_{dest_id}" in progress:
            print(f"  [{i+1}/{len(en_collections)}] Already done: {en_coll.get('title', '')[:50]}")
            continue

        ar_coll = ar_collections_by_id.get(en_coll["id"])
        if not ar_coll:
            continue

        arabic_fields = {
            "title": ar_coll.get("title", ""),
            "body_html": ar_coll.get("body_html", ""),
            "handle": ar_coll.get("handle", ""),
        }

        label = f"  [{i+1}/{len(en_collections)}] {en_coll.get('title', '')[:50]}"

        if args.dry_run:
            print(f"{label} — would register Arabic translations")
            continue

        gid = f"gid://shopify/Collection/{dest_id}"
        try:
            resource = client.get_translatable_resource(gid)
            if not resource:
                print(f"{label} — could not fetch translatable content")
                continue
            translations = build_translation_inputs(resource["translatableContent"], arabic_fields)
            if translations:
                client.register_translations(gid, ARABIC_LOCALE, translations)
                print(f"{label} — registered {len(translations)} translations")
            progress[f"collection_{dest_id}"] = True
            save_json(progress, progress_file)
        except Exception as e:
            print(f"{label} — error: {e}")

    # =============================================
    # Pages — register Arabic translations
    # =============================================
    en_pages = load_json(os.path.join(english_dir, "pages.json"))
    ar_pages = load_json(os.path.join(arabic_dir, "pages.json"))
    ar_pages_by_id = {p["id"]: p for p in ar_pages}

    print(f"\nRegistering Arabic translations for {len(en_pages)} pages...")
    for i, en_page in enumerate(en_pages):
        source_id = str(en_page["id"])
        dest_id = id_map.get("pages", {}).get(source_id)
        if not dest_id:
            continue

        if f"page_{dest_id}" in progress:
            print(f"  [{i+1}/{len(en_pages)}] Already done: {en_page.get('title', '')[:50]}")
            continue

        ar_page = ar_pages_by_id.get(en_page["id"])
        if not ar_page:
            continue

        arabic_fields = {
            "title": ar_page.get("title", ""),
            "body_html": ar_page.get("body_html", ""),
            "handle": ar_page.get("handle", ""),
        }

        label = f"  [{i+1}/{len(en_pages)}] {en_page.get('title', '')[:50]}"

        if args.dry_run:
            print(f"{label} — would register Arabic translations")
            continue

        gid = f"gid://shopify/OnlineStorePage/{dest_id}"
        try:
            resource = client.get_translatable_resource(gid)
            if not resource:
                print(f"{label} — could not fetch translatable content")
                continue
            translations = build_translation_inputs(resource["translatableContent"], arabic_fields)
            if translations:
                client.register_translations(gid, ARABIC_LOCALE, translations)
                print(f"{label} — registered {len(translations)} translations")
            progress[f"page_{dest_id}"] = True
            save_json(progress, progress_file)
        except Exception as e:
            print(f"{label} — error: {e}")

    # =============================================
    # Articles — register Arabic translations
    # =============================================
    en_articles = load_json(os.path.join(english_dir, "articles.json"))
    ar_articles = load_json(os.path.join(arabic_dir, "articles.json"))
    ar_articles_by_id = {a["id"]: a for a in ar_articles}

    print(f"\nRegistering Arabic translations for {len(en_articles)} articles...")
    for i, en_art in enumerate(en_articles):
        source_id = str(en_art["id"])
        dest_id = id_map.get("articles", {}).get(source_id)
        if not dest_id:
            continue

        if f"article_{dest_id}" in progress:
            print(f"  [{i+1}/{len(en_articles)}] Already done: {en_art.get('title', '')[:50]}")
            continue

        ar_art = ar_articles_by_id.get(en_art["id"])
        if not ar_art:
            continue

        arabic_fields = {
            "title": ar_art.get("title", ""),
            "body_html": ar_art.get("body_html", ""),
            "handle": ar_art.get("handle", ""),
            "summary_html": ar_art.get("summary_html", ""),
        }

        # Add article metafield translations
        if ar_art.get("metafields"):
            for mf in ar_art["metafields"]:
                mf_type = mf.get("type", "")
                if "reference" in mf_type:
                    continue
                key = f"custom.{mf['key']}" if mf.get("namespace") == "custom" else f"{mf.get('namespace', '')}.{mf['key']}"
                value = mf.get("value", "")
                if "rich_text" in mf_type or (isinstance(value, str) and value.strip().startswith('{"type":"root"')):
                    value = sanitize_rich_text_json(value)
                arabic_fields[key] = value

        label = f"  [{i+1}/{len(en_articles)}] {en_art.get('title', '')[:50]}"

        if args.dry_run:
            print(f"{label} — would register Arabic translations")
            continue

        gid = f"gid://shopify/OnlineStoreArticle/{dest_id}"
        try:
            resource = client.get_translatable_resource(gid)
            if not resource:
                print(f"{label} — could not fetch translatable content")
                continue
            translations = build_translation_inputs(resource["translatableContent"], arabic_fields)
            if translations:
                client.register_translations(gid, ARABIC_LOCALE, translations)
                print(f"{label} — registered {len(translations)} translations")
            progress[f"article_{dest_id}"] = True
            save_json(progress, progress_file)
        except Exception as e:
            print(f"{label} — error: {e}")

    # =============================================
    # Metaobjects — update with Arabic field values
    # =============================================
    ar_metaobjects_file = os.path.join(arabic_dir, "metaobjects.json")
    en_metaobjects_file = os.path.join(english_dir, "metaobjects.json")
    if os.path.exists(ar_metaobjects_file) and os.path.exists(en_metaobjects_file):
        ar_metaobjects = load_json(ar_metaobjects_file)
        en_metaobjects = load_json(en_metaobjects_file)

        for mo_type, ar_type_data in ar_metaobjects.items():
            ar_objects = ar_type_data.get("objects", [])
            en_objects = en_metaobjects.get(mo_type, {}).get("objects", [])
            en_by_handle = {o.get("handle"): o for o in en_objects}

            print(f"\nRegistering Arabic translations for '{mo_type}' metaobjects ({len(ar_objects)})...")
            for j, ar_obj in enumerate(ar_objects):
                handle = ar_obj.get("handle", "")
                source_id = ar_obj.get("id", "")
                map_key = f"metaobjects_{mo_type}"
                dest_id = id_map.get(map_key, {}).get(source_id)

                if not dest_id:
                    # Try to find by handle in en objects
                    en_obj = en_by_handle.get(handle)
                    if en_obj:
                        dest_id = id_map.get(map_key, {}).get(en_obj.get("id", ""))
                    if not dest_id:
                        print(f"  [{j+1}/{len(ar_objects)}] {handle} — no dest ID, skipping")
                        continue

                if f"metaobject_{dest_id}" in progress:
                    print(f"  [{j+1}/{len(ar_objects)}] {handle} — already done")
                    continue

                label = f"  [{j+1}/{len(ar_objects)}] {handle}"

                # Build Arabic field map from metaobject fields
                arabic_fields = {}
                for field in ar_obj.get("fields", []):
                    if field.get("value"):
                        arabic_fields[field["key"]] = field["value"]

                if args.dry_run:
                    print(f"{label} — would register {len(arabic_fields)} Arabic fields")
                    continue

                try:
                    resource = client.get_translatable_resource(dest_id)
                    if not resource:
                        print(f"{label} — could not fetch translatable content")
                        continue
                    translations = build_translation_inputs(
                        resource["translatableContent"], arabic_fields
                    )
                    if translations:
                        client.register_translations(dest_id, ARABIC_LOCALE, translations)
                        print(f"{label} — registered {len(translations)} translations")
                    else:
                        print(f"{label} — no matching translatable fields")
                    progress[f"metaobject_{dest_id}"] = True
                    save_json(progress, progress_file)
                except Exception as e:
                    print(f"{label} — error: {e}")

    # Summary
    products_done = sum(1 for k in progress if k.startswith("product_"))
    collections_done = sum(1 for k in progress if k.startswith("collection_"))
    pages_done = sum(1 for k in progress if k.startswith("page_"))
    articles_done = sum(1 for k in progress if k.startswith("article_"))
    metaobjects_done = sum(1 for k in progress if k.startswith("metaobject_"))

    print("\n--- Arabic Import Summary ---")
    print(f"  Products:    {products_done}")
    print(f"  Collections: {collections_done}")
    print(f"  Pages:       {pages_done}")
    print(f"  Articles:    {articles_done}")
    print(f"  Metaobjects: {metaobjects_done}")
    if args.dry_run:
        print("  (dry run — nothing was registered)")


if __name__ == "__main__":
    main()
