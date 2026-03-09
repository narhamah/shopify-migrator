#!/usr/bin/env python3
"""Migrate assets (images, files) from Spain store to Saudi store.

Handles:
  - Metaobject file_reference fields (avatar, image, icon, science_images)
  - Article metafield file_reference fields (listing_image, hero_image)
  - WebP optimization: all images are converted to WebP for optimal size

Product images, collection images, and article featured images are already
handled by import_english.py via src URL passthrough.

Run this AFTER import_english.py to populate file reference fields.
"""

import json
import os
import time

from dotenv import load_dotenv

from optimize_images import download_and_optimize
from shopify_client import ShopifyClient


def load_json(filepath):
    if not os.path.exists(filepath):
        return {} if not filepath.endswith(".json") else {}
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# Metaobject fields that are file references
METAOBJECT_FILE_FIELDS = {
    "blog_author": ["avatar"],
    "ingredient": ["image", "icon", "science_images"],
}

# Article metafield file references
ARTICLE_FILE_METAFIELDS = {
    "custom.listing_image",
    "custom.hero_image",
}


def extract_file_url_from_gid(source_client, file_gid):
    """Get the public URL for a file GID from the source store."""
    if not file_gid or not file_gid.startswith("gid://"):
        return None
    try:
        node = source_client.get_file_by_id(file_gid)
        if not node:
            return None
        # MediaImage has image.url, GenericFile has url
        if node.get("image", {}).get("url"):
            return node["image"]["url"]
        if node.get("url"):
            return node["url"]
    except Exception as e:
        print(f"    Could not fetch file URL for {file_gid}: {e}")
    return None


def upload_optimized(dest_client, url, alt="", preset=None):
    """Download image, convert to WebP, upload to destination store.

    Args:
        dest_client: ShopifyClient for destination store.
        url: Source image URL.
        alt: Alt text for the uploaded file.
        preset: Optimization preset (icon, thumbnail, product, hero, etc.).

    Returns the Shopify file GID on success, None on failure.
    """
    optimized_bytes, new_filename, mime_type = download_and_optimize(url, preset=preset)
    return dest_client.upload_file_bytes(optimized_bytes, new_filename, alt=alt)


def main():
    load_dotenv()

    spain_url = os.environ["SPAIN_SHOP_URL"]
    spain_token = os.environ["SPAIN_ACCESS_TOKEN"]
    saudi_url = os.environ["SAUDI_SHOP_URL"]
    saudi_token = os.environ["SAUDI_ACCESS_TOKEN"]

    source_client = ShopifyClient(spain_url, spain_token)
    dest_client = ShopifyClient(saudi_url, saudi_token)

    input_dir = "data/english"
    id_map_file = "data/id_map.json"
    file_map_file = "data/file_map.json"

    id_map = load_json(id_map_file)
    file_map = load_json(file_map_file) if os.path.exists(file_map_file) else {}

    # =============================================
    # Phase 1: Metaobject file references
    # =============================================
    metaobjects_file = os.path.join(input_dir, "metaobjects.json")
    if os.path.exists(metaobjects_file):
        all_metaobjects = load_json(metaobjects_file)

        for mo_type, file_fields in METAOBJECT_FILE_FIELDS.items():
            type_data = all_metaobjects.get(mo_type, {})
            objects = type_data.get("objects", [])
            if not objects:
                continue

            mo_map_key = f"metaobjects_{mo_type}"
            print(f"\n=== Migrating files for '{mo_type}' metaobjects ({len(objects)} objects) ===")

            for j, obj in enumerate(objects):
                handle = obj.get("handle", "")
                source_id = obj.get("id", "")
                dest_id = id_map.get(mo_map_key, {}).get(source_id)

                if not dest_id:
                    print(f"  [{j+1}/{len(objects)}] {handle} — no dest ID, skipping")
                    continue

                # Check for scraped image URL (from Magento, not Spain)
                scraped_image_url = obj.get("_scraped_image_url")

                fields_to_update = []
                for field in obj.get("fields", []):
                    field_key = field.get("key", "")
                    field_type = field.get("type", "")
                    field_value = field.get("value", "")

                    if field_key not in file_fields or not field_value:
                        # If field is empty but we have a scraped URL for "image", use it
                        if field_key == "image" and scraped_image_url and not field_value:
                            field_value = scraped_image_url  # Will be handled as URL below
                        elif not field_value:
                            continue

                    is_list = "list." in field_type

                    # Use scraped URL directly for "image" field if available
                    if field_key == "image" and scraped_image_url:
                        cache_key = f"url:{scraped_image_url}"
                        if cache_key in file_map:
                            dest_file_id = file_map[cache_key]
                        else:
                            try:
                                dest_file_id = upload_optimized(dest_client, scraped_image_url,
                                                                alt=f"{handle}_image",
                                                                preset="thumbnail")
                                if dest_file_id:
                                    file_map[cache_key] = dest_file_id
                                    print(f"    {handle}.image: uploaded from Magento → {dest_file_id}")
                                    save_json(file_map, file_map_file)
                                    time.sleep(0.5)
                                else:
                                    print(f"    {handle}.image: upload failed for {scraped_image_url}")
                                    continue
                            except Exception as e:
                                print(f"    {handle}.image: upload error: {e}")
                                continue
                        fields_to_update.append({
                            "key": "image",
                            "value": dest_file_id,
                        })
                        continue

                    if is_list:
                        # List of file references (e.g., science_images)
                        try:
                            source_gids = json.loads(field_value)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        dest_gids = []
                        for gid in source_gids:
                            if gid in file_map:
                                dest_gids.append(file_map[gid])
                                continue

                            url = extract_file_url_from_gid(source_client, gid)
                            if not url:
                                print(f"    {handle}.{field_key}: could not get URL for {gid}")
                                continue

                            # Pick preset based on field key
                            field_preset = "icon" if "icon" in field_key else "thumbnail"
                            try:
                                dest_file_id = upload_optimized(dest_client, url,
                                                                alt=f"{handle}_{field_key}",
                                                                preset=field_preset)
                                if dest_file_id:
                                    file_map[gid] = dest_file_id
                                    dest_gids.append(dest_file_id)
                                    print(f"    {handle}.{field_key}: uploaded → {dest_file_id}")
                                    save_json(file_map, file_map_file)
                                    time.sleep(0.5)  # Be gentle with rate limits
                            except Exception as e:
                                print(f"    {handle}.{field_key}: upload error: {e}")

                        if dest_gids:
                            fields_to_update.append({
                                "key": field_key,
                                "value": json.dumps(dest_gids),
                            })
                    else:
                        # Single file reference — value is a GID or a URL
                        source_gid = field_value
                        is_url = field_value.startswith("http")
                        cache_key = f"url:{field_value}" if is_url else source_gid

                        if cache_key in file_map:
                            dest_file_id = file_map[cache_key]
                        else:
                            if is_url:
                                url = field_value
                            else:
                                url = extract_file_url_from_gid(source_client, source_gid)
                            if not url:
                                print(f"    {handle}.{field_key}: could not get URL for {source_gid}")
                                continue

                            # Pick preset based on field key
                            field_preset = "icon" if "icon" in field_key else ("thumbnail" if "avatar" in field_key else "default")
                            try:
                                dest_file_id = upload_optimized(dest_client, url,
                                                                alt=f"{handle}_{field_key}",
                                                                preset=field_preset)
                                if dest_file_id:
                                    file_map[cache_key] = dest_file_id
                                    src_label = "URL" if is_url else "Spain"
                                    print(f"    {handle}.{field_key}: uploaded from {src_label} → {dest_file_id}")
                                    save_json(file_map, file_map_file)
                                    time.sleep(0.5)
                                else:
                                    continue
                            except Exception as e:
                                print(f"    {handle}.{field_key}: upload error: {e}")
                                continue

                        fields_to_update.append({
                            "key": field_key,
                            "value": dest_file_id,
                        })

                if fields_to_update:
                    try:
                        dest_client.update_metaobject(dest_id, fields_to_update)
                        print(f"  [{j+1}/{len(objects)}] {handle} — updated {len(fields_to_update)} file fields")
                    except Exception as e:
                        print(f"  [{j+1}/{len(objects)}] {handle} — update error: {e}")
                else:
                    print(f"  [{j+1}/{len(objects)}] {handle} — no file fields to update")

    # =============================================
    # Phase 2: Article metafield file references
    # =============================================
    articles_file = os.path.join(input_dir, "articles.json")
    if os.path.exists(articles_file):
        articles = load_json(articles_file)
        if not isinstance(articles, list):
            articles = []
        article_map = id_map.get("articles", {})

        print(f"\n=== Migrating files for articles ({len(articles)} articles) ===")

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
                    url = extract_file_url_from_gid(source_client, source_gid)
                    if not url:
                        print(f"  Article '{article.get('title', '')[:40]}': could not get URL for {ns_key}")
                        continue

                    # Article images are hero/listing images
                    article_preset = "hero" if "hero" in mf["key"] else "thumbnail"
                    try:
                        dest_file_id = upload_optimized(
                            dest_client, url,
                            alt=f"article_{article.get('handle', '')}_{mf['key']}",
                            preset=article_preset,
                        )
                        if dest_file_id:
                            file_map[source_gid] = dest_file_id
                            print(f"  Article '{article.get('title', '')[:40]}': {ns_key} uploaded → {dest_file_id}")
                            save_json(file_map, file_map_file)
                            time.sleep(0.5)
                        else:
                            continue
                    except Exception as e:
                        print(f"  Article '{article.get('title', '')[:40]}': {ns_key} upload error: {e}")
                        continue

                metafields_to_set.append({
                    "ownerId": f"gid://shopify/OnlineStoreArticle/{dest_id}",
                    "namespace": mf["namespace"],
                    "key": mf["key"],
                    "value": dest_file_id,
                    "type": mf_type,
                })

            if metafields_to_set:
                try:
                    dest_client.set_metafields(metafields_to_set)
                    print(f"  [{i+1}/{len(articles)}] '{article.get('title', '')[:40]}' — set {len(metafields_to_set)} file metafields")
                except Exception as e:
                    print(f"  [{i+1}/{len(articles)}] '{article.get('title', '')[:40]}' — error: {e}")

    save_json(file_map, file_map_file)

    print("\n--- Asset Migration Summary ---")
    print(f"  Total files uploaded: {len(file_map)}")
    print(f"  File map saved to: {file_map_file}")


if __name__ == "__main__":
    main()
