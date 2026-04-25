#!/usr/bin/env python3
"""Synchronize storefront-facing image/content parity from source to destination.

This script focuses on the parts of a cloned store that can still diverge after
the base migration pipeline:

- theme-level shop_images references (homepage heroes, logos, favicon, quiz art)
- product file_reference metafields (PDP galleries and other file fields)
- article file_reference metafields
- metaobject file_reference fields
- collection images
- rendered storefront page image output (audit report)

It is intentionally source-first: when something differs on the destination, the
fix always comes from the live source store or from a vetted local file fallback.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, save_json


LOCAL_FILE_SEARCH_ROOTS = [
    Path.cwd(),
    Path.cwd() / "temp",
    Path.home() / "Downloads",
]
SHOP_IMAGE_PREFIX = "shopify://shop_images/"


def file_name_from_url(url: str) -> str:
    if not url:
        return ""
    return url.split("/")[-1].split("?")[0]


def parse_file_reference_value(value: str, is_list: bool) -> list[str]:
    if not value:
        return []
    if not is_list:
        return [value]
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, str)]
    return []


def extract_shop_image_refs(node: object, refs: set[str]) -> None:
    if isinstance(node, str):
        if node.startswith(SHOP_IMAGE_PREFIX):
            refs.add(node.replace(SHOP_IMAGE_PREFIX, ""))
        return
    if isinstance(node, dict):
        for value in node.values():
            extract_shop_image_refs(value, refs)
        return
    if isinstance(node, list):
        for value in node:
            extract_shop_image_refs(value, refs)


def normalize_filename(filename: str) -> str:
    name = file_name_from_url(filename).lower()
    stem, dot, ext = name.rpartition(".")
    if not dot:
        stem = name
        ext = ""
    else:
        ext = f".{ext}"
    stem = re.sub(r"_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", "", stem)
    stem = re.sub(r"_[0-9a-f]{32,}$", "", stem)
    return f"{stem}{ext}"


def find_compatible_dest_file(filename: str, dest_files_by_name: dict[str, dict]) -> dict | None:
    existing = dest_files_by_name.get(filename)
    if existing:
        return existing

    normalized = normalize_filename(filename)
    for record in dest_files_by_name.values():
        if normalize_filename(record["name"]) == normalized:
            return record
    return None


def replace_shop_image_refs(node: object, filename_map: dict[str, str]) -> tuple[object, int]:
    if isinstance(node, str):
        if node.startswith(SHOP_IMAGE_PREFIX):
            source_name = node.replace(SHOP_IMAGE_PREFIX, "")
            dest_name = filename_map.get(source_name)
            if dest_name and dest_name != source_name:
                return f"{SHOP_IMAGE_PREFIX}{dest_name}", 1
        return node, 0

    if isinstance(node, dict):
        updated = {}
        changes = 0
        for key, value in node.items():
            new_value, delta = replace_shop_image_refs(value, filename_map)
            updated[key] = new_value
            changes += delta
        return updated, changes

    if isinstance(node, list):
        updated = []
        changes = 0
        for value in node:
            new_value, delta = replace_shop_image_refs(value, filename_map)
            updated.append(new_value)
            changes += delta
        return updated, changes

    return node, 0


def build_file_maps(client: ShopifyClient) -> tuple[dict[str, dict], dict[str, dict]]:
    files = client.get_files()
    by_id: dict[str, dict] = {}
    by_name: dict[str, dict] = {}
    for item in files:
        url = (item.get("image", {}) or {}).get("url", "") or item.get("url", "")
        name = file_name_from_url(url)
        file_id = item.get("id")
        if not name or not file_id:
            continue
        alt = item.get("alt", "") or ""
        record = {
            "id": file_id,
            "name": name,
            "url": url,
            "alt": alt,
        }
        by_id[file_id] = record
        by_name[name] = record
        if alt and alt not in by_name:
            by_name[alt] = record
    return by_id, by_name


def find_local_file(filename: str) -> Path | None:
    for root in LOCAL_FILE_SEARCH_ROOTS:
        if not root.exists():
            continue
        try:
            match = next(root.rglob(filename), None)
        except OSError:
            match = None
        if match:
            return match
    return None


def refresh_dest_file_record(
    dest_client: ShopifyClient,
    dest_files_by_id: dict[str, dict],
    dest_files_by_name: dict[str, dict],
    file_id: str,
) -> dict | None:
    file_info = dest_client.get_file_by_id(file_id)
    if not file_info:
        return None
    url = (file_info.get("image", {}) or {}).get("url", "") or file_info.get("url", "")
    name = file_name_from_url(url)
    if not name:
        return None
    record = {"id": file_id, "name": name, "url": url}
    dest_files_by_id[file_id] = record
    dest_files_by_name[name] = record
    return record


def ensure_destination_file_by_name(
    filename: str,
    source_client: ShopifyClient,
    dest_client: ShopifyClient,
    source_files_by_name: dict[str, dict],
    dest_files_by_id: dict[str, dict],
    dest_files_by_name: dict[str, dict],
    dry_run: bool,
) -> tuple[str | None, str | None, str]:
    existing = find_compatible_dest_file(filename, dest_files_by_name)
    if existing:
        return existing["id"], existing["name"], "reused"

    source_info = source_files_by_name.get(filename)
    if source_info and source_info.get("url"):
        if dry_run:
            return f"dry-run:{filename}", filename, "would_upload_from_source"
        file_id = dest_client.upload_file_from_url(source_info["url"], filename=filename, alt=filename)
        if file_id:
            record = refresh_dest_file_record(dest_client, dest_files_by_id, dest_files_by_name, file_id)
            if record:
                return record["id"], record["name"], "uploaded_from_source"
            return file_id, filename, "uploaded_from_source"

    local_path = find_local_file(filename)
    if local_path:
        if dry_run:
            return f"dry-run:{filename}", filename, f"would_upload_from_local:{local_path}"
        file_id = dest_client.upload_file_bytes(local_path.read_bytes(), local_path.name, alt=local_path.stem)
        if file_id:
            record = refresh_dest_file_record(dest_client, dest_files_by_id, dest_files_by_name, file_id)
            if record:
                return record["id"], record["name"], f"uploaded_from_local:{local_path}"
            return file_id, local_path.name, f"uploaded_from_local:{local_path}"

    return None, None, "missing_source_and_local"


def collect_theme_refs(client: ShopifyClient) -> tuple[int | None, dict[str, dict[str, object]]]:
    theme_id = client.get_main_theme_id()
    refs_by_asset: dict[str, dict[str, object]] = {}
    if not theme_id:
        return theme_id, refs_by_asset
    for asset in client.list_assets(theme_id):
        key = asset.get("key", "")
        if not key.endswith(".json"):
            continue
        raw = client.get_asset(theme_id, key).get("value", "")
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        refs: set[str] = set()
        extract_shop_image_refs(data, refs)
        if refs:
            refs_by_asset[key] = {
                "data": data,
                "refs": sorted(refs),
            }
    return theme_id, refs_by_asset


def sync_theme_refs(
    source_client: ShopifyClient,
    dest_client: ShopifyClient,
    source_files_by_name: dict[str, dict],
    dest_files_by_id: dict[str, dict],
    dest_files_by_name: dict[str, dict],
    dry_run: bool,
) -> dict[str, object]:
    source_theme_id, refs_by_asset = collect_theme_refs(source_client)
    dest_theme_id = dest_client.get_main_theme_id()
    missing_before = []
    uploaded = []
    missing_after = []
    asset_updates = []

    for asset_key, asset_info in refs_by_asset.items():
        refs = asset_info["refs"]
        filename_map: dict[str, str] = {}
        for filename in refs:
            existing = find_compatible_dest_file(filename, dest_files_by_name)
            if existing:
                filename_map[filename] = existing["name"]
                continue

            if filename not in dest_files_by_name:
                missing_before.append(filename)
                file_id, resolved_name, status = ensure_destination_file_by_name(
                    filename,
                    source_client,
                    dest_client,
                    source_files_by_name,
                    dest_files_by_id,
                    dest_files_by_name,
                    dry_run,
                )
                if status.startswith("uploaded") or status.startswith("would_upload"):
                    uploaded.append({
                        "filename": filename,
                        "resolved_name": resolved_name,
                        "status": status,
                        "id": file_id,
                    })
                if file_id is None:
                    missing_after.append(filename)
                elif resolved_name:
                    filename_map[filename] = resolved_name

        if not filename_map:
            continue

        updated_data, replacements = replace_shop_image_refs(asset_info["data"], filename_map)
        if replacements == 0:
            continue

        asset_updates.append({
            "asset": asset_key,
            "replacements": replacements,
            "mapping": {src: dst for src, dst in filename_map.items() if src != dst},
        })

        if not dry_run and dest_theme_id:
            dest_client.put_asset(
                dest_theme_id,
                asset_key,
                value=json.dumps(updated_data, ensure_ascii=False, indent=2),
            )

    return {
        "assets": {key: value["refs"] for key, value in refs_by_asset.items()},
        "missing_before": sorted(set(missing_before)),
        "uploaded": uploaded,
        "missing_after": sorted(set(missing_after)),
        "asset_updates": asset_updates,
        "source_theme_id": source_theme_id,
        "dest_theme_id": dest_theme_id,
    }


def resolve_product_media_map(product: dict) -> dict[str, dict]:
    resolved = {}
    for image in product.get("images", []):
        gid = image.get("admin_graphql_api_id")
        name = file_name_from_url(image.get("src", ""))
        url = image.get("src", "")
        if gid and name:
            resolved[gid] = {"name": name, "url": url}
    return resolved


def resolve_ref_name(
    ref_gid: str,
    file_map_by_id: dict[str, dict],
    media_map: dict[str, dict],
) -> str:
    if ref_gid in media_map:
        return media_map[ref_gid]["name"]
    if ref_gid in file_map_by_id:
        return file_map_by_id[ref_gid]["name"]
    return ""


def ensure_dest_ref_from_source(
    ref_gid: str,
    source_client: ShopifyClient,
    dest_client: ShopifyClient,
    source_files_by_id: dict[str, dict],
    source_files_by_name: dict[str, dict],
    dest_files_by_id: dict[str, dict],
    dest_files_by_name: dict[str, dict],
    source_media_map: dict[str, dict],
    dry_run: bool,
) -> tuple[str | None, str, str]:
    if ref_gid in source_media_map:
        source_name = source_media_map[ref_gid]["name"]
        source_url = source_media_map[ref_gid]["url"]
        existing = dest_files_by_name.get(source_name)
        if existing:
            return existing["id"], source_name, "reused"
        if dry_run:
            return f"dry-run:{source_name}", source_name, "would_upload_from_source_media"
        file_id = dest_client.upload_file_from_url(source_url, filename=source_name, alt=source_name)
        if file_id:
            record = refresh_dest_file_record(dest_client, dest_files_by_id, dest_files_by_name, file_id)
            if record:
                return record["id"], record["name"], "uploaded_from_source_media"
            return file_id, source_name, "uploaded_from_source_media"
        return None, source_name, "upload_failed"

    source_info = source_files_by_id.get(ref_gid)
    if not source_info:
        file_info = source_client.get_file_by_id(ref_gid)
        if file_info:
            url = (file_info.get("image", {}) or {}).get("url", "") or file_info.get("url", "")
            name = file_name_from_url(url)
            if name:
                source_info = {"id": ref_gid, "name": name, "url": url}
                source_files_by_id[ref_gid] = source_info
                source_files_by_name[name] = source_info

    if not source_info:
        return None, "", "missing_source_file"

    file_id, status = ensure_destination_file_by_name(
        source_info["name"],
        source_client,
        dest_client,
        source_files_by_name,
        dest_files_by_id,
        dest_files_by_name,
        dry_run,
    )
    return file_id, source_info["name"], status


def sync_product_file_metafields(
    source_client: ShopifyClient,
    dest_client: ShopifyClient,
    source_files_by_id: dict[str, dict],
    source_files_by_name: dict[str, dict],
    dest_files_by_id: dict[str, dict],
    dest_files_by_name: dict[str, dict],
    dry_run: bool,
) -> dict[str, object]:
    source_products = source_client.get_products()
    dest_products = {product.get("handle", ""): product for product in dest_client.get_products()}
    report = {"updated": [], "skipped": 0, "errors": []}

    for source_product in source_products:
        handle = source_product.get("handle", "")
        dest_product = dest_products.get(handle)
        if not dest_product:
            report["errors"].append(f"missing destination product:{handle}")
            continue

        source_metafields = source_client.get_metafields("products", source_product["id"])
        dest_metafields = {
            f"{mf['namespace']}.{mf['key']}": mf
            for mf in dest_client.get_metafields("products", dest_product["id"])
        }
        source_media_map = resolve_product_media_map(source_product)
        dest_media_map = resolve_product_media_map(dest_product)

        metafields_to_set = []
        changes = []

        for source_mf in source_metafields:
            mf_type = source_mf.get("type", "")
            if "file_reference" not in mf_type:
                continue
            ns_key = f"{source_mf['namespace']}.{source_mf['key']}"
            is_list = mf_type.startswith("list.")
            source_refs = parse_file_reference_value(source_mf.get("value", ""), is_list)
            if not source_refs:
                continue

            desired_refs = []
            desired_names = []
            for ref_gid in source_refs:
                dest_ref, source_name, status = ensure_dest_ref_from_source(
                    ref_gid,
                    source_client,
                    dest_client,
                    source_files_by_id,
                    source_files_by_name,
                    dest_files_by_id,
                    dest_files_by_name,
                    source_media_map,
                    dry_run,
                )
                if not dest_ref:
                    report["errors"].append(f"{handle}:{ns_key}:cannot_resolve:{ref_gid}:{status}")
                    desired_refs = []
                    break
                desired_refs.append(dest_ref)
                desired_names.append(source_name)

            if not desired_refs:
                continue

            dest_mf = dest_metafields.get(ns_key, {})
            current_refs = parse_file_reference_value(dest_mf.get("value", ""), is_list)
            current_names = [resolve_ref_name(ref_gid, dest_files_by_id, dest_media_map) for ref_gid in current_refs]

            if current_names == desired_names:
                report["skipped"] += 1
                continue

            value = json.dumps(desired_refs) if is_list else desired_refs[0]
            metafields_to_set.append({
                "ownerId": f"gid://shopify/Product/{dest_product['id']}",
                "namespace": source_mf["namespace"],
                "key": source_mf["key"],
                "value": value,
                "type": mf_type,
            })
            changes.append({
                "metafield": ns_key,
                "from": current_names,
                "to": desired_names,
            })

        if metafields_to_set:
            if not dry_run:
                dest_client.set_metafields(metafields_to_set)
            report["updated"].append({
                "handle": handle,
                "count": len(metafields_to_set),
                "changes": changes,
            })

    return report


def sync_collection_images(
    source_client: ShopifyClient,
    dest_client: ShopifyClient,
    dry_run: bool,
) -> dict[str, object]:
    source_collections = {collection.get("handle", ""): collection for collection in source_client.get_collections()}
    dest_collections = {collection.get("handle", ""): collection for collection in dest_client.get_collections()}
    updated = []
    errors = []

    for handle, source_collection in source_collections.items():
        dest_collection = dest_collections.get(handle)
        if not dest_collection:
            errors.append(f"missing destination collection:{handle}")
            continue

        source_name = file_name_from_url((source_collection.get("image") or {}).get("src", ""))
        dest_name = file_name_from_url((dest_collection.get("image") or {}).get("src", ""))
        source_url = (source_collection.get("image") or {}).get("src", "")

        if not source_url or source_name == dest_name:
            continue

        if dry_run:
            updated.append({"handle": handle, "from": dest_name, "to": source_name, "status": "would_update"})
            continue

        payload = {"id": int(dest_collection["id"]), "image": {"src": source_url}}
        try:
            dest_client._request("PUT", f"custom_collections/{dest_collection['id']}.json", json={"custom_collection": payload})
        except Exception:
            dest_client._request("PUT", f"smart_collections/{dest_collection['id']}.json", json={"smart_collection": payload})
        updated.append({"handle": handle, "from": dest_name, "to": source_name, "status": "updated"})

    return {"updated": updated, "errors": errors}


def sync_article_file_metafields(
    source_client: ShopifyClient,
    dest_client: ShopifyClient,
    source_files_by_id: dict[str, dict],
    source_files_by_name: dict[str, dict],
    dest_files_by_id: dict[str, dict],
    dest_files_by_name: dict[str, dict],
    dry_run: bool,
) -> dict[str, object]:
    source_blogs = source_client.get_blogs()
    dest_blogs = {blog.get("handle", ""): blog for blog in dest_client.get_blogs()}
    report = {"updated": [], "skipped": 0, "errors": []}

    for source_blog in source_blogs:
        dest_blog = dest_blogs.get(source_blog.get("handle", ""))
        if not dest_blog:
            report["errors"].append(f"missing destination blog:{source_blog.get('handle','')}")
            continue

        source_articles = source_client.get_articles(source_blog["id"])
        dest_articles = {article.get("handle", ""): article for article in dest_client.get_articles(dest_blog["id"])}

        for source_article in source_articles:
            handle = source_article.get("handle", "")
            dest_article = dest_articles.get(handle)
            if not dest_article:
                report["errors"].append(f"missing destination article:{handle}")
                continue

            source_metafields = source_client.get_metafields("articles", source_article["id"])
            dest_metafields = {
                f"{mf['namespace']}.{mf['key']}": mf
                for mf in dest_client.get_metafields("articles", dest_article["id"])
            }

            metafields_to_set = []
            changes = []

            for source_mf in source_metafields:
                mf_type = source_mf.get("type", "")
                if "file_reference" not in mf_type:
                    continue

                ns_key = f"{source_mf['namespace']}.{source_mf['key']}"
                is_list = mf_type.startswith("list.")
                source_refs = parse_file_reference_value(source_mf.get("value", ""), is_list)
                if not source_refs:
                    continue

                desired_refs = []
                desired_names = []
                for ref_gid in source_refs:
                    dest_ref, source_name, status = ensure_dest_ref_from_source(
                        ref_gid,
                        source_client,
                        dest_client,
                        source_files_by_id,
                        source_files_by_name,
                        dest_files_by_id,
                        dest_files_by_name,
                        {},
                        dry_run,
                    )
                    if not dest_ref:
                        report["errors"].append(f"{handle}:{ns_key}:cannot_resolve:{ref_gid}:{status}")
                        desired_refs = []
                        break
                    desired_refs.append(dest_ref)
                    desired_names.append(source_name)

                if not desired_refs:
                    continue

                dest_mf = dest_metafields.get(ns_key, {})
                current_refs = parse_file_reference_value(dest_mf.get("value", ""), is_list)
                current_names = [resolve_ref_name(ref_gid, dest_files_by_id, {}) for ref_gid in current_refs]

                if current_names == desired_names:
                    report["skipped"] += 1
                    continue

                metafields_to_set.append({
                    "ownerId": f"gid://shopify/OnlineStoreArticle/{dest_article['id']}",
                    "namespace": source_mf["namespace"],
                    "key": source_mf["key"],
                    "value": json.dumps(desired_refs) if is_list else desired_refs[0],
                    "type": mf_type,
                })
                changes.append({"metafield": ns_key, "from": current_names, "to": desired_names})

            if metafields_to_set:
                if not dry_run:
                    dest_client.set_metafields(metafields_to_set)
                report["updated"].append({"handle": handle, "count": len(metafields_to_set), "changes": changes})

    return report


def sync_metaobject_file_fields(
    source_client: ShopifyClient,
    dest_client: ShopifyClient,
    source_files_by_id: dict[str, dict],
    source_files_by_name: dict[str, dict],
    dest_files_by_id: dict[str, dict],
    dest_files_by_name: dict[str, dict],
    dry_run: bool,
) -> dict[str, object]:
    source_defs = source_client.get_metaobject_definitions()
    report = {"updated": [], "skipped": 0, "errors": []}

    for definition in source_defs:
        mo_type = definition.get("type", "")
        file_field_types = {}
        for field_def in definition.get("fieldDefinitions", []):
            type_name = (field_def.get("type") or {}).get("name", "")
            if "file_reference" in type_name:
                file_field_types[field_def["key"]] = type_name

        if not file_field_types:
            continue

        source_objects = {obj.get("handle", ""): obj for obj in source_client.get_metaobjects(mo_type)}
        dest_objects = {obj.get("handle", ""): obj for obj in dest_client.get_metaobjects(mo_type)}

        for handle, source_object in source_objects.items():
            dest_object = dest_objects.get(handle)
            if not dest_object:
                report["errors"].append(f"missing destination metaobject:{mo_type}:{handle}")
                continue

            dest_fields = {field["key"]: field for field in dest_object.get("fields", [])}
            updates = []
            changes = []

            for field in source_object.get("fields", []):
                key = field.get("key", "")
                type_name = file_field_types.get(key, field.get("type", ""))
                if "file_reference" not in type_name:
                    continue

                is_list = type_name.startswith("list.")
                source_refs = parse_file_reference_value(field.get("value", ""), is_list)
                if not source_refs:
                    continue

                desired_refs = []
                desired_names = []
                for ref_gid in source_refs:
                    dest_ref, source_name, status = ensure_dest_ref_from_source(
                        ref_gid,
                        source_client,
                        dest_client,
                        source_files_by_id,
                        source_files_by_name,
                        dest_files_by_id,
                        dest_files_by_name,
                        {},
                        dry_run,
                    )
                    if not dest_ref:
                        report["errors"].append(f"{mo_type}:{handle}:{key}:cannot_resolve:{ref_gid}:{status}")
                        desired_refs = []
                        break
                    desired_refs.append(dest_ref)
                    desired_names.append(source_name)

                if not desired_refs:
                    continue

                current_refs = parse_file_reference_value(dest_fields.get(key, {}).get("value", ""), is_list)
                current_names = [resolve_ref_name(ref_gid, dest_files_by_id, {}) for ref_gid in current_refs]

                if current_names == desired_names:
                    report["skipped"] += 1
                    continue

                updates.append({"key": key, "value": json.dumps(desired_refs) if is_list else desired_refs[0]})
                changes.append({"field": key, "from": current_names, "to": desired_names})

            if updates:
                if not dry_run:
                    dest_client.update_metaobject(dest_object["id"], updates)
                report["updated"].append({"type": mo_type, "handle": handle, "count": len(updates), "changes": changes})

    return report


def extract_cdn_basenames(html: str) -> set[str]:
    basenames: set[str] = set()
    patterns = [
        r'<img[^>]+src=["\']([^"\']+)["\']',
        r'srcset=["\']([^"\']+)["\']',
        r'url\(([^)]+)\)',
    ]
    for pattern in patterns:
        for match in re.findall(pattern, html, re.IGNORECASE):
            for part in str(match).split(","):
                candidate = part.strip().split(" ")[0].strip('"\'')
                if "/cdn/shop/" not in candidate:
                    continue
                basenames.add(file_name_from_url(candidate))
    return basenames


def build_storefront_paths(source_client: ShopifyClient) -> list[str]:
    paths = ["/", "/ar"]
    for product in source_client.get_products():
        handle = product.get("handle", "")
        if handle:
            paths.append(f"/products/{handle}")
            paths.append(f"/ar/products/{handle}")
    for collection in source_client.get_collections():
        handle = collection.get("handle", "")
        if handle:
            paths.append(f"/collections/{handle}")
            paths.append(f"/ar/collections/{handle}")
    for page in source_client.get_pages():
        handle = page.get("handle", "")
        if handle:
            paths.append(f"/pages/{handle}")
            paths.append(f"/ar/pages/{handle}")
    for blog in source_client.get_blogs():
        blog_handle = blog.get("handle", "")
        if blog_handle:
            paths.append(f"/blogs/{blog_handle}")
            paths.append(f"/ar/blogs/{blog_handle}")
        for article in source_client.get_articles(blog["id"]):
            article_handle = article.get("handle", "")
            if blog_handle and article_handle:
                paths.append(f"/blogs/{blog_handle}/{article_handle}")
                paths.append(f"/ar/blogs/{blog_handle}/{article_handle}")
    return sorted(set(paths))


def crawl_storefront_parity(source_client: ShopifyClient, dest_client: ShopifyClient) -> dict[str, object]:
    source_base = source_client._graphql("query { shop { primaryDomain { url } } }")["shop"]["primaryDomain"]["url"].rstrip("/")
    dest_base = dest_client._graphql("query { shop { primaryDomain { url } } }")["shop"]["primaryDomain"]["url"].rstrip("/")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    checked = []
    mismatched = []
    http_errors = []

    for path in build_storefront_paths(source_client):
        source_url = f"{source_base}{path}"
        dest_url = f"{dest_base}{path}"
        try:
            source_resp = session.get(source_url, timeout=20)
            dest_resp = session.get(dest_url, timeout=20)
        except requests.RequestException as exc:
            http_errors.append({"path": path, "error": str(exc)})
            continue

        checked.append({"path": path, "source_status": source_resp.status_code, "dest_status": dest_resp.status_code})
        if source_resp.status_code != 200 or dest_resp.status_code != 200:
            http_errors.append({"path": path, "source_status": source_resp.status_code, "dest_status": dest_resp.status_code})
            continue

        source_images = extract_cdn_basenames(source_resp.text)
        dest_images = extract_cdn_basenames(dest_resp.text)
        missing_in_dest = sorted(source_images - dest_images)
        if missing_in_dest:
            mismatched.append({
                "path": path,
                "missing_in_dest": missing_in_dest,
                "extra_in_dest": sorted(dest_images - source_images),
            })

    return {
        "source_base": source_base,
        "dest_base": dest_base,
        "checked": checked,
        "http_errors": http_errors,
        "mismatched_pages": mismatched,
        "summary": {
            "pages_checked": len(checked),
            "http_errors": len(http_errors),
            "mismatched_pages": len(mismatched),
        },
    }


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Sync storefront-facing image parity from source to destination")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing to Shopify")
    args = parser.parse_args()

    source_client = ShopifyClient(config.get_source_shop_url(), config.get_source_access_token())
    dest_client = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())

    source_files_by_id, source_files_by_name = build_file_maps(source_client)
    dest_files_by_id, dest_files_by_name = build_file_maps(dest_client)

    report = {
        "theme_refs": sync_theme_refs(
            source_client,
            dest_client,
            source_files_by_name,
            dest_files_by_id,
            dest_files_by_name,
            args.dry_run,
        ),
        "collection_images": sync_collection_images(source_client, dest_client, args.dry_run),
        "product_file_metafields": sync_product_file_metafields(
            source_client,
            dest_client,
            source_files_by_id,
            source_files_by_name,
            dest_files_by_id,
            dest_files_by_name,
            args.dry_run,
        ),
        "article_file_metafields": sync_article_file_metafields(
            source_client,
            dest_client,
            source_files_by_id,
            source_files_by_name,
            dest_files_by_id,
            dest_files_by_name,
            args.dry_run,
        ),
        "metaobject_file_fields": sync_metaobject_file_fields(
            source_client,
            dest_client,
            source_files_by_id,
            source_files_by_name,
            dest_files_by_id,
            dest_files_by_name,
            args.dry_run,
        ),
    }

    if not args.dry_run:
        time.sleep(3)

    report["storefront_audit"] = crawl_storefront_parity(source_client, dest_client)
    report["dry_run"] = args.dry_run

    save_json(report, config.get_progress_file("storefront_parity_report.json"))

    summary = report["storefront_audit"]["summary"]
    print("=" * 60)
    print("STOREFRONT PARITY SYNC COMPLETE")
    print("=" * 60)
    print(f"Dry run: {args.dry_run}")
    print(f"Theme files missing before: {len(report['theme_refs']['missing_before'])}")
    print(f"Theme files still missing: {len(report['theme_refs']['missing_after'])}")
    print(f"Product file metafields updated: {len(report['product_file_metafields']['updated'])}")
    print(f"Article file metafields updated: {len(report['article_file_metafields']['updated'])}")
    print(f"Metaobject file fields updated: {len(report['metaobject_file_fields']['updated'])}")
    print(f"Storefront pages checked: {summary['pages_checked']}")
    print(f"Storefront mismatched pages: {summary['mismatched_pages']}")
    print(f"Storefront HTTP errors: {summary['http_errors']}")
    print(f"Report: {config.get_progress_file('storefront_parity_report.json')}")


if __name__ == "__main__":
    main()
