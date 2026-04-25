#!/usr/bin/env python3
"""Sync live Arabic translations from the source Shopify store to destination.

This fills gaps that local export JSONs can miss, especially translated
metafields and metaobject fields. It is destination-scoped and reusable for
Saudi -> Kuwait/UAE/Qatar/Bahrain/Oman clones.
"""

from __future__ import annotations

from dataclasses import dataclass

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, load_json, sanitize_rich_text_json, save_json
from tara_migrate.core.graphql_queries import fetch_translatable_resources, upload_translations


ARABIC_LOCALE = "ar"
TRANSLATABLE_METAFIELD_TYPES = {
    "single_line_text_field",
    "multi_line_text_field",
    "rich_text_field",
}

RESOURCE_MAPPINGS = {
    "products": "gid://shopify/Product/",
    "collections": "gid://shopify/Collection/",
    "pages": "gid://shopify/Page/",
    "articles": "gid://shopify/Article/",
    "blogs": "gid://shopify/Blog/",
}


@dataclass
class SyncStats:
    synced: int = 0
    skipped: int = 0
    errors: int = 0

    def to_dict(self):
        return {
            "synced": self.synced,
            "skipped": self.skipped,
            "errors": self.errors,
        }


def _normalize_translation_value(value: str) -> str:
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if stripped.startswith('{"type":"root"'):
        return sanitize_rich_text_json(value)
    return value


def _build_translation_inputs(source_entry, dest_entry):
    inputs = []
    for key, source_translation in source_entry.get("translations", {}).items():
        if key == "handle":
            continue
        value = source_translation.get("value")
        if not value:
            continue
        dest_content = dest_entry.get("content", {}).get(key)
        if not dest_content:
            continue
        dest_translation = dest_entry.get("translations", {}).get(key, {})
        if dest_translation.get("value") == value and not dest_translation.get("outdated"):
            continue
        inputs.append({
            "locale": ARABIC_LOCALE,
            "key": key,
            "value": _normalize_translation_value(value),
            "translatableContentDigest": dest_content["digest"],
        })
    return inputs


def _sync_gid_pairs(source_client, dest_client, pairs, report_bucket, label):
    stats = SyncStats()
    if not pairs:
        return stats

    source_gids = [source_gid for source_gid, _, _ in pairs]
    dest_gids = [dest_gid for _, dest_gid, _ in pairs]
    source_map = fetch_translatable_resources(source_client, source_gids, ARABIC_LOCALE)
    dest_map = fetch_translatable_resources(dest_client, dest_gids, ARABIC_LOCALE)

    for source_gid, dest_gid, meta in pairs:
        source_entry = source_map.get(source_gid)
        dest_entry = dest_map.get(dest_gid)
        if not source_entry or not dest_entry:
            report_bucket.append({
                "label": label,
                "meta": meta,
                "issue": "missing_translatable_resource",
            })
            stats.errors += 1
            continue

        inputs = _build_translation_inputs(source_entry, dest_entry)
        if not inputs:
            stats.skipped += 1
            continue

        uploaded, errors = upload_translations(dest_client, dest_gid, inputs)
        if uploaded:
            stats.synced += uploaded
        if errors:
            stats.errors += errors
            report_bucket.append({
                "label": label,
                "meta": meta,
                "issue": "upload_errors",
                "count": errors,
            })

    return stats


def _metaobject_gid_pairs(id_map):
    pairs = []
    for map_key, mapping in id_map.items():
        if not map_key.startswith("metaobjects_") or not isinstance(mapping, dict):
            continue
        for source_gid, dest_gid in mapping.items():
            pairs.append((source_gid, dest_gid, {"type": map_key.replace("metaobjects_", "")}))
    return pairs


def _resource_gid_pairs(id_map):
    pairs_by_type = {}
    for map_key, prefix in RESOURCE_MAPPINGS.items():
        pairs = []
        for source_id, dest_id in id_map.get(map_key, {}).items():
            pairs.append((
                f"{prefix}{source_id}",
                f"{prefix}{dest_id}",
                {"resource_type": map_key, "source_id": source_id, "dest_id": dest_id},
            ))
        pairs_by_type[map_key] = pairs
    return pairs_by_type


def _theme_gid_pairs(source_client, dest_client):
    source_theme_id = source_client.get_main_theme_id()
    dest_theme_id = dest_client.get_main_theme_id()
    if not source_theme_id or not dest_theme_id:
        return []
    return [(
        f"gid://shopify/OnlineStoreTheme/{source_theme_id}",
        f"gid://shopify/OnlineStoreTheme/{dest_theme_id}",
        {"resource_type": "theme", "source_theme_id": source_theme_id, "dest_theme_id": dest_theme_id},
    )]


def _collect_text_metafield_pairs(source_client, dest_client, owner_resource, source_id, dest_id):
    source_metafields = {
        f"{mf['namespace']}.{mf['key']}": mf
        for mf in source_client.get_metafields(owner_resource, source_id)
        if mf.get("value") not in (None, "")
    }
    dest_metafields = {
        f"{mf['namespace']}.{mf['key']}": mf
        for mf in dest_client.get_metafields(owner_resource, dest_id)
        if mf.get("value") is not None
    }

    pairs = []
    for ns_key, source_mf in source_metafields.items():
        mf_type = source_mf.get("type", "")
        if "reference" in mf_type:
            continue
        if mf_type not in TRANSLATABLE_METAFIELD_TYPES:
            continue
        if source_mf.get("namespace") == "global":
            continue

        dest_mf = dest_metafields.get(ns_key)
        if not dest_mf:
            continue

        source_gid = source_mf.get("admin_graphql_api_id") or f"gid://shopify/Metafield/{source_mf['id']}"
        dest_gid = dest_mf.get("admin_graphql_api_id") or f"gid://shopify/Metafield/{dest_mf['id']}"
        pairs.append((source_gid, dest_gid, {"owner": owner_resource, "key": ns_key, "type": mf_type}))

    return pairs


def _sync_resource_metafields(source_client, dest_client, id_map, owner_resource, report_bucket):
    stats = SyncStats()
    map_key = "products" if owner_resource == "products" else "articles"
    all_pairs = []
    for source_id, dest_id in id_map.get(map_key, {}).items():
        all_pairs.extend(_collect_text_metafield_pairs(source_client, dest_client, owner_resource, source_id, dest_id))

    if not all_pairs:
        return stats

    source_map = fetch_translatable_resources(source_client, [p[0] for p in all_pairs], ARABIC_LOCALE)
    dest_map = fetch_translatable_resources(dest_client, [p[1] for p in all_pairs], ARABIC_LOCALE)

    for source_gid, dest_gid, meta in all_pairs:
        source_entry = source_map.get(source_gid)
        dest_entry = dest_map.get(dest_gid)
        if not source_entry or not dest_entry:
            report_bucket.append({
                "label": f"{owner_resource}_metafield",
                "meta": meta,
                "issue": "missing_translatable_resource",
            })
            stats.errors += 1
            continue

        source_translation = source_entry.get("translations", {}).get("value", {}).get("value")
        if not source_translation:
            stats.skipped += 1
            continue

        dest_content = dest_entry.get("content", {}).get("value")
        if not dest_content:
            stats.skipped += 1
            continue

        dest_existing = dest_entry.get("translations", {}).get("value", {})
        normalized_source = _normalize_translation_value(source_translation)
        normalized_dest = _normalize_translation_value(dest_existing.get("value", ""))
        if normalized_dest == normalized_source and not dest_existing.get("outdated"):
            stats.skipped += 1
            continue

        uploaded, errors = upload_translations(dest_client, dest_gid, [{
            "locale": ARABIC_LOCALE,
            "key": "value",
            "value": normalized_source,
            "translatableContentDigest": dest_content["digest"],
        }])
        if uploaded:
            stats.synced += uploaded
        if errors:
            stats.errors += errors
            report_bucket.append({
                "label": f"{owner_resource}_metafield",
                "meta": meta,
                "issue": "upload_errors",
                "count": errors,
            })

    return stats


def sync_live_source_arabic_translations(
    source_client: ShopifyClient | None = None,
    dest_client: ShopifyClient | None = None,
    *,
    dry_run: bool = False,
):
    """Copy live Arabic translations from source to destination."""
    load_dotenv()
    if dry_run:
        return {
            "dry_run": True,
            "resources": {},
            "theme": {},
            "metaobjects": {},
            "product_metafields": {},
            "article_metafields": {},
            "errors": [],
        }

    source_client = source_client or ShopifyClient(config.get_source_shop_url(), config.get_source_access_token())
    dest_client = dest_client or ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())
    id_map = load_json(config.get_id_map_file(), default={})

    report = {
        "dry_run": False,
        "resources": {},
        "theme": {},
        "metaobjects": {},
        "product_metafields": {},
        "article_metafields": {},
        "errors": [],
    }

    for map_key, pairs in _resource_gid_pairs(id_map).items():
        stats = _sync_gid_pairs(source_client, dest_client, pairs, report["errors"], map_key)
        report["resources"][map_key] = stats.to_dict()

    report["theme"] = _sync_gid_pairs(
        source_client,
        dest_client,
        _theme_gid_pairs(source_client, dest_client),
        report["errors"],
        "theme",
    ).to_dict()

    metaobject_stats = _sync_gid_pairs(
        source_client,
        dest_client,
        _metaobject_gid_pairs(id_map),
        report["errors"],
        "metaobjects",
    )
    report["metaobjects"] = metaobject_stats.to_dict()

    report["product_metafields"] = _sync_resource_metafields(
        source_client, dest_client, id_map, "products", report["errors"]
    ).to_dict()
    report["article_metafields"] = _sync_resource_metafields(
        source_client, dest_client, id_map, "articles", report["errors"]
    ).to_dict()

    save_json(report, config.get_progress_file("live_source_translation_sync.json"))
    return report
