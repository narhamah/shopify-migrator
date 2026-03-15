#!/usr/bin/env python3
"""Purge all Arabic translations from the Saudi store and retranslate from scratch.

This script:
  1. Fetches ALL Arabic translations across all resource types
  2. Removes them via Shopify's translationsRemove GraphQL mutation
  3. Restores Arabic content from Magento scrape (data/arabic/) where available
  4. AI-translates only the gaps (fields not in the scrape) using TARA tone of voice
  5. Uploads translations via translationsRegister

Usage:
    python purge_arabic.py --purge-only                    # Purge only, no retranslation
    python purge_arabic.py --purge-only --dry-run           # Preview what would be purged
    python purge_arabic.py --model gpt-5-mini               # Purge + retranslate (scrape first, AI for gaps)
    python purge_arabic.py --model gpt-5-mini --dry-run     # Preview full pipeline
    python purge_arabic.py --skip-purge                     # Retranslate only (assumes already purged)
    python purge_arabic.py --type PRODUCT,COLLECTION        # Only specific resource types
    python purge_arabic.py --skip-theme                     # Skip ONLINE_STORE_THEME (4000+ keys)
    python purge_arabic.py --skip-scraped                   # Skip Magento scrape, AI-translate everything
"""

import argparse
import json
import os
import sys
import time

from dotenv import load_dotenv

from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.core import config, load_json
from tara_migrate.core.config import AR_DIR
from tara_migrate.core.graphql_queries import (
    TRANSLATABLE_RESOURCES_QUERY,
    fetch_translatable_resources,
    upload_translations,
)
from tara_migrate.core.shopify_fields import TRANSLATABLE_RESOURCE_TYPES, is_skippable_field
from tara_migrate.translation.engine import TranslationEngine, load_developer_prompt
from tara_migrate.core.rich_text import is_rich_text_json


LOCALE = "ar"

REMOVE_TRANSLATIONS_MUTATION = """
mutation translationsRemove($resourceId: ID!, $translationKeys: [String!]!, $locales: [String!]!) {
  translationsRemove(resourceId: $resourceId, translationKeys: $translationKeys, locales: $locales) {
    userErrors {
      message
      field
    }
  }
}
"""


def build_scraped_lookup(id_map_path="data/id_map.json"):
    """Build dest_gid → field_key → arabic_value lookup from Magento scrape.

    Loads scraped Arabic content from data/arabic/ and maps it to destination
    Shopify GIDs using id_map.json. Returns a dict keyed by destination GID
    containing field-level Arabic content.
    """
    id_map = load_json(id_map_path, {})
    lookup = {}  # dest_gid -> {field_key: arabic_value}

    # ── Products ──
    products = load_json(os.path.join(AR_DIR, "products.json"), [])
    product_map = id_map.get("products", {})
    for p in products:
        src_id = str(p.get("id", ""))
        dest_id = product_map.get(src_id)
        if not dest_id:
            continue
        dest_gid = f"gid://shopify/Product/{dest_id}"
        fields = {}
        if p.get("title"):
            fields["title"] = p["title"]
        if p.get("body_html"):
            fields["body_html"] = p["body_html"]
        if p.get("meta_title"):
            fields["meta_title"] = p["meta_title"]
        if p.get("meta_description"):
            fields["meta_description"] = p["meta_description"]
        # Metafields (namespace.key is the translation key)
        for mf in p.get("metafields", []):
            ns = mf.get("namespace", "")
            key = mf.get("key", "")
            val = mf.get("value")
            mf_type = mf.get("type", "")
            # Only use text metafields, skip references/numbers/etc.
            if val and ns and key and mf_type in (
                "single_line_text_field", "multi_line_text_field",
                "rich_text_field",
            ):
                fields[f"{ns}.{key}"] = val
        if fields:
            lookup[dest_gid] = fields

    # ── Collections ──
    collections = load_json(os.path.join(AR_DIR, "collections.json"), [])
    collection_map = id_map.get("collections", {})
    for c in collections:
        src_id = str(c.get("id", ""))
        dest_id = collection_map.get(src_id)
        if not dest_id:
            continue
        dest_gid = f"gid://shopify/Collection/{dest_id}"
        fields = {}
        if c.get("title"):
            fields["title"] = c["title"]
        if c.get("body_html"):
            fields["body_html"] = c["body_html"]
        if fields:
            lookup[dest_gid] = fields

    # ── Pages ──
    pages = load_json(os.path.join(AR_DIR, "pages.json"), [])
    page_map = id_map.get("pages", {})
    for pg in pages:
        src_id = str(pg.get("id", ""))
        dest_id = page_map.get(src_id)
        if not dest_id:
            continue
        dest_gid = f"gid://shopify/OnlineStorePage/{dest_id}"
        fields = {}
        if pg.get("title"):
            fields["title"] = pg["title"]
        if pg.get("body_html"):
            fields["body_html"] = pg["body_html"]
        if fields:
            lookup[dest_gid] = fields

    # ── Articles ──
    articles = load_json(os.path.join(AR_DIR, "articles.json"), [])
    article_map = id_map.get("articles", {})
    for a in articles:
        src_id = str(a.get("id", ""))
        dest_id = article_map.get(src_id)
        if not dest_id:
            continue
        dest_gid = f"gid://shopify/OnlineStoreArticle/{dest_id}"
        fields = {}
        if a.get("title"):
            fields["title"] = a["title"]
        if a.get("body_html"):
            fields["body_html"] = a["body_html"]
        if fields:
            lookup[dest_gid] = fields

    # ── Metaobjects ──
    metaobjects = load_json(os.path.join(AR_DIR, "metaobjects.json"), {})
    for mo_type, type_data in metaobjects.items():
        map_key = f"metaobjects_{mo_type}"
        mo_map = id_map.get(map_key, {})
        for obj in type_data.get("objects", []):
            src_gid = obj.get("id", "")
            dest_gid = mo_map.get(src_gid)
            if not dest_gid:
                continue
            fields = {}
            for f in obj.get("fields", []):
                val = f.get("value")
                if val and val != "None":
                    fields[f["key"]] = val
            if fields:
                lookup[dest_gid] = fields

    return lookup


def fetch_all_translations(client, resource_types, locale=LOCALE):
    """Fetch all translatable fields with their Arabic translations.

    Returns list of dicts: [{resource_id, resource_type, key, english, arabic, digest}, ...]
    """
    query = TRANSLATABLE_RESOURCES_QUERY.replace("%LOCALE%", locale)
    all_fields = []

    for rtype in resource_types:
        count = 0
        field_count = 0
        has_translation = 0
        cursor = None

        while True:
            try:
                data = client._graphql(query, {
                    "resourceType": rtype,
                    "first": 50,
                    "after": cursor,
                })
            except Exception as e:
                print(f"  ERROR fetching {rtype}: {e}")
                break

            container = data.get("translatableResources", {})
            edges = container.get("edges", [])
            page_info = container.get("pageInfo", {})

            for edge in edges:
                node = edge["node"]
                rid = node["resourceId"]
                translations = {t["key"]: t for t in node.get("translations", [])}
                count += 1

                for field in node.get("translatableContent", []):
                    key = field["key"]
                    english = field.get("value") or ""
                    trans = translations.get(key)
                    arabic = trans["value"] if trans else None

                    all_fields.append({
                        "resource_id": rid,
                        "resource_type": rtype,
                        "key": key,
                        "english": english,
                        "arabic": arabic,
                        "digest": field.get("digest", ""),
                    })
                    field_count += 1
                    if arabic:
                        has_translation += 1

            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            time.sleep(0.3)

        print(f"  {rtype}: {count} resources, {field_count} fields, "
              f"{has_translation} translated")

    return all_fields


def purge_translations(client, fields, dry_run=False, locale=LOCALE):
    """Remove all Arabic translations.

    Groups by resource_id and sends batched translationsRemove mutations.
    """
    # Group translated fields by resource_id
    by_resource = {}
    for f in fields:
        if f["arabic"]:
            rid = f["resource_id"]
            if rid not in by_resource:
                by_resource[rid] = []
            by_resource[rid].append(f["key"])

    total_to_remove = sum(len(keys) for keys in by_resource.values())
    print(f"\n  {total_to_remove} translations across {len(by_resource)} resources to purge")

    if dry_run:
        # Show summary by resource type
        by_type = {}
        for f in fields:
            if f["arabic"]:
                rtype = f["resource_type"]
                by_type[rtype] = by_type.get(rtype, 0) + 1
        for rtype, count in sorted(by_type.items()):
            print(f"    {rtype}: {count}")
        return total_to_remove, 0

    removed = 0
    errors = 0

    for i, (rid, keys) in enumerate(by_resource.items()):
        # Batch in groups of 50 (Shopify limit)
        for j in range(0, len(keys), 50):
            batch = keys[j:j + 50]
            try:
                result = client._graphql(REMOVE_TRANSLATIONS_MUTATION, {
                    "resourceId": rid,
                    "translationKeys": batch,
                    "locales": [locale],
                })
                user_errors = result.get("translationsRemove", {}).get("userErrors", [])
                if user_errors:
                    for ue in user_errors:
                        print(f"    ERROR: {rid}: {ue['message']}")
                    errors += len(batch)
                else:
                    removed += len(batch)
            except Exception as e:
                print(f"    ERROR removing from {rid}: {e}")
                errors += len(batch)

            time.sleep(0.3)

        # Progress
        if (i + 1) % 20 == 0:
            print(f"  Purged {removed} / {total_to_remove} "
                  f"({i + 1}/{len(by_resource)} resources)...")

    print(f"\n  Purge complete: removed={removed}, errors={errors}")
    return removed, errors


def retranslate(client, engine, fields, scraped_lookup=None, dry_run=False,
                locale=LOCALE):
    """Translate all English fields to Arabic and upload.

    Uses scraped Magento Arabic content where available (scraped_lookup),
    then AI-translates only the remaining gaps.
    """
    scraped_lookup = scraped_lookup or {}

    # Filter to fields with English content worth translating
    translatable = []
    skipped_reasons = {"empty": 0, "url": 0, "field_pattern": 0}
    for f in fields:
        english = (f.get("english") or "").strip()
        if not english:
            skipped_reasons["empty"] += 1
            continue
        if is_skippable_field(f["key"]):
            skipped_reasons["field_pattern"] += 1
            continue
        if english.startswith("http") and not is_rich_text_json(english):
            skipped_reasons["url"] += 1
            continue
        translatable.append(f)
    print(f"  Skipped: {sum(skipped_reasons.values())} "
          f"(empty={skipped_reasons['empty']}, "
          f"field_pattern={skipped_reasons['field_pattern']}, "
          f"url={skipped_reasons['url']})")

    # Split into scraped (from Magento) vs gaps (need AI translation)
    from_scrape = []  # (field, arabic_value) — already have Arabic
    need_ai = []      # fields that need AI translation
    for f in translatable:
        rid = f["resource_id"]
        key = f["key"]
        scraped_fields = scraped_lookup.get(rid, {})
        if key in scraped_fields:
            from_scrape.append((f, scraped_fields[key]))
        else:
            need_ai.append(f)

    print(f"\n  Total translatable: {len(translatable)}")
    print(f"  From Magento scrape: {len(from_scrape)}")
    print(f"  Need AI translation: {len(need_ai)}")

    if dry_run:
        by_type_scrape = {}
        by_type_ai = {}
        for f, _ in from_scrape:
            rtype = f["resource_type"]
            by_type_scrape[rtype] = by_type_scrape.get(rtype, 0) + 1
        for f in need_ai:
            rtype = f["resource_type"]
            by_type_ai[rtype] = by_type_ai.get(rtype, 0) + 1
        print("\n  Scraped content by type:")
        for rtype, count in sorted(by_type_scrape.items()):
            print(f"    {rtype}: {count}")
        print("  AI translation by type:")
        for rtype, count in sorted(by_type_ai.items()):
            print(f"    {rtype}: {count}")
        return 0, 0, 0

    # Build combined translation map: field_id → arabic_value
    t_map = {}

    # 1) Add scraped content directly
    for f, arabic_value in from_scrape:
        field_id = f"{f['resource_id']}|{f['key']}"
        t_map[field_id] = arabic_value
    print(f"  Loaded {len(from_scrape)} fields from Magento scrape")

    # 2) AI-translate the gaps
    if need_ai and engine:
        engine_fields = []
        for f in need_ai:
            engine_fields.append({
                "id": f"{f['resource_id']}|{f['key']}",
                "value": f["english"],
            })
        ai_map = engine.translate_fields(engine_fields)
        t_map.update(ai_map)
        print(f"  AI-translated: {len(ai_map)} / {len(need_ai)} gap fields")
    elif need_ai:
        print(f"  WARNING: {len(need_ai)} fields need AI translation "
              f"but no engine provided")

    # Build resource → [(field, arabic_value)] mapping for upload
    all_fields = translatable
    by_resource = {}
    for f in all_fields:
        field_id = f"{f['resource_id']}|{f['key']}"
        if field_id not in t_map:
            continue
        rid = f["resource_id"]
        if rid not in by_resource:
            by_resource[rid] = []
        by_resource[rid].append((f, t_map[field_id]))

    # Fetch digests and upload
    gids = list(by_resource.keys())
    print(f"  Fetching digests for {len(gids)} resources...")
    digest_map = fetch_translatable_resources(client, gids, locale)

    uploaded = 0
    upload_errors = 0
    skipped = 0

    for rid, items in by_resource.items():
        dm = digest_map.get(rid)
        if not dm:
            skipped += len(items)
            continue

        translations_input = []
        for f, arabic_value in items:
            field_info = dm["content"].get(f["key"])
            if not field_info:
                skipped += 1
                continue

            # Validate JSON for rich_text fields
            if is_rich_text_json(arabic_value):
                try:
                    json.loads(arabic_value)
                except json.JSONDecodeError:
                    print(f"    WARNING: Invalid JSON for {rid} [{f['key']}], skipping")
                    upload_errors += 1
                    continue

            translations_input.append({
                "locale": locale,
                "key": f["key"],
                "value": arabic_value,
                "translatableContentDigest": field_info["digest"],
            })

        if translations_input:
            u, e = upload_translations(client, rid, translations_input)
            uploaded += u
            upload_errors += e

        time.sleep(0.3)

    print(f"\n  Upload complete: uploaded={uploaded}, errors={upload_errors}, "
          f"skipped={skipped}")
    return uploaded, upload_errors, skipped


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Purge all Arabic translations and retranslate with TARA tone of voice"
    )
    parser.add_argument("--purge-only", action="store_true",
                        help="Only purge, don't retranslate")
    parser.add_argument("--skip-purge", action="store_true",
                        help="Skip purge, only retranslate (assumes already purged)")
    parser.add_argument("--skip-theme", action="store_true",
                        help="Skip ONLINE_STORE_THEME (4000+ keys)")
    parser.add_argument("--type", type=str,
                        help="Comma-separated resource types (e.g. PRODUCT,COLLECTION)")
    parser.add_argument("--model", default="gpt-5-mini",
                        help="Translation model (default: gpt-5-mini)")
    parser.add_argument("--reasoning", default="minimal",
                        help="Reasoning effort (default: minimal)")
    parser.add_argument("--batch-size", type=int, default=80,
                        help="Translation batch size (default: 80)")
    parser.add_argument("--prompt", type=str,
                        help="Path to developer prompt file")
    parser.add_argument("--skip-scraped", action="store_true",
                        help="Skip Magento scrape data, AI-translate everything")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without making changes")
    args = parser.parse_args()

    # Determine resource types
    if args.type:
        resource_types = [t.strip() for t in args.type.split(",")]
    else:
        resource_types = list(TRANSLATABLE_RESOURCE_TYPES)
        if args.skip_theme:
            resource_types = [t for t in resource_types
                              if t != "ONLINE_STORE_THEME"]

    shop_url = config.get_dest_shop_url()
    access_token = config.get_dest_access_token()
    client = ShopifyClient(shop_url, access_token)

    print("=" * 60)
    print("ARABIC TRANSLATION PURGE + RETRANSLATE")
    print(f"  Store:           {shop_url}")
    print(f"  Resource types:  {', '.join(resource_types)}")
    print(f"  Model:           {args.model}")
    print(f"  Mode:            {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)

    # ── Step 1: Fetch all translations ──
    print("\nStep 1: Fetching all translations...")
    fields = fetch_all_translations(client, resource_types)

    translated_count = sum(1 for f in fields if f["arabic"])
    total_count = len(fields)
    print(f"\n  Total fields: {total_count}")
    print(f"  Currently translated: {translated_count}")

    # ── Step 2: Purge ──
    if not args.skip_purge:
        print(f"\n{'=' * 60}")
        print("Step 2: PURGING all Arabic translations")
        print("=" * 60)

        if translated_count == 0:
            print("  Nothing to purge — no existing translations found.")
        else:
            removed, purge_errors = purge_translations(
                client, fields, dry_run=args.dry_run
            )
    else:
        print("\n  Skipping purge (--skip-purge)")

    if args.purge_only:
        print("\n  Done (--purge-only). Run without --purge-only to retranslate.")
        return

    # ── Step 3: Load scraped Arabic content from Magento ──
    scraped_lookup = {}
    if not args.skip_scraped:
        print(f"\n{'=' * 60}")
        print("Step 3: Loading Magento scraped Arabic content")
        print("=" * 60)
        scraped_lookup = build_scraped_lookup()
        total_scraped_fields = sum(len(v) for v in scraped_lookup.values())
        print(f"  {len(scraped_lookup)} resources with scraped Arabic content")
        print(f"  {total_scraped_fields} total scraped fields available")
    else:
        print("\n  Skipping Magento scrape (--skip-scraped)")

    # ── Step 4: Retranslate (scrape first, AI for gaps) ──
    print(f"\n{'=' * 60}")
    print("Step 4: RETRANSLATING (Magento scrape + AI for gaps)")
    print("=" * 60)

    # Find developer prompt
    engine = None
    prompt_path = args.prompt
    if not prompt_path:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))))
        candidates = [
            os.path.join(project_root, "Arabic",
                         "tara_cached_developer_prompt.txt"),
            os.path.join(project_root, "developer_prompt.txt"),
        ]
        for c in candidates:
            if os.path.exists(c):
                prompt_path = c
                break

    developer_prompt = load_developer_prompt(
        prompt_path or "developer_prompt.txt",
    )
    print(f"  Developer prompt: {len(developer_prompt)} chars")

    engine = TranslationEngine(
        developer_prompt,
        model=args.model,
        reasoning_effort=args.reasoning,
        batch_size=args.batch_size,
    )

    uploaded, upload_errors, skipped = retranslate(
        client, engine, fields, scraped_lookup=scraped_lookup,
        dry_run=args.dry_run,
    )

    # ── Summary ──
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print("=" * 60)
    if not args.skip_purge:
        print(f"  Purged:     {translated_count} translations")
    print(f"  Translated: {uploaded} fields")
    print(f"  Errors:     {upload_errors}")
    print(f"  Skipped:    {skipped}")


if __name__ == "__main__":
    main()
