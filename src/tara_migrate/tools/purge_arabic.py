#!/usr/bin/env python3
"""Purge all Arabic translations from the Saudi store and retranslate from scratch.

This script:
  1. Fetches ALL Arabic translations across all resource types
  2. Removes them via Shopify's translationsRemove GraphQL mutation
  3. Re-translates everything EN→AR using TARA tone of voice
  4. Uploads the fresh translations via translationsRegister

Usage:
    python purge_arabic.py --purge-only                    # Purge only, no retranslation
    python purge_arabic.py --purge-only --dry-run           # Preview what would be purged
    python purge_arabic.py --model gpt-5-mini               # Purge + retranslate
    python purge_arabic.py --model gpt-5-mini --dry-run     # Preview full pipeline
    python purge_arabic.py --skip-purge                     # Retranslate only (assumes already purged)
    python purge_arabic.py --type PRODUCT,COLLECTION        # Only specific resource types
    python purge_arabic.py --skip-theme                     # Skip ONLINE_STORE_THEME (4000+ keys)
"""

import argparse
import json
import os
import sys
import time

from dotenv import load_dotenv

from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.core import config
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


def retranslate(client, engine, fields, dry_run=False, locale=LOCALE):
    """Translate all English fields to Arabic and upload.

    Only translates fields that have English content (skips empty fields).
    """
    # Filter to fields with English content worth translating
    to_translate = []
    skipped_reasons = {"empty": 0, "url": 0, "field_pattern": 0}
    for f in fields:
        english = (f.get("english") or "").strip()
        if not english:
            skipped_reasons["empty"] += 1
            continue
        # Skip non-translatable field patterns (images, URLs, config, etc.)
        if is_skippable_field(f["key"]):
            skipped_reasons["field_pattern"] += 1
            continue
        # Skip plain URLs
        if english.startswith("http") and not is_rich_text_json(english):
            skipped_reasons["url"] += 1
            continue
        to_translate.append(f)
    print(f"  Skipped: {sum(skipped_reasons.values())} "
          f"(empty={skipped_reasons['empty']}, "
          f"field_pattern={skipped_reasons['field_pattern']}, "
          f"url={skipped_reasons['url']})")

    print(f"\n  {len(to_translate)} fields to translate")

    if dry_run:
        by_type = {}
        for f in to_translate:
            rtype = f["resource_type"]
            by_type[rtype] = by_type.get(rtype, 0) + 1
        for rtype, count in sorted(by_type.items()):
            print(f"    {rtype}: {count}")
        return 0, 0, 0

    # Translate in batches using the engine's format: [{id, value}, ...]
    engine_fields = []
    for i, f in enumerate(to_translate):
        engine_fields.append({
            "id": f"{f['resource_id']}|{f['key']}",
            "value": f["english"],
        })

    t_map = engine.translate_fields(engine_fields)

    print(f"  Translated: {len(t_map)} / {len(to_translate)} fields")

    # Build resource → [(field, arabic_value)] mapping for upload
    by_resource = {}
    for i, f in enumerate(to_translate):
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

    # ── Step 3: Retranslate ──
    print(f"\n{'=' * 60}")
    print("Step 3: RETRANSLATING with TARA tone of voice")
    print("=" * 60)

    # Find developer prompt
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
        client, engine, fields, dry_run=args.dry_run
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
