#!/usr/bin/env python3
"""Translate Shopify 'Translate and adapt' CSV export and upload to store.

Reads the CSV exported from Shopify's Translate and adapt feature,
translates all untranslated strings via OpenAI (TOON batch format),
and automatically registers the translations on the Shopify store.

Usage:
    python translate_csv.py                           # Translate + upload to Shopify
    python translate_csv.py --dry-run                 # Show what would be translated
    python translate_csv.py --no-upload               # Translate CSV only, no Shopify upload
    python translate_csv.py --model gpt-4o            # Use specific model
    python translate_csv.py --input data/custom.csv   # Custom input file

Prerequisites:
    OPENAI_API_KEY in .env or environment
    SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN in .env (for upload)
"""

import argparse
import csv
import json
import os
import re
import sys

from dotenv import load_dotenv

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from tara_migrate.client import ShopifyClient
from tara_migrate.translation.translate_gaps import (
    adaptive_batch,
    translate_batch,
)

# CSV Type → Shopify GID type name
CSV_TYPE_TO_GID = {
    "PRODUCT": "Product",
    "COLLECTION": "Collection",
    "PAGE": "Page",
    "ARTICLE": "Article",
    "BLOG": "Blog",
    "METAOBJECT": "Metaobject",
    "MENU": "Menu",
    "LINK": "Link",
    "MEDIA_IMAGE": "MediaImage",
    "PRODUCT_OPTION": "ProductOption",
    "PRODUCT_OPTION_VALUE": "ProductOptionValue",
    "METAFIELD": "Metafield",
    "FILTER": "Filter",
    "ONLINE_STORE_THEME": "OnlineStoreTheme",
    "COOKIE_BANNER": "CookieBanner",
    "DELIVERY_METHOD_DEFINITION": "DeliveryMethodDefinition",
    "PACKING_SLIP_TEMPLATE": "PackingSlipTemplate",
    "SHOP_POLICY": "ShopPolicy",
}

ARABIC_LOCALE = "ar"

# Resource types that can't be used directly with translatableResource —
# their translations must be registered on the parent resource instead.
NEEDS_PARENT_RESOLUTION = {"METAFIELD", "MEDIA_IMAGE"}

# Resource types that aren't translatable via the Translations API
SKIP_TYPES = {"FILTER", "COOKIE_BANNER"}


def _resolve_metafield_owners(shopify, metafield_gids):
    """Batch-resolve Metafield GIDs to their parent resource GID + translation key.

    Returns dict: metafield_gid -> {"parent_gid": ..., "translation_key": "namespace.key"}
    """
    result = {}
    # Process in batches of 50 (GraphQL nodes query limit is 250, but keep it reasonable)
    for batch_start in range(0, len(metafield_gids), 50):
        batch = metafield_gids[batch_start:batch_start + 50]
        query = """
        query GetMetafieldOwners($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on Metafield {
              id
              namespace
              key
              owner {
                ... on Product { id }
                ... on Collection { id }
                ... on Page { id }
                ... on Article { id }
                ... on Blog { id }
                ... on Shop { id }
                ... on Metaobject { id }
              }
            }
          }
        }
        """
        data = shopify._graphql(query, {"ids": batch})
        for node in (data.get("nodes") or []):
            if not node:
                continue
            mf_id = node.get("id")
            owner = node.get("owner")
            ns = node.get("namespace", "")
            key = node.get("key", "")
            if mf_id and owner and owner.get("id"):
                result[mf_id] = {
                    "parent_gid": owner["id"],
                    "translation_key": f"{ns}.{key}",
                }
    return result



def _should_translate(row):
    """Determine if a CSV row needs translation.

    Returns False for:
    - Already translated rows
    - Empty default content
    - Handle fields (cause conflicts)
    - URLs, image references, shopify:// paths
    - JSON structure values (metafield references, etc.)
    - Numeric/coordinate values
    """
    default = row.get("Default content", "").strip()
    translated = row.get("Translated content", "").strip()
    field = row.get("Field", "")

    # Already translated or empty
    if not default or translated:
        return False

    # Skip handles
    if field == "handle":
        return False

    # Skip URLs and image references
    if default.startswith(("shopify://", "http://", "https://", "/")):
        return False

    # Skip Shopify GIDs
    if default.startswith("gid://"):
        return False

    # Skip pure numeric values (coordinates, IDs, etc.)
    if re.match(r"^-?\d+\.?\d*$", default):
        return False

    # Skip UUIDs and hex strings
    if re.match(r"^[0-9a-f]{8,}$", default):
        return False

    # Skip JSON that's just references or IDs (not translatable text)
    if default.startswith("[") and default.endswith("]"):
        try:
            parsed = json.loads(default)
            # Skip arrays of GIDs/references
            if isinstance(parsed, list) and all(
                isinstance(v, str) and (v.startswith("gid://") or re.match(r"^\d+$", v))
                for v in parsed
            ):
                return False
        except (json.JSONDecodeError, TypeError):
            pass

    return True


def _is_keep_as_is(row):
    """Check if a row's value should be copied as-is (same in both languages).

    For things like social media URLs, API keys, form IDs, etc.
    """
    field = row.get("Field", "")
    default = row.get("Default content", "").strip()

    # Social media URLs, API keys, form IDs, regions
    keep_patterns = [
        "facebook_url", "instagram_url", "tiktok_url", "twitter_url",
        "google_maps_api_key", "form_id", "portal_id", "region",
        "anchor_id", "worker_url", "default_lat", "default_lng",
        "custom_max_height", "custom_max_width",
    ]
    for pat in keep_patterns:
        if pat in field:
            return True

    # URLs that should be kept (links to internal pages, etc.)
    if field.endswith(".link") or field.endswith("_url"):
        return True

    # Image references in theme sections
    if field.endswith(".image") or field.endswith(".image_1") or field.endswith(".image_2"):
        return True
    if ".image_1:" in field or ".image_2:" in field or ".image_1_mobile:" in field or ".image_2_mobile:" in field:
        return True

    # Logo and favicon
    if field in ("general.logo", "general.logo_inverse", "general.favicon"):
        return True

    # Icon references
    if ".icon:" in field:
        return True

    return False


def main():
    parser = argparse.ArgumentParser(description="Translate Shopify CSV export to Arabic")
    parser.add_argument("--input", default="data/Tara_Saudi_translations_Mar-10-2026.csv",
                        help="Input CSV file")
    parser.add_argument("--output", default=None,
                        help="Output CSV file (default: input with _translated suffix)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be translated without making API calls")
    parser.add_argument("--model", default="gpt-4o-mini",
                        help="OpenAI model (default: gpt-4o-mini)")
    parser.add_argument("--no-upload", action="store_true",
                        help="Skip Shopify upload, only write translated CSV")
    args = parser.parse_args()

    if not args.output:
        base, ext = os.path.splitext(args.input)
        args.output = f"{base}_translated{ext}"

    load_dotenv()

    # Read CSV
    with open(args.input, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"Read {len(rows)} rows from {args.input}")

    # Categorize rows
    to_translate = []
    keep_as_is = []
    skip = []

    for i, row in enumerate(rows):
        default = row.get("Default content", "").strip()
        translated = row.get("Translated content", "").strip()

        if translated:
            skip.append((i, "already translated"))
        elif not default:
            skip.append((i, "empty"))
        elif _is_keep_as_is(row):
            keep_as_is.append(i)
        elif _should_translate(row):
            to_translate.append(i)
        else:
            skip.append((i, "non-translatable"))

    print("\nBreakdown:")
    print(f"  Already translated: {sum(1 for _, r in skip if r == 'already translated')}")
    print(f"  Keep as-is (URLs/images/config): {len(keep_as_is)}")
    print(f"  Need AI translation: {len(to_translate)}")
    print(f"  Skip (empty/non-translatable): {sum(1 for _, r in skip if r != 'already translated')}")

    # Apply keep-as-is: copy default content to translated content
    for idx in keep_as_is:
        rows[idx]["Translated content"] = rows[idx]["Default content"]

    if args.dry_run:
        print(f"\n--- DRY RUN: Would translate {len(to_translate)} strings ---")
        from collections import Counter
        by_type = Counter(rows[i]["Type"] for i in to_translate)
        for t, c in by_type.most_common():
            print(f"  {t}: {c}")
        print("\nSample strings to translate:")
        for idx in to_translate[:20]:
            r = rows[idx]
            print(f"  [{r['Type']}] {r['Field']}: {r['Default content'][:80]}")
        return

    # Initialize OpenAI
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set. Add it to .env or environment.")
        sys.exit(1)

    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    # Build TOON-compatible field list
    fields = []
    for idx in to_translate:
        r = rows[idx]
        field_id = f"{r['Type']}|{r['Identification']}|{r['Field']}"
        fields.append({
            "id": field_id,
            "value": r["Default content"],
            "_row_idx": idx,
        })

    # Batch and translate
    batches = adaptive_batch(fields, max_tokens=8000)
    print(f"\nTranslating {len(fields)} strings in {len(batches)} batches...")

    all_translations = {}
    total_tokens = 0

    for i, batch in enumerate(batches):
        # Strip internal _row_idx before sending to API
        api_batch = [{"id": f["id"], "value": f["value"]} for f in batch]
        t_map, tokens = translate_batch(
            client, args.model, api_batch,
            "English", "Arabic",
            i + 1, len(batches),
        )
        all_translations.update(t_map)
        total_tokens += tokens

    # Apply translations back to rows
    translated_count = 0
    for field in fields:
        field_id = field["id"]
        row_idx = field["_row_idx"]
        if field_id in all_translations:
            rows[row_idx]["Translated content"] = all_translations[field_id]
            translated_count += 1

    print(f"\nTranslated {translated_count}/{len(fields)} strings")
    print(f"Total tokens used: {total_tokens:,}")

    # Write output CSV
    with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Written to {args.output}")

    # Summary
    final_translated = sum(1 for r in rows if r.get("Translated content", "").strip())
    final_untranslated = sum(1 for r in rows
                            if r.get("Default content", "").strip()
                            and not r.get("Translated content", "").strip()
                            and r.get("Field") != "handle")
    print("\nFinal stats:")
    print(f"  Translated: {final_translated}/{len(rows)}")
    print(f"  Still untranslated: {final_untranslated}")

    # Upload to Shopify
    if args.no_upload:
        print("\nSkipping Shopify upload (--no-upload). Import manually via:")
        print("  Shopify Admin > Settings > Languages > Arabic > Import")
        return

    shop_url = os.environ.get("SAUDI_SHOP_URL")
    access_token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not access_token:
        print("\nSAUDI_SHOP_URL / SAUDI_ACCESS_TOKEN not set — skipping Shopify upload.")
        print("Import the CSV manually via Shopify Admin > Settings > Languages > Arabic > Import")
        return

    print(f"\n{'='*60}")
    print("Uploading translations to Shopify...")
    print(f"{'='*60}")

    shopify = ShopifyClient(shop_url, access_token)

    # Group translated rows by resource GID for efficient batch registration
    by_gid = {}
    metafield_gids_needed = set()
    skipped_types = {}

    for row in rows:
        translated = row.get("Translated content", "").strip()
        default = row.get("Default content", "").strip()
        if not translated or not default:
            continue

        csv_type = row["Type"]

        # Skip types that aren't translatable via the API
        if csv_type in SKIP_TYPES:
            skipped_types[csv_type] = skipped_types.get(csv_type, 0) + 1
            continue

        gid_type = CSV_TYPE_TO_GID.get(csv_type)
        if not gid_type:
            continue

        resource_id = row["Identification"].strip().lstrip("'")
        gid = f"gid://shopify/{gid_type}/{resource_id}"
        field = row["Field"]

        if csv_type == "METAFIELD":
            metafield_gids_needed.add(gid)

        by_gid.setdefault(gid, []).append({
            "field": field,
            "value": translated,
            "default": default,
        })

    # Resolve Metafield GIDs to parent resources
    if metafield_gids_needed:
        print(f"  Resolving {len(metafield_gids_needed)} metafield owners...")
        mf_owners = _resolve_metafield_owners(shopify, list(metafield_gids_needed))
        print(f"  Resolved {len(mf_owners)}/{len(metafield_gids_needed)} metafield owners")

        # Remap metafield entries to their parent resource
        remapped = 0
        unresolved = 0
        for mf_gid in list(metafield_gids_needed):
            if mf_gid not in by_gid:
                continue
            fields_list = by_gid.pop(mf_gid)
            owner_info = mf_owners.get(mf_gid)
            if not owner_info:
                unresolved += 1
                continue
            parent_gid = owner_info["parent_gid"]
            translation_key = owner_info["translation_key"]
            # Remap: the CSV field "value" becomes the metafield's namespace.key on the parent
            for f in fields_list:
                f["field"] = translation_key
            by_gid.setdefault(parent_gid, []).extend(fields_list)
            remapped += 1
        if unresolved:
            print(f"  WARNING: {unresolved} metafields could not be resolved to parent")
        print(f"  Remapped {remapped} metafields to parent resources")

    # Remove MediaImage entries — these need parent resolution which is hard to do
    media_gids = [gid for gid in by_gid if "/MediaImage/" in gid]
    if media_gids:
        for gid in media_gids:
            del by_gid[gid]
        print(f"  Skipped {len(media_gids)} MediaImage resources (import via CSV for these)")

    if skipped_types:
        for t, count in skipped_types.items():
            print(f"  Skipped {count} {t} fields (not translatable via API)")

    print(f"  {len(by_gid)} resources to update")

    # Progress tracking
    progress_file = "data/csv_upload_progress.json"
    progress = {}
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            progress = json.load(f)

    registered = 0
    skipped = 0
    errors = 0
    total = len(by_gid)

    for i, (gid, fields_list) in enumerate(by_gid.items()):
        if gid in progress:
            skipped += 1
            continue

        # Fetch translatable content to get digests
        try:
            resource = shopify.get_translatable_resource(gid)
            if not resource:
                print(f"  [{i+1}/{total}] {gid} — not found")
                errors += 1
                continue

            tc = resource.get("translatableContent", [])
            digest_map = {item["key"]: item["digest"] for item in tc}

            # Build translation inputs
            translations = []
            for field_data in fields_list:
                field = field_data["field"]
                if field in digest_map:
                    translations.append({
                        "key": field,
                        "value": field_data["value"],
                        "locale": ARABIC_LOCALE,
                        "translatableContentDigest": digest_map[field],
                    })

            if translations:
                # Sanitize JSON values: fix literal newlines inside JSON strings
                for t in translations:
                    val = t["value"]
                    if val.startswith(("{", "[")):
                        try:
                            json.loads(val)
                        except json.JSONDecodeError:
                            # Replace literal newlines with escaped \\n in JSON
                            t["value"] = val.replace("\n", "\\n")

                # Shopify limits translationsRegister to 250 items per call
                BATCH_LIMIT = 250
                for chunk_start in range(0, len(translations), BATCH_LIMIT):
                    chunk = translations[chunk_start:chunk_start + BATCH_LIMIT]
                    shopify.register_translations(gid, ARABIC_LOCALE, chunk)
                    registered += len(chunk)
                if (i + 1) % 50 == 0 or i + 1 == total:
                    print(f"  [{i+1}/{total}] {registered} translations registered...")

            progress[gid] = True

            # Save progress periodically
            if (i + 1) % 100 == 0:
                with open(progress_file, "w") as f:
                    json.dump(progress, f)

        except Exception as e:
            print(f"  [{i+1}/{total}] {gid} — error: {e}")
            errors += 1

    # Final progress save
    with open(progress_file, "w") as f:
        json.dump(progress, f)

    print("\nUpload complete:")
    print(f"  Registered: {registered} translations")
    print(f"  Skipped (already done): {skipped}")
    print(f"  Errors: {errors}")


if __name__ == "__main__":
    main()
