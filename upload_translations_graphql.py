#!/usr/bin/env python3
"""Upload Arabic translations to Shopify via GraphQL translationsRegister.

Replaces CSV import with digest-validated, per-field GraphQL mutations.
Reads the translated CSV and uploads translations directly to Shopify,
ensuring each translation lands on exactly the right field.

Usage:
    python upload_translations_graphql.py --input Arabic/Tara_Saudi_translations_Mar-10-2026.csv --dry-run
    python upload_translations_graphql.py --input Arabic/Tara_Saudi_translations_Mar-10-2026.csv
    python upload_translations_graphql.py --input Arabic/Tara_Saudi_translations_Mar-10-2026.csv --type PRODUCT
    python upload_translations_graphql.py --input Arabic/Tara_Saudi_translations_Mar-10-2026.csv --fix-misaligned
"""

import argparse
import csv
import json
import os
import re
import sys
import time

from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.core.utils import sanitize_rich_text_json

LOCALE = "ar"

# CSV Type → Shopify GID prefix mapping
TYPE_TO_GID_PREFIX = {
    "PRODUCT": "gid://shopify/Product/",
    "COLLECTION": "gid://shopify/Collection/",
    "METAFIELD": "gid://shopify/Metafield/",
    "METAOBJECT": "gid://shopify/Metaobject/",
    "ONLINE_STORE_THEME": "gid://shopify/OnlineStoreTheme/",
    "PAGE": "gid://shopify/Page/",
    "DELIVERY_METHOD_DEFINITION": "gid://shopify/DeliveryMethodDefinition/",
    "COOKIE_BANNER": "gid://shopify/CookieBanner/",
    "MEDIA_IMAGE": "gid://shopify/MediaImage/",
}

# GraphQL queries/mutations
FETCH_DIGESTS_QUERY = """
query($resourceIds: [ID!]!, $first: Int!) {
  translatableResourcesByIds(resourceIds: $resourceIds, first: $first) {
    edges {
      node {
        resourceId
        translatableContent {
          key
          value
          digest
          locale
        }
        translations(locale: "ar") {
          key
          value
          outdated
        }
      }
    }
  }
}
"""

REGISTER_TRANSLATIONS_MUTATION = """
mutation translationsRegister($resourceId: ID!, $translations: [TranslationInput!]!) {
  translationsRegister(resourceId: $resourceId, translations: $translations) {
    userErrors {
      message
      field
    }
    translations {
      key
      value
    }
  }
}
"""


def _extract_rich_text(text):
    """Extract plain text from Shopify rich_text JSON."""
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None
    parts = []
    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "text" and "value" in node:
                parts.append(node["value"])
            for child in node.get("children", []):
                walk(child)
        elif isinstance(node, list):
            for item in node:
                walk(item)
    walk(data)
    return " ".join(parts) if parts else None


def _has_arabic(text, min_ratio=0.3):
    """Check if text contains sufficient Arabic characters."""
    if not text:
        return False
    if text.startswith("{") and '"type"' in text:
        extracted = _extract_rich_text(text)
        if extracted and extracted.strip():
            text = extracted
    stripped = re.sub(r"<[^>]+>", " ", text)
    stripped = re.sub(r"\{[^}]*\}", " ", stripped)
    stripped = stripped.strip()
    if not stripped:
        return True
    arabic = len(re.findall(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", stripped))
    alpha = len(re.findall(r"[a-zA-ZÀ-ÿ\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", stripped))
    if alpha == 0:
        return True
    return arabic / alpha >= min_ratio


def _is_non_translatable(row):
    """Return True if this row should never be translated."""
    default = row.get("Default content", "").strip()
    field = row.get("Field", "")
    if not default:
        return True
    if field == "handle":
        return True
    if default.startswith(("shopify://", "http://", "https://", "/", "gid://")):
        return True
    if re.match(r"^-?\d+\.?\d*$", default):
        return True
    if re.match(r"^[0-9a-f]{8,}$", default):
        return True
    if default.startswith("[") and default.endswith("]"):
        try:
            parsed = json.loads(default)
            if isinstance(parsed, list) and all(
                isinstance(v, str) and (v.startswith("gid://") or re.match(r"^\d+$", v))
                for v in parsed
            ):
                return True
        except (json.JSONDecodeError, TypeError):
            pass
    return False


def _is_keep_as_is(row):
    """Check if a row's value should be copied as-is."""
    field = row.get("Field", "")
    keep_patterns = [
        "facebook_url", "instagram_url", "tiktok_url", "twitter_url",
        "google_maps_api_key", "form_id", "portal_id", "region",
        "anchor_id", "worker_url", "default_lat", "default_lng",
        "custom_max_height", "custom_max_width",
    ]
    for pat in keep_patterns:
        if pat in field:
            return True
    if field.endswith(".link") or field.endswith("_url"):
        return True
    if field.endswith(".image") or field.endswith(".image_1") or field.endswith(".image_2"):
        return True
    if ".image_1:" in field or ".image_2:" in field:
        return True
    if ".image_1_mobile:" in field or ".image_2_mobile:" in field:
        return True
    if field in ("general.logo", "general.logo_inverse", "general.favicon"):
        return True
    if ".icon:" in field:
        return True
    return False


def build_gid(csv_type, identification):
    """Build Shopify GID from CSV Type and Identification columns."""
    prefix = TYPE_TO_GID_PREFIX.get(csv_type)
    if not prefix:
        return None
    # Strip leading apostrophe from identification
    clean_id = identification.lstrip("'")
    return f"{prefix}{clean_id}"


def main():
    parser = argparse.ArgumentParser(
        description="Upload Arabic translations to Shopify via GraphQL")
    parser.add_argument("--input", required=True, help="Translated CSV file")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be uploaded without making changes")
    parser.add_argument("--type", default=None,
                        help="Only upload specific type (PRODUCT, COLLECTION, etc.)")
    parser.add_argument("--fix-misaligned", action="store_true",
                        help="Detect and skip misaligned translations (value doesn't match field)")
    parser.add_argument("--skip-identical", action="store_true", default=True,
                        help="Skip rows where Translated == Default (default: true)")
    parser.add_argument("--force", action="store_true",
                        help="Upload even if translation doesn't have Arabic")
    parser.add_argument("--batch-size", type=int, default=10,
                        help="Resources per GraphQL batch (default: 10)")
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ.get("SAUDI_SHOP_URL")
    token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not token:
        print("ERROR: Set SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN in .env")
        sys.exit(1)

    # Read CSV
    with open(args.input, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"Read {len(rows)} rows from {args.input}")

    # Group translatable rows by resource GID
    resources = {}  # gid → [{field, value, translated}, ...]
    skipped = {"empty": 0, "non_translatable": 0, "keep_as_is": 0,
               "no_arabic": 0, "identical": 0, "unknown_type": 0, "type_filter": 0}

    for row in rows:
        csv_type = row.get("Type", "").strip()
        identification = row.get("Identification", "").strip()
        field = row.get("Field", "").strip()
        default = row.get("Default content", "").strip()
        translated = row.get("Translated content", "").strip()

        if args.type and csv_type != args.type:
            skipped["type_filter"] += 1
            continue

        if _is_non_translatable(row):
            skipped["non_translatable"] += 1
            continue

        if _is_keep_as_is(row):
            skipped["keep_as_is"] += 1
            continue

        if not translated:
            skipped["empty"] += 1
            continue

        if args.skip_identical and translated == default and not _has_arabic(translated):
            skipped["identical"] += 1
            continue

        if not args.force and not _has_arabic(translated):
            skipped["no_arabic"] += 1
            continue

        gid = build_gid(csv_type, identification)
        if not gid:
            skipped["unknown_type"] += 1
            continue

        if gid not in resources:
            resources[gid] = []
        resources[gid].append({
            "field": field,
            "default": default,
            "translated": translated,
            "csv_type": csv_type,
        })

    total_fields = sum(len(fields) for fields in resources.values())
    print(f"\nTo upload: {total_fields} fields across {len(resources)} resources")
    print(f"Skipped: {json.dumps(skipped, indent=2)}")

    if args.dry_run:
        print("\n--- DRY RUN ---")
        for gid, fields in list(resources.items())[:5]:
            print(f"\n  {gid}:")
            for f in fields:
                ar_preview = f["translated"][:50]
                print(f"    {f['field']:20s} → {ar_preview}")
        if len(resources) > 5:
            print(f"\n  ... and {len(resources) - 5} more resources")
        return

    # Upload translations via GraphQL
    client = ShopifyClient(shop_url, token)

    uploaded = 0
    errors = 0
    skipped_digest = 0
    misaligned = 0
    gid_list = list(resources.keys())

    for batch_start in range(0, len(gid_list), args.batch_size):
        batch_gids = gid_list[batch_start:batch_start + args.batch_size]
        batch_num = batch_start // args.batch_size + 1
        total_batches = (len(gid_list) + args.batch_size - 1) // args.batch_size

        print(f"\nBatch {batch_num}/{total_batches}: "
              f"fetching digests for {len(batch_gids)} resources...")

        # Step 1: Fetch current digests from Shopify
        try:
            data = client._graphql(FETCH_DIGESTS_QUERY, {
                "resourceIds": batch_gids,
                "first": len(batch_gids),
            })
        except Exception as e:
            print(f"  ERROR fetching digests: {e}")
            errors += len(batch_gids)
            continue

        edges = data.get("translatableResourcesByIds", {}).get("edges", [])
        digest_map = {}  # gid → {key → {digest, value}}
        for edge in edges:
            node = edge["node"]
            rid = node["resourceId"]
            digest_map[rid] = {}
            for tc in node["translatableContent"]:
                digest_map[rid][tc["key"]] = {
                    "digest": tc["digest"],
                    "value": tc["value"],
                }

        # Step 2: Register translations for each resource
        for gid in batch_gids:
            csv_fields = resources[gid]

            if gid not in digest_map:
                print(f"  SKIP {gid}: not found in Shopify (deleted?)")
                skipped_digest += len(csv_fields)
                continue

            translations_input = []
            for cf in csv_fields:
                field_key = cf["field"]
                shopify_field = digest_map[gid].get(field_key)

                if not shopify_field:
                    # Try without section prefix for theme fields
                    # The CSV field might not match the GraphQL key exactly
                    skipped_digest += 1
                    continue

                # Misalignment check: compare CSV default with Shopify's current value
                if args.fix_misaligned and shopify_field["value"]:
                    shopify_value = shopify_field["value"].strip()
                    csv_default = cf["default"].strip()
                    if shopify_value != csv_default:
                        # Content changed since CSV was exported — skip to avoid wrong translation
                        misaligned += 1
                        continue

                # Sanitize rich_text JSON (fix newlines/control chars from translation)
                translated_value = cf["translated"]
                if translated_value.strip().startswith("{") and '"type"' in translated_value:
                    translated_value = sanitize_rich_text_json(translated_value)

                translations_input.append({
                    "locale": LOCALE,
                    "key": field_key,
                    "value": translated_value,
                    "translatableContentDigest": shopify_field["digest"],
                })

            if not translations_input:
                continue

            try:
                result = client._graphql(REGISTER_TRANSLATIONS_MUTATION, {
                    "resourceId": gid,
                    "translations": translations_input,
                })
                user_errors = result.get("translationsRegister", {}).get("userErrors", [])
                if user_errors:
                    print(f"  ERRORS for {gid}:")
                    for ue in user_errors:
                        print(f"    {ue['field']}: {ue['message']}")
                    errors += len(user_errors)
                    uploaded += len(translations_input) - len(user_errors)
                else:
                    uploaded += len(translations_input)
            except Exception as e:
                print(f"  ERROR uploading {gid}: {e}")
                errors += len(translations_input)

        # Brief pause between batches to stay under rate limits
        if batch_start + args.batch_size < len(gid_list):
            time.sleep(0.5)

    print(f"\n{'='*60}")
    print(f"  UPLOAD COMPLETE")
    print(f"{'='*60}")
    print(f"  Uploaded:          {uploaded} fields")
    print(f"  Errors:            {errors}")
    print(f"  Skipped (digest):  {skipped_digest}")
    if misaligned:
        print(f"  Misaligned:        {misaligned} (content changed since CSV export)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
