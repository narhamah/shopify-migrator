#!/usr/bin/env python3
"""Fix remaining untranslated Arabic fields on the Tara Saudi store.

Targets three categories of missing translations:
1. Metaobject fields (ingredient names, one_line_benefit, concern, type)
2. Product metafields (key_benefits_heading, key_benefits_content)
3. Re-upload ONLINE_STORE_THEME translations that failed due to batch errors

Queries Shopify directly, translates via OpenAI, and uploads via GraphQL.

Usage:
    python fix_remaining_ar.py --dry-run          # Preview what would be fixed
    python fix_remaining_ar.py                     # Fix all categories
    python fix_remaining_ar.py --only metaobjects  # Fix only metaobject names
    python fix_remaining_ar.py --only products     # Fix only product metafields
    python fix_remaining_ar.py --only theme        # Re-upload theme translations
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
from tara_migrate.translation.toon import DELIM, from_toon, to_toon

LOCALE = "ar"
ARABIC_DIR = os.path.join(os.path.dirname(__file__), "Arabic")
PROMPT_FILE = os.path.join(ARABIC_DIR, "tara_cached_developer_prompt.txt")

# GraphQL queries
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

# Fetch all metaobjects of a given type
FETCH_METAOBJECTS_QUERY = """
query($type: String!, $first: Int!, $after: String) {
  metaobjects(type: $type, first: $first, after: $after) {
    edges {
      node {
        id
        handle
        fields {
          key
          value
          type
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# Fetch all products (just IDs for metafield lookup)
FETCH_PRODUCTS_QUERY = """
query($first: Int!, $after: String) {
  products(first: $first, after: $after) {
    edges {
      node {
        id
        title
        metafields(first: 30) {
          edges {
            node {
              id
              namespace
              key
              value
              type
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


def _has_arabic(text, min_ratio=0.3):
    """Check if text contains sufficient Arabic characters."""
    if not text:
        return False
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


def translate_fields(fields, developer_prompt, model="gpt-4o-mini"):
    """Translate a list of {id, value} dicts using OpenAI."""
    import openai
    client = openai.OpenAI()

    toon_input = to_toon(fields)
    user_message = (
        "Translate the following TOON input into Tara Arabic and return TOON only.\n"
        "IMPORTANT: Translate ALL ingredient names, benefit names, and category labels into Arabic.\n"
        "Scientific names (INCI names like 'Ceramide NP', 'Allium Cepa') should be kept as-is,\n"
        "but common names MUST be translated: 'Activated Charcoal' → 'الفحم المنشط',\n"
        "'Mango Butter' → 'زبدة المانجو', 'Red Onion Extract' → 'خلاصة البصل الأحمر', etc.\n"
        "Also translate category/concern labels: 'Hair Loss' → 'تساقط الشعر',\n"
        "'Oiliness' → 'الزيتية', 'Dryness' → 'الجفاف', etc.\n\n"
        f"<TOON>\n{toon_input}\n</TOON>"
    )

    print(f"    Translating {len(fields)} fields...")
    for attempt in range(3):
        try:
            response = client.responses.create(
                model=model,
                instructions=developer_prompt,
                input=user_message,
                reasoning={"effort": "medium"},
            )

            result = ""
            for item in response.output:
                if item.type == "message":
                    for content in item.content:
                        if content.type == "output_text":
                            result += content.text

            result = result.strip()
            if result.startswith("```"):
                lines = result.split("\n")
                if lines[-1].strip() == "```":
                    result = "\n".join(lines[1:-1])
                else:
                    result = "\n".join(lines[1:])
            result = re.sub(r"</?TOON>", "", result).strip()

            translated = from_toon(result)
            t_map = {}
            for entry in translated:
                t_map[entry["id"]] = entry["value"]

            input_ids = {f["id"] for f in fields}
            matched = len(input_ids & set(t_map.keys()))
            print(f"    Got {matched}/{len(fields)} translations "
                  f"({response.usage.input_tokens + response.usage.output_tokens} tokens)")
            return t_map

        except Exception as e:
            print(f"    Error (attempt {attempt+1}): {e}")
            if attempt < 2:
                time.sleep(2 ** (attempt + 1))

    return {}


def fetch_translatable_resources(client, gids):
    """Fetch digest map for a list of GIDs."""
    digest_map = {}
    for i in range(0, len(gids), 10):
        batch = gids[i:i+10]
        try:
            data = client._graphql(FETCH_DIGESTS_QUERY, {
                "resourceIds": batch,
                "first": len(batch),
            })
            edges = data.get("translatableResourcesByIds", {}).get("edges", [])
            for edge in edges:
                node = edge["node"]
                rid = node["resourceId"]
                digest_map[rid] = {
                    "content": {tc["key"]: {"digest": tc["digest"], "value": tc["value"]}
                                for tc in node["translatableContent"]},
                    "translations": {t["key"]: {"value": t["value"], "outdated": t["outdated"]}
                                     for t in node["translations"]},
                }
        except Exception as e:
            print(f"  Error fetching digests for batch: {e}")
        time.sleep(0.3)
    return digest_map


def upload_translations(client, gid, translations_input):
    """Upload translations for a single resource."""
    try:
        result = client._graphql(REGISTER_TRANSLATIONS_MUTATION, {
            "resourceId": gid,
            "translations": translations_input,
        })
        user_errors = result.get("translationsRegister", {}).get("userErrors", [])
        if user_errors:
            for ue in user_errors:
                print(f"    ERROR {gid}: {ue['field']}: {ue['message']}")
            return len(translations_input) - len(user_errors), len(user_errors)
        return len(translations_input), 0
    except Exception as e:
        print(f"    ERROR uploading {gid}: {e}")
        return 0, len(translations_input)


def fix_metaobjects(client, developer_prompt, model, dry_run=False):
    """Find and fix untranslated metaobject fields."""
    print("\n=== FIXING METAOBJECT TRANSLATIONS ===")

    # Fetch all metaobject types we care about
    metaobject_types = ["ingredient", "benefit", "faq_entry"]
    all_metaobjects = {}

    for mo_type in metaobject_types:
        cursor = None
        while True:
            variables = {"type": mo_type, "first": 50}
            if cursor:
                variables["after"] = cursor
            try:
                data = client._graphql(FETCH_METAOBJECTS_QUERY, variables)
            except Exception as e:
                print(f"  Error fetching {mo_type}: {e}")
                break

            edges = data.get("metaobjects", {}).get("edges", [])
            for edge in edges:
                node = edge["node"]
                fields_dict = {f["key"]: f["value"] for f in node["fields"]}
                all_metaobjects[node["id"]] = {
                    "type": mo_type,
                    "handle": node["handle"],
                    "fields": fields_dict,
                }

            page_info = data.get("metaobjects", {}).get("pageInfo", {})
            if page_info.get("hasNextPage"):
                cursor = page_info["endCursor"]
            else:
                break
            time.sleep(0.3)

    print(f"  Found {len(all_metaobjects)} metaobjects")

    # Fetch current translations
    gids = list(all_metaobjects.keys())
    digest_map = fetch_translatable_resources(client, gids)

    # Find fields that need translation
    needs_translation = []  # {gid, key, english_value}
    for gid, mo in all_metaobjects.items():
        if gid not in digest_map:
            continue
        dm = digest_map[gid]
        translatable_keys = ["name", "one_line_benefit", "description", "question",
                             "answer", "concern", "type"]
        for key in translatable_keys:
            if key not in dm["content"]:
                continue
            english = dm["content"][key]["value"]
            if not english or not english.strip():
                continue
            # Check if already translated
            existing = dm["translations"].get(key, {}).get("value", "")
            if existing and _has_arabic(existing) and existing != english:
                continue
            # This field needs translation
            needs_translation.append({
                "gid": gid,
                "key": key,
                "english": english,
                "existing_ar": existing,
                "type": mo["type"],
                "handle": mo["handle"],
            })

    print(f"  Fields needing translation: {len(needs_translation)}")

    if not needs_translation:
        print("  Nothing to fix!")
        return 0, 0

    # Show preview
    for item in needs_translation[:15]:
        existing = f" (current: {item['existing_ar'][:40]})" if item['existing_ar'] else ""
        print(f"    [{item['type']}] {item['handle']}.{item['key']}: "
              f"{item['english'][:50]}{existing}")
    if len(needs_translation) > 15:
        print(f"    ... and {len(needs_translation) - 15} more")

    if dry_run:
        return 0, 0

    # Translate
    fields_for_ai = []
    for item in needs_translation:
        field_id = f"{item['type']}|{item['handle']}|{item['key']}"
        fields_for_ai.append({"id": field_id, "value": item["english"]})

    t_map = translate_fields(fields_for_ai, developer_prompt, model)

    # Upload translations
    uploaded = 0
    errors = 0
    # Group by GID
    by_gid = {}
    for item in needs_translation:
        field_id = f"{item['type']}|{item['handle']}|{item['key']}"
        ar_value = t_map.get(field_id)
        if not ar_value:
            continue
        if item["gid"] not in by_gid:
            by_gid[item["gid"]] = []
        dm = digest_map[item["gid"]]
        digest = dm["content"][item["key"]]["digest"]
        by_gid[item["gid"]].append({
            "locale": LOCALE,
            "key": item["key"],
            "value": ar_value,
            "translatableContentDigest": digest,
        })

    for gid, translations_input in by_gid.items():
        u, e = upload_translations(client, gid, translations_input)
        uploaded += u
        errors += e
        time.sleep(0.3)

    print(f"\n  Metaobjects: uploaded={uploaded}, errors={errors}")
    return uploaded, errors


def fix_product_metafields(client, developer_prompt, model, dry_run=False):
    """Fix key_benefits_heading and key_benefits_content for all products."""
    print("\n=== FIXING PRODUCT METAFIELD TRANSLATIONS ===")

    # Fetch all products with their metafields
    products = []
    cursor = None
    while True:
        variables = {"first": 50}
        if cursor:
            variables["after"] = cursor
        try:
            data = client._graphql(FETCH_PRODUCTS_QUERY, variables)
        except Exception as e:
            print(f"  Error fetching products: {e}")
            break

        edges = data.get("products", {}).get("edges", [])
        for edge in edges:
            node = edge["node"]
            metafields = {}
            for mf_edge in node.get("metafields", {}).get("edges", []):
                mf = mf_edge["node"]
                metafields[f"{mf['namespace']}.{mf['key']}"] = {
                    "id": mf["id"],
                    "value": mf["value"],
                    "type": mf["type"],
                }
            products.append({
                "id": node["id"],
                "title": node["title"],
                "metafields": metafields,
            })

        page_info = data.get("products", {}).get("pageInfo", {})
        if page_info.get("hasNextPage"):
            cursor = page_info["endCursor"]
        else:
            break
        time.sleep(0.3)

    print(f"  Found {len(products)} products")

    # Collect metafield GIDs that need translation
    target_keys = ["custom.key_benefits_heading", "custom.key_benefits_content"]
    metafield_gids = []
    metafield_info = {}  # gid → {product_title, key, english, type}

    for prod in products:
        for key in target_keys:
            mf = prod["metafields"].get(key)
            if mf and mf["value"]:
                metafield_gids.append(mf["id"])
                metafield_info[mf["id"]] = {
                    "product_title": prod["title"],
                    "key": key,
                    "english": mf["value"],
                    "mf_type": mf["type"],
                }

    print(f"  Target metafields: {len(metafield_gids)}")

    if not metafield_gids:
        return 0, 0

    # Fetch translations
    digest_map = fetch_translatable_resources(client, metafield_gids)

    needs_translation = []
    for gid, info in metafield_info.items():
        if gid not in digest_map:
            continue
        dm = digest_map[gid]
        existing = dm["translations"].get("value", {}).get("value", "")
        english = dm["content"].get("value", {}).get("value", "")

        if not english:
            continue

        # For key_benefits_heading: check if it's still English
        if info["key"] == "custom.key_benefits_heading":
            if existing and _has_arabic(existing) and existing != english:
                continue  # already good
        # For key_benefits_content: check if JSON is valid and has Arabic
        elif info["key"] == "custom.key_benefits_content":
            if existing and _has_arabic(existing):
                # Check if JSON is valid
                try:
                    json.loads(existing)
                    continue  # valid JSON with Arabic — skip
                except (json.JSONDecodeError, TypeError):
                    pass  # corrupted JSON — re-translate

        needs_translation.append({
            "gid": gid,
            "english": english,
            "existing_ar": existing,
            "product_title": info["product_title"],
            "key": info["key"],
            "mf_type": info["mf_type"],
        })

    print(f"  Metafields needing fix: {len(needs_translation)}")

    for item in needs_translation[:10]:
        existing_preview = item['existing_ar'][:40] if item['existing_ar'] else "(none)"
        print(f"    [{item['product_title'][:30]}] {item['key']}: {existing_preview}")
    if len(needs_translation) > 10:
        print(f"    ... and {len(needs_translation) - 10} more")

    if dry_run:
        return 0, 0

    # Translate
    fields_for_ai = []
    for item in needs_translation:
        field_id = f"METAFIELD|{item['gid']}|{item['key']}"
        fields_for_ai.append({"id": field_id, "value": item["english"]})

    if fields_for_ai:
        t_map = translate_fields(fields_for_ai, developer_prompt, model)
    else:
        t_map = {}

    # Upload
    uploaded = 0
    errors = 0
    for item in needs_translation:
        field_id = f"METAFIELD|{item['gid']}|{item['key']}"
        ar_value = t_map.get(field_id)
        if not ar_value:
            continue

        # Sanitize JSON if rich_text
        if item["mf_type"] in ("rich_text_field", "json"):
            ar_value = sanitize_rich_text_json(ar_value)

        dm = digest_map[item["gid"]]
        digest = dm["content"]["value"]["digest"]

        translations_input = [{
            "locale": LOCALE,
            "key": "value",
            "value": ar_value,
            "translatableContentDigest": digest,
        }]

        u, e = upload_translations(client, item["gid"], translations_input)
        uploaded += u
        errors += e
        time.sleep(0.3)

    print(f"\n  Product metafields: uploaded={uploaded}, errors={errors}")
    return uploaded, errors


def fix_theme_translations(client, csv_path, dry_run=False):
    """Re-upload ONLINE_STORE_THEME translations from the CSV that may have failed."""
    print("\n=== RE-UPLOADING THEME TRANSLATIONS ===")

    if not csv_path or not os.path.exists(csv_path):
        print(f"  CSV not found: {csv_path}")
        print("  Provide --csv path to re-upload theme translations")
        return 0, 0

    # Read CSV and filter ONLINE_STORE_THEME rows with Arabic translations
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    theme_rows = []
    for row in rows:
        if row.get("Type", "").strip() != "ONLINE_STORE_THEME":
            continue
        translated = row.get("Translated content", "").strip()
        default = row.get("Default content", "").strip()
        if not translated or not default:
            continue
        if translated == default:
            continue
        theme_rows.append(row)

    print(f"  Theme translations in CSV: {len(theme_rows)}")

    if not theme_rows:
        return 0, 0

    # Group by resource GID
    resources = {}
    for row in theme_rows:
        identification = row.get("Identification", "").strip().lstrip("'")
        gid = f"gid://shopify/OnlineStoreThemeSettingValue/{identification}"
        field = row.get("Field", "").strip()

        if gid not in resources:
            resources[gid] = []
        resources[gid].append({
            "field": field,
            "translated": row.get("Translated content", "").strip(),
        })

    print(f"  Theme resources: {len(resources)}")

    if dry_run:
        for gid, fields in list(resources.items())[:5]:
            print(f"    {gid}: {len(fields)} fields")
        return 0, 0

    # Fetch digests and upload
    gid_list = list(resources.keys())
    uploaded = 0
    errors = 0

    for i in range(0, len(gid_list), 10):
        batch_gids = gid_list[i:i+10]
        batch_num = i // 10 + 1
        total_batches = (len(gid_list) + 9) // 10
        print(f"  Batch {batch_num}/{total_batches}...")

        try:
            data = client._graphql(FETCH_DIGESTS_QUERY, {
                "resourceIds": batch_gids,
                "first": len(batch_gids),
            })
        except Exception as e:
            print(f"    ERROR fetching digests: {e}")
            continue

        edges = data.get("translatableResourcesByIds", {}).get("edges", [])
        digest_map = {}
        for edge in edges:
            node = edge["node"]
            rid = node["resourceId"]
            digest_map[rid] = {
                tc["key"]: tc["digest"] for tc in node["translatableContent"]
            }

        for gid in batch_gids:
            if gid not in digest_map:
                continue

            translations_input = []
            for cf in resources[gid]:
                digest = digest_map[gid].get(cf["field"])
                if not digest:
                    continue
                translations_input.append({
                    "locale": LOCALE,
                    "key": cf["field"],
                    "value": cf["translated"],
                    "translatableContentDigest": digest,
                })

            if translations_input:
                u, e = upload_translations(client, gid, translations_input)
                uploaded += u
                errors += e

        time.sleep(0.5)

    print(f"\n  Theme: uploaded={uploaded}, errors={errors}")
    return uploaded, errors


def main():
    parser = argparse.ArgumentParser(
        description="Fix remaining untranslated Arabic fields")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview what would be fixed without making changes")
    parser.add_argument("--only", choices=["metaobjects", "products", "theme"],
                        help="Fix only one category")
    parser.add_argument("--model", default="gpt-4o-mini",
                        help="OpenAI model for translation (default: gpt-4o-mini)")
    parser.add_argument("--csv", default=None,
                        help="CSV file for theme re-upload (clean CSV)")
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ.get("SAUDI_SHOP_URL")
    token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not token:
        print("ERROR: Set SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN in .env")
        sys.exit(1)

    # Load developer prompt for translations
    developer_prompt = ""
    if os.path.exists(PROMPT_FILE):
        with open(PROMPT_FILE, "r", encoding="utf-8") as f:
            developer_prompt = f.read()
        print(f"Loaded developer prompt ({len(developer_prompt):,} chars)")
    else:
        print(f"WARNING: Developer prompt not found: {PROMPT_FILE}")
        print("Translations may be lower quality without it.")
        developer_prompt = (
            "You are a translation engine. Translate English to Arabic. "
            "Return TOON format only. Translate ALL ingredient names and "
            "category labels into Arabic. Keep INCI/scientific names as-is."
        )

    client = ShopifyClient(shop_url, token)

    total_uploaded = 0
    total_errors = 0

    if args.only is None or args.only == "metaobjects":
        u, e = fix_metaobjects(client, developer_prompt, args.model, args.dry_run)
        total_uploaded += u
        total_errors += e

    if args.only is None or args.only == "products":
        u, e = fix_product_metafields(client, developer_prompt, args.model, args.dry_run)
        total_uploaded += u
        total_errors += e

    if args.only is None or args.only == "theme":
        csv_path = args.csv
        if not csv_path:
            # Try to find the clean CSV
            candidates = [
                os.path.join(ARABIC_DIR, "Tara_Saudi_translations_Mar-10-2026_clean.csv"),
                os.path.join(ARABIC_DIR, "Tara_Saudi_translations_Mar-10-2026.csv"),
            ]
            for c in candidates:
                if os.path.exists(c):
                    csv_path = c
                    break
        u, e = fix_theme_translations(client, csv_path, args.dry_run)
        total_uploaded += u
        total_errors += e

    print(f"\n{'='*60}")
    print(f"  TOTAL: uploaded={total_uploaded}, errors={total_errors}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
