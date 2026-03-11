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


def _extract_text_from_json(json_str):
    """Extract all text values from rich_text JSON."""
    try:
        data = json.loads(json_str)
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


def _is_rich_text_json(value):
    """Check if a value is Shopify rich_text JSON."""
    if not value or not isinstance(value, str):
        return False
    s = value.strip()
    if not s.startswith("{"):
        return False
    try:
        data = json.loads(s)
        return isinstance(data, dict) and data.get("type") == "root"
    except (json.JSONDecodeError, TypeError):
        return False


def _extract_rich_text_texts(json_str):
    """Extract text values from rich_text JSON, returning (texts, parsed_data).

    Returns list of (path, text_value) tuples and the parsed JSON.
    Path is a list of indices to navigate back to each text node.
    """
    data = json.loads(json_str)
    texts = []

    def walk(node, path):
        if isinstance(node, dict):
            if node.get("type") == "text" and "value" in node:
                texts.append((list(path) + ["value"], node["value"]))
            for i, child in enumerate(node.get("children", [])):
                walk(child, path + ["children", i])
        elif isinstance(node, list):
            for i, item in enumerate(node):
                walk(item, path + [i])

    walk(data, [])
    return texts, data


def _rebuild_rich_text(parsed_data, translations):
    """Replace text values in parsed rich_text JSON using path→translation map.

    translations: dict of (tuple(path) → arabic_text)
    """
    import copy
    data = copy.deepcopy(parsed_data)
    for path, ar_text in translations.items():
        node = data
        for step in path[:-1]:
            node = node[step]
        node[path[-1]] = ar_text
    return json.dumps(data, ensure_ascii=False)


def _has_significant_english(text, threshold=0.15):
    """Return True if text has significant English (>threshold ratio of Latin alpha chars)."""
    if not text:
        return False
    # Strip INCI/scientific terms that are OK to keep in English
    stripped = text.strip()
    latin = len(re.findall(r"[a-zA-ZÀ-ÿ]", stripped))
    arabic = len(re.findall(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", stripped))
    total_alpha = latin + arabic
    if total_alpha == 0:
        return False
    return latin / total_alpha > threshold


def _call_translate_api(fields, developer_prompt, model, reasoning_effort):
    """Single API call to translate fields. Returns (t_map, tokens_used)."""
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

    kwargs = {
        "model": model,
        "instructions": developer_prompt,
        "input": user_message,
    }
    if model.startswith("o") or "nano" in model:
        kwargs["reasoning"] = {"effort": reasoning_effort}
    response = client.responses.create(**kwargs)

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

    tokens = response.usage.input_tokens + response.usage.output_tokens
    return t_map, tokens


def translate_fields(fields, developer_prompt, model="gpt-5-nano", reasoning_effort="minimal"):
    """Translate a list of {id, value} dicts using OpenAI.

    Retries missing fields up to 2 times to handle partial API responses.
    """
    all_translated = {}
    remaining = list(fields)
    max_rounds = 3

    for round_num in range(max_rounds):
        if not remaining:
            break

        print(f"    Translating {len(remaining)} fields ({model}, reasoning={reasoning_effort})...")

        try:
            t_map, tokens = _call_translate_api(remaining, developer_prompt, model, reasoning_effort)
        except Exception as e:
            print(f"    Error (round {round_num + 1}): {e}")
            if round_num < max_rounds - 1:
                time.sleep(2 ** (round_num + 1))
            continue

        all_translated.update(t_map)
        matched = len(set(f["id"] for f in remaining) & set(t_map.keys()))
        print(f"    Got {matched}/{len(remaining)} translations ({tokens} tokens)")

        # Check for missing fields and retry them
        missing = [f for f in remaining if f["id"] not in t_map]
        if not missing:
            break
        if round_num < max_rounds - 1:
            print(f"    Retrying {len(missing)} missing fields...")
            remaining = missing
            time.sleep(2)
        else:
            print(f"    WARNING: {len(missing)} fields not translated after {max_rounds} rounds")

    return all_translated


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
    """Upload translations for a single resource, chunking if needed.

    Theme resources have a stricter limit (~20 keys) than other resources (~50).
    """
    is_theme = "OnlineStoreTheme" in gid
    MAX_PER_REQUEST = 20 if is_theme else 50
    total_uploaded = 0
    total_errors = 0

    for chunk_start in range(0, len(translations_input), MAX_PER_REQUEST):
        chunk = translations_input[chunk_start:chunk_start + MAX_PER_REQUEST]
        try:
            result = client._graphql(REGISTER_TRANSLATIONS_MUTATION, {
                "resourceId": gid,
                "translations": chunk,
            })
            user_errors = result.get("translationsRegister", {}).get("userErrors", [])
            if user_errors:
                for ue in user_errors:
                    print(f"    ERROR {gid}: {ue['field']}: {ue['message']}")
                total_uploaded += len(chunk) - len(user_errors)
                total_errors += len(user_errors)
            else:
                total_uploaded += len(chunk)
        except Exception as e:
            print(f"    ERROR uploading {gid}: {e}")
            total_errors += len(chunk)
        if chunk_start + MAX_PER_REQUEST < len(translations_input):
            time.sleep(0.3)

    return total_uploaded, total_errors


def fix_metaobjects(client, developer_prompt, model, reasoning_effort="minimal", dry_run=False):
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

    t_map = translate_fields(fields_for_ai, developer_prompt, model, reasoning_effort)

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


def fix_product_metafields(client, developer_prompt, model, reasoning_effort="minimal", dry_run=False):
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
        # For key_benefits_content: check if JSON is valid, has Arabic,
        # AND doesn't have significant English mixed in (e.g. untranslated benefit names)
        elif info["key"] == "custom.key_benefits_content":
            if existing and _has_arabic(existing):
                try:
                    json.loads(existing)
                    # Check for mixed English — extract text and see if English ratio is high
                    extracted = _extract_text_from_json(existing)
                    if extracted and not _has_significant_english(extracted):
                        continue  # fully translated — skip
                    # Has significant English mixed in — re-translate
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
        t_map = translate_fields(fields_for_ai, developer_prompt, model, reasoning_effort)
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


FETCH_THEME_DIGESTS_QUERY = """
query($resourceId: ID!) {
  translatableResource(resourceId: $resourceId) {
    resourceId
    translatableContent {
      key
      value
      digest
      locale
    }
  }
}
"""


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

    theme_fields = []  # {field, translated, theme_id}
    theme_id = None
    for row in rows:
        if row.get("Type", "").strip() != "ONLINE_STORE_THEME":
            continue
        translated = row.get("Translated content", "").strip()
        default = row.get("Default content", "").strip()
        if not translated or not default:
            continue
        if translated == default:
            continue
        identification = row.get("Identification", "").strip().lstrip("'")
        theme_id = identification
        theme_fields.append({
            "field": row.get("Field", "").strip(),
            "translated": translated,
        })

    print(f"  Theme translations in CSV: {len(theme_fields)}")

    if not theme_fields or not theme_id:
        return 0, 0

    # The correct GID for theme translations
    theme_gid = f"gid://shopify/OnlineStoreTheme/{theme_id}"
    print(f"  Theme GID: {theme_gid}")

    if dry_run:
        for f in theme_fields[:10]:
            print(f"    {f['field'][:60]} → {f['translated'][:40]}")
        if len(theme_fields) > 10:
            print(f"    ... and {len(theme_fields) - 10} more")
        return 0, 0

    # Fetch all digests for this theme
    print("  Fetching theme digests...")
    digest_map = {}
    try:
        data = client._graphql(FETCH_THEME_DIGESTS_QUERY, {"resourceId": theme_gid})
        resource = data.get("translatableResource")
        if not resource:
            print(f"    Theme not found: {theme_gid}")
            return 0, 0
        for tc in resource.get("translatableContent", []):
            digest_map[tc["key"]] = tc["digest"]
    except Exception as e:
        print(f"    ERROR fetching theme digests: {e}")
        return 0, 0

    print(f"  Total theme keys with digests: {len(digest_map)}")

    # Match CSV fields to digests and upload in batches
    uploaded = 0
    errors = 0
    matched = 0
    unmatched = 0

    # Build translations in batches of 20 (Shopify limit per mutation)
    batch = []
    for f in theme_fields:
        digest = digest_map.get(f["field"])
        if not digest:
            unmatched += 1
            continue
        matched += 1
        batch.append({
            "locale": LOCALE,
            "key": f["field"],
            "value": f["translated"],
            "translatableContentDigest": digest,
        })

        if len(batch) >= 20:
            u, e = upload_translations(client, theme_gid, batch)
            uploaded += u
            errors += e
            batch = []
            time.sleep(0.3)

    # Upload remaining
    if batch:
        u, e = upload_translations(client, theme_gid, batch)
        uploaded += u
        errors += e

    print(f"\n  Theme: matched={matched}, unmatched={unmatched}, "
          f"uploaded={uploaded}, errors={errors}")
    return uploaded, errors


def _progress_path(audit_file):
    """Return progress file path for a given audit file."""
    base, ext = os.path.splitext(audit_file)
    return f"{base}_progress.json"


def _load_progress(audit_file):
    """Load set of already-uploaded field IDs from progress file."""
    path = _progress_path(audit_file)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("uploaded", []))
    return set()


def _save_progress(audit_file, uploaded_ids):
    """Save set of successfully uploaded field IDs."""
    path = _progress_path(audit_file)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"uploaded": sorted(uploaded_ids)}, f, indent=2)


def fix_from_audit(client, developer_prompt, audit_file, model, reasoning_effort, dry_run=False):
    """Fix all problems identified by audit_translations.py.

    Saves progress after each upload batch so re-runs skip already-uploaded fields.
    """
    print(f"\n=== FIXING FROM AUDIT: {audit_file} ===")

    with open(audit_file, "r", encoding="utf-8") as f:
        problems = json.load(f)

    # Load progress from previous runs
    done_ids = _load_progress(audit_file)
    if done_ids:
        print(f"  Resuming: {len(done_ids)} fields already uploaded (skipping)")

    print(f"  Total problems in audit: {len(problems)}")

    # Group by resource_id for efficient batching
    by_resource = {}
    for p in problems:
        rid = p["resource_id"]
        field_id = f"{p['resource_type']}|{rid}|{p['key']}"
        if field_id in done_ids:
            continue  # already uploaded in a previous run
        if rid not in by_resource:
            by_resource[rid] = []
        by_resource[rid].append(p)

    remaining = sum(len(v) for v in by_resource.values())
    print(f"  Remaining: {remaining} fields across {len(by_resource)} resources")

    if remaining == 0:
        print("  Nothing to fix — all fields already uploaded!")
        return 0, 0

    # Collect all fields that need translation
    # For rich_text JSON fields, extract text nodes and translate those separately
    fields_for_ai = []       # plain text fields
    rich_text_fields = {}    # field_id → {parsed_data, texts: [(path, english)]}

    for rid, items in by_resource.items():
        for item in items:
            field_id = f"{item['resource_type']}|{rid}|{item['key']}"
            english = item["english"]

            if _is_rich_text_json(english):
                # Extract text nodes from JSON, send each as separate translation unit
                texts, parsed = _extract_rich_text_texts(english)
                if texts:
                    rich_text_fields[field_id] = {
                        "parsed": parsed,
                        "texts": texts,
                        "_resource_id": rid,
                        "_key": item["key"],
                        "_status": item["status"],
                    }
                    for idx, (path, text_val) in enumerate(texts):
                        if text_val and text_val.strip():
                            sub_id = f"{field_id}__TEXT_{idx}"
                            fields_for_ai.append({
                                "id": sub_id,
                                "value": text_val,
                                "_resource_id": rid,
                                "_key": item["key"],
                                "_digest": item["digest"],
                                "_status": item["status"],
                                "_is_rich_text_part": True,
                            })
                continue

            fields_for_ai.append({
                "id": field_id,
                "value": english,
                "_resource_id": rid,
                "_key": item["key"],
                "_digest": item["digest"],
                "_status": item["status"],
            })

    plain_count = sum(1 for f in fields_for_ai if not f.get("_is_rich_text_part"))
    rt_count = len(rich_text_fields)
    rt_text_count = sum(1 for f in fields_for_ai if f.get("_is_rich_text_part"))
    print(f"  Fields to translate: {plain_count} plain + {rt_count} rich_text ({rt_text_count} text nodes)")

    if dry_run:
        for f in fields_for_ai[:20]:
            print(f"    [{f['_status']:15s}] {f['_resource_id'][:50]}")
            print(f"      {f['_key']}: {f['value'][:70]}")
        if len(fields_for_ai) > 20:
            print(f"    ... and {len(fields_for_ai) - 20} more")
        return 0, 0

    # Translate in batches (up to 100 fields at a time)
    t_map = {}
    batch_size = 80
    for i in range(0, len(fields_for_ai), batch_size):
        batch = fields_for_ai[i:i+batch_size]
        ai_batch = [{"id": f["id"], "value": f["value"]} for f in batch]
        batch_map = translate_fields(ai_batch, developer_prompt, model, reasoning_effort)
        t_map.update(batch_map)
        if i + batch_size < len(fields_for_ai):
            time.sleep(1)

    print(f"  Translated: {len(t_map)} fields")

    # Rebuild rich_text JSON with translated text nodes
    for field_id, rt_info in rich_text_fields.items():
        translations = {}
        all_found = True
        for idx, (path, text_val) in enumerate(rt_info["texts"]):
            sub_id = f"{field_id}__TEXT_{idx}"
            ar_text = t_map.get(sub_id)
            if ar_text:
                translations[tuple(path)] = ar_text
            elif text_val and text_val.strip():
                all_found = False
        if translations:
            rebuilt = _rebuild_rich_text(rt_info["parsed"], translations)
            t_map[field_id] = rebuilt
            if not all_found:
                print(f"    WARNING: Partial rich_text rebuild for {field_id}")

    # Fetch digests and upload, grouped by resource
    uploaded = 0
    errors = 0
    gid_list = list(by_resource.keys())

    for batch_start in range(0, len(gid_list), 10):
        batch_gids = gid_list[batch_start:batch_start + 10]
        batch_num = batch_start // 10 + 1
        total_batches = (len(gid_list) + 9) // 10

        if batch_num % 10 == 1:
            print(f"  Upload batch {batch_num}/{total_batches}...")

        # Fetch current digests
        digest_map = fetch_translatable_resources(client, batch_gids)

        for gid in batch_gids:
            if gid not in digest_map:
                continue

            dm = digest_map[gid]
            translations_input = []
            field_ids_in_batch = []

            for item in by_resource[gid]:
                field_id = f"{item['resource_type']}|{gid}|{item['key']}"
                ar_value = t_map.get(field_id)
                if not ar_value:
                    continue

                shopify_field = dm["content"].get(item["key"])
                if not shopify_field:
                    continue

                # Skip handle fields (cause conflicts with existing handles)
                if item["key"] == "handle":
                    continue

                # Validate JSON fields before uploading
                # Only treat as JSON if it looks like Shopify rich_text ({"type":...)
                # or a JSON array ([{...}]). Skip ICU/template strings like {count}.
                stripped_val = ar_value.strip()
                if stripped_val.startswith('{"type"') or stripped_val.startswith("[{"):
                    try:
                        parsed = json.loads(ar_value)
                        # Re-serialize to ensure clean JSON
                        ar_value = json.dumps(parsed, ensure_ascii=False)
                    except json.JSONDecodeError:
                        print(f"    WARNING: Skipping invalid JSON for {gid} {item['key']} ({len(ar_value)} chars)")
                        errors += 1
                        continue

                translations_input.append({
                    "locale": LOCALE,
                    "key": item["key"],
                    "value": ar_value,
                    "translatableContentDigest": shopify_field["digest"],
                })
                field_ids_in_batch.append(field_id)

            if translations_input:
                u, e = upload_translations(client, gid, translations_input)
                uploaded += u
                errors += e
                # Mark successfully uploaded fields in progress
                if e == 0:
                    done_ids.update(field_ids_in_batch)
                elif u > 0:
                    # Partial success — still save what we can
                    done_ids.update(field_ids_in_batch)
                _save_progress(audit_file, done_ids)

        time.sleep(0.3)

    print(f"\n  Audit fix: uploaded={uploaded}, errors={errors}")
    return uploaded, errors


def main():
    parser = argparse.ArgumentParser(
        description="Fix remaining untranslated Arabic fields")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview what would be fixed without making changes")
    parser.add_argument("--only", choices=["metaobjects", "products", "theme", "audit"],
                        help="Fix only one category")
    parser.add_argument("--audit", default=None,
                        help="Fix from audit JSON file (from audit_translations.py --fix-json)")
    parser.add_argument("--model", default="gpt-5-nano",
                        help="OpenAI model for translation (default: gpt-5-nano)")
    parser.add_argument("--reasoning", default="minimal",
                        choices=["minimal", "low", "medium", "high"],
                        help="Reasoning effort (default: minimal)")
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

    # Audit mode: fix from audit JSON
    if args.audit or args.only == "audit":
        audit_file = args.audit
        if not audit_file:
            audit_file = os.path.join(ARABIC_DIR, "audit_fix.json")
        if not os.path.exists(audit_file):
            print(f"ERROR: Audit file not found: {audit_file}")
            print("Run: python audit_translations.py --fix-json audit_fix.json")
            sys.exit(1)
        u, e = fix_from_audit(client, developer_prompt, audit_file,
                              args.model, args.reasoning, args.dry_run)
        total_uploaded += u
        total_errors += e
    else:
        if args.only is None or args.only == "metaobjects":
            u, e = fix_metaobjects(client, developer_prompt, args.model, args.reasoning, args.dry_run)
            total_uploaded += u
            total_errors += e

        if args.only is None or args.only == "products":
            u, e = fix_product_metafields(client, developer_prompt, args.model, args.reasoning, args.dry_run)
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
