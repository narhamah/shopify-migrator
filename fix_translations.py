#!/usr/bin/env python3
"""Fix untranslated fields on a Shopify store.

Generic translation fixer that works for any locale. Reads an audit JSON
(produced by audit_translations.py), translates missing/broken fields via
OpenAI, and uploads them via Shopify GraphQL.

Handles:
- Plain text fields (title, description, body_html, etc.)
- Rich_text JSON fields (decomposes → translates text nodes → rebuilds)
- Metaobject fields (ingredient names, FAQ, benefits, etc.)
- Product metafields (key_benefits, how_to_use, etc.)
- Theme translations (from CSV export)
- Batch size limits (Shopify max 100 per mutation)
- JSON validation before upload (skips truncated/corrupted JSON)

Usage:
    python fix_translations.py --audit Arabic/audit_fix.json --dry-run
    python fix_translations.py --audit Arabic/audit_fix.json
    python fix_translations.py --audit Arabic/audit_fix.json --model gpt-5-mini
    python fix_translations.py --only metaobjects
    python fix_translations.py --only theme --csv translations.csv
"""

import argparse
import csv
import json
import os
import sys
import time

from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.core.language import has_arabic, has_significant_english
from tara_migrate.core.rich_text import extract_text, is_rich_text_json
from tara_migrate.translation.engine import TranslationEngine, load_developer_prompt

# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

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
        translations(locale: "%LOCALE%") {
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


# ---------------------------------------------------------------------------
# Shopify API helpers
# ---------------------------------------------------------------------------

def fetch_translatable_resources(client, gids, locale):
    """Fetch digest map for a list of GIDs."""
    query = FETCH_DIGESTS_QUERY.replace("%LOCALE%", locale)
    digest_map = {}
    for i in range(0, len(gids), 10):
        batch = gids[i:i + 10]
        try:
            data = client._graphql(query, {
                "resourceIds": batch,
                "first": len(batch),
            })
            edges = data.get("translatableResourcesByIds", {}).get("edges", [])
            for edge in edges:
                node = edge["node"]
                rid = node["resourceId"]
                digest_map[rid] = {
                    "content": {
                        tc["key"]: {"digest": tc["digest"], "value": tc["value"]}
                        for tc in node["translatableContent"]
                    },
                    "translations": {
                        t["key"]: {"value": t["value"], "outdated": t["outdated"]}
                        for t in node["translations"]
                    },
                }
        except Exception as e:
            print(f"  Error fetching digests for batch: {e}")
        time.sleep(0.3)
    return digest_map


def upload_translations(client, gid, translations_input):
    """Upload translations for a single resource, chunking to Shopify's 100 limit."""
    MAX_PER_REQUEST = 100
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


def paginate_query(client, query, result_key, variables=None, page_size=50):
    """Generic paginated GraphQL query. Yields nodes."""
    cursor = None
    variables = dict(variables or {})
    variables["first"] = page_size
    while True:
        if cursor:
            variables["after"] = cursor
        elif "after" in variables:
            del variables["after"]
        try:
            data = client._graphql(query, variables)
        except Exception as e:
            print(f"  Error fetching {result_key}: {e}")
            break
        container = data.get(result_key, {})
        edges = container.get("edges", [])
        for edge in edges:
            yield edge["node"]
        page_info = container.get("pageInfo", {})
        if page_info.get("hasNextPage"):
            cursor = page_info["endCursor"]
        else:
            break
        time.sleep(0.3)


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Fix from audit JSON
# ---------------------------------------------------------------------------

def fix_from_audit(client, engine, locale, audit_file, dry_run=False):
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

    # Group by resource_id, skipping already-done fields
    by_resource = {}
    for p in problems:
        rid = p["resource_id"]
        field_id = f"{p['resource_type']}|{rid}|{p['key']}"
        if field_id in done_ids:
            continue
        if rid not in by_resource:
            by_resource[rid] = []
        by_resource[rid].append(p)

    remaining = sum(len(v) for v in by_resource.values())
    print(f"  Remaining: {remaining} fields across {len(by_resource)} resources")

    if remaining == 0:
        print("  Nothing to fix — all fields already uploaded!")
        return 0, 0

    # Build translation input — engine handles rich_text decomposition
    fields_for_ai = []
    for rid, items in by_resource.items():
        for item in items:
            # Skip handle fields (cause "already taken" conflicts)
            if item["key"] == "handle":
                continue
            field_id = f"{item['resource_type']}|{rid}|{item['key']}"
            fields_for_ai.append({
                "id": field_id,
                "value": item["english"],
            })

    print(f"  Fields to translate: {len(fields_for_ai)}")

    if dry_run:
        for f in fields_for_ai[:20]:
            print(f"    {f['id'][:70]}")
            print(f"      {f['value'][:70]}")
        if len(fields_for_ai) > 20:
            print(f"    ... and {len(fields_for_ai) - 20} more")
        return 0, 0

    # Translate (engine handles rich_text JSON safely)
    t_map = engine.translate_fields(fields_for_ai)
    print(f"  Translated: {len(t_map)} fields")

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

        digest_map = fetch_translatable_resources(client, batch_gids, locale)

        for gid in batch_gids:
            if gid not in digest_map:
                continue
            dm = digest_map[gid]
            translations_input = []
            field_ids_in_batch = []

            for item in by_resource[gid]:
                if item["key"] == "handle":
                    continue
                field_id = f"{item['resource_type']}|{gid}|{item['key']}"
                ar_value = t_map.get(field_id)
                if not ar_value:
                    continue

                shopify_field = dm["content"].get(item["key"])
                if not shopify_field:
                    continue

                # Validate JSON before uploading — only for actual JSON
                # (rich_text or arrays), not ICU/template strings like {count}
                stripped_val = ar_value.strip()
                if stripped_val.startswith('{"type"') or stripped_val.startswith("[{"):
                    try:
                        parsed = json.loads(ar_value)
                        ar_value = json.dumps(parsed, ensure_ascii=False)
                    except json.JSONDecodeError:
                        print(f"    WARNING: Skipping invalid JSON for "
                              f"{gid} {item['key']} ({len(ar_value)} chars)")
                        errors += 1
                        continue

                translations_input.append({
                    "locale": locale,
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
                if u > 0:
                    done_ids.update(field_ids_in_batch)
                    _save_progress(audit_file, done_ids)

        time.sleep(0.3)

    print(f"\n  Audit fix: uploaded={uploaded}, errors={errors}")
    return uploaded, errors


# ---------------------------------------------------------------------------
# Fix metaobjects
# ---------------------------------------------------------------------------

def fix_metaobjects(client, engine, locale, metaobject_types, dry_run=False):
    """Find and fix untranslated metaobject fields."""
    print("\n=== FIXING METAOBJECT TRANSLATIONS ===")

    all_metaobjects = {}
    for mo_type in metaobject_types:
        for node in paginate_query(client, FETCH_METAOBJECTS_QUERY, "metaobjects",
                                   {"type": mo_type}):
            fields_dict = {f["key"]: f["value"] for f in node["fields"]}
            all_metaobjects[node["id"]] = {
                "type": mo_type,
                "handle": node["handle"],
                "fields": fields_dict,
            }

    print(f"  Found {len(all_metaobjects)} metaobjects")

    gids = list(all_metaobjects.keys())
    digest_map = fetch_translatable_resources(client, gids, locale)

    # Find fields needing translation
    translatable_keys = [
        "name", "one_line_benefit", "description", "question",
        "answer", "concern", "type", "category", "source", "origin",
        "title", "bio",
    ]

    needs_translation = []
    for gid, mo in all_metaobjects.items():
        if gid not in digest_map:
            continue
        dm = digest_map[gid]
        for key in translatable_keys:
            if key not in dm["content"]:
                continue
            english = dm["content"][key]["value"]
            if not english or not english.strip():
                continue
            existing = dm["translations"].get(key, {}).get("value", "")
            if existing and has_arabic(existing) and existing != english:
                continue
            needs_translation.append({
                "gid": gid, "key": key, "english": english,
                "type": mo["type"], "handle": mo["handle"],
            })

    print(f"  Fields needing translation: {len(needs_translation)}")
    if not needs_translation:
        return 0, 0

    for item in needs_translation[:10]:
        print(f"    [{item['type']}] {item['handle']}.{item['key']}: "
              f"{item['english'][:50]}")
    if len(needs_translation) > 10:
        print(f"    ... and {len(needs_translation) - 10} more")

    if dry_run:
        return 0, 0

    fields_for_ai = [
        {"id": f"{it['type']}|{it['handle']}|{it['key']}", "value": it["english"]}
        for it in needs_translation
    ]
    t_map = engine.translate_fields(fields_for_ai)

    uploaded = 0
    errors = 0
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
            "locale": locale, "key": item["key"],
            "value": ar_value, "translatableContentDigest": digest,
        })

    for gid, translations_input in by_gid.items():
        u, e = upload_translations(client, gid, translations_input)
        uploaded += u
        errors += e
        time.sleep(0.3)

    print(f"\n  Metaobjects: uploaded={uploaded}, errors={errors}")
    return uploaded, errors


# ---------------------------------------------------------------------------
# Fix product metafields
# ---------------------------------------------------------------------------

def fix_product_metafields(client, engine, locale, dry_run=False):
    """Fix untranslated product metafields (key_benefits, how_to_use, etc.)."""
    print("\n=== FIXING PRODUCT METAFIELD TRANSLATIONS ===")

    products = []
    for node in paginate_query(client, FETCH_PRODUCTS_QUERY, "products"):
        metafields = {}
        for mf_edge in node.get("metafields", {}).get("edges", []):
            mf = mf_edge["node"]
            metafields[f"{mf['namespace']}.{mf['key']}"] = {
                "id": mf["id"], "value": mf["value"], "type": mf["type"],
            }
        products.append({
            "id": node["id"], "title": node["title"],
            "metafields": metafields,
        })

    print(f"  Found {len(products)} products")

    # Collect metafield GIDs needing translation
    metafield_gids = []
    metafield_info = {}
    for prod in products:
        for key, mf in prod["metafields"].items():
            if mf["value"]:
                metafield_gids.append(mf["id"])
                metafield_info[mf["id"]] = {
                    "product_title": prod["title"], "key": key,
                    "english": mf["value"], "mf_type": mf["type"],
                }

    print(f"  Total metafields: {len(metafield_gids)}")
    if not metafield_gids:
        return 0, 0

    digest_map = fetch_translatable_resources(client, metafield_gids, locale)

    needs_translation = []
    for gid, info in metafield_info.items():
        if gid not in digest_map:
            continue
        dm = digest_map[gid]
        existing = dm["translations"].get("value", {}).get("value", "")
        english = dm["content"].get("value", {}).get("value", "")
        if not english:
            continue

        if existing and has_arabic(existing) and existing != english:
            # For rich_text, also check for mixed English
            if is_rich_text_json(existing):
                extracted = extract_text(existing)
                if extracted and not has_significant_english(extracted):
                    continue
            else:
                continue

        needs_translation.append({
            "gid": gid, "english": english, "existing_ar": existing,
            "product_title": info["product_title"], "key": info["key"],
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

    fields_for_ai = [
        {"id": f"METAFIELD|{it['gid']}|{it['key']}", "value": it["english"]}
        for it in needs_translation
    ]
    t_map = engine.translate_fields(fields_for_ai) if fields_for_ai else {}

    uploaded = 0
    errors = 0
    for item in needs_translation:
        field_id = f"METAFIELD|{item['gid']}|{item['key']}"
        ar_value = t_map.get(field_id)
        if not ar_value:
            continue

        # Validate JSON — only for actual JSON (rich_text or arrays),
        # not ICU/template strings like {count}
        stripped_val = ar_value.strip()
        if stripped_val.startswith('{"type"') or stripped_val.startswith("[{"):
            try:
                parsed = json.loads(ar_value)
                ar_value = json.dumps(parsed, ensure_ascii=False)
            except json.JSONDecodeError:
                print(f"    WARNING: Skipping invalid JSON for {item['gid']}")
                errors += 1
                continue

        dm = digest_map[item["gid"]]
        digest = dm["content"]["value"]["digest"]
        u, e = upload_translations(client, item["gid"], [{
            "locale": locale, "key": "value",
            "value": ar_value, "translatableContentDigest": digest,
        }])
        uploaded += u
        errors += e
        time.sleep(0.3)

    print(f"\n  Product metafields: uploaded={uploaded}, errors={errors}")
    return uploaded, errors


# ---------------------------------------------------------------------------
# Fix theme translations from CSV
# ---------------------------------------------------------------------------

def fix_theme_translations(client, locale, csv_path, dry_run=False):
    """Re-upload theme translations from a Shopify CSV export."""
    print("\n=== RE-UPLOADING THEME TRANSLATIONS ===")

    if not csv_path or not os.path.exists(csv_path):
        print(f"  CSV not found: {csv_path}")
        return 0, 0

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    theme_fields = []
    theme_id = None
    for row in rows:
        if row.get("Type", "").strip() != "ONLINE_STORE_THEME":
            continue
        translated = row.get("Translated content", "").strip()
        default = row.get("Default content", "").strip()
        if not translated or not default or translated == default:
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

    theme_gid = f"gid://shopify/OnlineStoreTheme/{theme_id}"
    print(f"  Theme GID: {theme_gid}")

    if dry_run:
        for f in theme_fields[:10]:
            print(f"    {f['field'][:60]} -> {f['translated'][:40]}")
        if len(theme_fields) > 10:
            print(f"    ... and {len(theme_fields) - 10} more")
        return 0, 0

    # Fetch digests
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

    uploaded = 0
    errors = 0
    matched = 0
    unmatched = 0
    batch = []

    for f in theme_fields:
        digest = digest_map.get(f["field"])
        if not digest:
            unmatched += 1
            continue
        matched += 1
        batch.append({
            "locale": locale, "key": f["field"],
            "value": f["translated"], "translatableContentDigest": digest,
        })
        if len(batch) >= 20:
            u, e = upload_translations(client, theme_gid, batch)
            uploaded += u
            errors += e
            batch = []
            time.sleep(0.3)

    if batch:
        u, e = upload_translations(client, theme_gid, batch)
        uploaded += u
        errors += e

    print(f"\n  Theme: matched={matched}, unmatched={unmatched}, "
          f"uploaded={uploaded}, errors={errors}")
    return uploaded, errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fix untranslated fields on a Shopify store")
    parser.add_argument("--locale", default="ar",
                        help="Target locale code (default: ar)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview what would be fixed")
    parser.add_argument("--only", choices=["metaobjects", "products", "theme", "audit"],
                        help="Fix only one category")
    parser.add_argument("--audit", default=None,
                        help="Fix from audit JSON file")
    parser.add_argument("--model", default="gpt-5-nano",
                        help="OpenAI model (default: gpt-5-nano)")
    parser.add_argument("--reasoning", default="minimal",
                        choices=["minimal", "low", "medium", "high"],
                        help="Reasoning effort (default: minimal)")
    parser.add_argument("--batch-size", type=int, default=80,
                        help="Fields per translation batch (default: 80)")
    parser.add_argument("--csv", default=None,
                        help="CSV file for theme re-upload")
    parser.add_argument("--prompt", default=None,
                        help="Path to developer prompt file")
    parser.add_argument("--shop-url-env", default="SAUDI_SHOP_URL",
                        help="Env var name for shop URL (default: SAUDI_SHOP_URL)")
    parser.add_argument("--token-env", default="SAUDI_ACCESS_TOKEN",
                        help="Env var name for access token (default: SAUDI_ACCESS_TOKEN)")
    parser.add_argument("--metaobject-types", nargs="+",
                        default=["ingredient", "benefit", "faq_entry"],
                        help="Metaobject types to fix (default: ingredient benefit faq_entry)")
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ.get(args.shop_url_env)
    token = os.environ.get(args.token_env)
    if not shop_url or not token:
        print(f"ERROR: Set {args.shop_url_env} and {args.token_env} in .env")
        sys.exit(1)

    # Load developer prompt
    prompt_path = args.prompt
    if not prompt_path:
        # Look in common locations
        script_dir = os.path.dirname(__file__)
        candidates = [
            os.path.join(script_dir, "Arabic", "tara_cached_developer_prompt.txt"),
            os.path.join(script_dir, "developer_prompt.txt"),
        ]
        for c in candidates:
            if os.path.exists(c):
                prompt_path = c
                break

    developer_prompt = load_developer_prompt(prompt_path or "developer_prompt.txt")

    engine = TranslationEngine(
        developer_prompt,
        model=args.model,
        reasoning_effort=args.reasoning,
        batch_size=args.batch_size,
    )

    client = ShopifyClient(shop_url, token)
    total_uploaded = 0
    total_errors = 0

    if args.audit or args.only == "audit":
        audit_file = args.audit
        if not audit_file:
            print("ERROR: Provide --audit <path> to fix from audit JSON")
            sys.exit(1)
        if not os.path.exists(audit_file):
            print(f"ERROR: Audit file not found: {audit_file}")
            sys.exit(1)
        u, e = fix_from_audit(client, engine, args.locale, audit_file, args.dry_run)
        total_uploaded += u
        total_errors += e
    else:
        if args.only is None or args.only == "metaobjects":
            u, e = fix_metaobjects(client, engine, args.locale,
                                   args.metaobject_types, args.dry_run)
            total_uploaded += u
            total_errors += e

        if args.only is None or args.only == "products":
            u, e = fix_product_metafields(client, engine, args.locale, args.dry_run)
            total_uploaded += u
            total_errors += e

        if args.only is None or args.only == "theme":
            csv_path = args.csv
            if not csv_path:
                # Try to find a clean CSV
                script_dir = os.path.dirname(__file__)
                for f in os.listdir(os.path.join(script_dir, "Arabic")):
                    if f.endswith("_clean.csv") or f.endswith(".csv"):
                        csv_path = os.path.join(script_dir, "Arabic", f)
                        break
            u, e = fix_theme_translations(client, args.locale, csv_path, args.dry_run)
            total_uploaded += u
            total_errors += e

    print(f"\n{'=' * 60}")
    print(f"  TOTAL: uploaded={total_uploaded}, errors={total_errors}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
