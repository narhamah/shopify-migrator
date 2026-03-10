#!/usr/bin/env python3
"""Step 5: Import Arabic translations into the Saudi Shopify store.

Uses the Shopify Translations API to register Arabic translations for all
resources on the Saudi store. Queries the store for translatable resources,
matches against local Arabic data, and optionally uses AI (TOON batch)
to fill gaps.

Usage:
    python import_arabic.py                          # Local data only
    python import_arabic.py --dry-run                # Preview without API calls
    python import_arabic.py --ai-fallback            # Fill gaps with AI
    python import_arabic.py --resource-type PRODUCT  # Single resource type
    python import_arabic.py --replace-images         # OCR-based image replacement

Prerequisites:
  - data/arabic/_translation_progress_ar.json — flat key-value Arabic translations
  - data/arabic/*.json — full Arabic content (body_html, images, etc.)
  - The Saudi store must have Arabic (ar) enabled as a locale
"""

import argparse
import os

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import load_json, sanitize_rich_text_json, save_json
from tara_migrate.core.config import AR_DIR, EN_DIR, ID_MAP_FILE

ARABIC_LOCALE = "ar"

# Resource type → (GID prefix, progress prefix, type_prefix in progress file)
RESOURCE_TYPE_CONFIG = {
    "PRODUCT": ("gid://shopify/Product/", "product", "prod"),
    "COLLECTION": ("gid://shopify/Collection/", "collection", "coll"),
    "ONLINE_STORE_PAGE": ("gid://shopify/Page/", "page", "page"),
    "ONLINE_STORE_ARTICLE": ("gid://shopify/Article/", "article", "art"),
    "ONLINE_STORE_BLOG": ("gid://shopify/Blog/", "blog", "blog"),
    "METAOBJECT": ("gid://shopify/Metaobject/", "metaobject", "mo"),
}


# =====================================================================
# Field builders — extract Arabic fields from full JSON objects
# =====================================================================

def build_product_arabic_fields(ar_product):
    """Extract translatable fields from a full Arabic product JSON."""
    fields = {
        "title": ar_product.get("title", ""),
        "body_html": ar_product.get("body_html", ""),
        "handle": ar_product.get("handle", ""),
        "product_type": ar_product.get("product_type", ""),
    }
    if ar_product.get("metafields"):
        for mf in ar_product["metafields"]:
            mf_type = mf.get("type", "")
            if "reference" in mf_type:
                continue
            ns = mf.get("namespace", "")
            key = f"{ns}.{mf['key']}" if ns != "custom" else f"custom.{mf['key']}"
            value = mf.get("value", "")
            if "rich_text" in mf_type or (isinstance(value, str) and value.strip().startswith('{"type":"root"')):
                value = sanitize_rich_text_json(value)
            fields[key] = value
    # Options/variants
    for idx, opt in enumerate(ar_product.get("options", [])):
        if opt.get("name"):
            fields[f"option{idx+1}.name"] = opt["name"]
    return fields


def build_collection_arabic_fields(ar_coll):
    """Extract translatable fields from a full Arabic collection JSON."""
    return {
        "title": ar_coll.get("title", ""),
        "body_html": ar_coll.get("body_html", ""),
        "handle": ar_coll.get("handle", ""),
    }


def build_page_arabic_fields(ar_page):
    """Extract translatable fields from a full Arabic page JSON."""
    return {
        "title": ar_page.get("title", ""),
        "body_html": ar_page.get("body_html", ""),
        "handle": ar_page.get("handle", ""),
    }


def build_article_arabic_fields(ar_art):
    """Extract translatable fields from a full Arabic article JSON."""
    fields = {
        "title": ar_art.get("title", ""),
        "body_html": ar_art.get("body_html", ""),
        "handle": ar_art.get("handle", ""),
        "summary_html": ar_art.get("summary_html", ""),
        "author": ar_art.get("author", ""),
        "tags": ar_art.get("tags", ""),
    }
    if ar_art.get("metafields"):
        for mf in ar_art["metafields"]:
            mf_type = mf.get("type", "")
            if "reference" in mf_type:
                continue
            ns = mf.get("namespace", "")
            key = f"{ns}.{mf['key']}" if ns != "custom" else f"custom.{mf['key']}"
            value = mf.get("value", "")
            if "rich_text" in mf_type or (isinstance(value, str) and value.strip().startswith('{"type":"root"')):
                value = sanitize_rich_text_json(value)
            fields[key] = value
    return fields


def build_blog_arabic_fields(ar_blog):
    """Extract translatable fields from a full Arabic blog JSON."""
    return {
        "title": ar_blog.get("title", ""),
        "handle": ar_blog.get("handle", ""),
    }


def build_metaobject_arabic_fields(ar_obj):
    """Extract translatable fields from a full Arabic metaobject JSON."""
    fields = {}
    if ar_obj.get("handle"):
        fields["handle"] = ar_obj["handle"]
    for field in ar_obj.get("fields", []):
        if field.get("value"):
            fields[field["key"]] = field["value"]
    return fields


# Map of type_prefix → field builder for full JSON fallback
FIELD_BUILDERS = {
    "prod": ("products.json", build_product_arabic_fields),
    "coll": ("collections.json", build_collection_arabic_fields),
    "page": ("pages.json", build_page_arabic_fields),
    "art": ("articles.json", build_article_arabic_fields),
    "blog": ("blogs.json", build_blog_arabic_fields),
}


# =====================================================================
# Local lookup builder
# =====================================================================

def build_local_lookup(progress_ar, type_prefix, ar_items=None, field_builder=None):
    """Build {english_handle: {field_key: arabic_value}} from progress + full JSON.

    Args:
        progress_ar: dict from _translation_progress_ar.json
        type_prefix: "prod", "coll", "page", "art", "blog", "mo"
        ar_items: optional list of full Arabic JSON objects (for body_html fallback)
        field_builder: function to extract fields from full JSON objects
    """
    lookup = {}
    # 1. From progress file: group by handle
    prefix = f"{type_prefix}."
    for key, value in progress_ar.items():
        if not key.startswith(prefix):
            continue
        rest = key[len(prefix):]
        parts = rest.split(".", 1)
        if len(parts) == 2:
            handle, field = parts
            lookup.setdefault(handle, {})[field] = value

    # 2. From full JSON: add body_html and other large fields not in progress file
    if ar_items and field_builder:
        for item in ar_items:
            handle = item.get("handle", "")
            if not handle:
                continue
            extra = field_builder(item)
            if handle in lookup:
                for k, v in extra.items():
                    if k not in lookup[handle]:  # progress file takes priority
                        lookup[handle][k] = v
            else:
                lookup[handle] = extra
    return lookup


def build_metaobject_lookup(progress_ar, ar_metaobjects):
    """Build metaobject lookup from progress file + full JSON.

    Metaobject keys use: mo.{type}.{handle}.{field}

    Returns: {dest_gid: {field_key: arabic_value}}
    """
    # First build {type.handle: {field: value}} from progress
    lookup_by_type_handle = {}
    prefix = "mo."
    for key, value in progress_ar.items():
        if not key.startswith(prefix):
            continue
        rest = key[len(prefix):]
        # mo.shopify--suitable-for-hair-type.damaged.label
        # Split into: type=shopify--suitable-for-hair-type, handle=damaged, field=label
        parts = rest.split(".", 2)
        if len(parts) == 3:
            mo_type, handle, field = parts
            composite_key = f"{mo_type}.{handle}"
            lookup_by_type_handle.setdefault(composite_key, {})[field] = value

    # Also add from full JSON (for fields not in progress)
    if ar_metaobjects:
        for mo_type, type_data in ar_metaobjects.items():
            for obj in type_data.get("objects", []):
                handle = obj.get("handle", "")
                if not handle:
                    continue
                composite_key = f"{mo_type}.{handle}"
                extra = build_metaobject_arabic_fields(obj)
                if composite_key in lookup_by_type_handle:
                    for k, v in extra.items():
                        if k not in lookup_by_type_handle[composite_key]:
                            lookup_by_type_handle[composite_key][k] = v
                else:
                    lookup_by_type_handle[composite_key] = extra

    return lookup_by_type_handle


# =====================================================================
# Translation input builder
# =====================================================================

def build_translation_inputs(translatable_content, arabic_fields):
    """Match Arabic translations to their translatable content digests.

    Args:
        translatable_content: List from Shopify's translatableContent
            (each has: key, value, digest, locale)
        arabic_fields: Dict of {key: arabic_value} to register
    Returns:
        List of TranslationInput dicts ready for translationsRegister
    """
    translations = []
    for tc in translatable_content:
        key = tc["key"]
        if key in arabic_fields and arabic_fields[key]:
            translations.append({
                "key": key,
                "value": arabic_fields[key],
                "locale": ARABIC_LOCALE,
                "translatableContentDigest": tc["digest"],
            })
    return translations


# =====================================================================
# Generic resource processor
# =====================================================================

def _extract_handle_from_resource(resource):
    """Extract the English handle from a translatable resource."""
    for tc in resource.get("translatableContent", []):
        if tc["key"] == "handle":
            return tc.get("value", "")
    return ""


def _should_translate_field(key, en_value):
    """Check if a translatable field should be translated.

    Skips empty values, handles (which cause conflicts), and reference fields.
    """
    if not en_value or not en_value.strip():
        return False
    # Skip handle fields — they cause "already taken" errors
    if key == "handle":
        return False
    # Skip fields that are just IDs or references
    if en_value.startswith("gid://"):
        return False
    return True


def process_resource_type(
    client, resource_type, lookup, progress, progress_file,
    openai_client=None, model="gpt-5-mini", dry_run=False, ai_fallback=False,
):
    """Process a single resource type: query ALL store resources, translate everything.

    Store-first approach:
      1. Query the store for ALL translatable resources of this type
      2. For each resource, check each translatable field:
         - If local Arabic data exists → use it
         - If not and AI fallback enabled → add to AI batch
      3. Batch AI-translate all gaps
      4. Register all translations

    Args:
        client: ShopifyClient instance (None for dry run)
        resource_type: e.g. "PRODUCT", "COLLECTION"
        lookup: {handle: {field: arabic_value}} for this resource type
        progress: progress dict (mutated in place)
        progress_file: path to save progress
        openai_client: OpenAI client (for AI fallback)
        model: AI model name
        dry_run: if True, don't make API calls
        ai_fallback: if True, use AI for missing translations
    """
    config = RESOURCE_TYPE_CONFIG[resource_type]
    _, progress_prefix, _ = config

    print(f"\n{'='*60}")
    print(f"Processing {resource_type} translations...")
    print(f"{'='*60}")
    print(f"  Local lookup: {len(lookup)} entries")

    if dry_run:
        total_fields = sum(len(v) for v in lookup.values())
        print(f"  Total local fields available: {total_fields}")
        matched = 0
        for handle, fields in lookup.items():
            filled = sum(1 for v in fields.values() if v)
            if filled:
                matched += 1
                print(f"    {handle}: {filled} fields")
        print(f"  Handles with data: {matched}/{len(lookup)}")
        return

    # Metaobjects have special GID resolution via id_map
    if resource_type == "METAOBJECT":
        _process_metaobjects(client, lookup, progress, progress_file,
                             openai_client, model, ai_fallback)
        return

    # ---- Store-first: query ALL translatable resources from the store ----
    print(f"  Fetching all {resource_type} resources from store...")
    all_resources = client.get_translatable_resources(resource_type)
    total = len(all_resources)
    print(f"  Found {total} translatable resources on store")

    gaps = []  # fields needing AI translation
    done_count = 0
    skip_count = 0
    local_count = 0

    for i, resource in enumerate(all_resources):
        gid = resource["resourceId"]
        # Extract numeric ID from GID for progress tracking
        resource_id = gid.rsplit("/", 1)[-1]
        progress_key = f"{progress_prefix}_{resource_id}"

        if progress_key in progress:
            skip_count += 1
            continue

        tc = resource.get("translatableContent", [])
        handle = _extract_handle_from_resource(resource)
        label = f"  [{i+1}/{total}] {handle or resource_id}"

        # Look up local Arabic data by handle
        ar_fields = lookup.get(handle, {}) if handle else {}

        # Build translation inputs: local data for known fields
        local_translations = []
        for item in tc:
            key = item["key"]
            en_value = item.get("value", "")

            if not _should_translate_field(key, en_value):
                continue

            if key in ar_fields and ar_fields[key]:
                # Have local Arabic data
                local_translations.append({
                    "key": key,
                    "value": ar_fields[key],
                    "locale": ARABIC_LOCALE,
                    "translatableContentDigest": item["digest"],
                })
            elif ai_fallback and openai_client:
                # No local data — add to AI gaps
                gaps.append({
                    "id": f"{gid}|{key}",
                    "value": en_value,
                    "_digest": item["digest"],
                })

        # Register local translations immediately
        try:
            if local_translations:
                client.register_translations(gid, ARABIC_LOCALE, local_translations)
                local_count += len(local_translations)
                print(f"{label} — registered {len(local_translations)} local translations")
            elif not (ai_fallback and openai_client):
                # No local data and no AI — nothing to do
                print(f"{label} — no local Arabic data")

            # Product image alt text
            if resource_type == "PRODUCT" and handle:
                img_count = _register_product_image_alts(
                    client, resource_id, handle, lookup
                )
                if img_count:
                    print(f"    + {img_count} image alts")

            progress[progress_key] = True
            done_count += 1
            save_json(progress, progress_file)
        except Exception as e:
            print(f"{label} — error: {e}")

    # ---- Pass 2: AI batch translate all gaps ----
    if gaps and ai_fallback and openai_client:
        print(f"\n  AI fallback: {len(gaps)} fields to translate...")
        # Extract just id+value for the translation batch
        gap_inputs = [{"id": g["id"], "value": g["value"]} for g in gaps]
        ai_translations = _translate_gaps_batch(openai_client, model, gap_inputs)

        # Build digest lookup for registration
        digest_by_gap_id = {g["id"]: g["_digest"] for g in gaps}

        # Group AI translations by GID for efficient registration
        by_gid = {}
        for gap_id, translated_value in ai_translations.items():
            gid, key = gap_id.split("|", 1)
            digest = digest_by_gap_id.get(gap_id, "")
            by_gid.setdefault(gid, []).append({
                "key": key,
                "value": translated_value,
                "locale": ARABIC_LOCALE,
                "translatableContentDigest": digest,
            })

        ai_registered = 0
        ai_errors = 0
        for gid, translations in by_gid.items():
            try:
                client.register_translations(gid, ARABIC_LOCALE, translations)
                ai_registered += len(translations)
            except Exception as e:
                ai_errors += 1
                print(f"    AI registration error for {gid}: {e}")
        print(f"  AI fallback: registered {ai_registered} translations ({ai_errors} errors)")
    elif gaps:
        print(f"\n  {len(gaps)} fields need translation but AI fallback not enabled")

    if skip_count:
        print(f"  Skipped {skip_count} already-done resources")
    print(f"  Completed: {done_count} resources, {local_count} local translations")


def _process_metaobjects(client, lookup, progress, progress_file,
                         openai_client, model, ai_fallback):
    """Process ALL metaobject translations — store-first approach.

    Queries the store for all METAOBJECT translatable resources, then
    matches against local data. AI-translates any gaps.
    """
    print("  Fetching all METAOBJECT resources from store...")
    all_resources = client.get_translatable_resources("METAOBJECT")
    total = len(all_resources)
    print(f"  Found {total} translatable metaobjects on store")

    gaps = []
    total_registered = 0
    skip_count = 0
    done_count = 0

    for i, resource in enumerate(all_resources):
        gid = resource["resourceId"]
        resource_id = gid.rsplit("/", 1)[-1]
        progress_key = f"metaobject_{gid}"

        # Also check old-style progress keys
        if progress_key in progress or f"metaobject_{resource_id}" in progress:
            skip_count += 1
            continue

        tc = resource.get("translatableContent", [])
        handle = _extract_handle_from_resource(resource)

        # Try to determine the metaobject type from GID or content
        # The lookup uses composite keys like "shopify--suitable-for-hair-type.damaged"
        # Try matching by handle across all type prefixes in lookup
        ar_fields = {}
        for composite_key, fields in lookup.items():
            if composite_key.endswith(f".{handle}"):
                ar_fields = fields
                break

        label = f"  [{i+1}/{total}] {handle or resource_id}"

        # Build translation inputs: local data + gaps
        local_translations = []
        for item in tc:
            key = item["key"]
            en_value = item.get("value", "")

            if not _should_translate_field(key, en_value):
                continue

            if key in ar_fields and ar_fields[key]:
                local_translations.append({
                    "key": key,
                    "value": ar_fields[key],
                    "locale": ARABIC_LOCALE,
                    "translatableContentDigest": item["digest"],
                })
            elif ai_fallback and openai_client:
                gaps.append({
                    "id": f"{gid}|{key}",
                    "value": en_value,
                    "_digest": item["digest"],
                })

        try:
            if local_translations:
                client.register_translations(gid, ARABIC_LOCALE, local_translations)
                total_registered += len(local_translations)
                print(f"{label} — registered {len(local_translations)}")

            progress[progress_key] = True
            done_count += 1
            save_json(progress, progress_file)
        except Exception as e:
            print(f"{label} — error: {e}")

    # AI fallback for metaobject gaps
    if gaps and ai_fallback and openai_client:
        print(f"\n  AI fallback for metaobjects: {len(gaps)} fields...")
        gap_inputs = [{"id": g["id"], "value": g["value"]} for g in gaps]
        ai_translations = _translate_gaps_batch(openai_client, model, gap_inputs)

        digest_by_gap_id = {g["id"]: g["_digest"] for g in gaps}

        by_gid = {}
        for gap_id, translated_value in ai_translations.items():
            gid, key = gap_id.split("|", 1)
            digest = digest_by_gap_id.get(gap_id, "")
            by_gid.setdefault(gid, []).append({
                "key": key,
                "value": translated_value,
                "locale": ARABIC_LOCALE,
                "translatableContentDigest": digest,
            })

        ai_registered = 0
        for gid, translations in by_gid.items():
            try:
                client.register_translations(gid, ARABIC_LOCALE, translations)
                ai_registered += len(translations)
            except Exception as e:
                print(f"    AI error for {gid}: {e}")
        print(f"  AI fallback: registered {ai_registered} metaobject translations")
    elif gaps:
        print(f"\n  {len(gaps)} metaobject fields need translation but AI fallback not enabled")

    if skip_count:
        print(f"  Skipped {skip_count} already-done metaobjects")
    print(f"  Completed: {done_count} resources, {total_registered} local translations")


# =====================================================================
# Product image alt text
# =====================================================================

def _register_product_image_alts(client, dest_id, handle, lookup):
    """Register Arabic alt text for product images.

    Returns number of image alts registered.
    """
    ar_fields = lookup.get(handle, {})
    # Check if there are image alt entries in the full Arabic data
    ar_products = load_json(os.path.join(AR_DIR, "products.json"))
    ar_by_handle = {p.get("handle", ""): p for p in (ar_products if isinstance(ar_products, list) else [])}

    # Try matching by handle in Arabic data
    ar_product = ar_by_handle.get(handle)
    if not ar_product:
        return 0

    ar_image_alts = []
    for img in ar_product.get("images", []):
        alt = img.get("alt", "")
        ar_image_alts.append(alt)

    if not any(ar_image_alts):
        return 0

    try:
        img_resp = client._request("GET", f"products/{dest_id}.json",
                                   params={"fields": "id,images"})
        shopify_images = img_resp.json().get("product", {}).get("images", [])
        img_translated = 0
        for idx, shopify_img in enumerate(shopify_images):
            if idx >= len(ar_image_alts) or not ar_image_alts[idx]:
                continue
            img_gid = f"gid://shopify/ProductImage/{shopify_img['id']}"
            img_resource = client.get_translatable_resource(img_gid)
            if img_resource and img_resource.get("translatableContent"):
                img_translations = build_translation_inputs(
                    img_resource["translatableContent"],
                    {"alt": ar_image_alts[idx]}
                )
                if img_translations:
                    client.register_translations(img_gid, ARABIC_LOCALE, img_translations)
                    img_translated += 1
        return img_translated
    except Exception as e:
        print(f"    Image alt error for {handle}: {e}")
        return 0


# =====================================================================
# AI translation via TOON batch
# =====================================================================

def _translate_gaps_batch(openai_client, model, gaps):
    """Translate a list of gaps using TOON batch format.

    Args:
        openai_client: OpenAI client instance
        model: model name (e.g. "gpt-5-mini")
        gaps: list of {"id": "gid|key", "value": "english text"}

    Returns:
        dict of {gap_id: translated_value}
    """
    from tara_migrate.translation.translate_gaps import (
        adaptive_batch,
        translate_batch,
    )

    batches = adaptive_batch(gaps)
    all_translations = {}
    total_batches = len(batches)

    print(f"    Translating {len(gaps)} gaps in {total_batches} batches...")
    for i, batch in enumerate(batches):
        t_map, tokens = translate_batch(
            openai_client, model, batch,
            "English", "Arabic",
            i + 1, total_batches,
        )
        all_translations.update(t_map)

    return all_translations


# =====================================================================
# Image language audit & replacement (OCR-driven)
# =====================================================================

def _run_image_replacement(client, dry_run=False):
    """OCR-scan all product images and replace wrong-language ones.

    Downloads each product image from the Saudi store, runs OCR to detect
    text language, and replaces images with English/Spanish text using
    the Arabic version from taraformula.ae.
    """
    from tara_migrate.tools.image_lang_detect import classify_image_language

    ar_products = load_json(os.path.join(AR_DIR, "products.json"))
    if not isinstance(ar_products, list):
        print("  No Arabic product data for image replacement")
        return

    ar_by_handle = {p.get("handle", ""): p for p in ar_products}
    en_products = load_json(os.path.join(EN_DIR, "products.json"))
    en_by_handle = {p.get("handle", ""): p for p in (en_products if isinstance(en_products, list) else [])}

    report = {
        "total_images_scanned": 0,
        "no_text": 0,
        "already_arabic": 0,
        "replaced_from_en": 0,
        "replaced_from_es": 0,
        "both_wrong": 0,
        "errors": 0,
    }

    # Get all products from the store
    id_map = load_json(ID_MAP_FILE)
    products_map = id_map.get("products", {})

    print(f"\n{'='*60}")
    print("Image Language Audit & Replacement (OCR)")
    print(f"{'='*60}")

    import requests

    for handle, ar_product in ar_by_handle.items():
        en_product = en_by_handle.get(handle)
        if not en_product:
            continue

        source_id = str(en_product.get("id", ""))
        dest_id = products_map.get(source_id)
        if not dest_id:
            continue

        ar_images = ar_product.get("images", [])
        if not ar_images:
            continue

        # Get current images from Saudi store
        try:
            img_resp = client._request("GET", f"products/{dest_id}.json",
                                       params={"fields": "id,images,handle"})
            store_product = img_resp.json().get("product", {})
            store_images = store_product.get("images", [])
        except Exception as e:
            print(f"  {handle}: error fetching images: {e}")
            report["errors"] += 1
            continue

        for idx, store_img in enumerate(store_images):
            report["total_images_scanned"] += 1
            store_img_url = store_img.get("src", "")
            if not store_img_url:
                continue

            # Download current image from store
            try:
                resp = requests.get(store_img_url, timeout=30)
                resp.raise_for_status()
                current_bytes = resp.content
            except Exception as e:
                print(f"  {handle} img{idx+1}: download error: {e}")
                report["errors"] += 1
                continue

            # OCR classify
            current_lang = classify_image_language(current_bytes)

            if current_lang is None:
                report["no_text"] += 1
                continue
            if current_lang == "ar":
                report["already_arabic"] += 1
                continue

            # Needs replacement — current has en/es text
            if idx >= len(ar_images):
                print(f"  {handle} img{idx+1}: no Arabic image at this position")
                continue

            ar_img_url = ar_images[idx].get("src", "")
            if not ar_img_url:
                continue

            print(f"  {handle} img{idx+1}: detected '{current_lang}' text, replacing...")

            if dry_run:
                if current_lang == "es":
                    report["replaced_from_es"] += 1
                else:
                    report["replaced_from_en"] += 1
                continue

            # Download Arabic candidate
            try:
                ar_resp = requests.get(ar_img_url, timeout=30)
                ar_resp.raise_for_status()
                ar_bytes = ar_resp.content
            except Exception as e:
                print(f"    Download error from taraformula.ae: {e}")
                report["errors"] += 1
                continue

            # Verify Arabic candidate
            candidate_lang = classify_image_language(ar_bytes)
            if candidate_lang in ("en", "es"):
                print(f"    WARNING: Arabic candidate also has '{candidate_lang}' text, skipping")
                report["both_wrong"] += 1
                continue

            # Optimize and upload
            try:
                from tara_migrate.pipeline.image_helpers import download_and_optimize
                optimized = download_and_optimize(ar_img_url, preset="product")
                if optimized:
                    filename = f"{handle}_pdp_{idx+1}_ar.webp"
                    client.upload_file_bytes(optimized, filename, "IMAGE")
                    if current_lang == "es":
                        report["replaced_from_es"] += 1
                    else:
                        report["replaced_from_en"] += 1
            except Exception as e:
                print(f"    Upload error: {e}")
                report["errors"] += 1

    # Save report
    save_json(report, "data/image_language_audit.json")
    print("\n  Image audit complete:")
    for k, v in report.items():
        print(f"    {k}: {v}")


# =====================================================================
# Main
# =====================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Import Arabic translations into Saudi Shopify store"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be translated without making API calls")
    parser.add_argument("--ai-fallback", action="store_true",
                        help="Use AI (TOON batch) to translate missing fields")
    parser.add_argument("--no-ai-fallback", action="store_true",
                        help="Disable AI fallback (default behavior)")
    parser.add_argument("--model", default="gpt-5-mini",
                        help="OpenAI model for AI translation (default: gpt-5-mini)")
    parser.add_argument("--resource-type", type=str, default=None,
                        choices=list(RESOURCE_TYPE_CONFIG.keys()),
                        help="Only process a single resource type")
    parser.add_argument("--replace-images", action="store_true",
                        help="OCR-scan and replace wrong-language product images")
    args = parser.parse_args()

    load_dotenv()
    progress_file = "data/arabic_import_progress.json"
    progress = load_json(progress_file) if os.path.exists(progress_file) else {}

    # Load local Arabic translation data
    progress_ar_file = os.path.join(AR_DIR, "_translation_progress_ar.json")
    progress_ar = load_json(progress_ar_file) if os.path.exists(progress_ar_file) else {}

    if args.dry_run:
        print("=== DRY RUN MODE — no API calls will be made ===\n")
        client = None
    else:
        shop_url = os.environ["SAUDI_SHOP_URL"]
        access_token = os.environ["SAUDI_ACCESS_TOKEN"]
        client = ShopifyClient(shop_url, access_token)

    # Set up AI client if needed
    openai_client = None
    if args.ai_fallback and not args.no_ai_fallback:
        api_key = os.environ.get("OPENAI_API_KEY")
        if api_key:
            from openai import OpenAI
            openai_client = OpenAI(api_key=api_key)
            print(f"AI fallback enabled (model: {args.model})")
        else:
            print("WARNING: --ai-fallback specified but OPENAI_API_KEY not set")

    # Build local lookups for each resource type
    ar_metaobjects = load_json(os.path.join(AR_DIR, "metaobjects.json"))
    metaobject_lookup = build_metaobject_lookup(
        progress_ar, ar_metaobjects if isinstance(ar_metaobjects, dict) else {}
    )

    lookups = {}
    for type_prefix, (filename, builder) in FIELD_BUILDERS.items():
        ar_path = os.path.join(AR_DIR, filename)
        ar_items = load_json(ar_path) if os.path.exists(ar_path) else []
        if isinstance(ar_items, dict):
            ar_items = []
        lookups[type_prefix] = build_local_lookup(
            progress_ar, type_prefix, ar_items, builder
        )

    # Process each resource type
    resource_types = [
        ("PRODUCT", lookups.get("prod", {})),
        ("COLLECTION", lookups.get("coll", {})),
        ("ONLINE_STORE_PAGE", lookups.get("page", {})),
        ("ONLINE_STORE_ARTICLE", lookups.get("art", {})),
        ("ONLINE_STORE_BLOG", lookups.get("blog", {})),
        ("METAOBJECT", metaobject_lookup),
    ]

    for resource_type, lookup in resource_types:
        if args.resource_type and args.resource_type != resource_type:
            continue

        process_resource_type(
            client, resource_type, lookup,
            progress, progress_file,
            openai_client=openai_client,
            model=args.model,
            dry_run=args.dry_run,
            ai_fallback=args.ai_fallback and not args.no_ai_fallback,
        )

    # Image replacement (separate pass)
    if args.replace_images:
        if args.dry_run:
            print("\n  [dry-run] Would scan all product images via OCR")
        elif client:
            _run_image_replacement(client, dry_run=args.dry_run)

    # Summary
    products_done = sum(1 for k in progress if k.startswith("product_"))
    collections_done = sum(1 for k in progress if k.startswith("collection_"))
    pages_done = sum(1 for k in progress if k.startswith("page_"))
    articles_done = sum(1 for k in progress if k.startswith("article_"))
    blogs_done = sum(1 for k in progress if k.startswith("blog_"))
    metaobjects_done = sum(1 for k in progress if k.startswith("metaobject_"))

    print("\n--- Arabic Import Summary ---")
    print(f"  Products:    {products_done}")
    print(f"  Collections: {collections_done}")
    print(f"  Pages:       {pages_done}")
    print(f"  Articles:    {articles_done}")
    print(f"  Blogs:       {blogs_done}")
    print(f"  Metaobjects: {metaobjects_done}")
    if args.dry_run:
        print("  (dry run — nothing was registered)")


if __name__ == "__main__":
    main()
