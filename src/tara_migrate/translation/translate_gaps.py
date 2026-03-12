#!/usr/bin/env python3
"""Step 2b: Translate gaps using TOON-format batched payloads.

Uses TOON (Token-Oriented Object Notation) to send batched translation
requests, reducing API calls by ~40x compared to per-field translation.

Reads Spain export data, compares with scraped EN/AR data, and translates
only the missing content. Merges translated content back into the
EN/AR output files.

Usage:
    python translate_gaps.py --lang en          # Translate gaps to English
    python translate_gaps.py --lang ar          # Translate gaps to Arabic
    python translate_gaps.py --lang en --dry    # Show what would be translated
"""

import argparse
import copy
import os
import re
import sys
import time

from dotenv import load_dotenv
from openai import OpenAI

from tara_migrate.core import load_json, save_json
from tara_migrate.core.config import AR_DIR, EN_DIR, SPAIN_DIR
from tara_migrate.core.utils import unicode_slugify as _slugify
from tara_migrate.translation.field_extractors import (  # noqa: F401
    TEXT_METAFIELD_TYPES,
    extract_article_fields,
    extract_blog_fields,
    extract_collection_fields,
    extract_metaobject_fields,
    extract_page_fields,
    extract_product_fields,
)
from tara_migrate.translation.toon import _toon_escape, _toon_unescape, from_toon, to_toon  # noqa: F401
from tara_migrate.core.language import replace_range_names_ar, TARA_RANGE_NAMES_AR
from tara_migrate.translation.translator import TARA_TONE_AR, TARA_TONE_EN

# Max fields per TOON batch — large batches = fewer API calls
# GPT-5-mini handles ~8K output tokens, so we can fit ~200 short fields
# or ~80 long fields (HTML body) per batch.
BATCH_SIZE = 120

# TPM (tokens per minute) budget
TPM_LIMIT = 30000


# =====================================================================
# Apply translated fields back to data structures
# =====================================================================


# Map metaobject type → field key that contains the "name" for handle generation
# Must match METAOBJECT_NAME_FIELDS in scrape_kuwait.py
_METAOBJECT_NAME_FIELDS = {
    "ingredient": "name",
    "benefit": "title",
    "blog_author": "full_name",
    "faq_entry": "question",
}


def _regenerate_metaobject_handles(metaobjects, skip_ids=None):
    """Regenerate metaobject handles from their translated name/title field.

    Also deduplicates entries: when multiple source entries translate to the
    same handle (e.g., identical FAQ questions on different products), only
    the first is kept.

    Args:
        skip_ids: set of id(obj) for objects that already have an explicit
            handle translation and should not be overwritten.
    """
    skip_ids = skip_ids or set()
    for mo_type, type_data in metaobjects.items():
        name_field_key = _METAOBJECT_NAME_FIELDS.get(mo_type, "name")
        for obj in type_data.get("objects", []):
            if id(obj) in skip_ids:
                continue
            name_val = ""
            for field in obj.get("fields", []):
                if field["key"] == name_field_key:
                    name_val = field.get("value", "")
                    break
            if name_val:
                obj["handle"] = _slugify(name_val)

        # Deduplicate by handle (keep first occurrence)
        seen = set()
        unique_objs = []
        for obj in type_data.get("objects", []):
            h = obj.get("handle", "")
            if h and h in seen:
                continue
            seen.add(h)
            unique_objs.append(obj)
        before = len(type_data.get("objects", []))
        type_data["objects"] = unique_objs
        if before != len(unique_objs):
            print(f"    {mo_type}: deduplicated {before} → {len(unique_objs)} entries")


def post_process_arabic_range_names(products, collections, pages, articles, metaobjects, blogs=None):
    """Replace English TARA collection/range names with Arabic equivalents.

    Runs after translation to fix product names the AI left in English
    (e.g. 'Date + Multivitamin' → 'التمر + فيتامينات متعددة').
    """
    count = 0

    def _fix(text):
        nonlocal count
        if not text:
            return text
        fixed = replace_range_names_ar(text)
        if fixed != text:
            count += 1
        return fixed

    def _fix_rich_text(val):
        """Fix range names inside rich text JSON."""
        nonlocal count
        if not val:
            return val
        try:
            import json
            data = json.loads(val)

            def fix_nodes(nodes):
                for node in nodes:
                    if node.get("type") == "text" and node.get("value"):
                        node["value"] = _fix(node["value"])
                    if node.get("children"):
                        fix_nodes(node["children"])

            if isinstance(data, dict) and data.get("children"):
                fix_nodes(data["children"])
            return json.dumps(data, ensure_ascii=False)
        except (json.JSONDecodeError, TypeError):
            return _fix(val)

    for p in products:
        p["title"] = _fix(p.get("title", ""))
        p["body_html"] = _fix(p.get("body_html", ""))
        for mf in p.get("metafields", []):
            mf_type = mf.get("type", "")
            if mf_type == "rich_text_field":
                mf["value"] = _fix_rich_text(mf.get("value", ""))
            elif mf_type in TEXT_METAFIELD_TYPES:
                mf["value"] = _fix(mf.get("value", ""))

    for c in collections:
        c["title"] = _fix(c.get("title", ""))
        c["body_html"] = _fix(c.get("body_html", ""))

    for pg in pages:
        pg["title"] = _fix(pg.get("title", ""))
        pg["body_html"] = _fix(pg.get("body_html", ""))

    for a in articles:
        a["title"] = _fix(a.get("title", ""))
        a["body_html"] = _fix(a.get("body_html", ""))
        for mf in a.get("metafields", []):
            mf_type = mf.get("type", "")
            if mf_type == "rich_text_field":
                mf["value"] = _fix_rich_text(mf.get("value", ""))
            elif mf_type in TEXT_METAFIELD_TYPES:
                mf["value"] = _fix(mf.get("value", ""))

    if isinstance(metaobjects, dict):
        for mo_type, type_data in metaobjects.items():
            for obj in type_data.get("objects", []):
                for field in obj.get("fields", []):
                    ftype = field.get("type", "")
                    if ftype == "rich_text_field":
                        field["value"] = _fix_rich_text(field.get("value", ""))
                    elif ftype in TEXT_METAFIELD_TYPES:
                        field["value"] = _fix(field.get("value", ""))

    for b in (blogs or []):
        b["title"] = _fix(b.get("title", ""))

    if count:
        print(f"  Post-processed {count} fields: replaced English range names with Arabic")


def apply_translations(translations, products, collections, pages, articles, metaobjects, blogs=None):
    """Apply a dict of {field_id: translated_value} back to data structures."""
    t = translations

    for p in products:
        pid = p.get("handle", p.get("id", ""))
        for prefix in ["prod", "product"]:
            # Handle translation
            handle_key = f"{prefix}.{pid}.handle"
            if handle_key in t:
                new_handle = _slugify(t[handle_key])
                if new_handle:
                    p["handle"] = new_handle

            if f"{prefix}.{pid}.title" in t:
                p["title"] = t[f"{prefix}.{pid}.title"]
            if f"{prefix}.{pid}.body_html" in t:
                p["body_html"] = t[f"{prefix}.{pid}.body_html"]
            if f"{prefix}.{pid}.product_type" in t:
                p["product_type"] = t[f"{prefix}.{pid}.product_type"]
            if f"{prefix}.{pid}.vendor" in t:
                p["vendor"] = t[f"{prefix}.{pid}.vendor"]
            if f"{prefix}.{pid}.tags" in t:
                p["tags"] = t[f"{prefix}.{pid}.tags"]

            for i, v in enumerate(p.get("variants", [])):
                if f"{prefix}.{pid}.v{i}.title" in t:
                    v["title"] = t[f"{prefix}.{pid}.v{i}.title"]
                for opt_key in ["option1", "option2", "option3"]:
                    if f"{prefix}.{pid}.v{i}.{opt_key}" in t:
                        v[opt_key] = t[f"{prefix}.{pid}.v{i}.{opt_key}"]

            for i, opt in enumerate(p.get("options", [])):
                if f"{prefix}.{pid}.opt{i}.name" in t:
                    opt["name"] = t[f"{prefix}.{pid}.opt{i}.name"]
                for j in range(len(opt.get("values", []))):
                    if f"{prefix}.{pid}.opt{i}.val{j}" in t:
                        opt["values"][j] = t[f"{prefix}.{pid}.opt{i}.val{j}"]

            # Image alt text
            for i, img in enumerate(p.get("images", [])):
                if f"{prefix}.{pid}.img{i}.alt" in t:
                    img["alt"] = t[f"{prefix}.{pid}.img{i}.alt"]

            for mf in p.get("metafields", []):
                ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
                fid = f"{prefix}.{pid}.mf.{ns_key}"
                if fid in t:
                    mf["value"] = t[fid]

    for c in collections:
        cid = c.get("handle", c.get("id", ""))
        for prefix in ["coll", "collection"]:
            handle_key = f"{prefix}.{cid}.handle"
            if handle_key in t:
                new_handle = _slugify(t[handle_key])
                if new_handle:
                    c["handle"] = new_handle

            if f"{prefix}.{cid}.title" in t:
                c["title"] = t[f"{prefix}.{cid}.title"]
            if f"{prefix}.{cid}.body_html" in t:
                c["body_html"] = t[f"{prefix}.{cid}.body_html"]
            # Collection image alt
            if f"{prefix}.{cid}.image.alt" in t:
                if c.get("image"):
                    c["image"]["alt"] = t[f"{prefix}.{cid}.image.alt"]

            for mf in c.get("metafields", []):
                ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
                fid = f"{prefix}.{cid}.mf.{ns_key}"
                if fid in t:
                    mf["value"] = t[fid]

    for pg in pages:
        pid = pg.get("handle", pg.get("id", ""))
        for prefix in ["page"]:
            handle_key = f"{prefix}.{pid}.handle"
            if handle_key in t:
                new_handle = _slugify(t[handle_key])
                if new_handle:
                    pg["handle"] = new_handle

            if f"{prefix}.{pid}.title" in t:
                pg["title"] = t[f"{prefix}.{pid}.title"]
            if f"{prefix}.{pid}.body_html" in t:
                pg["body_html"] = t[f"{prefix}.{pid}.body_html"]

            for mf in pg.get("metafields", []):
                ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
                fid = f"{prefix}.{pid}.mf.{ns_key}"
                if fid in t:
                    mf["value"] = t[fid]

    if blogs:
        for b in blogs:
            bid = b.get("handle", b.get("id", ""))
            for prefix in ["blog"]:
                if f"{prefix}.{bid}.title" in t:
                    b["title"] = t[f"{prefix}.{bid}.title"]
                handle_key = f"{prefix}.{bid}.handle"
                if handle_key in t:
                    new_handle = _slugify(t[handle_key])
                    if new_handle:
                        b["handle"] = new_handle
                if f"{prefix}.{bid}.tags" in t:
                    b["tags"] = t[f"{prefix}.{bid}.tags"]

    for a in articles:
        aid = a.get("handle", a.get("id", ""))
        for prefix in ["art", "article"]:
            handle_key = f"{prefix}.{aid}.handle"
            if handle_key in t:
                new_handle = _slugify(t[handle_key])
                if new_handle:
                    a["handle"] = new_handle
            if f"{prefix}.{aid}.title" in t:
                a["title"] = t[f"{prefix}.{aid}.title"]
            if f"{prefix}.{aid}.body_html" in t:
                a["body_html"] = t[f"{prefix}.{aid}.body_html"]
            if f"{prefix}.{aid}.summary_html" in t:
                a["summary_html"] = t[f"{prefix}.{aid}.summary_html"]
            if f"{prefix}.{aid}.author" in t:
                a["author"] = t[f"{prefix}.{aid}.author"]
            if f"{prefix}.{aid}.tags" in t:
                a["tags"] = t[f"{prefix}.{aid}.tags"]
            # Article image alt
            if f"{prefix}.{aid}.image.alt" in t:
                if a.get("image"):
                    a["image"]["alt"] = t[f"{prefix}.{aid}.image.alt"]
            for mf in a.get("metafields", []):
                ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
                fid = f"{prefix}.{aid}.mf.{ns_key}"
                if fid in t:
                    mf["value"] = t[fid]

    if isinstance(metaobjects, dict):
        # Track which objects have explicit handle translations so
        # _regenerate_metaobject_handles won't overwrite them.
        explicitly_handled = set()
        for mo_type, type_data in metaobjects.items():
            for obj in type_data.get("objects", []):
                handle = obj.get("handle", obj.get("id", ""))
                for prefix in ["mo", "metaobject"]:
                    # Metaobject handle
                    handle_key = f"{prefix}.{mo_type}.{handle}.handle"
                    if handle_key in t:
                        new_handle = _slugify(t[handle_key])
                        if new_handle:
                            obj["handle"] = new_handle
                            explicitly_handled.add(id(obj))
                    for field in obj.get("fields", []):
                        fid = f"{prefix}.{mo_type}.{handle}.{field['key']}"
                        if fid in t:
                            field["value"] = t[fid]

        # Regenerate handles from translated name fields, but skip
        # objects that already have an explicit handle translation.
        _regenerate_metaobject_handles(metaobjects, skip_ids=explicitly_handled)


# =====================================================================
# Batch translation via OpenAI with TOON format
# =====================================================================

def build_system_prompt(target_lang):
    tov = TARA_TONE_EN if target_lang == "English" else TARA_TONE_AR
    return f"""You are a professional translator for TARA, a luxury scalp-care and hair-health brand.

INPUT/OUTPUT FORMAT: TOON (Token-Oriented Object Notation)
Each line is: id|value
- id is a field identifier — DO NOT translate it, keep it exactly as-is
- value is the text to translate
- Escape: \\n = newline, \\p = pipe, \\\\ = backslash
- Return the SAME number of lines with the SAME ids, only the values translated

TRANSLATION RULES:
- Keep "TARA" unchanged — never translate the brand name
- Keep tool names unchanged: "Kansa Wand", "Gua Sha", "Scalp Massager"
- DO translate collection/range names into the target language. For Arabic: "Date+ Multivitamin" → "التمر + فيتامينات متعددة", "Black Garlic+ Ceramides" → "الثوم الأسود + سيراميدات", "Onion+ Peptides" → "البصل + ببتيدات", "Strawberry+ NMF" → "الفراولة + عوامل الترطيب الطبيعية", "Rosemary+ Peptides" → "إكليل الجبل + ببتيدات", "Sage+ Multivitamin" → "الميرمية + فيتامينات متعددة", "Detox" → "ديتوكس"
- Keep ingredient scientific names (INCI names) unchanged
- Preserve ALL HTML tags and attributes exactly
- Preserve Shopify Liquid tags ({{{{ }}}}, {{% %}}) unchanged
- Keep URLs, JSON structure keys, and GIDs unchanged
- For rich_text_field JSON: translate only "value" keys inside text nodes
- For .handle fields: translate the slug to {target_lang}. Use lowercase with hyphens as separators. For English: "mascarilla-reparadora" → "repairing-hair-mask". For Arabic: "mascarilla-reparadora" → "قناع-الشعر-المصلح" (Arabic words separated by hyphens).
- For .mf.global.title_tag: translate the SEO page title
- For .mf.global.description_tag: translate the SEO meta description
- Return ONLY the translated TOON lines, no explanations

TARA {target_lang.upper()} TONE OF VOICE:
{tov}"""


def _estimate_tokens(text):
    """Rough token estimate: ~4 chars per token for English, ~2 for Arabic/CJK."""
    return max(1, len(text) // 3)


def adaptive_batch(fields, max_tokens=12000):
    """Split fields into batches sized by estimated token count, not fixed count.

    Prevents oversized batches when fields contain long HTML/JSON bodies.
    Short fields (headings, taglines) get packed densely.
    Long fields (body_html, rich_text JSON) get smaller batches.
    """
    batches = []
    current_batch = []
    current_tokens = 0

    for field in fields:
        field_tokens = _estimate_tokens(field["value"])
        # If a single field exceeds max, it gets its own batch
        if current_batch and (current_tokens + field_tokens > max_tokens):
            batches.append(current_batch)
            current_batch = []
            current_tokens = 0
        current_batch.append(field)
        current_tokens += field_tokens

    if current_batch:
        batches.append(current_batch)
    return batches


def translate_batch(client, model, fields, source_lang, target_lang, batch_num, total_batches):
    """Translate a batch of fields using TOON format.

    Returns (translation_map, total_tokens_used).
    """
    toon_input = to_toon(fields)

    prompt = (
        f"Translate the following TOON data from {source_lang} to {target_lang}. "
        f"Keep all IDs unchanged. Translate only the values. "
        f"Follow the TARA {target_lang} tone of voice.\n\n"
        f"{toon_input}"
    )

    est_tokens = sum(_estimate_tokens(f["value"]) for f in fields)
    print(f"  Batch {batch_num}/{total_batches}: {len(fields)} fields (~{est_tokens:,} value tokens)...")

    # Reasoning models (o3, gpt-5-mini, etc.) use reasoning_effort instead of temperature
    REASONING_MODELS = {"o3", "o3-mini", "o4-mini", "gpt-5-mini", "gpt-5"}
    is_reasoning = any(model.startswith(rm) for rm in REASONING_MODELS)

    for attempt in range(4):
        try:
            api_kwargs = {
                "model": model,
                "messages": [
                    {"role": "system", "content": build_system_prompt(target_lang)},
                    {"role": "user", "content": prompt},
                ],
            }
            if is_reasoning:
                # Try new SDK format first, fall back to legacy
                api_kwargs["reasoning_effort"] = "low"
            else:
                api_kwargs["temperature"] = 0.3

            response = client.chat.completions.create(**api_kwargs)
            result = response.choices[0].message.content.strip()

            # Strip markdown code fences if model wraps output
            if result.startswith("```"):
                lines = result.split("\n")
                if lines[-1].strip() == "```":
                    result = "\n".join(lines[1:-1])
                else:
                    result = "\n".join(lines[1:])

            translated = from_toon(result)

            # Validate we got the right number back
            if len(translated) != len(fields):
                print(f"    WARNING: Expected {len(fields)} fields, got {len(translated)}.")
                # Accept if we got at least 90% — save what we have, retry missing later
                if len(translated) >= len(fields) * 0.9:
                    print(f"    Accepting partial result ({len(translated)}/{len(fields)})")
                elif attempt < 3:
                    print("    Retrying...")
                    time.sleep(2)
                    continue

            # Build translation map
            t_map = {}
            for entry in translated:
                t_map[entry["id"]] = entry["value"]

            # Verify IDs match
            input_ids = {f["id"] for f in fields}
            output_ids = set(t_map.keys())
            missing = input_ids - output_ids
            extra = output_ids - input_ids
            if extra:
                # Model fabricated IDs — remove them
                for eid in extra:
                    del t_map[eid]
            if missing:
                print(f"    WARNING: {len(missing)} untranslated fields (will retry on next run)")

            usage = response.usage
            total_tokens = usage.prompt_tokens + usage.completion_tokens
            print(f"    Done ({usage.prompt_tokens} prompt + {usage.completion_tokens} completion = {total_tokens} tokens)")
            return t_map, total_tokens

        except Exception as e:
            err_str = str(e)
            print(f"    Error: {e}")
            if attempt < 3:
                # Parse retry-after from rate limit errors
                wait = 2 ** (attempt + 1)
                retry_match = re.search(r"try again in (\d+\.?\d*)s", err_str)
                if retry_match:
                    wait = max(wait, float(retry_match.group(1)) + 2)
                elif "429" in err_str or "rate" in err_str.lower():
                    wait = max(wait, 45)  # Default 45s for rate limits
                print(f"    Retrying in {wait:.0f}s...")
                time.sleep(wait)

    print("    FAILED after 4 attempts")
    return {}, 0


# =====================================================================
# Main
# =====================================================================

# load_json, save_json imported from tara_migrate.core


def find_gaps(spain_items, scraped_items, key_field="handle"):
    """Find Spain items not present in scraped data."""
    if not scraped_items:
        return spain_items  # Everything needs translation

    if key_field == "sku":
        scraped_skus = set()
        scraped_handles = set()
        for p in scraped_items:
            scraped_handles.add(p.get("handle", ""))
            for v in p.get("variants", []):
                if v.get("sku"):
                    scraped_skus.add(v["sku"])

        missing = []
        for p in spain_items:
            skus = [v.get("sku", "") for v in p.get("variants", []) if v.get("sku")]
            handle = p.get("handle", "")
            if not any(s in scraped_skus for s in skus) and handle not in scraped_handles:
                missing.append(p)
        return missing

    scraped_keys = {item.get(key_field, "") for item in scraped_items}
    return [item for item in spain_items if item.get(key_field, "") not in scraped_keys]


def match_products_by_sku(source_products, scraped_products):
    """Match source products to scraped products by SKU.

    Returns a list of (source_product, scraped_product) tuples for products
    that exist in both datasets. Used to copy metafields from source to
    scraped products that lack them (Magento doesn't have Shopify metafields).
    """
    if not scraped_products:
        return []

    # Build scraped lookup: SKU → scraped product
    scraped_by_sku = {}
    scraped_by_handle = {}
    for p in scraped_products:
        scraped_by_handle[p.get("handle", "")] = p
        for v in p.get("variants", []):
            sku = v.get("sku", "")
            if sku:
                scraped_by_sku[sku] = p

    matched = []
    seen_scraped = set()
    for sp in source_products:
        scraped = None
        # Try SKU match first
        for v in sp.get("variants", []):
            sku = v.get("sku", "")
            if sku and sku in scraped_by_sku:
                scraped = scraped_by_sku[sku]
                break
        # Fallback to handle match
        if not scraped:
            scraped = scraped_by_handle.get(sp.get("handle", ""))
        if scraped:
            scraped_id = id(scraped)
            if scraped_id not in seen_scraped:
                seen_scraped.add(scraped_id)
                matched.append((sp, scraped))
    return matched


def apply_metafields_to_scraped(matched_pairs, translations):
    """Apply translated metafields from source products onto scraped products.

    For each (source_product, scraped_product) pair:
    - Source metafields were extracted with IDs like prod.{source_handle}.mf.{ns.key}
    - The translated values are in the translations dict under those IDs
    - We apply them onto the scraped product's metafields list

    Also copies any source metafields that are missing from the scraped product.
    """
    for source_prod, scraped_prod in matched_pairs:
        src_handle = source_prod.get("handle", source_prod.get("id", ""))

        if "metafields" not in scraped_prod:
            scraped_prod["metafields"] = []

        # Build lookup of existing scraped metafields
        scraped_mf_lookup = {}
        for mf in scraped_prod["metafields"]:
            ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
            scraped_mf_lookup[ns_key] = mf

        for mf in source_prod.get("metafields", []):
            mf_type = mf.get("type", "")
            ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
            fid = f"prod.{src_handle}.mf.{ns_key}"

            if mf_type in TEXT_METAFIELD_TYPES:
                # Use translated value if available, otherwise use source value
                value = translations.get(fid, mf.get("value", ""))
            else:
                # Non-text metafields (references, numbers): copy as-is
                value = mf.get("value", "")

            if ns_key in scraped_mf_lookup:
                # Update existing metafield only if it's text and we have a translation
                if mf_type in TEXT_METAFIELD_TYPES and fid in translations:
                    scraped_mf_lookup[ns_key]["value"] = value
            else:
                # Add missing metafield to scraped product
                scraped_prod["metafields"].append({
                    "namespace": mf.get("namespace", ""),
                    "key": mf.get("key", ""),
                    "value": value,
                    "type": mf_type,
                })


def translate_with_gaps(
    source_dir,
    output_dir,
    source_lang,
    target_lang,
    lang_code,
    dry=False,
    model="gpt-5-mini",
    batch_size=BATCH_SIZE,
    tpm=TPM_LIMIT,
):
    """Core scrape-first translation: use scraped data where available, translate gaps.

    Loads source data (Spain export or EN output), compares with scraped data
    already in output_dir (from scrape_kuwait.py), identifies gaps, translates
    only the missing content, and merges everything into output_dir.

    Products are matched by SKU between source and scraped data.

    Args:
        source_dir: Directory with source data (SPAIN_DIR for ES→EN, EN_DIR for EN→AR)
        output_dir: Directory for output (EN_DIR or AR_DIR), also read for scraped data
        source_lang: Source language name ("Spanish" or "English")
        target_lang: Target language name ("English" or "Arabic")
        lang_code: Short code ("en" or "ar") for progress file naming
        dry: If True, show what would be translated without API calls
        model: OpenAI model to use
        batch_size: Fields per batch
        tpm: Tokens-per-minute budget
    """
    os.makedirs(output_dir, exist_ok=True)

    # Load source data
    source_products = load_json(os.path.join(source_dir, "products.json"))
    source_collections = load_json(os.path.join(source_dir, "collections.json"))
    source_pages = load_json(os.path.join(source_dir, "pages.json"))
    source_blogs = load_json(os.path.join(source_dir, "blogs.json"))
    source_articles = load_json(os.path.join(source_dir, "articles.json"))
    source_metaobjects = load_json(os.path.join(source_dir, "metaobjects.json"))

    if not source_products and not source_collections and not source_pages:
        print(f"ERROR: Source data in {source_dir} is empty.")
        sys.exit(1)

    # Load scraped data from output dir (if available from scrape_kuwait.py)
    scraped_products = load_json(os.path.join(output_dir, "products.json"))
    scraped_collections = load_json(os.path.join(output_dir, "collections.json"))
    scraped_pages = load_json(os.path.join(output_dir, "pages.json"))
    scraped_articles = load_json(os.path.join(output_dir, "articles.json"))
    scraped_metaobjects = load_json(os.path.join(output_dir, "metaobjects.json"))

    has_scraped = bool(scraped_products or scraped_collections or scraped_pages)

    print(f"{'=' * 60}")
    print(f"TRANSLATE {source_lang.upper()} → {target_lang.upper()} (scrape-first)")
    print(f"{'=' * 60}")

    if has_scraped:
        print(f"\n  Scraped data found in {output_dir}/:")
        print(f"    Products:    {len(scraped_products)}")
        print(f"    Collections: {len(scraped_collections)}")
        print(f"    Pages:       {len(scraped_pages)}")
        print(f"    Articles:    {len(scraped_articles)}")
    else:
        print("\n  No scraped data found — translating everything.")

    # ---- Identify gaps: what's NOT in scraped data ----
    gap_products = find_gaps(source_products, scraped_products, key_field="sku")
    gap_collections = find_gaps(source_collections, scraped_collections)
    gap_pages = find_gaps(source_pages, scraped_pages)
    # Articles are never in Magento — always need full translation
    gap_articles = source_articles

    # Metaobjects: text-type fields always need full translation (scraper
    # copies source data as-is with just slugified handles — the text is
    # still in the source language). Non-text types only include genuinely
    # missing items.
    gap_metaobjects = {}
    # Track which types need full replacement (not merge) because their
    # scraped data is just untranslated source text.
    _full_replace_mo_types = set()

    # When the Spain export has no metaobjects, the scraper's output
    # (which is a copy of Spain data with slugified handles) IS the
    # source — its text fields still need translation.
    effective_mo_source = source_metaobjects
    if not effective_mo_source or (isinstance(effective_mo_source, dict) and not effective_mo_source):
        effective_mo_source = scraped_metaobjects

    if isinstance(effective_mo_source, dict):
        for mo_type, type_data in effective_mo_source.items():
            objs = type_data.get("objects", [])
            if not objs:
                continue
            has_text_fields = any(
                field.get("type", "") in TEXT_METAFIELD_TYPES and field.get("value")
                for obj in objs
                for field in obj.get("fields", [])
            )
            if has_text_fields:
                gap_metaobjects[mo_type] = {
                    "definition": type_data.get("definition", {}),
                    "objects": objs,
                }
                _full_replace_mo_types.add(mo_type)
            else:
                scraped_objs = []
                if isinstance(scraped_metaobjects, dict) and mo_type in scraped_metaobjects:
                    scraped_objs = scraped_metaobjects[mo_type].get("objects", [])
                scraped_handles = {o.get("handle", "") for o in scraped_objs}
                missing = [o for o in objs if o.get("handle") not in scraped_handles]
                if missing:
                    gap_metaobjects[mo_type] = {
                        "definition": type_data.get("definition", {}),
                        "objects": missing,
                    }

    # Products in scraped data still need their metafields translated
    # (Magento doesn't have Shopify accordion metafields, tagline, etc.)
    # Match by SKU so cross-language handles work correctly
    matched_pairs = match_products_by_sku(source_products, scraped_products)

    print(f"\n  Source: {len(source_products)} products, {len(source_collections)} collections, "
          f"{len(source_pages)} pages")
    print("  Gaps to translate:")
    print(f"    Products (full):       {len(gap_products)}")
    print(f"    Products (metafields): {len(matched_pairs)}")
    print(f"    Collections:           {len(gap_collections)}")
    print(f"    Pages:                 {len(gap_pages)}")
    print(f"    Articles:              {len(gap_articles)}")
    mo_gap_count = sum(len(td.get("objects", [])) for td in gap_metaobjects.values())
    print(f"    Metaobjects:           {mo_gap_count}")

    # ---- Extract translatable fields from gaps only ----
    all_fields = []

    # Full extraction for gap products (not in scraped data)
    for p in gap_products:
        all_fields.extend(extract_product_fields(p, "prod"))

    # Metafield-only extraction for matched products
    for source_prod, _scraped_prod in matched_pairs:
        pid = source_prod.get("handle", source_prod.get("id", ""))
        for mf in source_prod.get("metafields", []):
            mf_type = mf.get("type", "")
            ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
            if mf_type in TEXT_METAFIELD_TYPES and mf.get("value"):
                all_fields.append({"id": f"prod.{pid}.mf.{ns_key}", "value": mf["value"]})

    for c in gap_collections:
        all_fields.extend(extract_collection_fields(c, "coll"))
    for pg in gap_pages:
        all_fields.extend(extract_page_fields(pg, "page"))
    for a in gap_articles:
        all_fields.extend(extract_article_fields(a, "art"))
    for b in source_blogs:
        all_fields.extend(extract_blog_fields(b, "blog"))
    all_fields.extend(extract_metaobject_fields(gap_metaobjects, "mo"))

    # Filter out empty values
    all_fields = [f for f in all_fields if f.get("value") and f["value"].strip()]

    print(f"\n  Total fields to translate: {len(all_fields)}")

    # Breakdown
    field_types = {}
    for f in all_fields:
        category = f["id"].split(".")[0]
        field_types[category] = field_types.get(category, 0) + 1
    print("  Breakdown:")
    for cat, count in sorted(field_types.items()):
        print(f"    {cat}: {count} fields")

    if dry:
        print("\n  DRY RUN — no API calls made")
        print("\n  Sample fields (first 10):")
        for f in all_fields[:10]:
            val = f["value"][:80] + "..." if len(f["value"]) > 80 else f["value"]
            print(f"    {f['id']}: {val}")
        return

    all_translations = {}

    if not all_fields:
        print("\n  Nothing to translate!")
    else:
        # ---- Load progress ----
        progress_file = os.path.join(output_dir, f"_translation_progress_{lang_code}.json")
        if os.path.exists(progress_file):
            all_translations = load_json(progress_file)
            if isinstance(all_translations, dict):
                print(f"\n  Resuming: {len(all_translations)} fields already translated")
            else:
                all_translations = {}

        remaining = [f for f in all_fields if f["id"] not in all_translations]
        print(f"  Remaining: {len(remaining)} fields")

        if not remaining:
            print("  All fields already translated!")
        else:
            api_key = os.environ.get("OPENAI_API_KEY")
            if not api_key:
                print("ERROR: OPENAI_API_KEY not set. Add it to .env")
                sys.exit(1)

            client = OpenAI(api_key=api_key)

            max_batch_tokens = batch_size * 100
            batches = adaptive_batch(remaining, max_tokens=max_batch_tokens)
            total_batches = len(batches)
            batch_sizes = [len(b) for b in batches]

            print(f"\n  {total_batches} adaptive batches "
                  f"(sizes: {min(batch_sizes)}-{max(batch_sizes)} fields)")
            print(f"  TPM budget: {tpm:,}")

            window_start = time.time()
            window_tokens = 0
            failed_fields = []

            for i, batch_items in enumerate(batches):
                now = time.time()
                elapsed = now - window_start
                if elapsed >= 60:
                    window_start = now
                    window_tokens = 0
                elif window_tokens >= tpm * 0.85:
                    wait = 60 - elapsed + 2
                    print(f"    TPM throttle: {window_tokens:,} tokens used, waiting {wait:.0f}s...")
                    time.sleep(wait)
                    window_start = time.time()
                    window_tokens = 0

                t_map, tokens_used = translate_batch(
                    client, model, batch_items,
                    source_lang, target_lang,
                    i + 1, total_batches,
                )
                window_tokens += tokens_used

                if t_map:
                    all_translations.update(t_map)
                    save_json(all_translations, progress_file)
                    batch_ids = {f["id"] for f in batch_items}
                    missing_from_batch = batch_ids - set(t_map.keys())
                    if missing_from_batch:
                        failed_fields.extend([f for f in batch_items if f["id"] in missing_from_batch])
                else:
                    failed_fields.extend(batch_items)

            if failed_fields:
                print(f"\n  WARNING: {len(failed_fields)} fields failed translation")
                print("  Re-run to retry (progress is saved)")
                save_json([f["id"] for f in failed_fields],
                          os.path.join(output_dir, f"_failed_fields_{lang_code}.json"))

    # ---- Merge translations into output data ----
    print("\n  Merging translations into output files...")

    # Start with scraped data as base
    output_products = list(scraped_products) if scraped_products else []
    output_collections = list(scraped_collections) if scraped_collections else []
    output_pages = list(scraped_pages) if scraped_pages else []
    output_metaobjects = dict(scraped_metaobjects) if isinstance(scraped_metaobjects, dict) else {}

    # Add gap items (dedup by source ID to prevent accumulation on re-runs)
    existing_product_ids = {str(p.get("id", "")) for p in output_products}
    for p in gap_products:
        if str(p.get("id", "")) not in existing_product_ids:
            output_products.append(copy.deepcopy(p))
    existing_collection_ids = {str(c.get("id", "")) for c in output_collections}
    for c in gap_collections:
        if str(c.get("id", "")) not in existing_collection_ids:
            output_collections.append(copy.deepcopy(c))
    existing_page_ids = {str(pg.get("id", "")) for pg in output_pages}
    for pg in gap_pages:
        if str(pg.get("id", "")) not in existing_page_ids:
            output_pages.append(copy.deepcopy(pg))

    # Articles: always from source, fully translated
    output_articles = [copy.deepcopy(a) for a in gap_articles]

    # Metaobjects: for text-field types, REPLACE scraped data entirely
    # (scraped data is just untranslated source text with slugified handles).
    # For non-text types, merge missing items into scraped base.
    print("\n  Metaobject merge:")
    for mo_type, type_data in gap_metaobjects.items():
        source_count = len(type_data.get("objects", []))
        scraped_count = len(output_metaobjects.get(mo_type, {}).get("objects", []))
        if mo_type in _full_replace_mo_types:
            # Replace: use source objects (will be translated below)
            output_metaobjects[mo_type] = copy.deepcopy(type_data)
            print(f"    {mo_type}: REPLACE scraped ({scraped_count}) with source ({source_count})")
        elif mo_type not in output_metaobjects:
            output_metaobjects[mo_type] = copy.deepcopy(type_data)
            print(f"    {mo_type}: NEW ({source_count} entries)")
        else:
            existing_handles = {o.get("handle") for o in output_metaobjects[mo_type].get("objects", [])}
            added = 0
            for obj in type_data.get("objects", []):
                if obj.get("handle") not in existing_handles:
                    output_metaobjects[mo_type]["objects"].append(copy.deepcopy(obj))
                    added += 1
            if added:
                print(f"    {mo_type}: MERGE +{added} new entries (was {scraped_count})")

    # Blogs: always from source, fully translated
    output_blogs = [copy.deepcopy(b) for b in source_blogs]

    # Apply translations to gap items
    apply_translations(
        all_translations,
        output_products, output_collections, output_pages,
        output_articles, output_metaobjects, blogs=output_blogs,
    )

    # Apply translated metafields to scraped products (matched by SKU).
    # Uses source-handle-based field IDs to find translations and applies
    # them onto the correct scraped products.
    apply_metafields_to_scraped(matched_pairs, all_translations)

    # For Arabic: post-process to replace English range/collection names
    if target_lang == "Arabic":
        post_process_arabic_range_names(
            output_products, output_collections, output_pages,
            output_articles, output_metaobjects, blogs=output_blogs,
        )

    # ---- Save output ----
    save_json(output_products, os.path.join(output_dir, "products.json"))
    save_json(output_collections, os.path.join(output_dir, "collections.json"))
    save_json(output_pages, os.path.join(output_dir, "pages.json"))
    save_json(output_blogs, os.path.join(output_dir, "blogs.json"))
    save_json(output_articles, os.path.join(output_dir, "articles.json"))
    save_json(output_metaobjects, os.path.join(output_dir, "metaobjects.json"))

    # Copy non-translatable files from source
    for fname in ["metaobject_definitions.json"]:
        src = os.path.join(source_dir, fname)
        if os.path.exists(src):
            save_json(load_json(src), os.path.join(output_dir, fname))

    print(f"\n{'=' * 60}")
    print(f"TRANSLATION COMPLETE → {target_lang.upper()}")
    print(f"{'=' * 60}")
    scraped_prod_count = len(output_products) - len(gap_products)
    scraped_coll_count = len(output_collections) - len(gap_collections)
    scraped_page_count = len(output_pages) - len(gap_pages)
    print(f"  Products:    {len(output_products)} ({scraped_prod_count} from live site, {len(gap_products)} translated)")
    print(f"  Collections: {len(output_collections)} ({scraped_coll_count} from live site, {len(gap_collections)} translated)")
    print(f"  Pages:       {len(output_pages)} ({scraped_page_count} from live site, {len(gap_pages)} translated)")
    print(f"  Blogs:       {len(output_blogs)}")
    print(f"  Articles:    {len(output_articles)} (translated)")
    if output_metaobjects:
        mo_total = sum(len(v.get("objects", [])) for v in output_metaobjects.values())
        print(f"  Metaobjects: {mo_total}")
    print(f"  Output:      {output_dir}/")

    translated_count = len(all_translations)
    total_needed = len(all_fields)
    completeness = (translated_count / total_needed * 100) if total_needed else 100
    print(f"\n  Completeness: {translated_count}/{total_needed} fields ({completeness:.1f}%)")
    if completeness < 100:
        print("  Re-run to retry failed fields (progress is saved)")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Translate gaps using TOON batched format")
    parser.add_argument("--lang", required=True, choices=["en", "ar"],
                        help="Target language: en or ar")
    parser.add_argument("--dry", action="store_true",
                        help="Dry run: show what would be translated without calling API")
    parser.add_argument("--model", default="gpt-5-mini",
                        help="OpenAI model (default: gpt-5-mini)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"Fields per batch (default: {BATCH_SIZE})")
    parser.add_argument("--tpm", type=int, default=TPM_LIMIT,
                        help=f"Tokens-per-minute budget (default: {TPM_LIMIT})")
    args = parser.parse_args()

    if args.lang == "en":
        translate_with_gaps(
            source_dir=SPAIN_DIR, output_dir=EN_DIR,
            source_lang="Spanish", target_lang="English", lang_code="en",
            dry=args.dry, model=args.model, batch_size=args.batch_size, tpm=args.tpm,
        )
    else:
        translate_with_gaps(
            source_dir=EN_DIR, output_dir=AR_DIR,
            source_lang="English", target_lang="Arabic", lang_code="ar",
            dry=args.dry, model=args.model, batch_size=args.batch_size, tpm=args.tpm,
        )


if __name__ == "__main__":
    main()
