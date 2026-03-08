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
import json
import os
import re
import sys
import time

from dotenv import load_dotenv
from openai import OpenAI


SPAIN_DIR = "data/spain_export"
EN_DIR = "data/english"
AR_DIR = "data/arabic"

# Max fields per TOON batch — large batches = fewer API calls
# GPT-4o handles ~8K output tokens, so we can fit ~200 short fields
# or ~80 long fields (HTML body) per batch.
BATCH_SIZE = 120

# TPM (tokens per minute) budget — OpenAI free/tier-1 is 30K for gpt-4o
TPM_LIMIT = 30000

# =====================================================================
# TOON encoding / decoding
# =====================================================================

def to_toon(entries):
    """Convert a list of {id, value} dicts to TOON format.

    TOON uses | as field separator and newlines as record separator.
    Format:  id|value
    Escaping: newlines → \\n, pipes → \\p, backslashes → \\\\
    """
    lines = []
    for entry in entries:
        eid = _toon_escape(str(entry["id"]))
        val = _toon_escape(str(entry["value"]))
        lines.append(f"{eid}|{val}")
    return "\n".join(lines)


def from_toon(toon_text):
    """Parse TOON format back to list of {id, value} dicts."""
    entries = []
    for line in toon_text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        parts = line.split("|", 1)
        if len(parts) == 2:
            entries.append({
                "id": _toon_unescape(parts[0]),
                "value": _toon_unescape(parts[1]),
            })
    return entries


def _toon_escape(text):
    """Escape special characters for TOON."""
    text = text.replace("\\", "\\\\")
    text = text.replace("|", "\\p")
    text = text.replace("\n", "\\n")
    return text


def _toon_unescape(text):
    """Unescape TOON special characters."""
    text = text.replace("\\n", "\n")
    text = text.replace("\\p", "|")
    text = text.replace("\\\\", "\\")
    return text


# =====================================================================
# Load tone of voice
# =====================================================================

_TOV_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_tov(filename):
    filepath = os.path.join(_TOV_DIR, filename)
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()
    return ""


TARA_TONE_EN = _load_tov("tara_tov_en.txt")
TARA_TONE_AR = _load_tov("tara_tov_ar.txt")

# =====================================================================
# Translatable field definitions (from translator.py)
# =====================================================================

PRODUCT_TRANSLATABLE_METAFIELDS = {
    "custom.tagline", "custom.short_description", "custom.size_ml",
    "custom.key_benefits_heading", "custom.key_benefits_content",
    "custom.clinical_results_heading", "custom.clinical_results_content",
    "custom.how_to_use_heading", "custom.how_to_use_content",
    "custom.whats_inside_heading", "custom.whats_inside_content",
    "custom.free_of_heading", "custom.free_of_content",
    "custom.awards_heading", "custom.awards_content",
    "custom.fragrance_heading", "custom.fragrance_content",
    # SEO fields
    "global.title_tag", "global.description_tag",
}

ARTICLE_TRANSLATABLE_METAFIELDS = {
    "custom.blog_summary", "custom.hero_caption", "custom.short_title",
}

METAOBJECT_TRANSLATABLE_FIELDS = {
    "benefit": {"title", "description", "category", "icon_label"},
    "faq_entry": {"question", "answer"},
    "blog_author": {"name", "bio"},
    "ingredient": {
        "name", "one_line_benefit", "description", "source", "origin",
        "category", "concern",
    },
}


# =====================================================================
# Extract translatable fields from Spain data
# =====================================================================

def extract_product_fields(product, prefix):
    """Extract all translatable text fields from a product."""
    fields = []
    pid = product.get("handle", product.get("id", ""))

    # Handle (URL slug) — translate to English slug
    if product.get("handle"):
        fields.append({"id": f"{prefix}.{pid}.handle", "value": product["handle"]})

    # Core fields
    if product.get("title"):
        fields.append({"id": f"{prefix}.{pid}.title", "value": product["title"]})
    if product.get("body_html"):
        fields.append({"id": f"{prefix}.{pid}.body_html", "value": product["body_html"]})
    if product.get("product_type"):
        fields.append({"id": f"{prefix}.{pid}.product_type", "value": product["product_type"]})
    if product.get("tags"):
        tags = product["tags"] if isinstance(product["tags"], str) else ", ".join(product["tags"])
        fields.append({"id": f"{prefix}.{pid}.tags", "value": tags})

    # Variant options
    for i, v in enumerate(product.get("variants", [])):
        if v.get("title") and v["title"] != "Default Title":
            fields.append({"id": f"{prefix}.{pid}.v{i}.title", "value": v["title"]})
        for opt_key in ["option1", "option2", "option3"]:
            if v.get(opt_key) and v[opt_key] != "Default Title":
                fields.append({"id": f"{prefix}.{pid}.v{i}.{opt_key}", "value": v[opt_key]})

    # Options
    for i, opt in enumerate(product.get("options", [])):
        if opt.get("name"):
            fields.append({"id": f"{prefix}.{pid}.opt{i}.name", "value": opt["name"]})
        for j, val in enumerate(opt.get("values", [])):
            fields.append({"id": f"{prefix}.{pid}.opt{i}.val{j}", "value": val})

    # Metafields
    for mf in product.get("metafields", []):
        ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
        if ns_key in PRODUCT_TRANSLATABLE_METAFIELDS and mf.get("value"):
            fields.append({"id": f"{prefix}.{pid}.mf.{ns_key}", "value": mf["value"]})

    return fields


def extract_collection_fields(collection, prefix):
    fields = []
    cid = collection.get("handle", collection.get("id", ""))
    if collection.get("handle"):
        fields.append({"id": f"{prefix}.{cid}.handle", "value": collection["handle"]})
    if collection.get("title"):
        fields.append({"id": f"{prefix}.{cid}.title", "value": collection["title"]})
    if collection.get("body_html"):
        fields.append({"id": f"{prefix}.{cid}.body_html", "value": collection["body_html"]})
    # SEO metafields
    for mf in collection.get("metafields", []):
        ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
        if ns_key in ("global.title_tag", "global.description_tag") and mf.get("value"):
            fields.append({"id": f"{prefix}.{cid}.mf.{ns_key}", "value": mf["value"]})
    return fields


def extract_page_fields(page, prefix):
    fields = []
    pid = page.get("handle", page.get("id", ""))
    if page.get("handle"):
        fields.append({"id": f"{prefix}.{pid}.handle", "value": page["handle"]})
    if page.get("title"):
        fields.append({"id": f"{prefix}.{pid}.title", "value": page["title"]})
    if page.get("body_html"):
        fields.append({"id": f"{prefix}.{pid}.body_html", "value": page["body_html"]})
    return fields


def extract_article_fields(article, prefix):
    fields = []
    aid = article.get("handle", article.get("id", ""))
    if article.get("title"):
        fields.append({"id": f"{prefix}.{aid}.title", "value": article["title"]})
    if article.get("body_html"):
        fields.append({"id": f"{prefix}.{aid}.body_html", "value": article["body_html"]})
    if article.get("summary_html"):
        fields.append({"id": f"{prefix}.{aid}.summary_html", "value": article["summary_html"]})
    if article.get("tags"):
        tags = article["tags"] if isinstance(article["tags"], str) else ", ".join(article["tags"])
        fields.append({"id": f"{prefix}.{aid}.tags", "value": tags})

    for mf in article.get("metafields", []):
        ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
        if ns_key in ARTICLE_TRANSLATABLE_METAFIELDS and mf.get("value"):
            fields.append({"id": f"{prefix}.{aid}.mf.{ns_key}", "value": mf["value"]})

    return fields


def extract_metaobject_fields(metaobjects_data, prefix):
    fields = []
    for mo_type, type_data in metaobjects_data.items():
        translatable_keys = METAOBJECT_TRANSLATABLE_FIELDS.get(mo_type, set())
        if not translatable_keys:
            continue
        for obj in type_data.get("objects", []):
            handle = obj.get("handle", obj.get("id", ""))
            for field in obj.get("fields", []):
                if field["key"] in translatable_keys and field.get("value"):
                    fid = f"{prefix}.{mo_type}.{handle}.{field['key']}"
                    fields.append({"id": fid, "value": field["value"]})
    return fields


# =====================================================================
# Apply translated fields back to data structures
# =====================================================================

def _slugify(text):
    """Convert a translated handle to a valid URL slug."""
    import unicodedata
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text.lower())
    text = re.sub(r"[-\s]+", "-", text).strip("-")
    return text


def apply_translations(translations, products, collections, pages, articles, metaobjects):
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

    for a in articles:
        aid = a.get("handle", a.get("id", ""))
        for prefix in ["art", "article"]:
            if f"{prefix}.{aid}.title" in t:
                a["title"] = t[f"{prefix}.{aid}.title"]
            if f"{prefix}.{aid}.body_html" in t:
                a["body_html"] = t[f"{prefix}.{aid}.body_html"]
            if f"{prefix}.{aid}.summary_html" in t:
                a["summary_html"] = t[f"{prefix}.{aid}.summary_html"]
            if f"{prefix}.{aid}.tags" in t:
                a["tags"] = t[f"{prefix}.{aid}.tags"]
            for mf in a.get("metafields", []):
                ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
                fid = f"{prefix}.{aid}.mf.{ns_key}"
                if fid in t:
                    mf["value"] = t[fid]

    if isinstance(metaobjects, dict):
        for mo_type, type_data in metaobjects.items():
            for obj in type_data.get("objects", []):
                handle = obj.get("handle", obj.get("id", ""))
                for prefix in ["mo", "metaobject"]:
                    for field in obj.get("fields", []):
                        fid = f"{prefix}.{mo_type}.{handle}.{field['key']}"
                        if fid in t:
                            field["value"] = t[fid]


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
- Keep product-specific names unchanged (e.g., "Kansa Wand", "Gua Sha")
- Keep ingredient scientific names (INCI names) unchanged
- Preserve ALL HTML tags and attributes exactly
- Preserve Shopify Liquid tags ({{{{ }}}}, {{% %}}) unchanged
- Keep URLs, JSON structure keys, and GIDs unchanged
- For rich_text_field JSON: translate only "value" keys inside text nodes
- For .handle fields: translate the slug to {target_lang} (e.g., "mascarilla-reparadora" → "repairing-hair-mask"). Keep lowercase, hyphens only, no special characters.
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

    for attempt in range(4):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": build_system_prompt(target_lang)},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
            )
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
                    print(f"    Retrying...")
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

    print(f"    FAILED after 4 attempts")
    return {}, 0


# =====================================================================
# Main
# =====================================================================

def load_json(filepath):
    if not os.path.exists(filepath):
        return [] if filepath.endswith(".json") else {}
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Translate gaps using TOON batched format")
    parser.add_argument("--lang", required=True, choices=["en", "ar"],
                        help="Target language: en or ar")
    parser.add_argument("--dry", action="store_true",
                        help="Dry run: show what would be translated without calling API")
    parser.add_argument("--model", default="gpt-4o",
                        help="OpenAI model (default: gpt-4o)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"Fields per batch (default: {BATCH_SIZE})")
    parser.add_argument("--tpm", type=int, default=TPM_LIMIT,
                        help=f"Tokens-per-minute budget (default: {TPM_LIMIT})")
    args = parser.parse_args()

    target_lang = "English" if args.lang == "en" else "Arabic"
    output_dir = EN_DIR if args.lang == "en" else AR_DIR

    # Load Spain data
    spain_products = load_json(os.path.join(SPAIN_DIR, "products.json"))
    spain_collections = load_json(os.path.join(SPAIN_DIR, "collections.json"))
    spain_pages = load_json(os.path.join(SPAIN_DIR, "pages.json"))
    spain_articles = load_json(os.path.join(SPAIN_DIR, "articles.json"))
    spain_metaobjects = load_json(os.path.join(SPAIN_DIR, "metaobjects.json"))

    if not spain_products:
        print("ERROR: Spain export is empty. Run export_spain.py first.")
        sys.exit(1)

    # Load scraped data
    scraped_products = load_json(os.path.join(output_dir, "products.json"))
    scraped_collections = load_json(os.path.join(output_dir, "collections.json"))
    scraped_pages = load_json(os.path.join(output_dir, "pages.json"))
    scraped_articles = load_json(os.path.join(output_dir, "articles.json"))
    scraped_metaobjects = load_json(os.path.join(output_dir, "metaobjects.json"))

    print(f"{'=' * 60}")
    print(f"TRANSLATING GAPS → {target_lang.upper()}")
    print(f"{'=' * 60}")

    # ---- Identify gaps ----
    gap_products = find_gaps(spain_products, scraped_products, key_field="sku")
    gap_collections = find_gaps(spain_collections, scraped_collections)
    gap_pages = find_gaps(spain_pages, scraped_pages)
    # Articles always need translation (not in Magento)
    gap_articles = spain_articles

    # Metaobjects: types with translatable fields ALWAYS need LLM translation.
    # The scraper copies Spain metaobjects as-is (still Spanish text).
    # Types without translatable fields (shopify-- prefixed) can be skipped.
    gap_metaobjects = {}
    if isinstance(spain_metaobjects, dict):
        for mo_type, type_data in spain_metaobjects.items():
            has_translatable = mo_type in METAOBJECT_TRANSLATABLE_FIELDS
            objs = type_data.get("objects", [])
            if not objs:
                continue

            if has_translatable:
                # Always include — scraper only copies, doesn't translate
                gap_metaobjects[mo_type] = {
                    "definition": type_data.get("definition", {}),
                    "objects": objs,
                }
            else:
                # Non-translatable types: check for genuinely missing items
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

    # Also, products that WERE scraped still need metafield translation
    # (Magento doesn't have Shopify accordion metafields)
    matched_products_needing_metafields = []
    if scraped_products:
        scraped_handles = {p.get("handle", "") for p in scraped_products}
        for sp in spain_products:
            if sp.get("handle") in scraped_handles:
                # Check if it has translatable metafields not in scraped version
                has_metafields = any(
                    f"{mf.get('namespace', '')}.{mf.get('key', '')}" in PRODUCT_TRANSLATABLE_METAFIELDS
                    for mf in sp.get("metafields", [])
                    if mf.get("value")
                )
                if has_metafields:
                    matched_products_needing_metafields.append(sp)

    # ---- Extract all translatable fields ----
    all_fields = []

    for p in gap_products:
        all_fields.extend(extract_product_fields(p, "prod"))

    # For matched products, only extract metafields (title/body already scraped)
    for p in matched_products_needing_metafields:
        pid = p.get("handle", p.get("id", ""))
        for mf in p.get("metafields", []):
            ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
            if ns_key in PRODUCT_TRANSLATABLE_METAFIELDS and mf.get("value"):
                all_fields.append({"id": f"prod.{pid}.mf.{ns_key}", "value": mf["value"]})

    for c in gap_collections:
        all_fields.extend(extract_collection_fields(c, "coll"))

    for pg in gap_pages:
        all_fields.extend(extract_page_fields(pg, "page"))

    for a in gap_articles:
        all_fields.extend(extract_article_fields(a, "art"))

    all_fields.extend(extract_metaobject_fields(gap_metaobjects, "mo"))

    # Filter out empty values
    all_fields = [f for f in all_fields if f.get("value") and f["value"].strip()]

    print(f"\n  Total fields to translate: {len(all_fields)}")
    print(f"  Batch size: {args.batch_size}")
    print(f"  Estimated batches: {max(1, (len(all_fields) + args.batch_size - 1) // args.batch_size)}")

    # Breakdown
    field_types = {}
    for f in all_fields:
        category = f["id"].split(".")[0]
        field_types[category] = field_types.get(category, 0) + 1
    print(f"\n  Breakdown:")
    for cat, count in sorted(field_types.items()):
        print(f"    {cat}: {count} fields")

    if args.dry:
        print("\n  DRY RUN — no API calls made")
        print(f"\n  Sample fields (first 10):")
        for f in all_fields[:10]:
            val = f["value"][:80] + "..." if len(f["value"]) > 80 else f["value"]
            print(f"    {f['id']}: {val}")
        return

    if not all_fields:
        print("\n  Nothing to translate!")
        return

    # ---- Load progress ----
    progress_file = os.path.join(output_dir, f"_translation_progress_{args.lang}.json")
    all_translations = {}
    if os.path.exists(progress_file):
        all_translations = load_json(progress_file)
        if isinstance(all_translations, dict):
            print(f"\n  Resuming: {len(all_translations)} fields already translated")
        else:
            all_translations = {}

    # Filter out already translated
    remaining = [f for f in all_fields if f["id"] not in all_translations]
    print(f"  Remaining: {len(remaining)} fields")

    if not remaining:
        print("  All fields already translated!")
    else:
        # ---- Translate in parallel batches ----
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            print("ERROR: OPENAI_API_KEY not set. Add it to .env")
            sys.exit(1)

        client = OpenAI(api_key=api_key)

        # Adaptive batching: size by token count, not fixed field count
        max_batch_tokens = args.batch_size * 100  # ~100 tokens per field average
        batches = adaptive_batch(remaining, max_tokens=max_batch_tokens)
        total_batches = len(batches)

        tpm_limit = args.tpm
        batch_sizes = [len(b) for b in batches]
        print(f"\n  {total_batches} adaptive batches (sizes: {min(batch_sizes)}-{max(batch_sizes)} fields)")
        print(f"  TPM budget: {tpm_limit:,}")

        # Track tokens used in the current 60-second window
        window_start = time.time()
        window_tokens = 0
        failed_fields = []

        for i, batch in enumerate(batches):
            # TPM throttle: if we'd exceed the budget, wait for the window to reset
            now = time.time()
            elapsed = now - window_start
            if elapsed >= 60:
                # New window
                window_start = now
                window_tokens = 0
            elif window_tokens >= tpm_limit * 0.85:
                # We've used 85%+ of the budget — wait for window to reset
                wait = 60 - elapsed + 2
                print(f"    TPM throttle: {window_tokens:,} tokens used, waiting {wait:.0f}s for window reset...")
                time.sleep(wait)
                window_start = time.time()
                window_tokens = 0

            t_map, tokens_used = translate_batch(
                client, args.model, batch,
                "Spanish", target_lang,
                i + 1, total_batches,
            )
            window_tokens += tokens_used

            if t_map:
                all_translations.update(t_map)
                save_json(all_translations, progress_file)

                # Track fields that were in this batch but not in the response
                batch_ids = {f["id"] for f in batch}
                missing_from_batch = batch_ids - set(t_map.keys())
                if missing_from_batch:
                    failed_fields.extend([f for f in batch if f["id"] in missing_from_batch])
            else:
                # Entire batch failed
                failed_fields.extend(batch)

        # Report failed fields
        if failed_fields:
            print(f"\n  WARNING: {len(failed_fields)} fields failed translation")
            print(f"  Re-run this command to retry them (progress is saved)")
            # Save failed field IDs for debugging
            failed_ids = [f["id"] for f in failed_fields]
            save_json(failed_ids, os.path.join(output_dir, f"_failed_fields_{args.lang}.json"))

    # ---- Merge translations into output data ----
    print(f"\n  Merging {len(all_translations)} translations into output files...")

    # Start with scraped data as base, add gap items
    output_products = list(scraped_products) if scraped_products else []
    output_collections = list(scraped_collections) if scraped_collections else []
    output_pages = list(scraped_pages) if scraped_pages else []
    output_articles = list(scraped_articles) if scraped_articles else []
    output_metaobjects = dict(scraped_metaobjects) if isinstance(scraped_metaobjects, dict) else {}

    # Add gap items (deep copies with Spain data as base)
    for p in gap_products:
        output_products.append(copy.deepcopy(p))
    for c in gap_collections:
        output_collections.append(copy.deepcopy(c))
    for pg in gap_pages:
        output_pages.append(copy.deepcopy(pg))

    # Articles: replace entirely (all need translation)
    output_articles = [copy.deepcopy(a) for a in gap_articles]

    # Metaobjects: merge gap types
    for mo_type, type_data in gap_metaobjects.items():
        if mo_type not in output_metaobjects:
            output_metaobjects[mo_type] = copy.deepcopy(type_data)
        else:
            existing_handles = {o.get("handle") for o in output_metaobjects[mo_type].get("objects", [])}
            for obj in type_data.get("objects", []):
                if obj.get("handle") not in existing_handles:
                    output_metaobjects[mo_type]["objects"].append(copy.deepcopy(obj))

    # Apply translations to all output data
    apply_translations(
        all_translations,
        output_products, output_collections, output_pages,
        output_articles, output_metaobjects,
    )

    # Also apply metafield translations to scraped products
    for p in output_products:
        pid = p.get("handle", p.get("id", ""))
        for mf in p.get("metafields", []):
            ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
            fid = f"prod.{pid}.mf.{ns_key}"
            if fid in all_translations:
                mf["value"] = all_translations[fid]

    # ---- Save output ----
    save_json(output_products, os.path.join(output_dir, "products.json"))
    save_json(output_collections, os.path.join(output_dir, "collections.json"))
    save_json(output_pages, os.path.join(output_dir, "pages.json"))
    save_json(output_articles, os.path.join(output_dir, "articles.json"))
    save_json(output_metaobjects, os.path.join(output_dir, "metaobjects.json"))

    # Copy non-translatable files from Spain
    for fname in ["blogs.json", "metaobject_definitions.json"]:
        src = os.path.join(SPAIN_DIR, fname)
        if os.path.exists(src):
            data = load_json(src)
            save_json(data, os.path.join(output_dir, fname))

    print(f"\n{'=' * 60}")
    print(f"TRANSLATION COMPLETE → {target_lang.upper()}")
    print(f"{'=' * 60}")
    print(f"  Products:    {len(output_products)}")
    print(f"  Collections: {len(output_collections)}")
    print(f"  Pages:       {len(output_pages)}")
    print(f"  Articles:    {len(output_articles)}")
    if output_metaobjects:
        mo_total = sum(len(v.get("objects", [])) for v in output_metaobjects.values())
        print(f"  Metaobjects: {mo_total}")
    print(f"  Output:      {output_dir}/")

    # Completeness check
    translated_count = len(all_translations)
    total_needed = len(all_fields)
    completeness = (translated_count / total_needed * 100) if total_needed else 100
    print(f"\n  Completeness: {translated_count}/{total_needed} fields ({completeness:.1f}%)")
    if completeness < 100:
        print(f"  ⚠ {total_needed - translated_count} fields still untranslated")
        print(f"  Re-run: python translate_gaps.py --lang {args.lang}")
    else:
        print(f"\n  Next: python import_english.py" if args.lang == "en" else
              f"\n  Next: python import_arabic.py")


if __name__ == "__main__":
    main()
