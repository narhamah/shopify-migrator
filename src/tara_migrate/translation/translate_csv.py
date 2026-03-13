"""Consolidated CSV translation module for Shopify 'Translate and adapt' exports.

Supports three translation modes:
1. Batch TOON via translate_gaps (fast, ~120 fields/call) — original translate_csv.py
2. Per-field with Arabic scraping reference (max quality) — translate_csv_ar.py
3. TOON-batched via Responses API with cached developer prompt — translate_tara_ar.py

Usage (CLI):
    python -m tara_migrate.translation.translate_csv --input data/export.csv
    python -m tara_migrate.translation.translate_csv --input data/export.csv --dry-run
    python -m tara_migrate.translation.translate_csv --input data/export.csv --no-upload
    python -m tara_migrate.translation.translate_csv --input data/export.csv --upload-only translated.csv
    python -m tara_migrate.translation.translate_csv --input data/export.csv --no-scrape
    python -m tara_migrate.translation.translate_csv --input data/export.csv --start-batch 5

Usage (library):
    from tara_migrate.translation.translate_csv import translate_csv
    translate_csv("data/export.csv", output_dir="Arabic/", model="gpt-5-nano")
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time

from tara_migrate.core.csv_utils import (
    ARABIC_LOCALE,
    CSV_TYPE_TO_GID,
    NEEDS_PARENT_RESOLUTION,
    SKIP_TYPES,
    is_keep_as_is,
    is_non_translatable,
)
from tara_migrate.core.language import has_arabic
from tara_migrate.core.rich_text import (
    extract_text_nodes,
    is_rich_text_json,
    extract_text,
    rebuild,
    sanitize,
    validate_structure,
)
from tara_migrate.translation.engine import load_developer_prompt
from tara_migrate.translation.toon import DELIM, from_toon, to_toon


# =====================================================================
# Token estimation & adaptive batching
# =====================================================================

def _estimate_tokens(text):
    """Rough token estimate: ~3 chars per token for mixed EN/AR content."""
    return max(1, len(text) // 3)


def adaptive_batch(fields, max_tokens=6000, chunk_threshold=6000):
    """Split fields into batches sized by estimated token count.

    Short fields (titles, buttons) get packed densely.
    Long fields (body_html, rich_text JSON) get smaller batches.
    Fields exceeding chunk_threshold are split into sub-fields using
    _split_oversized_field() and reassembled after translation.

    Args:
        fields: List of field dicts with 'id' and 'value'.
        max_tokens: Pack multiple fields into a batch up to this limit.
        chunk_threshold: Split a single field only when it exceeds this
            limit.  Defaults to 6000 tokens (~18K chars).  The packing
            limit (max_tokens) is intentionally independent — a small
            max_tokens just means each normal field gets its own batch,
            but does NOT trigger chunking of large fields.
    """
    batches = []
    current_batch = []
    current_tokens = 0

    for field in fields:
        field_tokens = _estimate_tokens(field["value"])

        # Only chunk when a single field exceeds chunk_threshold
        if field_tokens > chunk_threshold:
            # Flush current batch first
            if current_batch:
                batches.append(current_batch)
                current_batch = []
                current_tokens = 0

            chunks = _split_oversized_field(field, chunk_threshold)
            for chunk in chunks:
                batches.append([chunk])
            continue

        if current_batch and (current_tokens + field_tokens > max_tokens):
            batches.append(current_batch)
            current_batch = []
            current_tokens = 0
        current_batch.append(field)
        current_tokens += field_tokens

    if current_batch:
        batches.append(current_batch)
    return batches


def _split_oversized_field(field, max_tokens):
    """Split a single oversized field into multiple chunk-fields.

    Each chunk-field has the same id with a `:chunk_N` suffix.
    The original field gets a `_chunks` key recording how many parts.

    For rich_text JSON, splits by top-level children array.
    For HTML, splits by block elements (<p>, <div>, <h1-h6>, <ul>, <ol>, <li>).
    For plain text, splits by paragraphs (double newlines).
    """
    value = field["value"]
    field_id = field["id"]

    # Try to detect rich_text JSON
    segments = None
    is_rich_text = False
    if value.strip().startswith("{") and '"type"' in value[:100]:
        try:
            parsed = json.loads(value)
            children = parsed.get("children", [])
            if children and isinstance(children, list):
                is_rich_text = True
                segments = []
                current_group = []
                current_tokens = 0
                for child in children:
                    child_json = json.dumps(child, ensure_ascii=False)
                    child_tokens = _estimate_tokens(child_json)
                    if current_group and current_tokens + child_tokens > max_tokens * 0.8:
                        seg = json.dumps(
                            {**parsed, "children": current_group},
                            ensure_ascii=False)
                        segments.append(seg)
                        current_group = []
                        current_tokens = 0
                    current_group.append(child)
                    current_tokens += child_tokens
                if current_group:
                    seg = json.dumps(
                        {**parsed, "children": current_group},
                        ensure_ascii=False)
                    segments.append(seg)
        except (json.JSONDecodeError, TypeError):
            pass

    # Try HTML splitting
    if not segments and ("<p" in value or "<div" in value or "<h" in value):
        segments = _split_html_blocks(value, max_tokens)

    # Fallback: split by paragraphs / lines
    if not segments:
        segments = _split_text_blocks(value, max_tokens)

    # Build chunk fields
    chunks = []
    for i, seg in enumerate(segments):
        chunk_field = dict(field)
        chunk_field["id"] = f"{field_id}:chunk_{i}"
        chunk_field["value"] = seg
        chunk_field["_is_chunk"] = True
        chunk_field["_chunk_index"] = i
        chunk_field["_chunk_total"] = len(segments)
        chunk_field["_parent_id"] = field_id
        chunk_field["_is_rich_text_chunk"] = is_rich_text
        chunks.append(chunk_field)

    # Mark original field
    field["_chunks"] = len(segments)
    return chunks


def _split_html_blocks(html, max_tokens):
    """Split HTML by block-level elements."""
    block_pattern = re.compile(
        r'(<(?:p|div|h[1-6]|ul|ol|li|section|article|blockquote|table|tr|thead|tbody)'
        r'[\s>])', re.IGNORECASE)
    parts = block_pattern.split(html)

    segments = []
    current = ""
    current_tokens = 0

    for part in parts:
        part_tokens = _estimate_tokens(part)
        if current and current_tokens + part_tokens > max_tokens * 0.8:
            segments.append(current)
            current = ""
            current_tokens = 0
        current += part
        current_tokens += part_tokens

    if current:
        segments.append(current)

    return segments if len(segments) > 1 else [html]


def _split_text_blocks(text, max_tokens):
    """Split plain text by double newlines, then single newlines if needed."""
    # Try double newline first
    paragraphs = text.split("\n\n")
    if len(paragraphs) > 1:
        return _merge_text_chunks(paragraphs, "\n\n", max_tokens)

    # Try single newline
    lines = text.split("\n")
    if len(lines) > 1:
        return _merge_text_chunks(lines, "\n", max_tokens)

    # Last resort: split by sentences
    sentences = re.split(r'(?<=[.!?。])\s+', text)
    if len(sentences) > 1:
        return _merge_text_chunks(sentences, " ", max_tokens)

    # Absolute last resort: hard split by character count
    char_limit = max_tokens * 3  # ~3 chars per token
    return [text[i:i + char_limit] for i in range(0, len(text), char_limit)]


def _merge_text_chunks(parts, separator, max_tokens):
    """Merge text parts into chunks that fit within token limits."""
    segments = []
    current = ""
    current_tokens = 0

    for part in parts:
        part_tokens = _estimate_tokens(part)
        if current and current_tokens + part_tokens > max_tokens * 0.8:
            segments.append(current)
            current = ""
            current_tokens = 0
        if current:
            current += separator
        current += part
        current_tokens += part_tokens

    if current:
        segments.append(current)
    return segments


def _reassemble_chunks(fields, all_translations, our_translations):
    """Reassemble chunked field translations back into their parent fields.

    Finds all chunk translations (id contains ':chunk_N'), groups by parent,
    and merges them in order. For rich_text JSON chunks, merges children arrays.
    For HTML/text chunks, concatenates.

    Updates all_translations and our_translations in place.
    """
    # Find chunked fields
    chunked_parents = {}
    for field in fields:
        if field.get("_chunks"):
            chunked_parents[field["id"]] = field

    if not chunked_parents:
        return

    # Group chunk translations by parent id
    chunk_groups = {}  # parent_id -> [(index, translated_value, field)]
    for field in fields:
        if not field.get("_is_chunk"):
            # Also scan batches — chunks have ids like "parent_id:chunk_N"
            continue
        parent_id = field.get("_parent_id")
        if not parent_id:
            continue
        chunk_id = field["id"]
        if chunk_id in all_translations:
            chunk_groups.setdefault(parent_id, []).append(
                (field["_chunk_index"], all_translations[chunk_id], field))

    # Also scan all_translations for chunk keys we may have missed
    for key, value in list(all_translations.items()):
        if ":chunk_" in key:
            parts = key.rsplit(":chunk_", 1)
            if len(parts) == 2:
                parent_id, idx_str = parts
                if parent_id in chunked_parents:
                    try:
                        idx = int(idx_str)
                    except ValueError:
                        continue
                    group = chunk_groups.setdefault(parent_id, [])
                    if not any(g[0] == idx for g in group):
                        group.append((idx, value, None))

    reassembled = 0
    for parent_id, chunks in chunk_groups.items():
        parent = chunked_parents.get(parent_id)
        if not parent:
            continue

        expected = parent.get("_chunks", 0)
        if len(chunks) < expected:
            print(f"    WARNING: Only {len(chunks)}/{expected} chunks translated for {parent_id}")

        # Sort by index and merge
        chunks.sort(key=lambda x: x[0])
        translated_parts = [c[1] for c in chunks]

        # Check if chunks are rich_text JSON
        is_rt = any(c[2] and c[2].get("_is_rich_text_chunk") for c in chunks if c[2])

        if is_rt:
            merged = _merge_rich_text_chunks(translated_parts)
        else:
            merged = "".join(translated_parts)

        all_translations[parent_id] = merged
        our_translations[parent_id] = merged
        reassembled += 1

    if reassembled:
        print(f"  Reassembled {reassembled} chunked field(s)")


def _merge_rich_text_chunks(parts):
    """Merge rich_text JSON chunks by combining their children arrays."""
    all_children = []
    base = None
    for part in parts:
        try:
            parsed = json.loads(part)
            if base is None:
                base = parsed
            children = parsed.get("children", [])
            all_children.extend(children)
        except (json.JSONDecodeError, TypeError):
            # If parsing fails, treat as text
            return "".join(parts)

    if base is not None:
        base["children"] = all_children
        return json.dumps(base, ensure_ascii=False)
    return "".join(parts)


# =====================================================================
# Arabic reference scraping (from translate_csv_ar.py)
# =====================================================================

def scrape_arabic_reference(output_dir):
    """Scrape Arabic content from taraformula.ae into the output directory.

    Returns the raw scraped data dict. Caches results to
    ``<output_dir>/ar_scraped_reference.json``.
    """
    cache_file = os.path.join(output_dir, "ar_scraped_reference.json")

    if os.path.exists(cache_file):
        print(f"  Using cached scrape from {cache_file}")
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    print("  Scraping Arabic content from taraformula.ae (ae-ar)...")
    from tara_migrate.tools.scrape_kuwait import MagentoGraphQL

    gql = MagentoGraphQL(base_url="https://taraformula.ae", delay=3.0)
    store_code = "ae-ar"

    reference = {"products": [], "collections": [], "pages": []}

    # --- Products (all pages) ---
    current_page = 1
    while True:
        query = f"""
        {{
          products(search: "", pageSize: 50, currentPage: {current_page}) {{
            total_count
            items {{
              name
              sku
              url_key
              description {{ html }}
              short_description {{ html }}
              meta_title
              meta_description
            }}
            page_info {{ total_pages current_page }}
          }}
        }}
        """
        data = gql.query(query, store_code=store_code)
        if not data or "data" not in data:
            break

        products = data["data"].get("products", {})
        items = products.get("items", [])
        for item in items:
            reference["products"].append({
                "name": item.get("name", ""),
                "sku": item.get("sku", ""),
                "url_key": item.get("url_key", ""),
                "description": item.get("description", {}).get("html", ""),
                "short_description": item.get("short_description", {}).get("html", ""),
                "meta_title": item.get("meta_title", ""),
                "meta_description": item.get("meta_description", ""),
            })

        page_info = products.get("page_info", {})
        total_pages = page_info.get("total_pages", 1)
        print(f"    Products page {current_page}/{total_pages}: {len(items)} items")
        if current_page >= total_pages:
            break
        current_page += 1

    # --- Categories ---
    cat_query = """
    {
      categories(filters: {}) {
        items {
          name
          url_key
          description
          meta_title
          meta_description
          children {
            name url_key description meta_title meta_description
            children {
              name url_key description meta_title meta_description
            }
          }
        }
      }
    }
    """
    data = gql.query(cat_query, store_code=store_code)
    if data and "data" in data:
        def _flatten_cats(items, out):
            for item in items:
                out.append({
                    "name": item.get("name", ""),
                    "url_key": item.get("url_key", ""),
                    "description": item.get("description", ""),
                    "meta_title": item.get("meta_title", ""),
                    "meta_description": item.get("meta_description", ""),
                })
                _flatten_cats(item.get("children", []), out)
        _flatten_cats(data["data"].get("categories", {}).get("items", []), reference["collections"])
        print(f"    Categories: {len(reference['collections'])} items")

    # --- CMS Pages ---
    for page_id in ["home", "about", "about-us", "faq", "contact",
                     "privacy-policy", "terms-and-conditions", "shipping", "returns"]:
        data = gql.query(f"""
        {{
          cmsPage(identifier: "{page_id}") {{
            identifier
            title
            content
            meta_title
            meta_description
          }}
        }}
        """, store_code)
        if data and "data" in data and data["data"].get("cmsPage"):
            page = data["data"]["cmsPage"]
            reference["pages"].append({
                "identifier": page.get("identifier", page_id),
                "title": page.get("title", ""),
                "content": page.get("content", ""),
                "meta_title": page.get("meta_title", ""),
                "meta_description": page.get("meta_description", ""),
            })
            print(f"    Page: {page_id} — {page.get('title', '')}")

    # --- Ingredients page ---
    try:
        import requests as http_requests
        resp = http_requests.get(
            "https://taraformula.ae/ae-ar/ingredients",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
        )
        if resp.status_code == 200:
            reference["ingredients_html"] = resp.text[:50000]
            print(f"    Ingredients page: {len(resp.text):,} chars")
    except Exception as e:
        print(f"    Ingredients page error: {e}")

    # Save
    os.makedirs(output_dir, exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(reference, f, ensure_ascii=False, indent=2)
    print(f"  Saved scrape to {cache_file}")

    return reference


def _build_optimized_reference(reference, output_dir):
    """Build an optimized, deduplicated Arabic reference text file.

    Saved to ``<output_dir>/ar_optimized_reference.txt`` for inspection/reuse.
    Returns the reference text.
    """
    ref_file = os.path.join(output_dir, "ar_optimized_reference.txt")

    sections = []

    # Product names + taglines
    product_lines = []
    for p in reference.get("products", []):
        name = p.get("name", "").strip()
        if not name:
            continue
        tagline = p.get("short_description", "").strip()
        tagline = re.sub(r"<[^>]+>", "", tagline).strip()
        if tagline:
            product_lines.append(f"  {name}: {tagline[:200]}")
        else:
            product_lines.append(f"  {name}")
    if product_lines:
        sections.append("PRODUCT NAMES & TAGLINES (Arabic):\n" + "\n".join(product_lines))

    # Product descriptions (first 5 for style reference)
    desc_lines = []
    for p in reference.get("products", [])[:5]:
        name = p.get("name", "").strip()
        desc = p.get("description", "").strip()
        desc = re.sub(r"<[^>]+>", " ", desc).strip()
        desc = re.sub(r"\s+", " ", desc)
        if name and desc:
            desc_lines.append(f"  [{name}]\n  {desc[:500]}")
    if desc_lines:
        sections.append("PRODUCT DESCRIPTIONS (Arabic, first 5):\n" + "\n\n".join(desc_lines))

    # Category/collection names
    cat_lines = []
    seen_cats = set()
    for c in reference.get("collections", []):
        name = c.get("name", "").strip()
        if name and name not in seen_cats:
            seen_cats.add(name)
            cat_lines.append(f"  {name}")
    if cat_lines:
        sections.append("COLLECTION/CATEGORY NAMES (Arabic):\n" + "\n".join(cat_lines))

    # SEO titles & descriptions
    seo_lines = []
    for p in reference.get("products", [])[:10]:
        mt = p.get("meta_title", "").strip()
        md = p.get("meta_description", "").strip()
        if mt or md:
            line = f"  title: {mt}" if mt else ""
            if md:
                line += f"\n  desc: {md[:200]}"
            seo_lines.append(line.strip())
    if seo_lines:
        sections.append("SEO SAMPLES (Arabic):\n" + "\n".join(seo_lines))

    ref_text = "\n\n".join(sections)

    with open(ref_file, "w", encoding="utf-8") as f:
        f.write(ref_text)
    print(f"  Saved optimized reference to {ref_file} ({len(ref_text):,} chars)")

    return ref_text


# =====================================================================
# System prompt builder (for per-field / chat completions mode)
# =====================================================================

def _build_system_prompt(tov_text, reference_text):
    """Build the system prompt with TOV + reference at the top for caching."""
    prompt = f"""You are a professional Arabic translator for TARA, a luxury scalp-care and hair-health brand.

=== TARA ARABIC TONE OF VOICE ===
{tov_text}

"""
    if reference_text:
        prompt += f"""=== ARABIC REFERENCE CONTENT (from taraformula.ae) ===
Use this as your guide for terminology, product names, and writing style:

{reference_text}

"""

    prompt += """=== TRANSLATION FORMAT ===
INPUT/OUTPUT: TOON (Token-Oriented Object Notation)
Each line is: id|value
- id = field identifier — keep EXACTLY as-is, never translate
- value = text to translate to Arabic
- Escape: \\n = newline, \\p = pipe, \\\\ = backslash
- Return the SAME id with translated value only

=== TRANSLATION RULES ===
- Keep "TARA" unchanged — never translate the brand name
- Keep untranslatable tool proper nouns unchanged (e.g., "Kansa Wand", "Gua Sha")
- TRANSLATE product type words to Arabic: Shampoo→شامبو, Serum→سيروم, Conditioner→بلسم, Mask→ماسك, Oil→زيت, etc.
- TRANSLATE action verbs: Shop→تسوّق, Discover→اكتشف, Buy→اشترِ, Subscribe→اشترك, etc.
- Keep ingredient scientific/INCI names unchanged
- Preserve ALL HTML tags and attributes exactly
- Preserve Shopify Liquid tags ({{ }}, {% %}) unchanged
- Keep URLs, JSON structure keys, and GIDs unchanged
- For rich_text_field JSON: translate only "value" keys inside text nodes
- Translate meaning, not words — rebuild the sentence in natural Arabic
- Always use فروة الرأس (never الفروة alone), خصلات الشعر, الجذور
- Present tense verbs: ينظّف، يعزّز، يرطّب، يرمّم
- No marketing fluff: avoid سحري، فاخر، مثالي، مذهل
- Start with benefit, then mechanism
- Return ONLY the translated TOON line, no explanations or commentary"""

    return prompt


# =====================================================================
# Per-field translation (from translate_csv_ar.py)
# =====================================================================

def _translate_field_perfield(client, model, field, system_prompt, field_num, total_fields):
    """Translate a single field using TOON format via chat completions.

    Sending one field at a time ensures maximum quality per translation.
    The system prompt is cached by OpenAI after the first call.

    Returns (translated_value, total_tokens).
    """
    toon_input = to_toon([field])

    prompt = (
        "Translate this TOON field from English to Arabic. "
        "Follow the TARA Arabic tone of voice strictly.\n\n"
        f"{toon_input}"
    )

    REASONING_MODELS = {"o3", "o3-mini", "o4-mini", "gpt-5-mini", "gpt-5", "gpt-5-nano"}
    is_reasoning = any(model.startswith(rm) for rm in REASONING_MODELS)

    for attempt in range(4):
        try:
            api_kwargs = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt},
                ],
            }
            if is_reasoning:
                api_kwargs["reasoning_effort"] = "low"
            else:
                api_kwargs["temperature"] = 0.3

            response = client.chat.completions.create(**api_kwargs)
            result = response.choices[0].message.content.strip()

            # Strip markdown code fences
            if result.startswith("```"):
                lines = result.split("\n")
                if lines[-1].strip() == "```":
                    result = "\n".join(lines[1:-1])
                else:
                    result = "\n".join(lines[1:])

            translated = from_toon(result)

            if not translated:
                if attempt < 3:
                    time.sleep(2)
                    continue
                return None, 0

            # Find our field in the response
            translated_value = None
            for entry in translated:
                if entry["id"] == field["id"]:
                    translated_value = entry["value"]
                    break
            if translated_value is None and translated:
                # Model may have mangled the ID -- take the first value
                translated_value = translated[0]["value"]

            usage = response.usage
            total_tokens = usage.prompt_tokens + usage.completion_tokens
            cached = getattr(usage, "prompt_tokens_details", None)
            cached_tokens = getattr(cached, "cached_tokens", 0) if cached else 0

            # Compact progress line
            val_preview = field["value"][:50].replace("\n", " ")
            ar_preview = (translated_value or "")[:50].replace("\n", " ")
            cache_pct = f" [cached:{cached_tokens}]" if cached_tokens > 0 else ""
            print(f"  [{field_num}/{total_fields}] {val_preview}... -> {ar_preview}...{cache_pct}")

            return translated_value, total_tokens

        except Exception as e:
            err_str = str(e)
            print(f"  [{field_num}/{total_fields}] Error: {e}")
            if attempt < 3:
                wait = 2 ** (attempt + 1)
                retry_match = re.search(r"try again in (\d+\.?\d*)s", err_str)
                if retry_match:
                    wait = max(wait, float(retry_match.group(1)) + 2)
                elif "429" in err_str or "rate" in err_str.lower():
                    wait = max(wait, 45)
                print(f"    Retrying in {wait:.0f}s...")
                time.sleep(wait)

    print(f"  [{field_num}/{total_fields}] FAILED after 4 attempts")
    return None, 0



# =====================================================================
# Responses API batch translation (from translate_tara_ar.py)
# =====================================================================

def _retry_missing_responses_api(client, model, missing_fields, developer_prompt,
                                 reasoning_effort="medium"):
    """Retry translating a small set of fields missed in a batch.

    Uses a focused prompt with the same developer prompt for cache hits.
    Returns (translation_map, tokens_used).
    """
    # Use opaque numeric IDs so the model can't translate them
    idx_to_real_id = {str(i): f["id"] for i, f in enumerate(missing_fields)}
    opaque_fields = [{"id": str(i), "value": f["value"]}
                     for i, f in enumerate(missing_fields)]
    toon_input = to_toon(opaque_fields)
    user_message = (
        "Translate the following TOON input into Tara Arabic and return TOON only.\n\n"
        f"<TOON>\n{toon_input}\n</TOON>"
    )
    try:
        response = client.responses.create(
            model=model,
            instructions=developer_prompt,
            input=user_message,
            reasoning={"effort": reasoning_effort},
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
        # Map opaque numeric IDs back to real field IDs
        t_map = {}
        for e in translated:
            real_id = idx_to_real_id.get(e["id"])
            if real_id:
                t_map[real_id] = e["value"]
        tokens = (response.usage.input_tokens or 0) + (response.usage.output_tokens or 0)
        print(f"    Retry got {len(t_map)}/{len(missing_fields)} translations ({tokens} tokens)")
        return t_map, tokens
    except Exception as e:
        print(f"    Retry failed: {e}")
        return {}, 0


def _translate_batch_responses_api(client, model, fields, developer_prompt,
                                   batch_num, total_batches, output_dir,
                                   reasoning_effort="medium"):
    """Translate a batch of fields using the OpenAI Responses API with prompt caching.

    Returns (translation_map, total_tokens).
    """
    # Use opaque numeric IDs so the model only sees content to translate
    idx_to_real_id = {str(i): f["id"] for i, f in enumerate(fields)}
    opaque_fields = [{"id": str(i), "value": f["value"]}
                     for i, f in enumerate(fields)]
    toon_input = to_toon(opaque_fields)
    user_message = (
        "Translate the following TOON input into Tara Arabic and return TOON only.\n"
        "The source text may be in English or Spanish -- translate both to Arabic.\n\n"
        f"<TOON>\n{toon_input}\n</TOON>"
    )

    est_tokens = sum(_estimate_tokens(f["value"]) for f in fields)
    print(f"  Batch {batch_num}/{total_batches}: {len(fields)} fields "
          f"(~{est_tokens:,} value tokens)...")

    for attempt in range(4):
        try:
            response = client.responses.create(
                model=model,
                instructions=developer_prompt,
                input=user_message,
                reasoning={"effort": reasoning_effort},
            )

            # Check for refusal before parsing
            refusal_found = False
            for item in response.output:
                if item.type == "message":
                    for content in item.content:
                        if getattr(content, "type", "") == "refusal":
                            refusal_text = getattr(content, "refusal", str(content))
                            print(f"    REFUSAL (attempt {attempt + 1}): {refusal_text}")
                            debug_file = os.path.join(
                                output_dir, f".debug_refusal_batch_{batch_num}.txt")
                            with open(debug_file, "w", encoding="utf-8") as df:
                                df.write(f"=== REFUSAL (attempt {attempt + 1}) ===\n")
                                df.write(f"{refusal_text}\n\n")
                                df.write(f"=== INPUT TOON ({len(fields)} fields) ===\n")
                                df.write(toon_input[:5000])
                            refusal_found = True
                            break
                    if refusal_found:
                        break

            if refusal_found:
                if attempt < 3:
                    time.sleep(2)
                    continue
                return {}, 0

            # Extract text output
            result = ""
            for item in response.output:
                if item.type == "message":
                    for content in item.content:
                        if content.type == "output_text":
                            result += content.text

            result = result.strip()

            # Detect text-based refusal
            if result and DELIM not in result and (
                    "sorry" in result.lower() or "can't process" in result.lower()):
                print(f"    TEXT REFUSAL (attempt {attempt + 1}): {result[:200]}")
                debug_file = os.path.join(output_dir, f".debug_refusal_batch_{batch_num}.txt")
                with open(debug_file, "w", encoding="utf-8") as df:
                    df.write(f"=== TEXT REFUSAL (attempt {attempt + 1}) ===\n")
                    df.write(f"{result}\n\n")
                    df.write(f"=== INPUT TOON ({len(fields)} fields, first 5000 chars) ===\n")
                    df.write(toon_input[:5000])
                if attempt < 3:
                    time.sleep(2)
                    continue
                return {}, 0

            # Strip markdown code fences
            if result.startswith("```"):
                lines = result.split("\n")
                if lines[-1].strip() == "```":
                    result = "\n".join(lines[1:-1])
                else:
                    result = "\n".join(lines[1:])

            # Strip <TOON> tags
            result = re.sub(r"</?TOON>", "", result).strip()

            translated = from_toon(result)

            if len(translated) != len(fields):
                print(f"    WARNING: Expected {len(fields)} fields, got {len(translated)}.")
                debug_file = os.path.join(output_dir, f".debug_batch_{batch_num}.txt")
                with open(debug_file, "w", encoding="utf-8") as df:
                    df.write(f"=== RAW RESPONSE (attempt {attempt + 1}) ===\n")
                    df.write(result)
                    df.write(f"\n\n=== PARSED {len(translated)} entries ===\n")
                    for e in translated:
                        df.write(f"  {e['id'][:60]}  ->  {e['value'][:80]}\n")
                    df.write(f"\n=== EXPECTED {len(fields)} IDS ===\n")
                    for f_item in fields:
                        df.write(f"  {f_item['id']}\n")
                print(f"    Debug dumped to {debug_file}")

                if len(translated) >= len(fields) * 0.9:
                    print(f"    Accepting partial result ({len(translated)}/{len(fields)})")
                elif attempt < 3:
                    print("    Retrying...")
                    time.sleep(2)
                    continue

            # Build translation map — map opaque IDs back to real field IDs
            t_map = {}
            for entry in translated:
                real_id = idx_to_real_id.get(entry["id"])
                if real_id:
                    t_map[real_id] = entry["value"]

            # Check for missing translations
            real_ids = set(idx_to_real_id.values())
            missing = real_ids - set(t_map.keys())

            # Usage stats
            usage = response.usage
            total_tokens = (usage.input_tokens or 0) + (usage.output_tokens or 0)
            cached = getattr(usage, "input_tokens_details", None)
            cached_tokens = getattr(cached, "cached_tokens", 0) if cached else 0
            cache_info = f" [cached: {cached_tokens:,}]" if cached_tokens else ""

            # Retry missing fields in a focused smaller batch
            if missing and len(missing) <= 10:
                print(f"    Retrying {len(missing)} missing fields...")
                missing_fields = [f for f in fields if f["id"] in missing]
                retry_map, retry_tokens = _retry_missing_responses_api(
                    client, model, missing_fields, developer_prompt, reasoning_effort)
                t_map.update(retry_map)
                total_tokens += retry_tokens
                still_missing = missing - set(retry_map.keys())
                if still_missing:
                    print(f"    WARNING: {len(still_missing)} fields still missing after retry")
            elif missing:
                print(f"    WARNING: {len(missing)} untranslated fields")

            print(f"    Done: {len(t_map)} translated "
                  f"({usage.input_tokens} in + {usage.output_tokens} out = "
                  f"{total_tokens} tokens){cache_info}")

            return t_map, total_tokens

        except Exception as e:
            err_str = str(e)
            print(f"    Error: {e}")
            if attempt < 3:
                wait = 2 ** (attempt + 1)
                retry_match = re.search(r"try again in (\d+\.?\d*)s", err_str)
                if retry_match:
                    wait = max(wait, float(retry_match.group(1)) + 2)
                elif "429" in err_str or "rate" in err_str.lower():
                    wait = max(wait, 45)
                print(f"    Retrying in {wait:.0f}s...")
                time.sleep(wait)

    print(f"    FAILED after 4 attempts")
    return {}, 0


# =====================================================================
# Batch translation via translate_gaps (from translate_csv.py)
# =====================================================================

def _translate_batch_chat(client, model, fields, rich_text_map, batch_size=8000):
    """Translate fields using chat completions TOON batching (translate_gaps style).

    Handles rich_text JSON by extracting text nodes, translating them,
    and rebuilding the JSON structure.

    Returns (all_translations dict, total_tokens).
    """
    from tara_migrate.translation.translate_gaps import adaptive_batch as gaps_adaptive_batch
    from tara_migrate.translation.translate_gaps import translate_batch

    batches = gaps_adaptive_batch(fields, max_tokens=batch_size)
    print(f"\nTranslating {len(fields)} strings in {len(batches)} batches...")

    all_translations = {}
    total_tokens = 0

    for i, batch in enumerate(batches):
        api_batch = [{"id": f["id"], "value": f["value"]} for f in batch]
        t_map, tokens = translate_batch(
            client, model, api_batch,
            "English", "Arabic",
            i + 1, len(batches),
        )
        all_translations.update(t_map)
        total_tokens += tokens

    return all_translations, total_tokens


# =====================================================================
# Metafield owner resolution (for upload mode)
# =====================================================================

def _resolve_metafield_owners(client, metafield_gids):
    """Batch-resolve Metafield GIDs to their parent resource GID + translation key.

    Args:
        client: ShopifyClient instance.
        metafield_gids: List of metafield GID strings.

    Returns dict: ``{metafield_gid: {"parent_gid": ..., "translation_key": "namespace.key"}}``
    """
    result = {}
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
        data = client._graphql(query, {"ids": batch})
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


# =====================================================================
# Upload translated CSV to Shopify
# =====================================================================

def _upload_csv_translations(client, csv_path):
    """Upload a translated CSV file to Shopify via GraphQL translations API.

    Groups rows by resource GID, resolves metafield parents, and registers
    translations in batches.

    Args:
        client: ShopifyClient instance.
        csv_path: Path to the translated CSV file.
    """
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"\n{'=' * 60}")
    print("Uploading translations to Shopify...")
    print(f"{'=' * 60}")

    # Group translated rows by resource GID
    by_gid = {}
    metafield_gids_needed = set()
    skipped_types = {}

    for row in rows:
        translated = row.get("Translated content", "").strip()
        default = row.get("Default content", "").strip()
        if not translated or not default:
            continue

        csv_type = row["Type"]

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
        mf_owners = _resolve_metafield_owners(client, list(metafield_gids_needed))
        print(f"  Resolved {len(mf_owners)}/{len(metafield_gids_needed)} metafield owners")

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
            for f in fields_list:
                f["field"] = translation_key
            by_gid.setdefault(parent_gid, []).extend(fields_list)
            remapped += 1
        if unresolved:
            print(f"  WARNING: {unresolved} metafields could not be resolved to parent")
        print(f"  Remapped {remapped} metafields to parent resources")

    # Remove MediaImage entries
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
    progress_file = os.path.join(os.path.dirname(csv_path) or ".", "csv_upload_progress.json")
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

        try:
            resource = client.get_translatable_resource(gid)
            if not resource:
                print(f"  [{i + 1}/{total}] {gid} -- not found")
                errors += 1
                continue

            tc = resource.get("translatableContent", [])
            digest_map = {item["key"]: item["digest"] for item in tc}

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
                            t["value"] = val.replace("\n", "\\n")

                BATCH_LIMIT = 100
                for chunk_start in range(0, len(translations), BATCH_LIMIT):
                    chunk = translations[chunk_start:chunk_start + BATCH_LIMIT]
                    client.register_translations(gid, ARABIC_LOCALE, chunk)
                    registered += len(chunk)
                if (i + 1) % 50 == 0 or i + 1 == total:
                    print(f"  [{i + 1}/{total}] {registered} translations registered...")

            progress[gid] = True

            # Save progress periodically
            if (i + 1) % 100 == 0:
                with open(progress_file, "w") as f:
                    json.dump(progress, f)

        except Exception as e:
            print(f"  [{i + 1}/{total}] {gid} -- error: {e}")
            errors += 1

    # Final progress save
    with open(progress_file, "w") as f:
        json.dump(progress, f)

    print("\nUpload complete:")
    print(f"  Registered: {registered} translations")
    print(f"  Skipped (already done): {skipped}")
    print(f"  Errors: {errors}")


# =====================================================================
# LLM-based translation quality validation (Haiku)
# =====================================================================

_HAIKU_MODEL = "claude-haiku-4-5-20251001"

_QUALITY_SYSTEM_PROMPT = """\
You are a strict translation QA checker for TARA, a luxury scalp-care brand.
You will receive pairs of (English source, Arabic translation).
Flag any translation that has English words that should be in Arabic.

=== REQUIRED ARABIC DICTIONARY ===
These English terms MUST appear as their Arabic equivalents — flag if English is used:
- scalp = فروة الرأس
- roots = الجذور
- fiber / strand = الألياف
- follicle = البصيلة / البصيلات
- routine = روتين
- active ingredients = المكوّنات الفعّالة
- peptides = الببتيدات
- ceramides = السيراميدات
- niacinamide = النياسيناميد
- biotin = البيوتين
- glutathione = الجلوتاثيون
- black garlic = الثوم الأسود
- rosemary = إكليل الجبل
- sage = الميرمية
- date = التمر
- strawberry = الفراولة
- hydration = ترطيب
- repair = ترميم / إصلاح
- balance = توازن
- strength = تقوية
- density = كثافة
- purify = تنقية
- multivitamin = فيتامينات متعددة
- shampoo = شامبو
- conditioner = بلسم
- serum = سيروم
- mask = ماسك

=== PRODUCT NAME EXAMPLES ===
Product names MUST be fully translated. Each product name must always use the SAME Arabic:
- "Sage+ Multivitamin Shampoo" → "الميرمية+ شامبو فيتامينات متعددة"
- "Date+ Multivitamin Conditioner" → "التمر+ بلسم فيتامينات متعددة"
- "Anti-Hair Fall Serum" → "سيروم مضاد لتساقط الشعر"
- "Scalp Serum" → "سيروم فروة الرأس"
If the same product appears in multiple pairs, the Arabic name MUST be identical.

=== MUST be translated (flag if left in English) ===
- ALL product type words (Shampoo, Conditioner, Serum, Mask, Oil, Complex, etc.)
- ALL ingredient names from the dictionary above
- Action verbs: Shop, Buy, Add to Cart, Subscribe, Learn More, Discover, etc.
- Body/hair terms: Hair, Scalp, Hair Fall, Hair Growth, Hair Care, Roots
- Descriptors: Anti-Aging, Well-Aging, Nourishing, Hydrating, Strengthening
- UI labels: Benefits, How to Use, Description, Ingredients, Free of, Awards
- Category words: Collection, Best Sellers, New Arrivals, Discovery Set
- ANY common English word that has a standard Arabic equivalent

=== ALLOWED in English (do NOT flag) ===
- Brand name "TARA" only
- Scientific INCI names (e.g., Tocopherol, Glycerin, Panthenol, Aqua)
- Tool proper nouns: "Kansa Wand", "Gua Sha"
- URLs, emails, numbers, currency codes (SAR, USD), units (ml, g, pH)
- HTML tags, JSON keys, Liquid syntax, trademark symbols (™, ®)

=== Flag as BAD ===
- A product-type word (Shampoo, Conditioner, Serum, Mask, Multivitamin) left in English
- A full product name left entirely in English instead of Arabic
- An action verb or UI label left in English (Shop, Buy, Learn More, Benefits, etc.)
- Arabic is the English text copied verbatim (no translation done at all)
- Arabic is about a completely different topic

=== EXAMPLES ===
OK: EN: "Sage+ Multivitamin Shampoo" → AR: "الميرمية+ شامبو فيتامينات متعددة"
  (all product words translated to Arabic)

BAD: EN: "Sage+ Multivitamin Shampoo" → AR: "Sage+ Multivitamin Shampoo"
  (English copied verbatim, nothing translated)

BAD: EN: "Anti-Hair Fall Scalp Serum" → AR: "Anti-Hair Fall سيروم فروة الرأس"
  ("Anti-Hair Fall" left in English — should be "مضاد لتساقط الشعر")

OK: EN: "Formulated with Tocopherol and Niacinamide" → AR: "تركيبة غنية بالتوكوفيرول والنياسيناميد"
  (INCI names can stay in English/transliterated form)

OK: EN: "TARA Kansa Wand" → AR: "TARA Kansa Wand"
  (brand name + tool proper noun — allowed in English)

BAD: EN: "Shop the Collection" → AR: "Shop the Collection"
  (action verb + category word not translated)

OK: EN: "Key Benefits" → AR: "الفوائد الرئيسية"
  (UI label properly translated)

BAD: EN: "How to Use" → AR: "How to Use"
  (UI label left in English)

Return a JSON array. Only include BAD pairs — omit OK ones:
[{"i": 1, "ok": false, "reason": "'Shampoo' left in English, should be شامبو"}]
All OK → return []"""


def _validate_with_haiku(translations_to_check, batch_size=30):
    """Validate translation quality using Haiku.

    Args:
        translations_to_check: list of dicts with keys:
            - id: field identifier
            - english: source English text
            - arabic: translated Arabic text
        batch_size: pairs per LLM call

    Returns:
        set of ids that are BAD translations.
    """
    if not translations_to_check:
        return set()

    from dotenv import load_dotenv
    load_dotenv()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("  WARNING: ANTHROPIC_API_KEY not set, falling back to regex quality check")
        return None  # Signal caller to use fallback

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    bad_ids = set()
    total_batches = (len(translations_to_check) + batch_size - 1) // batch_size
    print(f"  Validating {len(translations_to_check)} translations with Haiku ({total_batches} batches)...")

    for start in range(0, len(translations_to_check), batch_size):
        batch = translations_to_check[start:start + batch_size]
        bnum = start // batch_size + 1

        lines = []
        for i, item in enumerate(batch):
            eng = item["english"][:500]
            ara = item["arabic"][:500]
            field = item.get("field", "")
            header = f"{i}. [{field}]" if field else f"{i}."
            lines.append(f"{header} EN: {eng}")
            lines.append(f"   AR: {ara}")

        prompt = "\n".join(lines)

        try:
            resp = client.messages.create(
                model=_HAIKU_MODEL,
                max_tokens=4096,
                system=_QUALITY_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            text = resp.content[0].text
            # Parse JSON from response
            json_match = re.search(r"\[.*\]", text, re.DOTALL)
            if json_match:
                results = json.loads(json_match.group())
                batch_bad = 0
                for r in results:
                    idx = r.get("i", -1)
                    if 0 <= idx < len(batch) and not r.get("ok", True):
                        bad_ids.add(batch[idx]["id"])
                        reason = r.get("reason", "")
                        field = batch[idx].get("field", "")
                        print(f"      [{field}] {reason}")
                        batch_bad += 1
                status = f" {batch_bad} flagged" if batch_bad else " all OK"
            else:
                status = " parse-error"
            print(f"    Batch {bnum}/{total_batches}...{status}")
            time.sleep(0.5)
        except Exception as e:
            print(f"    Batch {bnum}/{total_batches}... ERROR({e})")

    return bad_ids


def _get_visible_for_validation(text):
    """Extract visible text for validation (strip HTML/JSON structure)."""
    if not text:
        return ""
    # Rich text JSON: extract text values
    if text.startswith("{") and '"type"' in text:
        extracted = extract_text(text)
        if extracted and extracted.strip():
            return extracted
    # HTML: strip tags
    stripped = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    stripped = re.sub(r"<script[^>]*>.*?</script>", " ", stripped, flags=re.DOTALL | re.IGNORECASE)
    stripped = re.sub(r"<[^>]+>", " ", stripped)
    return re.sub(r"\s+", " ", stripped).strip()


# =====================================================================
# Quality detection helpers (from translate_tara_ar.py)
# =====================================================================

# Product-type English words that MUST be translated to Arabic
_MUST_TRANSLATE_WORDS = re.compile(
    r"\b(?:"
    r"Multivitamin|Shampoo|Conditioner|Serum|Mask|Routine"
    r"|Scalp\s+Serum|Scalp\s+Shampoo|Scalp\s+Treatment"
    r"|Hair\s+Fall|Hair\s+Loss|Hair\s+Growth|Hair\s+Care"
    r"|Anti[- ]?Aging|Anti[- ]?Hair[- ]?Fall|Well[- ]?Aging"
    r"|Luxury\s+Sample|Discovery\s+Set"
    r")\b",
    re.IGNORECASE,
)


def _get_visible_text(text):
    """Extract visible text from any format (rich_text, HTML, plain)."""
    if text.startswith("{") and '"type"' in text:
        extracted = extract_text(text)
        if extracted and extracted.strip():
            text = extracted
    stripped = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    stripped = re.sub(r"<script[^>]*>.*?</script>", " ", stripped, flags=re.DOTALL | re.IGNORECASE)
    stripped = re.sub(r"<[^>]+>", " ", stripped)
    return stripped


def _has_untranslated_english(text):
    """Detect English words that should have been translated (regex blocklist)."""
    visible = _get_visible_text(text)

    # Must have some Arabic to be a "partial" translation
    if not re.search(r"[\u0600-\u06FF]", visible):
        return False

    # Remove INCI blocks
    stripped = re.split(r"(?:Full INCI|INCI \u0627\u0644\u0643\u0627\u0645\u0644)\s*:", visible)[0]

    return bool(_MUST_TRANSLATE_WORDS.search(stripped))


# =====================================================================
# CSV reading / row categorization
# =====================================================================

def _read_csv(input_path):
    """Read a Shopify translation CSV and return (fieldnames, rows)."""
    # Try utf-8-sig first (handles BOM), fall back to utf-8
    for encoding in ("utf-8-sig", "utf-8"):
        try:
            with open(input_path, "r", encoding=encoding) as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                rows = list(reader)
            return fieldnames, rows
        except UnicodeDecodeError:
            continue
    raise RuntimeError(f"Cannot read CSV: {input_path}")


def _categorize_rows(rows, overwrite=False, fix_mode=False,
                     previous_translations=None, llm_bad_ids=None):
    """Categorize CSV rows into translation buckets.

    Returns a dict with keys:
        to_translate: list of row indices needing translation
        keep_as_is: list of row indices to copy default as-is
        from_csv: list of row indices already translated in CSV
        from_previous: list of (row_idx, field_id) pairs from previous runs
        skip: list of (row_idx, reason) pairs
        fix_bad: list of row indices with bad existing translations
    """
    to_translate = []
    keep_as_is_indices = []
    from_csv = []
    from_previous = []
    skip = []
    fix_bad = []

    previous = previous_translations or {}
    bad_ids = llm_bad_ids or set()

    for i, row in enumerate(rows):
        default = row.get("Default content", "").strip()
        translated = row.get("Translated content", "").strip()
        field_id = f"{row['Type']}|{row['Identification']}|{row['Field']}"

        if not default:
            skip.append((i, "empty"))
        elif is_non_translatable(row):
            skip.append((i, "non-translatable"))
        elif is_keep_as_is(row):
            keep_as_is_indices.append(i)
        elif field_id in previous:
            from_previous.append((i, field_id))
        elif translated and not overwrite:
            # Use LLM validation results if available, otherwise fall back to regex
            if bad_ids:
                is_bad = field_id in bad_ids
            else:
                low_arabic = not has_arabic(translated)
                has_english_gaps = _has_untranslated_english(translated)
                is_bad = low_arabic or has_english_gaps
                if not is_bad and translated == default and len(default) > 2:
                    if not has_arabic(translated):
                        is_bad = True

            if is_bad and fix_mode:
                fix_bad.append(i)
                to_translate.append(i)
            elif is_bad:
                to_translate.append(i)
            else:
                from_csv.append(i)
        else:
            to_translate.append(i)

    return {
        "to_translate": to_translate,
        "keep_as_is": keep_as_is_indices,
        "from_csv": from_csv,
        "from_previous": from_previous,
        "skip": skip,
        "fix_bad": fix_bad,
    }


def _build_field_list(rows, indices):
    """Build TOON-compatible field list from row indices."""
    fields = []
    for idx in indices:
        r = rows[idx]
        field_id = f"{r['Type']}|{r['Identification']}|{r['Field']}"
        fields.append({
            "id": field_id,
            "value": r["Default content"],
            "_row_idx": idx,
        })
    return fields


def _build_rich_text_fields(rows, indices):
    """Build field list with rich_text decomposition (for chat completions mode).

    Returns (fields, rich_text_map) where rich_text_map holds the parsed
    JSON + node paths needed for rebuild.
    """
    fields = []
    rich_text_map = {}

    for idx in indices:
        r = rows[idx]
        field_id = f"{r['Type']}|{r['Identification']}|{r['Field']}"
        value = r["Default content"]

        if is_rich_text_json(value):
            text_nodes, parsed = extract_text_nodes(value)
            if text_nodes:
                rich_text_map[field_id] = {
                    "parsed": parsed,
                    "row_idx": idx,
                    "nodes": [],
                }
                for ni, (path, text) in enumerate(text_nodes):
                    if text.strip():
                        node_field_id = f"{field_id}::node{ni}"
                        fields.append({
                            "id": node_field_id,
                            "value": text,
                            "_row_idx": idx,
                            "_rich_text_parent": field_id,
                            "_rich_text_path": tuple(path),
                        })
                        rich_text_map[field_id]["nodes"].append(
                            (node_field_id, tuple(path)))
            else:
                rows[idx]["Translated content"] = value
        else:
            fields.append({
                "id": field_id,
                "value": value,
                "_row_idx": idx,
            })

    return fields, rich_text_map


def _apply_rich_text_translations(rows, all_translations, rich_text_map):
    """Rebuild rich_text fields from translated text nodes and apply to rows.

    Returns the number of rows updated.
    """
    count = 0
    for parent_id, rt_info in rich_text_map.items():
        translations_for_rebuild = {}
        for node_field_id, path in rt_info["nodes"]:
            if node_field_id in all_translations:
                translations_for_rebuild[path] = all_translations[node_field_id]
        if translations_for_rebuild:
            rebuilt = rebuild(rt_info["parsed"], translations_for_rebuild)
            rebuilt = sanitize(rebuilt)
            default_json = rows[rt_info["row_idx"]]["Default content"]
            rebuilt = validate_structure(rebuilt, default_json)
            rows[rt_info["row_idx"]]["Translated content"] = rebuilt
            count += 1
    return count


def _apply_plain_translations(rows, fields, all_translations):
    """Apply plain-text translations to rows. Returns count applied."""
    count = 0
    for field in fields:
        if "_rich_text_parent" in field:
            continue
        field_id = field["id"]
        row_idx = field["_row_idx"]
        if field_id in all_translations:
            rows[row_idx]["Translated content"] = all_translations[field_id]
            count += 1
    return count


def _strip_handle_translations(rows):
    """Strip handle translations that match default (Shopify rejects these)."""
    count = 0
    for row in rows:
        if row.get("Field") == "handle":
            translated = row.get("Translated content", "").strip()
            default = row.get("Default content", "").strip()
            if translated and translated == default:
                row["Translated content"] = ""
                count += 1
    return count


def _write_csv(output_path, fieldnames, rows):
    """Write the translated CSV."""
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved to {output_path}")


def _print_summary(rows):
    """Print final translation stats."""
    final_translated = sum(1 for r in rows if r.get("Translated content", "").strip())
    final_empty = sum(
        1 for r in rows
        if r.get("Default content", "").strip()
        and not r.get("Translated content", "").strip()
        and not is_non_translatable(r)
    )
    print(f"\nFinal: {final_translated}/{len(rows)} rows have Arabic content")
    if final_empty:
        print(f"  Still untranslated: {final_empty} (re-run to retry)")


# =====================================================================
# Main entry point
# =====================================================================

def translate_csv(
    input_path,
    output_dir=None,
    output_path=None,
    model="gpt-5-nano",
    batch_size=120,
    dry_run=False,
    scrape=True,
    upload=True,
    prompt_path=None,
    tov_path=None,
    start_batch=0,
    max_batches=0,
    reasoning="medium",
    agents=1,
    overwrite=False,
    fix=False,
    use_responses_api=True,
):
    """Main entry point: translate a Shopify CSV export to Arabic.

    Supports two translation modes:
    - Responses API batched (default, ``use_responses_api=True``): Uses
      cached developer prompt + TOON batching for efficiency.
    - Chat completions batched (``use_responses_api=False``): Uses
      translate_gaps-style batching with rich_text decomposition.

    Args:
        input_path: Path to input Shopify CSV export.
        output_dir: Directory for output files (default: ``Arabic/``).
        output_path: Explicit output CSV path (overrides output_dir).
        model: OpenAI model name.
        batch_size: Max tokens per batch (for Responses API mode) or max
            tokens per TOON batch (for chat mode).
        dry_run: Show what would be translated without making API calls.
        scrape: Scrape taraformula.ae for Arabic reference content.
        upload: Upload translations to Shopify after translating.
        prompt_path: Path to developer prompt file (for Responses API mode).
        tov_path: Path to Arabic TOV file (for per-field mode).
        start_batch: Skip to batch N (0-indexed, for resume/parallel runs).
        max_batches: Stop after N batches (0 = unlimited).
        reasoning: Reasoning effort for Responses API (minimal/low/medium/high).
        agents: Number of parallel translation workers.
        overwrite: Re-translate rows that already have translations.
        fix: Re-translate fields with no/low Arabic (bad translations).
        use_responses_api: Use OpenAI Responses API (True) or chat completions (False).
    """
    from dotenv import load_dotenv
    load_dotenv()

    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(input_path) or ".", "Arabic")
    os.makedirs(output_dir, exist_ok=True)

    if output_path is None:
        input_filename = os.path.basename(input_path)
        output_path = os.path.join(output_dir, input_filename)

    # ------------------------------------------------------------------
    # 1. Read CSV
    # ------------------------------------------------------------------
    fieldnames, rows = _read_csv(input_path)
    print(f"Read {len(rows)} rows from {input_path}")

    # ------------------------------------------------------------------
    # 2. Load progress (resumable)
    # ------------------------------------------------------------------
    progress_file = os.path.join(output_dir, ".translation_progress.json")
    our_translations = {}
    if os.path.exists(progress_file):
        with open(progress_file, "r", encoding="utf-8") as f:
            our_translations = json.load(f)
        print(f"Resuming: {len(our_translations)} fields from previous runs")

    # --fix: validate translations with Haiku LLM
    if fix and (our_translations or any(r.get("Translated content", "").strip() for r in rows)):
        # Build field_id → English lookup from CSV rows
        csv_english = {}
        for row in rows:
            default = row.get("Default content", "").strip()
            if default:
                fid = f"{row['Type']}|{row['Identification']}|{row['Field']}"
                csv_english[fid] = default

        # Collect all translations to validate:
        # 1. From progress file (keyed by field_id)
        # 2. From CSV rows (keyed by field_id, where translated != default)
        to_validate = []

        # Progress entries (look up English + field name from CSV)
        for key, value in our_translations.items():
            if ":chunk_" in key:
                continue
            ar_visible = _get_visible_for_validation(value)
            en_source = csv_english.get(key, "")
            en_visible = _get_visible_for_validation(en_source) if en_source else ""
            # Extract human-readable field name from key (TYPE|ID|field_name)
            parts = key.split("|")
            field_label = f"{parts[0].lower()}:{parts[2]}" if len(parts) >= 3 else ""
            if ar_visible and len(ar_visible) > 2:
                to_validate.append({
                    "id": key,
                    "source": "progress",
                    "english": en_visible,
                    "arabic": ar_visible,
                    "field": field_label,
                })

        # CSV rows with existing translations
        for i, row in enumerate(rows):
            default = row.get("Default content", "").strip()
            translated = row.get("Translated content", "").strip()
            if not default or not translated:
                continue
            if is_non_translatable(row) or is_keep_as_is(row):
                continue
            field_id = f"{row['Type']}|{row['Identification']}|{row['Field']}"
            if field_id in our_translations:
                continue  # Already checked above via progress
            eng_visible = _get_visible_for_validation(default)
            ar_visible = _get_visible_for_validation(translated)
            field_label = f"{row['Type'].lower()}:{row['Field']}"
            if eng_visible and ar_visible and len(eng_visible) > 2:
                to_validate.append({
                    "id": field_id,
                    "source": "csv",
                    "english": eng_visible,
                    "arabic": ar_visible,
                    "field": field_label,
                    "_row_idx": i,
                })

        if to_validate:
            bad_ids = _validate_with_haiku(to_validate)
            if bad_ids is None:
                # Fallback: no API key, use simple regex check
                bad_ids = set()
                for item in to_validate:
                    if item["source"] == "progress":
                        if not has_arabic(item["arabic"], min_ratio=0.05):
                            bad_ids.add(item["id"])
                    else:
                        if not has_arabic(item["arabic"]) or _has_untranslated_english(item["arabic"]):
                            bad_ids.add(item["id"])

            # Purge bad progress entries
            progress_purged = [k for k in bad_ids if k in our_translations]
            if progress_purged:
                for k in progress_purged:
                    del our_translations[k]
                with open(progress_file, "w", encoding="utf-8") as f:
                    json.dump(our_translations, f, ensure_ascii=False)
                print(f"--fix: purged {len(progress_purged)} bad translations from progress")

            # Track bad CSV field_ids so _categorize_rows can use them
            _fix_bad_field_ids = bad_ids
        else:
            _fix_bad_field_ids = set()
    else:
        _fix_bad_field_ids = set()

    # ------------------------------------------------------------------
    # 3. Categorize rows
    # ------------------------------------------------------------------
    cats = _categorize_rows(
        rows, overwrite=overwrite, fix_mode=fix,
        previous_translations=our_translations,
        llm_bad_ids=_fix_bad_field_ids,
    )

    to_translate = cats["to_translate"]
    keep_as_is = cats["keep_as_is"]
    from_csv = cats["from_csv"]
    from_previous = cats["from_previous"]
    skip = cats["skip"]
    fix_bad = cats["fix_bad"]

    print(f"\nBreakdown:")
    print(f"  From original CSV (already done):  {len(from_csv)}")
    print(f"  From previous run (resuming):      {len(from_previous)}")
    print(f"  Keep as-is (URLs/images/config):   {len(keep_as_is)}")
    print(f"  Need AI translation NOW:           {len(to_translate)}")
    if fix_bad:
        print(f"    -> {len(fix_bad)} bad translations to re-translate")
    print(f"  Skip (empty/non-translatable):     {len(skip)}")

    # Apply keep-as-is
    for idx in keep_as_is:
        rows[idx]["Translated content"] = rows[idx]["Default content"]

    # Apply previous translations
    for idx, field_id in from_previous:
        rows[idx]["Translated content"] = our_translations[field_id]

    if not to_translate:
        skip_indices = {idx for idx, _reason in skip}
        filtered_rows = [r for i, r in enumerate(rows) if i not in skip_indices]
        if from_previous:
            _write_csv(output_path, fieldnames, filtered_rows)
            if skip_indices:
                print(f"Dropped {len(skip_indices)} untranslatable rows from output")
            print(f"Nothing new to translate. Applied {len(from_previous)} from previous run.")
        else:
            _write_csv(output_path, fieldnames, filtered_rows)
            if skip_indices:
                print(f"Dropped {len(skip_indices)} untranslatable rows from output")
            print("Nothing to translate. All rows are done.")
        return output_path

    # ------------------------------------------------------------------
    # 4. Build field list
    # ------------------------------------------------------------------
    if use_responses_api:
        fields = _build_field_list(rows, to_translate)
        rich_text_map = {}
    else:
        fields, rich_text_map = _build_rich_text_fields(rows, to_translate)

    # ------------------------------------------------------------------
    # 5. Batch fields
    # ------------------------------------------------------------------
    batches = adaptive_batch(fields, max_tokens=batch_size)
    total_value_tokens = sum(_estimate_tokens(f["value"]) for f in fields)

    print(f"\n{len(fields)} fields -> {len(batches)} batches "
          f"(~{total_value_tokens:,} value tokens)")

    # ------------------------------------------------------------------
    # 6. Dry run
    # ------------------------------------------------------------------
    if dry_run:
        from collections import Counter
        by_type = Counter(rows[i]["Type"] for i in to_translate)
        print("\nFields by type:")
        for t, c in by_type.most_common():
            print(f"  {t}: {c}")
        print(f"\nSample fields:")
        for idx in to_translate[:20]:
            r = rows[idx]
            existing = r.get("Translated content", "").strip()
            marker = " [overwrite]" if existing else ""
            print(f"  [{r['Type']}] {r['Field']}: {r['Default content'][:80]}{marker}")
        return None

    # ------------------------------------------------------------------
    # 7. Initialize OpenAI
    # ------------------------------------------------------------------
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set. Add it to .env or environment.")
        sys.exit(1)

    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    # ------------------------------------------------------------------
    # 8. Load developer prompt / system prompt
    # ------------------------------------------------------------------
    if use_responses_api:
        # Responses API mode: use cached developer prompt file
        if prompt_path is None:
            prompt_path = os.path.join(output_dir, "tara_cached_developer_prompt.txt")
        developer_prompt = load_developer_prompt(prompt_path)
        print(f"Developer prompt: ~{_estimate_tokens(developer_prompt):,} est. tokens\n")
    else:
        # Chat completions mode: build system prompt from TOV + reference
        tov_text = ""
        reference_text = ""

        if tov_path and os.path.exists(tov_path):
            with open(tov_path, "r", encoding="utf-8") as f:
                tov_text = f.read()
            print(f"Loaded TOV from {tov_path} ({len(tov_text):,} chars)")
        elif tov_path:
            tov_file = os.path.join(output_dir, "tara_arabic_tov.txt")
            if os.path.exists(tov_file):
                with open(tov_file, "r", encoding="utf-8") as f:
                    tov_text = f.read()

        if scrape:
            print("\nScraping Arabic reference content...")
            reference = scrape_arabic_reference(output_dir)
            reference_text = _build_optimized_reference(reference, output_dir)
        else:
            ref_file = os.path.join(output_dir, "ar_optimized_reference.txt")
            if os.path.exists(ref_file):
                with open(ref_file, "r", encoding="utf-8") as f:
                    reference_text = f.read()

    # ------------------------------------------------------------------
    # 9. Translate
    # ------------------------------------------------------------------
    all_translations = {}
    total_tokens = 0

    # Filter batches based on --start-batch / --max-batches
    work_items = []
    for i, batch in enumerate(batches):
        if i < start_batch:
            continue
        if max_batches and (i - start_batch) >= max_batches:
            break
        work_items.append((i, batch))

    if use_responses_api:
        # Responses API batch mode (translate_tara_ar.py style)
        start_time = time.time()
        progress_lock = __import__("threading").Lock()

        def _translate_one(item):
            idx, batch = item
            api_batch = [{"id": f["id"], "value": f["value"]} for f in batch]
            t_map, tokens = _translate_batch_responses_api(
                client, model, api_batch, developer_prompt,
                idx + 1, len(batches), output_dir,
                reasoning_effort=reasoning,
            )
            # Thread-safe progress update
            with progress_lock:
                our_translations.update(t_map)
                with open(progress_file, "w", encoding="utf-8") as pf:
                    json.dump(our_translations, pf, ensure_ascii=False)
            return t_map, tokens

        n_agents = min(agents, len(work_items)) if work_items else 1

        if n_agents > 1:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            print(f"Running {len(work_items)} batches with {n_agents} parallel agents...")
            with ThreadPoolExecutor(max_workers=n_agents) as pool:
                futures = {pool.submit(_translate_one, item): item for item in work_items}
                for future in as_completed(futures):
                    try:
                        t_map, tokens = future.result()
                        all_translations.update(t_map)
                        total_tokens += tokens
                    except Exception as e:
                        idx_val, _ = futures[future]
                        print(f"    Batch {idx_val + 1} failed: {e}")
        else:
            for item in work_items:
                t_map, tokens = _translate_one(item)
                all_translations.update(t_map)
                total_tokens += tokens

        elapsed = time.time() - start_time
        print(f"\nTranslation complete: {total_tokens:,} tokens in {elapsed:.1f}s")

    else:
        # Chat completions batch mode (translate_csv.py style)
        all_translations, total_tokens = _translate_batch_chat(
            client, model, fields, rich_text_map, batch_size=batch_size)

        # Save to progress file
        our_translations.update(all_translations)
        with open(progress_file, "w", encoding="utf-8") as f:
            json.dump(our_translations, f, ensure_ascii=False)

    # ------------------------------------------------------------------
    # 9b. Reassemble chunked fields and persist
    # ------------------------------------------------------------------
    _reassemble_chunks(fields, all_translations, our_translations)

    # Clean chunk keys from progress — only keep parent keys
    chunk_keys = [k for k in our_translations if ":chunk_" in k]
    if chunk_keys:
        for k in chunk_keys:
            del our_translations[k]
    # Save progress with reassembled parent translations
    with open(progress_file, "w", encoding="utf-8") as f:
        json.dump(our_translations, f, ensure_ascii=False)

    # ------------------------------------------------------------------
    # 10. Apply translations to CSV rows
    # ------------------------------------------------------------------
    if rich_text_map:
        rt_count = _apply_rich_text_translations(rows, all_translations, rich_text_map)
        plain_count = _apply_plain_translations(rows, fields, all_translations)
        applied = rt_count + plain_count
    else:
        applied = 0
        for field in fields:
            row_idx = field["_row_idx"]
            if field["id"] in our_translations:
                rows[row_idx]["Translated content"] = our_translations[field["id"]]
                applied += 1

    print(f"\nApplied {applied}/{len(fields)} translations")
    print(f"Total tokens used: {total_tokens:,}")

    # Strip handle translations
    handle_stripped = _strip_handle_translations(rows)
    if handle_stripped:
        print(f"Stripped {handle_stripped} handle translations matching default")

    # ------------------------------------------------------------------
    # 11. Write output CSV (drop untranslatable rows)
    # ------------------------------------------------------------------
    skip_indices = {idx for idx, _reason in skip}
    filtered_rows = [r for i, r in enumerate(rows) if i not in skip_indices]
    _write_csv(output_path, fieldnames, filtered_rows)
    if skip_indices:
        print(f"Dropped {len(skip_indices)} untranslatable rows from output")
    _print_summary(filtered_rows)

    print(f"\nProgress saved to {progress_file}")
    print(f"  ({len(our_translations)} total fields translated)")

    # ------------------------------------------------------------------
    # 12. Upload to Shopify (optional)
    # ------------------------------------------------------------------
    if upload:
        shop_url = os.environ.get("SAUDI_SHOP_URL")
        access_token = os.environ.get("SAUDI_ACCESS_TOKEN")
        if not shop_url or not access_token:
            print("\nSAUDI_SHOP_URL / SAUDI_ACCESS_TOKEN not set -- skipping Shopify upload.")
            print("Import the CSV manually via Shopify Admin > Settings > Languages > Arabic > Import")
        else:
            from tara_migrate.client import ShopifyClient
            shopify = ShopifyClient(shop_url, access_token)
            _upload_csv_translations(shopify, output_path)

    return output_path


# =====================================================================
# CLI
# =====================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Translate Shopify CSV export to Arabic (consolidated module)")
    parser.add_argument("--input", required=True,
                        help="Input CSV file (Shopify translation export)")
    parser.add_argument("--output-dir", default=None,
                        help="Output directory (default: Arabic/)")
    parser.add_argument("--output", default=None,
                        help="Explicit output CSV path (overrides --output-dir)")
    parser.add_argument("--model", default="gpt-5-nano",
                        help="OpenAI model (default: gpt-5-nano)")
    parser.add_argument("--batch-size", type=int, default=120,
                        help="Max tokens per batch (default: 120)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be translated without API calls")
    parser.add_argument("--no-scrape", action="store_true",
                        help="Skip Arabic reference scraping")
    parser.add_argument("--no-upload", action="store_true",
                        help="Skip Shopify upload, only write translated CSV")
    parser.add_argument("--upload-only", default=None, metavar="CSV_PATH",
                        help="Upload an already-translated CSV to Shopify (no translation)")
    parser.add_argument("--tov", default=None,
                        help="Arabic TOV file path (for chat completions mode)")
    parser.add_argument("--prompt", default=None,
                        help="Developer prompt file path (for Responses API mode)")
    parser.add_argument("--start-batch", type=int, default=0,
                        help="Skip to batch N (0-indexed, for resume/parallel runs)")
    parser.add_argument("--max-batches", type=int, default=0,
                        help="Stop after N batches (0 = unlimited)")
    parser.add_argument("--reasoning", default="medium",
                        choices=["minimal", "low", "medium", "high"],
                        help="Reasoning effort for Responses API (default: medium)")
    parser.add_argument("--agents", type=int, default=1,
                        help="Number of parallel translation workers (default: 1)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Re-translate rows that already have translations")
    parser.add_argument("--fix", action="store_true",
                        help="Re-translate fields with no/low Arabic (bad translations)")
    parser.add_argument("--chat-mode", action="store_true",
                        help="Use chat completions instead of Responses API "
                             "(enables rich_text decomposition)")
    args = parser.parse_args()

    # Upload-only mode
    if args.upload_only:
        from dotenv import load_dotenv
        load_dotenv()

        shop_url = os.environ.get("SAUDI_SHOP_URL")
        access_token = os.environ.get("SAUDI_ACCESS_TOKEN")
        if not shop_url or not access_token:
            print("ERROR: SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN required for upload.")
            sys.exit(1)

        from tara_migrate.client import ShopifyClient
        shopify = ShopifyClient(shop_url, access_token)
        _upload_csv_translations(shopify, args.upload_only)
        return

    translate_csv(
        input_path=args.input,
        output_dir=args.output_dir,
        output_path=args.output,
        model=args.model,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        scrape=not args.no_scrape,
        upload=not args.no_upload,
        prompt_path=args.prompt,
        tov_path=args.tov,
        start_batch=args.start_batch,
        max_batches=args.max_batches,
        reasoning=args.reasoning,
        agents=args.agents,
        overwrite=args.overwrite,
        fix=args.fix,
        use_responses_api=not args.chat_mode,
    )


if __name__ == "__main__":
    main()
