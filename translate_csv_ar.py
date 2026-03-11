#!/usr/bin/env python3
"""Translate Shopify CSV export to high-quality Arabic.

Workflow:
1. Scrapes Arabic content from taraformula.ae/ae-ar into Arabic/ folder
2. Builds an optimized Arabic reference file from scraped data
3. Loads TARA Arabic TOV guidelines
4. Caches the system prompt in OpenAI for efficiency
5. Sends each field individually via TOON for maximum translation quality
6. Saves fully translated CSV to Arabic/ folder

Usage:
    python translate_csv_ar.py --input data/Tara_Saudi_translations_Mar-10-2026.csv
    python translate_csv_ar.py --input data/export.csv --tov Arabic/tara_arabic_tov.txt
    python translate_csv_ar.py --input data/export.csv --dry-run
    python translate_csv_ar.py --input data/export.csv --no-scrape
    python translate_csv_ar.py --input data/export.csv --model gpt-4o

Prerequisites:
    OPENAI_API_KEY in .env or environment
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time

from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from tara_migrate.translation.toon import from_toon, to_toon  # noqa: E402

ARABIC_DIR = os.path.join(os.path.dirname(__file__), "Arabic")


# =====================================================================
# CSV filtering — what to translate vs skip
# =====================================================================

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
    """Check if a row's value should be copied as-is (same in both languages)."""
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
    if ".image_1:" in field or ".image_2:" in field or ".image_1_mobile:" in field or ".image_2_mobile:" in field:
        return True
    if field in ("general.logo", "general.logo_inverse", "general.favicon"):
        return True
    if ".icon:" in field:
        return True

    return False


# =====================================================================
# Scrape Arabic reference content from taraformula.ae → Arabic/ folder
# =====================================================================

def scrape_arabic_reference(output_dir):
    """Scrape Arabic content from taraformula.ae into the Arabic/ folder.

    Returns the raw scraped data dict.
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
            reference["ingredients_html"] = resp.text[:50000]  # Cap at 50KB
            print(f"    Ingredients page: {len(resp.text):,} chars")
    except Exception as e:
        print(f"    Ingredients page error: {e}")

    # Save to Arabic/ folder
    os.makedirs(output_dir, exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(reference, f, ensure_ascii=False, indent=2)
    print(f"  Saved scrape to {cache_file}")

    return reference


def build_optimized_reference(reference, output_dir):
    """Build an optimized, deduplicated Arabic reference text file.

    Saved to Arabic/ar_optimized_reference.txt for inspection/reuse.
    Returns the reference text.
    """
    ref_file = os.path.join(output_dir, "ar_optimized_reference.txt")

    sections = []

    # --- Product names + taglines ---
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

    # --- Product descriptions (first 5 for style reference) ---
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

    # --- Category/collection names ---
    cat_lines = []
    seen_cats = set()
    for c in reference.get("collections", []):
        name = c.get("name", "").strip()
        if name and name not in seen_cats:
            seen_cats.add(name)
            cat_lines.append(f"  {name}")
    if cat_lines:
        sections.append("COLLECTION/CATEGORY NAMES (Arabic):\n" + "\n".join(cat_lines))

    # --- SEO titles & descriptions ---
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
# OpenAI prompt caching
# =====================================================================

def build_system_prompt(tov_text, reference_text):
    """Build the system prompt. Long static content goes first for caching."""
    # OpenAI caches the longest prefix of the system prompt automatically.
    # Put static content (TOV + reference) at the top so it gets cached.
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
- Keep product-specific names unchanged (e.g., "Kansa Wand", "Gua Sha")
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
# Per-field translation with prompt caching
# =====================================================================

def _estimate_tokens(text):
    return max(1, len(text) // 3)


def translate_field(client, model, field, system_prompt, field_num, total_fields):
    """Translate a single field using TOON format.

    Sending one field at a time ensures maximum quality per translation.
    The system prompt is cached by OpenAI after the first call.
    """
    toon_input = to_toon([field])

    prompt = (
        "Translate this TOON field from English to Arabic. "
        "Follow the TARA Arabic tone of voice strictly.\n\n"
        f"{toon_input}"
    )

    REASONING_MODELS = {"o3", "o3-mini", "o4-mini", "gpt-5-mini", "gpt-5"}
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
                # Model may have mangled the ID — take the first value
                translated_value = translated[0]["value"]

            usage = response.usage
            total_tokens = usage.prompt_tokens + usage.completion_tokens
            cached = getattr(usage, "prompt_tokens_details", None)
            cached_tokens = getattr(cached, "cached_tokens", 0) if cached else 0

            # Compact progress line
            val_preview = field["value"][:50].replace("\n", " ")
            ar_preview = (translated_value or "")[:50].replace("\n", " ")
            cache_pct = f" [cached:{cached_tokens}]" if cached_tokens > 0 else ""
            print(f"  [{field_num}/{total_fields}] {val_preview}... → {ar_preview}...{cache_pct}")

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
# Main
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="Translate Shopify CSV to high-quality Arabic")
    parser.add_argument("--input", required=True, help="Input CSV file (Shopify translation export)")
    parser.add_argument("--output", default=None,
                        help="Output CSV (default: Arabic/<input_filename>)")
    parser.add_argument("--tov", default=os.path.join(ARABIC_DIR, "tara_arabic_tov.txt"),
                        help="Arabic TOV file (default: Arabic/tara_arabic_tov.txt)")
    parser.add_argument("--model", default="gpt-4o-mini",
                        help="OpenAI model (default: gpt-4o-mini)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be translated")
    parser.add_argument("--no-scrape", action="store_true",
                        help="Skip scraping, use cached reference if available")
    args = parser.parse_args()

    # Default output: Arabic/<input_filename>
    if not args.output:
        input_filename = os.path.basename(args.input)
        args.output = os.path.join(ARABIC_DIR, input_filename)

    os.makedirs(ARABIC_DIR, exist_ok=True)
    load_dotenv()

    # ----------------------------------------------------------------
    # 1. Load TOV
    # ----------------------------------------------------------------
    if not os.path.exists(args.tov):
        print(f"ERROR: TOV file not found: {args.tov}")
        sys.exit(1)
    with open(args.tov, "r", encoding="utf-8") as f:
        tov_text = f.read()
    print(f"Loaded TOV from {args.tov} ({len(tov_text):,} chars)")

    # ----------------------------------------------------------------
    # 2. Scrape Arabic reference → Arabic/ folder
    # ----------------------------------------------------------------
    reference_text = ""
    if not args.no_scrape:
        print("\nStep 1: Scraping Arabic reference content...")
        reference = scrape_arabic_reference(ARABIC_DIR)
        print("\nStep 2: Building optimized reference file...")
        reference_text = build_optimized_reference(reference, ARABIC_DIR)
    else:
        # Try to load existing optimized reference
        ref_file = os.path.join(ARABIC_DIR, "ar_optimized_reference.txt")
        if os.path.exists(ref_file):
            with open(ref_file, "r", encoding="utf-8") as f:
                reference_text = f.read()
            print(f"\nUsing existing reference: {ref_file} ({len(reference_text):,} chars)")
        else:
            print("\nNo scrape and no cached reference — translating without reference")

    # ----------------------------------------------------------------
    # 3. Read CSV
    # ----------------------------------------------------------------
    with open(args.input, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    print(f"\nRead {len(rows)} rows from {args.input}")

    # ----------------------------------------------------------------
    # 4. Categorize rows — translate everything with English content
    # ----------------------------------------------------------------
    to_translate = []
    keep_as_is = []
    skip = []

    for i, row in enumerate(rows):
        default = row.get("Default content", "").strip()

        if not default:
            skip.append((i, "empty"))
        elif _is_non_translatable(row):
            skip.append((i, "non-translatable"))
        elif _is_keep_as_is(row):
            keep_as_is.append(i)
        else:
            to_translate.append(i)

    print(f"\nBreakdown:")
    print(f"  Will translate (overwrite all): {len(to_translate)}")
    print(f"  Keep as-is (URLs/images/config): {len(keep_as_is)}")
    print(f"  Skip (empty/non-translatable):   {len(skip)}")

    # Apply keep-as-is
    for idx in keep_as_is:
        rows[idx]["Translated content"] = rows[idx]["Default content"]

    if args.dry_run:
        print(f"\n--- DRY RUN: Would translate {len(to_translate)} fields (1 API call each) ---")
        from collections import Counter
        by_type = Counter(rows[i]["Type"] for i in to_translate)
        for t, c in by_type.most_common():
            print(f"  {t}: {c}")

        # Estimate cost
        system_tokens = _estimate_tokens(build_system_prompt(tov_text, reference_text))
        total_input = sum(_estimate_tokens(rows[i]["Default content"]) for i in to_translate)
        # System prompt cached after 1st call = only charged once
        est_total = system_tokens + (system_tokens * len(to_translate) * 0.5) + total_input
        print(f"\n  System prompt: ~{system_tokens:,} tokens (cached after 1st call)")
        print(f"  Content tokens: ~{total_input:,}")
        print(f"  Est. total tokens: ~{int(est_total):,} (with ~50% cache hit)")

        print("\nSample fields:")
        for idx in to_translate[:10]:
            r = rows[idx]
            existing = r.get("Translated content", "").strip()
            marker = " [overwrite]" if existing else ""
            print(f"  [{r['Type']}] {r['Field']}: {r['Default content'][:80]}{marker}")
        return

    # ----------------------------------------------------------------
    # 5. Initialize OpenAI
    # ----------------------------------------------------------------
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set. Add it to .env or environment.")
        sys.exit(1)

    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    # ----------------------------------------------------------------
    # 6. Build system prompt (TOV + reference at top for caching)
    # ----------------------------------------------------------------
    system_prompt = build_system_prompt(tov_text, reference_text)
    prompt_hash = hashlib.md5(system_prompt.encode()).hexdigest()[:8]
    print(f"\nSystem prompt: {_estimate_tokens(system_prompt):,} est. tokens (hash: {prompt_hash})")
    print("OpenAI will cache the system prompt after the first call.\n")

    # ----------------------------------------------------------------
    # 7. Build field list
    # ----------------------------------------------------------------
    fields = []
    for idx in to_translate:
        r = rows[idx]
        field_id = f"{r['Type']}|{r['Identification']}|{r['Field']}"
        fields.append({
            "id": field_id,
            "value": r["Default content"],
            "_row_idx": idx,
        })

    # ----------------------------------------------------------------
    # 8. Load progress (resumable)
    # ----------------------------------------------------------------
    progress_file = os.path.join(ARABIC_DIR, f".translation_progress_{prompt_hash}.json")
    all_translations = {}
    if os.path.exists(progress_file):
        with open(progress_file, "r", encoding="utf-8") as f:
            all_translations = json.load(f)
        print(f"Resuming: {len(all_translations)}/{len(fields)} fields already done")

    remaining = [f for f in fields if f["id"] not in all_translations]
    print(f"Remaining: {len(remaining)} fields to translate\n")

    # ----------------------------------------------------------------
    # 9. Translate each field individually
    # ----------------------------------------------------------------
    if remaining:
        total_tokens = 0
        total_fields = len(fields)
        done_before = len(all_translations)

        for i, field in enumerate(remaining):
            field_num = done_before + i + 1
            value, tokens = translate_field(
                client, args.model, {"id": field["id"], "value": field["value"]},
                system_prompt, field_num, total_fields,
            )
            total_tokens += tokens

            if value is not None:
                all_translations[field["id"]] = value

            # Save progress every 10 fields
            if (i + 1) % 10 == 0 or i == len(remaining) - 1:
                with open(progress_file, "w", encoding="utf-8") as f:
                    json.dump(all_translations, f, ensure_ascii=False)

        print(f"\nTranslation complete. Total tokens: {total_tokens:,}")

    # ----------------------------------------------------------------
    # 10. Apply translations to CSV rows
    # ----------------------------------------------------------------
    applied = 0
    for field in fields:
        row_idx = field["_row_idx"]
        if field["id"] in all_translations:
            rows[row_idx]["Translated content"] = all_translations[field["id"]]
            applied += 1

    print(f"Applied {applied}/{len(fields)} translations")

    # ----------------------------------------------------------------
    # 11. Write output CSV to Arabic/ folder
    # ----------------------------------------------------------------
    with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved to {args.output}")

    # Keep progress file for future runs
    print(f"Progress file kept at {progress_file}")

    # Summary
    final_translated = sum(1 for r in rows if r.get("Translated content", "").strip())
    final_empty = sum(1 for r in rows
                      if r.get("Default content", "").strip()
                      and not r.get("Translated content", "").strip()
                      and r.get("Field") != "handle")
    print(f"\nFinal: {final_translated}/{len(rows)} rows have Arabic content")
    if final_empty:
        print(f"  {final_empty} rows still untranslated (re-run to retry)")
    print(f"\nImport: Shopify Admin > Settings > Languages > Arabic > Import")
    print(f"  File: {args.output}")
    print(f"  Check 'Overwrite existing translations'")


if __name__ == "__main__":
    main()
