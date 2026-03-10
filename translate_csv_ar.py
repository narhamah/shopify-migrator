#!/usr/bin/env python3
"""Translate Shopify CSV export to high-quality Arabic using scraped reference content.

Workflow:
1. Scrapes Arabic content from taraformula.ae/ae-ar as translation reference
2. Reads the TARA Arabic TOV (Tone of Voice) guidelines
3. Sends English content in batches to OpenAI for translation
4. Overwrites ALL translated content in the CSV (not just gaps)

Usage:
    python translate_csv_ar.py --input data/Tara_Saudi_translations_Mar-10-2026.csv
    python translate_csv_ar.py --input data/export.csv --tov tara_tov_ar.txt
    python translate_csv_ar.py --input data/export.csv --dry-run
    python translate_csv_ar.py --input data/export.csv --no-scrape  # skip scraping
    python translate_csv_ar.py --input data/export.csv --model gpt-4o

Prerequisites:
    OPENAI_API_KEY in .env or environment
"""

import argparse
import csv
import json
import os
import re
import sys
import time

from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from tara_migrate.translation.toon import from_toon, to_toon  # noqa: E402


# =====================================================================
# CSV filtering — what to translate vs skip
# =====================================================================

def _is_non_translatable(row):
    """Return True if this row should never be translated (URLs, handles, GIDs, etc.)."""
    default = row.get("Default content", "").strip()
    field = row.get("Field", "")

    if not default:
        return True

    # Skip handles
    if field == "handle":
        return True

    # Skip URLs, image refs, GIDs
    if default.startswith(("shopify://", "http://", "https://", "/", "gid://")):
        return True

    # Skip pure numeric
    if re.match(r"^-?\d+\.?\d*$", default):
        return True

    # Skip hex strings / UUIDs
    if re.match(r"^[0-9a-f]{8,}$", default):
        return True

    # Skip JSON arrays of GIDs
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
# Scrape Arabic reference content from taraformula.ae
# =====================================================================

def scrape_arabic_reference(cache_file="data/ar_reference_cache.json"):
    """Scrape Arabic content from taraformula.ae to use as translation reference.

    Returns a dict mapping content types to scraped Arabic text samples.
    """
    if os.path.exists(cache_file):
        print(f"  Using cached Arabic reference from {cache_file}")
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    print("  Scraping Arabic content from taraformula.ae...")
    from tara_migrate.tools.scrape_kuwait import MagentoGraphQL

    gql = MagentoGraphQL(base_url="https://taraformula.ae", delay=3.0)
    store_code = "ae-ar"

    reference = {"products": [], "collections": [], "pages": []}

    # Scrape product titles and descriptions
    product_query = """
    {
      products(filter: {}, pageSize: 50, currentPage: 1) {
        items {
          name
          sku
          description { html }
          short_description { html }
          meta_title
          meta_description
        }
      }
    }
    """
    data = gql.query(product_query, store_code=store_code)
    if data and "data" in data:
        items = data["data"].get("products", {}).get("items", [])
        for item in items:
            reference["products"].append({
                "name": item.get("name", ""),
                "description": item.get("description", {}).get("html", ""),
                "short_description": item.get("short_description", {}).get("html", ""),
                "meta_title": item.get("meta_title", ""),
                "meta_description": item.get("meta_description", ""),
            })
        print(f"    Scraped {len(items)} Arabic products")

    # Scrape collection/category names
    category_query = """
    {
      categories(filters: {}) {
        items {
          name
          description
          meta_title
          meta_description
        }
      }
    }
    """
    data = gql.query(category_query, store_code=store_code)
    if data and "data" in data:
        items = data["data"].get("categories", {}).get("items", [])
        for item in items:
            reference["collections"].append({
                "name": item.get("name", ""),
                "description": item.get("description", ""),
                "meta_title": item.get("meta_title", ""),
                "meta_description": item.get("meta_description", ""),
            })
        print(f"    Scraped {len(items)} Arabic categories")

    # Scrape CMS pages
    pages_query = """
    {
      cmsPage(identifier: "home") {
        title
        content
        meta_title
        meta_description
      }
    }
    """
    data = gql.query(pages_query, store_code=store_code)
    if data and "data" in data and data["data"].get("cmsPage"):
        page = data["data"]["cmsPage"]
        reference["pages"].append({
            "title": page.get("title", ""),
            "content": page.get("content", ""),
        })

    # Save cache
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(reference, f, ensure_ascii=False, indent=2)
    print(f"    Saved reference cache to {cache_file}")

    return reference


def build_reference_samples(reference):
    """Build a concise reference text from scraped Arabic content."""
    samples = []

    for p in reference.get("products", [])[:10]:
        name = p.get("name", "").strip()
        desc = p.get("short_description", "").strip()
        if not desc:
            desc = p.get("description", "").strip()
        # Strip HTML for reference
        desc = re.sub(r"<[^>]+>", "", desc).strip()
        if name:
            sample = f"- {name}"
            if desc:
                sample += f": {desc[:150]}"
            samples.append(sample)

    for c in reference.get("collections", [])[:5]:
        name = c.get("name", "").strip()
        if name:
            samples.append(f"- {name}")

    return "\n".join(samples) if samples else ""


# =====================================================================
# Translation system prompt
# =====================================================================

def build_system_prompt(tov_text, reference_samples):
    """Build the system prompt with TOV and scraped reference."""
    prompt = f"""You are a professional Arabic translator for TARA, a luxury scalp-care and hair-health brand.

TARA ARABIC TONE OF VOICE:
{tov_text}

"""
    if reference_samples:
        prompt += f"""REFERENCE — Here are examples of existing Arabic content from TARA's Arabic site (taraformula.ae).
Use these as a guide for terminology, style, and tone:
{reference_samples}

"""

    prompt += """INPUT/OUTPUT FORMAT: TOON (Token-Oriented Object Notation)
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
- Preserve Shopify Liquid tags ({{ }}, {% %}) unchanged
- Keep URLs, JSON structure keys, and GIDs unchanged
- For rich_text_field JSON: translate only "value" keys inside text nodes
- Translate meaning, not words — rebuild the sentence in natural Arabic
- Use فروة الرأس (never الفروة alone), خصلات الشعر, الجذور
- Use present tense verbs: ينظّف، يعزّز، يرطّب، يرمّم
- No marketing fluff: avoid سحري، فاخر، مثالي، مذهل
- Start with benefit, then mechanism
- Return ONLY the translated TOON lines, no explanations"""

    return prompt


# =====================================================================
# Batch translation
# =====================================================================

def _estimate_tokens(text):
    return max(1, len(text) // 3)


def adaptive_batch(fields, max_tokens=8000):
    """Split fields into batches by estimated token count."""
    batches = []
    current_batch = []
    current_tokens = 0

    for field in fields:
        field_tokens = _estimate_tokens(field["value"])
        if current_batch and (current_tokens + field_tokens > max_tokens):
            batches.append(current_batch)
            current_batch = []
            current_tokens = 0
        current_batch.append(field)
        current_tokens += field_tokens

    if current_batch:
        batches.append(current_batch)
    return batches


def translate_batch(client, model, fields, system_prompt, batch_num, total_batches):
    """Translate a batch of fields using TOON format."""
    toon_input = to_toon(fields)

    prompt = (
        "Translate the following TOON data from English to Arabic. "
        "Keep all IDs unchanged. Translate only the values. "
        "Follow the TARA Arabic tone of voice strictly.\n\n"
        f"{toon_input}"
    )

    est_tokens = sum(_estimate_tokens(f["value"]) for f in fields)
    print(f"  Batch {batch_num}/{total_batches}: {len(fields)} fields (~{est_tokens:,} tokens)...")

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

            if len(translated) != len(fields):
                print(f"    WARNING: Expected {len(fields)} fields, got {len(translated)}.")
                if len(translated) >= len(fields) * 0.9:
                    print(f"    Accepting partial ({len(translated)}/{len(fields)})")
                elif attempt < 3:
                    print("    Retrying...")
                    time.sleep(2)
                    continue

            t_map = {}
            for entry in translated:
                t_map[entry["id"]] = entry["value"]

            # Verify IDs
            input_ids = {f["id"] for f in fields}
            extra = set(t_map.keys()) - input_ids
            for eid in extra:
                del t_map[eid]

            missing = input_ids - set(t_map.keys())
            if missing:
                print(f"    WARNING: {len(missing)} untranslated fields")

            usage = response.usage
            total_tokens = usage.prompt_tokens + usage.completion_tokens
            print(f"    Done ({total_tokens:,} tokens)")
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

    print("    FAILED after 4 attempts")
    return {}, 0


# =====================================================================
# Main
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="Translate Shopify CSV to high-quality Arabic")
    parser.add_argument("--input", required=True, help="Input CSV file (Shopify translation export)")
    parser.add_argument("--output", default=None, help="Output CSV file (default: input with _arabic suffix)")
    parser.add_argument("--tov", default="tara_tov_ar.txt", help="Arabic TOV text file (default: tara_tov_ar.txt)")
    parser.add_argument("--model", default="gpt-4o-mini", help="OpenAI model (default: gpt-4o-mini)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be translated")
    parser.add_argument("--no-scrape", action="store_true", help="Skip scraping reference content")
    parser.add_argument("--batch-tokens", type=int, default=8000, help="Max tokens per batch (default: 8000)")
    args = parser.parse_args()

    if not args.output:
        base, ext = os.path.splitext(args.input)
        args.output = f"{base}_arabic{ext}"

    load_dotenv()

    # 1. Load TOV
    if not os.path.exists(args.tov):
        print(f"ERROR: TOV file not found: {args.tov}")
        sys.exit(1)
    with open(args.tov, "r", encoding="utf-8") as f:
        tov_text = f.read()
    print(f"Loaded TOV from {args.tov} ({len(tov_text):,} chars)")

    # 2. Scrape Arabic reference content
    reference_samples = ""
    if not args.no_scrape:
        print("\nScraping Arabic reference content...")
        reference = scrape_arabic_reference()
        reference_samples = build_reference_samples(reference)
        if reference_samples:
            print(f"  Built {len(reference_samples.splitlines())} reference samples")
        else:
            print("  No reference samples scraped (will translate without reference)")
    else:
        print("\nSkipping scrape (--no-scrape)")

    # 3. Read CSV
    with open(args.input, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    print(f"\nRead {len(rows)} rows from {args.input}")

    # 4. Categorize ALL rows — translate everything that has English content
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
            # Translate everything — even if already translated (overwrite)
            to_translate.append(i)

    print(f"\nBreakdown:")
    print(f"  Will translate (overwrite all): {len(to_translate)}")
    print(f"  Keep as-is (URLs/images/config): {len(keep_as_is)}")
    print(f"  Skip (empty/non-translatable):   {sum(1 for _ in skip)}")

    # Apply keep-as-is
    for idx in keep_as_is:
        rows[idx]["Translated content"] = rows[idx]["Default content"]

    if args.dry_run:
        print(f"\n--- DRY RUN: Would translate {len(to_translate)} strings ---")
        from collections import Counter
        by_type = Counter(rows[i]["Type"] for i in to_translate)
        for t, c in by_type.most_common():
            print(f"  {t}: {c}")
        print("\nSample strings:")
        for idx in to_translate[:15]:
            r = rows[idx]
            existing = r.get("Translated content", "").strip()
            marker = " [overwrite]" if existing else ""
            print(f"  [{r['Type']}] {r['Field']}: {r['Default content'][:80]}{marker}")
        return

    # 5. Initialize OpenAI
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set. Add it to .env or environment.")
        sys.exit(1)

    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    # 6. Build system prompt
    system_prompt = build_system_prompt(tov_text, reference_samples)

    # 7. Build field list
    fields = []
    for idx in to_translate:
        r = rows[idx]
        field_id = f"{r['Type']}|{r['Identification']}|{r['Field']}"
        fields.append({
            "id": field_id,
            "value": r["Default content"],
            "_row_idx": idx,
        })

    # 8. Load progress (for resumability)
    progress_file = args.output + ".progress.json"
    all_translations = {}
    if os.path.exists(progress_file):
        with open(progress_file, "r", encoding="utf-8") as f:
            all_translations = json.load(f)
        print(f"\nResuming: {len(all_translations)} fields already translated")

    remaining = [f for f in fields if f["id"] not in all_translations]
    print(f"\nTotal: {len(fields)} fields, remaining: {len(remaining)}")

    if remaining:
        batches = adaptive_batch(remaining, max_tokens=args.batch_tokens)
        print(f"Translating in {len(batches)} batches...\n")

        total_tokens = 0
        for i, batch in enumerate(batches):
            api_batch = [{"id": f["id"], "value": f["value"]} for f in batch]
            t_map, tokens = translate_batch(
                client, args.model, api_batch, system_prompt,
                i + 1, len(batches),
            )
            all_translations.update(t_map)
            total_tokens += tokens

            # Save progress after each batch
            with open(progress_file, "w", encoding="utf-8") as f:
                json.dump(all_translations, f, ensure_ascii=False)

        print(f"\nTranslation complete. Total tokens: {total_tokens:,}")

    # 9. Apply translations back to CSV rows
    applied = 0
    for field in fields:
        field_id = field["id"]
        row_idx = field["_row_idx"]
        if field_id in all_translations:
            rows[row_idx]["Translated content"] = all_translations[field_id]
            applied += 1

    print(f"Applied {applied}/{len(fields)} translations to CSV")

    # 10. Write output CSV
    with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Written to {args.output}")

    # Clean up progress file on success
    if len(all_translations) >= len(fields) * 0.95:
        if os.path.exists(progress_file):
            os.remove(progress_file)
            print("Cleaned up progress file")

    # Summary
    final_translated = sum(1 for r in rows if r.get("Translated content", "").strip())
    print(f"\nFinal: {final_translated}/{len(rows)} rows have Arabic content")
    print("\nImport via: Shopify Admin > Settings > Languages > Arabic > Import")
    print("Check 'Overwrite existing translations' to apply all changes.")


if __name__ == "__main__":
    main()
