#!/usr/bin/env python3
"""Translate Shopify CSV to Tara Arabic using OpenAI Responses API with prompt caching.

Reads a Shopify "Translate and adapt" CSV export, batches English fields as TOON,
sends them to GPT-5-nano with the cached Tara developer prompt, and writes a
fully translated Arabic CSV ready for Shopify re-import.

Usage:
    python translate_tara_ar.py --input data/Tara_Saudi_translations_Mar-10-2026.csv
    python translate_tara_ar.py --input data/export.csv --dry-run
    python translate_tara_ar.py --input data/export.csv --batch-size 60
    python translate_tara_ar.py --input data/export.csv --model gpt-4o-mini

Prerequisites:
    OPENAI_API_KEY in .env or environment
    pip install openai python-dotenv
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

from tara_migrate.translation.toon import DELIM, from_toon, to_toon  # noqa: E402

ARABIC_DIR = os.path.join(os.path.dirname(__file__), "Arabic")
PROMPT_FILE = os.path.join(ARABIC_DIR, "tara_cached_developer_prompt.txt")
CACHE_KEY = "tara-ar-translation-v1"


# =====================================================================
# CSV row classification
# =====================================================================

def _is_non_translatable(row):
    """Return True if this row should never be translated."""
    default = row.get("Default content", "").strip()
    field = row.get("Field", "")

    if not default:
        return True
    if default.startswith(("shopify://", "http://", "https://", "/", "gid://")):
        return True
    if re.match(r"^-?\d+\.?\d*$", default):
        return True
    if re.match(r"^[0-9a-f]{8,}$", default):
        return True

    # Arrays of GIDs or numeric IDs
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

    # Pure Liquid template expressions — no human-readable text to translate.
    # e.g. "{{ closest.product.title }}", "<h1>{{ article.title }}</h1>",
    #      "{{ block.repeater.question.value }}", "{{ x | metafield_tag }}"
    stripped = re.sub(r"<[^>]+>", "", default).strip()
    if stripped and re.match(r"^(\{\{[^}]+\}\}\s*[:;,]?\s*)+$", stripped):
        return True

    return False


def _is_keep_as_is(row):
    """Check if a row's value should be copied as-is (URLs, images, config)."""
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


def _extract_rich_text(text):
    """Extract plain text from Shopify rich_text JSON (recursive)."""
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None
    parts = []
    def _walk(node):
        if isinstance(node, dict):
            if node.get("type") == "text" and "value" in node:
                parts.append(node["value"])
            for child in node.get("children", []):
                _walk(child)
        elif isinstance(node, list):
            for item in node:
                _walk(item)
    _walk(data)
    return " ".join(parts) if parts else None


def _has_arabic(text, min_ratio=0.3):
    """Check if text contains sufficient Arabic characters.

    Returns False for text that's mostly Latin/Spanish/English.
    Handles HTML, CSS, and Shopify rich_text JSON.
    """
    # Try rich_text JSON first — extract actual text values
    if text.startswith("{") and '"type"' in text:
        extracted = _extract_rich_text(text)
        if extracted and extracted.strip():
            text = extracted

    # Strip HTML tags and CSS for ratio check
    stripped = re.sub(r"<[^>]+>", " ", text)
    stripped = re.sub(r"\{[^}]*\}", " ", stripped)  # CSS blocks
    stripped = stripped.strip()
    if not stripped:
        return True  # empty after stripping = structural content, OK

    arabic = len(re.findall(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", stripped))
    alpha = len(re.findall(r"[a-zA-ZÀ-ÿ\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", stripped))

    if alpha == 0:
        return True  # no letters at all (numbers, symbols) = OK
    return arabic / alpha >= min_ratio


# =====================================================================
# Token estimation & batching
# =====================================================================

def _estimate_tokens(text):
    """Rough token estimate: ~3 chars per token for mixed EN/AR content."""
    return max(1, len(text) // 3)


def adaptive_batch(fields, max_tokens=6000):
    """Split fields into batches sized by estimated token count.

    Short fields (titles, buttons) get packed densely.
    Long fields (body_html, rich_text JSON) get smaller batches.
    """
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


# =====================================================================
# OpenAI Responses API translation
# =====================================================================

def load_developer_prompt():
    """Load the cached developer prompt from file."""
    if not os.path.exists(PROMPT_FILE):
        print(f"ERROR: Developer prompt not found: {PROMPT_FILE}")
        sys.exit(1)
    with open(PROMPT_FILE, "r", encoding="utf-8") as f:
        return f.read()


def translate_batch_responses_api(client, model, fields, developer_prompt,
                                  batch_num, total_batches, reasoning_effort="medium"):
    """Translate a batch of fields using the OpenAI Responses API with prompt caching.

    Uses prompt_cache_key so the developer prompt is cached across all requests.
    Returns (translation_map, usage_dict).
    """
    toon_input = to_toon(fields)
    user_message = (
        "Translate the following TOON input into Tara Arabic and return TOON only.\n"
        "The source text may be in English or Spanish — translate both to Arabic.\n\n"
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
            for item in response.output:
                if item.type == "message":
                    for content in item.content:
                        if getattr(content, "type", "") == "refusal":
                            refusal_text = getattr(content, "refusal", str(content))
                            print(f"    REFUSAL (attempt {attempt+1}): {refusal_text}")
                            # Dump the input that triggered refusal
                            debug_file = os.path.join(ARABIC_DIR, f".debug_refusal_batch_{batch_num}.txt")
                            with open(debug_file, "w", encoding="utf-8") as df:
                                df.write(f"=== REFUSAL (attempt {attempt+1}) ===\n")
                                df.write(f"{refusal_text}\n\n")
                                df.write(f"=== INPUT TOON ({len(fields)} fields) ===\n")
                                df.write(toon_input[:5000])
                            print(f"    Refusal debug dumped to {debug_file}")
                            if attempt < 3:
                                print("    Retrying...")
                                time.sleep(2)
                                break
                    else:
                        continue
                    break  # break outer loop if refusal found
            else:
                pass  # no refusal found, continue normally

            # Extract text output from the response
            result = ""
            for item in response.output:
                if item.type == "message":
                    for content in item.content:
                        if content.type == "output_text":
                            result += content.text

            result = result.strip()

            # Detect text-based refusal (model says sorry instead of TOON)
            if result and not DELIM in result and ("sorry" in result.lower() or "can't process" in result.lower()):
                print(f"    TEXT REFUSAL (attempt {attempt+1}): {result[:200]}")
                debug_file = os.path.join(ARABIC_DIR, f".debug_refusal_batch_{batch_num}.txt")
                with open(debug_file, "w", encoding="utf-8") as df:
                    df.write(f"=== TEXT REFUSAL (attempt {attempt+1}) ===\n")
                    df.write(f"{result}\n\n")
                    df.write(f"=== INPUT TOON ({len(fields)} fields, first 5000 chars) ===\n")
                    df.write(toon_input[:5000])
                print(f"    Refusal debug dumped to {debug_file}")
                if attempt < 3:
                    time.sleep(2)
                    continue
                return {}, 0

            # Strip markdown code fences if model wraps output
            if result.startswith("```"):
                lines = result.split("\n")
                if lines[-1].strip() == "```":
                    result = "\n".join(lines[1:-1])
                else:
                    result = "\n".join(lines[1:])

            # Strip <TOON> tags if model echoes them
            result = re.sub(r"</?TOON>", "", result).strip()

            translated = from_toon(result)

            if len(translated) != len(fields):
                print(f"    WARNING: Expected {len(fields)} fields, got {len(translated)}.")
                # Debug: dump raw response to file for inspection
                debug_file = os.path.join(ARABIC_DIR, f".debug_batch_{batch_num}.txt")
                with open(debug_file, "w", encoding="utf-8") as df:
                    df.write(f"=== RAW RESPONSE (attempt {attempt+1}) ===\n")
                    df.write(result)
                    df.write(f"\n\n=== PARSED {len(translated)} entries ===\n")
                    for e in translated:
                        df.write(f"  {e['id'][:60]}  →  {e['value'][:80]}\n")
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

            # Build translation map
            t_map = {}
            for entry in translated:
                t_map[entry["id"]] = entry["value"]

            # Verify IDs match
            input_ids = {f["id"] for f in fields}
            output_ids = set(t_map.keys())
            extra = output_ids - input_ids
            missing = input_ids - output_ids
            if extra:
                print(f"    DEBUG: {len(extra)} extra IDs (hallucinated): {list(extra)[:3]}")
                for eid in extra:
                    del t_map[eid]
            if missing:
                print(f"    WARNING: {len(missing)} untranslated fields")
                print(f"    DEBUG: missing IDs: {list(missing)[:5]}")

            # Usage stats
            usage = response.usage
            total_tokens = (usage.input_tokens or 0) + (usage.output_tokens or 0)
            cached = getattr(usage, "input_tokens_details", None)
            cached_tokens = getattr(cached, "cached_tokens", 0) if cached else 0
            cache_info = f" [cached: {cached_tokens:,}]" if cached_tokens else ""
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
# Main
# =====================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Translate Shopify CSV to Tara Arabic (GPT-5-nano + prompt caching)")
    parser.add_argument("--input", required=True,
                        help="Input CSV file (Shopify translation export)")
    parser.add_argument("--output", default=None,
                        help="Output CSV (default: Arabic/<input_filename>)")
    parser.add_argument("--model", default="gpt-5-nano",
                        help="OpenAI model (default: gpt-5-nano)")
    parser.add_argument("--batch-size", type=int, default=4000,
                        help="Max tokens per batch (default: 4000; keep low to avoid output truncation)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be translated without API calls")
    parser.add_argument("--max-batches", type=int, default=0,
                        help="Stop after N batches (0 = unlimited, for testing)")
    parser.add_argument("--agents", type=int, default=1,
                        help="Number of parallel workers (default: 1)")
    parser.add_argument("--start-batch", type=int, default=0,
                        help="Skip to batch N (0-indexed, for parallel runs)")
    parser.add_argument("--progress-suffix", default="",
                        help="Suffix for progress file (e.g. '_b0' for parallel runs)")
    parser.add_argument("--reasoning", default="medium",
                        choices=["minimal", "low", "medium", "high"],
                        help="Reasoning effort (default: medium)")
    parser.add_argument("--fix", action="store_true",
                        help="Re-translate fields that have no/low Arabic (bad translations)")
    parser.add_argument("--todo", default=None,
                        help="To-do JSON from verify_translation.py (translate only listed items)")
    parser.add_argument("--fix-spanish", action="store_true",
                        help="Also translate Spanish 'Default content' to English")
    parser.add_argument("--overwrite", action="store_true",
                        help="Ignore original CSV translations (re-translate them)")
    parser.add_argument("--reset", action="store_true",
                        help="Clear progress file (re-translate what this script did before)")
    args = parser.parse_args()

    # Default output: Arabic/<input_filename>
    if not args.output:
        input_filename = os.path.basename(args.input)
        args.output = os.path.join(ARABIC_DIR, input_filename)

    os.makedirs(ARABIC_DIR, exist_ok=True)
    load_dotenv()

    # ----------------------------------------------------------------
    # 1. Load developer prompt
    # ----------------------------------------------------------------
    developer_prompt = load_developer_prompt()
    print(f"Loaded developer prompt ({len(developer_prompt):,} chars, "
          f"~{_estimate_tokens(developer_prompt):,} tokens)")
    print(f"Cache key: {CACHE_KEY}")

    # ----------------------------------------------------------------
    # 2. Load progress file (tracks what THIS SCRIPT translated)
    # ----------------------------------------------------------------
    progress_file = os.path.join(ARABIC_DIR, f".tara_ar_progress{args.progress_suffix}.json")

    if args.reset and os.path.exists(progress_file):
        os.remove(progress_file)
        print("Progress file cleared (--reset)")

    our_translations = {}  # field_id → Arabic value (from previous runs)
    if os.path.exists(progress_file):
        with open(progress_file, "r", encoding="utf-8") as f:
            our_translations = json.load(f)

    # --fix: purge bad translations from progress (non-Arabic results)
    if args.fix and our_translations:
        bad_keys = [k for k, v in our_translations.items() if not _has_arabic(v)]
        if bad_keys:
            for k in bad_keys:
                del our_translations[k]
            with open(progress_file, "w", encoding="utf-8") as f:
                json.dump(our_translations, f, ensure_ascii=False)
            print(f"--fix: purged {len(bad_keys)} bad translations from progress")
        else:
            print("--fix: progress file is clean (all have Arabic)")

    # ----------------------------------------------------------------
    # 3. Read CSV
    # ----------------------------------------------------------------
    with open(args.input, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    print(f"Read {len(rows)} rows from {args.input}")

    # ----------------------------------------------------------------
    # 4. Categorize rows
    # ----------------------------------------------------------------
    to_translate = []       # Need AI translation this run
    to_fix_spanish = []     # Spanish "Default content" → translate to English
    from_csv = []           # Already translated in the original CSV
    from_previous_run = []  # Translated by us in a previous run
    keep_as_is = []         # URLs, images, config — copy as-is
    skip = []               # Empty, non-translatable
    fix_bad_csv = []        # --fix: CSV has translation but it's not Arabic

    # Build row index by field_id for --todo lookups
    row_by_field_id = {}
    for i, row in enumerate(rows):
        field_id = f"{row['Type']}|{row['Identification']}|{row['Field']}"
        row_by_field_id[field_id] = i

    if args.todo:
        # --todo mode: only process items from the to-do file
        # Accepts both formats:
        #   - verify_translation.py _todo.json:       {"field_id": "TYPE|ID|FIELD", "action": "translate"}
        #   - validate_csv.py _mismatches.json:       {"type": "PRODUCT", "identification": "123", "field": "title", "reason": "..."}
        with open(args.todo, "r", encoding="utf-8") as f:
            todo_items = json.load(f)

        for item in todo_items:
            # Normalize: build field_id from either format
            if "field_id" in item:
                fid = item["field_id"]
            elif "type" in item and "identification" in item and "field" in item:
                fid = f"{item['type']}|{item['identification']}|{item['field']}"
            else:
                continue

            action = item.get("action", "translate")  # default to translate for mismatches
            idx = row_by_field_id.get(fid)
            if idx is None:
                continue

            if action == "fix_default_es_to_en" and args.fix_spanish:
                to_fix_spanish.append(idx)
            else:
                # Skip if progress already has good Arabic for this field
                if fid in our_translations and _has_arabic(our_translations[fid]):
                    from_previous_run.append((idx, fid))
                    continue
                # Purge bad translation from progress
                if fid in our_translations:
                    del our_translations[fid]
                to_translate.append(idx)

        print(f"\n--todo mode: {len(todo_items)} items from {args.todo}")
        print(f"  Already good (in progress):  {len(from_previous_run)}")
        print(f"  To translate (→ Arabic):     {len(to_translate)}")
        if to_fix_spanish:
            print(f"  Fix Spanish → English:       {len(to_fix_spanish)}")

    else:
        # Normal mode: classify all rows
        for i, row in enumerate(rows):
            default = row.get("Default content", "").strip()
            translated = row.get("Translated content", "").strip()
            field_id = f"{row['Type']}|{row['Identification']}|{row['Field']}"

            if not default:
                skip.append((i, "empty"))
            elif _is_non_translatable(row):
                skip.append((i, "non-translatable"))
            elif _is_keep_as_is(row):
                keep_as_is.append(i)
            elif field_id in our_translations and not args.reset:
                from_previous_run.append((i, field_id))
            elif translated and not args.overwrite:
                # Detect untranslated content: identical to default or no Arabic
                is_fake = (translated == default and not _has_arabic(translated))
                needs_fix = args.fix and not _has_arabic(translated)
                if is_fake or needs_fix:
                    fix_bad_csv.append(i)
                    to_translate.append(i)
                else:
                    from_csv.append(i)
            else:
                to_translate.append(i)

        n_gaps = len(to_translate) - len(fix_bad_csv) if our_translations and not args.reset else 0

        print(f"\nBreakdown:")
        print(f"  From original CSV (already done):  {len(from_csv)}")
        print(f"  From previous run (resuming):      {len(from_previous_run)}")
        print(f"  Keep as-is (URLs/images/config):   {len(keep_as_is)}")
        print(f"  Need AI translation NOW:           {len(to_translate)}")
        if fix_bad_csv:
            print(f"    ↳ {len(fix_bad_csv)} bad translations (no Arabic) to re-translate")
        if n_gaps > 0:
            print(f"    ↳ {n_gaps} gaps/retries from previous run")
        print(f"  Skip (empty/non-translatable):     {len(skip)}")

    # Apply keep-as-is
    for idx in keep_as_is:
        rows[idx]["Translated content"] = rows[idx]["Default content"]

    # Apply translations from previous runs
    for idx, field_id in from_previous_run:
        rows[idx]["Translated content"] = our_translations[field_id]

    if not to_translate and not to_fix_spanish:
        if from_previous_run:
            with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f"\nNothing new to translate. Applied {len(from_previous_run)} "
                  f"from previous run → {args.output}")
        else:
            print("\nNothing to translate. All rows are done.")
        return

    # ----------------------------------------------------------------
    # 5. Build TOON field list (only what needs translating NOW)
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
    # 6. Batch fields
    # ----------------------------------------------------------------
    batches = adaptive_batch(fields, max_tokens=args.batch_size)
    total_value_tokens = sum(_estimate_tokens(f["value"]) for f in fields)

    print(f"\n{len(fields)} fields → {len(batches)} batches "
          f"(~{total_value_tokens:,} value tokens)")
    print(f"Developer prompt: ~{_estimate_tokens(developer_prompt):,} tokens "
          f"(cached after 1st request)\n")

    if args.dry_run:
        from collections import Counter
        by_type = Counter(rows[i]["Type"] for i in to_translate)
        print("Fields by type:")
        for t, c in by_type.most_common():
            print(f"  {t}: {c}")

        # Cost estimate: prompt cached after 1st call
        prompt_tokens = _estimate_tokens(developer_prompt)
        est_input = prompt_tokens + total_value_tokens + (prompt_tokens * (len(batches) - 1) * 0.1)
        est_output = total_value_tokens * 1.5  # Arabic is ~1.5x English in tokens
        print(f"\nEstimated tokens:")
        print(f"  Input:  ~{int(est_input):,} (prompt cached after 1st batch)")
        print(f"  Output: ~{int(est_output):,}")

        print(f"\nSample fields:")
        for idx in to_translate[:10]:
            r = rows[idx]
            existing = r.get("Translated content", "").strip()
            marker = " [overwrite]" if existing else ""
            print(f"  [{r['Type']}] {r['Field']}: {r['Default content'][:80]}{marker}")
        return

    # ----------------------------------------------------------------
    # 7. Initialize OpenAI
    # ----------------------------------------------------------------
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set. Add it to .env or environment.")
        sys.exit(1)

    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    # ----------------------------------------------------------------
    # 7b. Fix Spanish → English in "Default content" (if --fix-spanish)
    # ----------------------------------------------------------------
    if to_fix_spanish:
        print(f"\nStep 1: Translating {len(to_fix_spanish)} Spanish fields → English...")
        es_fields = []
        for idx in to_fix_spanish:
            r = rows[idx]
            field_id = f"{r['Type']}|{r['Identification']}|{r['Field']}"
            es_fields.append({
                "id": field_id,
                "value": r["Default content"],
                "_row_idx": idx,
            })

        es_batches = adaptive_batch(es_fields, max_tokens=args.batch_size)
        es_prompt = (
            "Translate the following TOON input from Spanish to English.\n"
            "Keep brand names (Tara, CapixylTM, etc.) as-is.\n"
            "Keep all HTML tags intact. Return TOON only.\n"
        )

        es_tokens = 0
        for i, batch in enumerate(es_batches):
            api_batch = [{"id": f["id"], "value": f["value"]} for f in batch]
            toon_input = to_toon(api_batch)
            user_msg = f"{es_prompt}\n<TOON>\n{toon_input}\n</TOON>"

            print(f"  ES→EN batch {i+1}/{len(es_batches)}: {len(batch)} fields...")
            try:
                response = client.responses.create(
                    model=args.model,
                    input=user_msg,
                    reasoning={"effort": "low"},
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

                translated_es = from_toon(result)
                t_map = {e["id"]: e["value"] for e in translated_es}
                usage = response.usage
                es_tokens += (usage.input_tokens or 0) + (usage.output_tokens or 0)

                # Apply English translations back to "Default content"
                for field in batch:
                    if field["id"] in t_map:
                        rows[field["_row_idx"]]["Default content"] = t_map[field["id"]]

                print(f"    Done: {len(t_map)} fields")
            except Exception as e:
                print(f"    Error: {e}")

        print(f"  Spanish→English complete ({es_tokens:,} tokens)")

    # ----------------------------------------------------------------
    # 8. Translate batches → Arabic
    # ----------------------------------------------------------------
    # Filter batches based on --start-batch / --max-batches
    work_items = []
    for i, batch in enumerate(batches):
        if i < args.start_batch:
            continue
        if args.max_batches and (i - args.start_batch) >= args.max_batches:
            break
        work_items.append((i, batch))

    total_tokens = 0
    start_time = time.time()
    progress_lock = __import__("threading").Lock()

    def _translate_one(item):
        idx, batch = item
        api_batch = [{"id": f["id"], "value": f["value"]} for f in batch]
        t_map, tokens = translate_batch_responses_api(
            client, args.model, api_batch, developer_prompt,
            idx + 1, len(batches), reasoning_effort=args.reasoning,
        )
        # Thread-safe progress update
        with progress_lock:
            our_translations.update(t_map)
            with open(progress_file, "w", encoding="utf-8") as pf:
                json.dump(our_translations, pf, ensure_ascii=False)
        return t_map, tokens

    n_agents = min(args.agents, len(work_items)) if work_items else 1

    if n_agents > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        print(f"Running {len(work_items)} batches with {n_agents} parallel agents...")
        with ThreadPoolExecutor(max_workers=n_agents) as pool:
            futures = {pool.submit(_translate_one, item): item for item in work_items}
            for future in as_completed(futures):
                try:
                    t_map, tokens = future.result()
                    total_tokens += tokens
                except Exception as e:
                    idx, _ = futures[future]
                    print(f"    Batch {idx+1} failed: {e}")
    else:
        for item in work_items:
            t_map, tokens = _translate_one(item)
            total_tokens += tokens

    elapsed = time.time() - start_time
    print(f"\nTranslation complete: {total_tokens:,} tokens in {elapsed:.1f}s")

    # ----------------------------------------------------------------
    # 9. Apply NEW translations to CSV rows
    # ----------------------------------------------------------------
    applied = 0
    for field in fields:
        row_idx = field["_row_idx"]
        if field["id"] in our_translations:
            rows[row_idx]["Translated content"] = our_translations[field["id"]]
            applied += 1

    print(f"Applied {applied}/{len(fields)} new translations")

    # ----------------------------------------------------------------
    # 10. Write output CSV
    # ----------------------------------------------------------------
    with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved to {args.output}")

    # ----------------------------------------------------------------
    # 11. Summary
    # ----------------------------------------------------------------
    final_translated = sum(1 for r in rows if r.get("Translated content", "").strip())
    final_empty = sum(
        1 for r in rows
        if r.get("Default content", "").strip()
        and not r.get("Translated content", "").strip()
        and not _is_non_translatable(r)
    )
    print(f"\nFinal: {final_translated}/{len(rows)} rows have Arabic content")
    print(f"  From original CSV:    {len(from_csv)}")
    print(f"  From previous runs:   {len(from_previous_run)}")
    print(f"  Translated this run:  {applied}")
    print(f"  Keep-as-is:           {len(keep_as_is)}")
    if final_empty:
        print(f"  Still untranslated:   {final_empty} (re-run to retry)")

    print(f"\nProgress saved to {progress_file}")
    print(f"  ({len(our_translations)} total fields translated by this script)")

    print(f"\nReady to import:")
    print(f"  File: {args.output}")
    print(f"  Shopify Admin → Settings → Languages → Arabic → Import")
    print(f"  Check 'Overwrite existing translations'")


if __name__ == "__main__":
    main()
