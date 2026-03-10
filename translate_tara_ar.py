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

from tara_migrate.translation.toon import from_toon, to_toon  # noqa: E402

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
    if field == "handle":
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
        "Translate the following TOON input into Tara Arabic and return TOON only.\n\n"
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

            # Extract text output from the response
            result = ""
            for item in response.output:
                if item.type == "message":
                    for content in item.content:
                        if content.type == "output_text":
                            result += content.text

            result = result.strip()

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
            for eid in extra:
                del t_map[eid]
            if missing:
                print(f"    WARNING: {len(missing)} untranslated fields")

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
    parser.add_argument("--batch-size", type=int, default=6000,
                        help="Max tokens per batch (default: 6000)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be translated without API calls")
    parser.add_argument("--reasoning", default="medium",
                        choices=["none", "low", "medium", "high", "xhigh"],
                        help="Reasoning effort (default: medium)")
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
    progress_file = os.path.join(ARABIC_DIR, ".tara_ar_progress.json")

    if args.reset and os.path.exists(progress_file):
        os.remove(progress_file)
        print("Progress file cleared (--reset)")

    our_translations = {}  # field_id → Arabic value (from previous runs)
    if os.path.exists(progress_file):
        with open(progress_file, "r", encoding="utf-8") as f:
            our_translations = json.load(f)

    # ----------------------------------------------------------------
    # 3. Read CSV
    # ----------------------------------------------------------------
    with open(args.input, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    print(f"Read {len(rows)} rows from {args.input}")

    # ----------------------------------------------------------------
    # 4. Categorize rows — distinguish original CSV vs our progress
    # ----------------------------------------------------------------
    to_translate = []       # Need AI translation this run
    from_csv = []           # Already translated in the original CSV
    from_previous_run = []  # Translated by us in a previous run
    keep_as_is = []         # URLs, images, config — copy as-is
    skip = []               # Empty, handles, non-translatable

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
            # We translated this in a previous run — apply it, skip API call
            from_previous_run.append((i, field_id))
        elif translated and not args.overwrite:
            # Was already in the original CSV export
            from_csv.append(i)
        else:
            to_translate.append(i)

    print(f"\nBreakdown:")
    print(f"  From original CSV (already done):  {len(from_csv)}")
    print(f"  From previous run (resuming):      {len(from_previous_run)}")
    print(f"  Keep as-is (URLs/images/config):   {len(keep_as_is)}")
    print(f"  Need AI translation NOW:           {len(to_translate)}")
    print(f"  Skip (empty/non-translatable):     {len(skip)}")

    # Apply keep-as-is
    for idx in keep_as_is:
        rows[idx]["Translated content"] = rows[idx]["Default content"]

    # Apply translations from previous runs
    for idx, field_id in from_previous_run:
        rows[idx]["Translated content"] = our_translations[field_id]

    if not to_translate:
        # Still write the output with previous-run translations applied
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
    # 8. Translate batches
    # ----------------------------------------------------------------
    total_tokens = 0
    start_time = time.time()

    for i, batch in enumerate(batches):
        api_batch = [{"id": f["id"], "value": f["value"]} for f in batch]
        t_map, tokens = translate_batch_responses_api(
            client, args.model, api_batch, developer_prompt,
            i + 1, len(batches), reasoning_effort=args.reasoning,
        )
        our_translations.update(t_map)
        total_tokens += tokens

        # Save progress after every batch
        with open(progress_file, "w", encoding="utf-8") as f:
            json.dump(our_translations, f, ensure_ascii=False)

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
        and r.get("Field") != "handle"
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
