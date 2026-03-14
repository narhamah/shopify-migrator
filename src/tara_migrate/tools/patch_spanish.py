#!/usr/bin/env python3
"""Patch remaining Spanish content in translation progress without re-running full pipeline.

Scans _translation_progress_en.json for fields where the LLM kept the Spanish
text unchanged, re-translates just those fields, updates the progress file,
then re-runs the merge step to rebuild the English output files.

Usage:
    python patch_spanish.py --dry           # Show what would be re-translated
    python patch_spanish.py                 # Re-translate and rebuild output
    python patch_spanish.py --model gpt-5   # Use a different model
"""

import argparse
import json
import os
import re
import sys

from dotenv import load_dotenv
from openai import OpenAI

from tara_migrate.translation.translate_gaps import (
    build_system_prompt,
    from_toon,
    load_json,
    save_json,
    to_toon,
    translate_with_gaps,
)

PROGRESS_FILE = "data/english/_translation_progress_en.json"

# ── Spanish detection ──────────────────────────────────────────────

# Common Spanish function words / prepositions
_ES_FUNCTION_WORDS = re.compile(
    r'\b(de la|del|de los|de las|para el|para la|sin |con el|con la|'
    r'que el|que la|los |las |una |un |'
    r'tu |su |muy |más |también|además|sobre|entre|hacia|desde|'
    r'por el|por la|como )\b',
    re.IGNORECASE,
)

# Domain-specific Spanish words that should have been translated
_ES_DOMAIN_WORDS = re.compile(
    r'\b(extracto|aceite|semilla|carbón|efecto|bloqueo|reactivación|'
    r'calma|densidad|grosor|raíz|fortalecid[ao]|estimulación|frena|caída|'
    r'mecánica|folicular|visible|inmediato|olor|ancla|miniaturización|'
    r'ciclo|cabello|cabelludo|cuero|piel|champú|mascarilla|acondicionador|'
    r'crema|suavizante|fortalecedor[a]?|nutritiv[oa]|reparador[a]?|protección|pérdida|'
    r'cebolla|romero|salvia|dátil|fresa|nopal|aguacate|apio|levadura|'
    r'oliva|uva|sésamo|argán|soja|vitamínic[oa]|crecimiento|capilar|'
    r'enjuague|aplicación|resultado|beneficio|ingrediente|tratamiento|'
    # SEO / meta title patterns
    r'rutina|hidratante|revitalizante|ceramidas|ajo negro|negro|'
    r'anticaída|limpieza profunda|exfoliante)\b',
    re.IGNORECASE,
)


def is_spanish(text):
    """Heuristic: returns True if text appears to be in Spanish."""
    if not text or not isinstance(text, str):
        return False

    # For rich_text JSON, extract text values
    if text.startswith('{"type":'):
        try:
            obj = json.loads(text)
            text_values = _extract_text_from_richtext(obj)
            return any(is_spanish(t) for t in text_values if len(t) > 5)
        except json.JSONDecodeError:
            pass

    # Skip very short strings (handles, abbreviations)
    if len(text) < 4:
        return False

    return bool(_ES_FUNCTION_WORDS.search(text) or _ES_DOMAIN_WORDS.search(text))


def _extract_text_from_richtext(node):
    """Recursively extract text values from rich_text JSON."""
    texts = []
    if isinstance(node, dict):
        if node.get("type") == "text" and "value" in node:
            texts.append(node["value"])
        for v in node.values():
            texts.extend(_extract_text_from_richtext(v))
    elif isinstance(node, list):
        for item in node:
            texts.extend(_extract_text_from_richtext(item))
    return texts


def find_spanish_fields(progress):
    """Find all fields in the progress file that still contain Spanish."""
    spanish = {}
    for key, value in progress.items():
        if not isinstance(value, str):
            continue
        # Skip handle fields — they're URL slugs, not display text
        if key.endswith(".handle"):
            continue
        # Skip INCI names — they're scientific Latin, not Spanish
        if key.endswith(".inci_name"):
            continue
        if is_spanish(value):
            spanish[key] = value
    return spanish


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Patch Spanish content in translation progress")
    parser.add_argument("--dry", action="store_true", help="Show what would be re-translated")
    parser.add_argument("--model", default="gpt-5-mini", help="OpenAI model (default: gpt-5-mini)")
    args = parser.parse_args()

    if not os.path.exists(PROGRESS_FILE):
        print(f"ERROR: {PROGRESS_FILE} not found. Run translate_gaps.py --lang en first.")
        sys.exit(1)

    progress = load_json(PROGRESS_FILE)
    spanish_fields = find_spanish_fields(progress)

    print(f"{'=' * 60}")
    print("PATCH SPANISH → ENGLISH")
    print(f"{'=' * 60}")
    print(f"  Total fields in progress: {len(progress)}")
    print(f"  Fields still in Spanish:  {len(spanish_fields)}")

    if not spanish_fields:
        print("\n  No Spanish content found — nothing to patch!")
        return

    # Group by category for display
    groups = {}
    for key in spanish_fields:
        parts = key.split(".")
        if parts[0] == "mo":
            group = f"{parts[0]}.{parts[1]}"
        else:
            group = parts[0]
        groups.setdefault(group, []).append(key)

    print("\n  Breakdown:")
    for group, keys in sorted(groups.items()):
        print(f"    {group}: {len(keys)} fields")

    if args.dry:
        print("\n  DRY RUN — fields that would be re-translated:\n")
        for key, value in sorted(spanish_fields.items()):
            # Truncate display
            display = value[:80].replace("\n", "\\n")
            if len(value) > 80:
                display += "..."
            print(f"    {key}")
            print(f"      {display}")
        return

    # ── Re-translate ──────────────────────────────────────────────

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set in environment or .env")
        sys.exit(1)

    client = OpenAI(api_key=api_key)

    # Build fields list for TOON translation
    fields = [{"id": k, "value": v} for k, v in spanish_fields.items()]

    # Use the exact same system prompt as translate_gaps.py but add
    # an extra instruction emphasizing that ALL text must be translated
    system_prompt = build_system_prompt("English")
    system_prompt += (
        "\n\nCRITICAL: Every value MUST be translated to English. "
        "Do NOT keep any Spanish text. Even short brand-adjacent terms like "
        "'Calma Folicular', 'Efecto Ancla', 'Extracto de Romero' MUST be "
        "translated ('Follicular Calm', 'Anchor Effect', 'Rosemary Extract'). "
        "The ONLY exception is the brand name 'TARA' and INCI ingredient "
        "names (scientific Latin names)."
    )

    toon_input = to_toon(fields)
    prompt = (
        f"Translate the following TOON data from Spanish to English. "
        f"Keep all IDs unchanged. Translate ALL values — no Spanish should remain. "
        f"Follow the TARA English tone of voice.\n\n"
        f"{toon_input}"
    )

    print(f"\n  Sending {len(fields)} fields to {args.model}...")

    REASONING_MODELS = {"o3", "o3-mini", "o4-mini", "gpt-5-mini", "gpt-5"}
    is_reasoning = any(args.model.startswith(rm) for rm in REASONING_MODELS)

    api_kwargs = {
        "model": args.model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
    }
    if is_reasoning:
        api_kwargs["reasoning_effort"] = "medium"
    else:
        api_kwargs["temperature"] = 0.3

    try:
        response = client.chat.completions.create(**api_kwargs)
    except Exception as e:
        print(f"  ERROR: {e}")
        sys.exit(1)

    result = response.choices[0].message.content.strip()
    if result.startswith("```"):
        lines = result.split("\n")
        if lines[-1].strip() == "```":
            result = "\n".join(lines[1:-1])
        else:
            result = "\n".join(lines[1:])

    translated = from_toon(result)
    usage = response.usage
    print(f"  Received {len(translated)} fields "
          f"({usage.prompt_tokens} prompt + {usage.completion_tokens} completion tokens)")

    # Build translation map
    t_map = {}
    for entry in translated:
        t_map[entry["id"]] = entry["value"]

    # Verify
    input_ids = set(spanish_fields.keys())
    output_ids = set(t_map.keys())
    missing = input_ids - output_ids
    extra = output_ids - input_ids

    if extra:
        print(f"  Removing {len(extra)} fabricated IDs")
        for eid in extra:
            del t_map[eid]
    if missing:
        print(f"  WARNING: {len(missing)} fields not returned by model:")
        for m in sorted(missing):
            print(f"    {m}")

    # ── Validate translations ─────────────────────────────────────

    still_spanish = 0
    for key, value in t_map.items():
        if is_spanish(value):
            still_spanish += 1
            print(f"  STILL SPANISH: {key}: {value[:60]}...")

    if still_spanish:
        print(f"\n  WARNING: {still_spanish} fields are still Spanish after re-translation")

    # ── Update progress file ──────────────────────────────────────

    updated = 0
    for key, value in t_map.items():
        if key in progress and progress[key] != value:
            progress[key] = value
            updated += 1

    save_json(progress, PROGRESS_FILE)
    print(f"\n  Updated {updated} fields in {PROGRESS_FILE}")

    # ── Show before/after for verification ────────────────────────

    print("\n  Sample translations:")
    for key in list(spanish_fields.keys())[:15]:
        old = spanish_fields[key][:50].replace("\n", "\\n")
        new = t_map.get(key, "MISSING")
        if isinstance(new, str):
            new = new[:50].replace("\n", "\\n")
        print(f"    {key.split('.')[-2] + '.' + key.split('.')[-1] if '.' in key else key}:")
        print(f"      ES: {old}")
        print(f"      EN: {new}")

    # ── Re-run merge to update output files ───────────────────────

    print("\n  Re-running translate_gaps.py to rebuild output files...")
    translate_with_gaps(
        source_dir="data/spain_export",
        output_dir="data/english",
        source_lang="Spanish",
        target_lang="English",
        lang_code="en",
        model=args.model,
        dry=False,
        batch_size=120,
        tpm=30000,
    )

    print(f"\n{'=' * 60}")
    print("PATCH COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Re-translated: {updated} fields")
    print("  Output rebuilt: data/english/")


if __name__ == "__main__":
    main()
