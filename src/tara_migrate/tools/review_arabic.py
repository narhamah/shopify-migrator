#!/usr/bin/env python3
"""Review and fix Arabic translations on the Saudi Shopify store.

7-step pipeline:
  1. Fetch all English content from Shopify
  2. Fetch matching Arabic translations (via Translations API)
  3. AI semantic check: does Arabic actually correspond to English? (Haiku)
  4. Re-translate mismatches EN→AR via TranslationEngine
  5. Detect and strip HTML bloat from Arabic translations
  6. Detect English/Spanish remnants in Arabic → re-translate
  7. Verify: re-audit to ensure Arabic is clean

Content checked (all translatable resource types):
  - Products (title, body_html, description_tag, + 19 custom metafields)
  - Collections (title, body_html, SEO fields)
  - Pages (title, body_html)
  - Articles (title, body_html, metafields)
  - Metaobjects (benefit, faq_entry, ingredient, blog_author fields)
  - Metafields (all text types)

Usage:
    python review_arabic.py --audit                # Audit only, no changes
    python review_arabic.py --dry-run              # Show planned changes
    python review_arabic.py                        # Full pipeline: audit + fix + verify
    python review_arabic.py --type PRODUCT         # Only audit products
    python review_arabic.py --skip-semantic        # Skip AI correspondence check
    python review_arabic.py --model gpt-5-mini     # Override translation model
    python review_arabic.py --save-report FILE     # Save audit report to JSON
"""

import argparse
import json
import os
import re
import sys
import time

import anthropic
from dotenv import load_dotenv

from tara_migrate.audit.audit_translations import classify_translation
from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.core.graphql_queries import (
    TRANSLATABLE_RESOURCES_QUERY,
    fetch_translatable_resources,
    upload_translations,
)
from tara_migrate.core.language import (
    count_chars,
    has_significant_english,
    replace_range_names_ar,
)
from tara_migrate.core.rich_text import extract_text, is_rich_text_json
from tara_migrate.core.shopify_fields import TRANSLATABLE_RESOURCE_TYPES
from tara_migrate.tools.patch_spanish import is_spanish
from tara_migrate.tools.review_content import has_html_bloat, strip_html_bloat
from tara_migrate.translation.engine import TranslationEngine, load_developer_prompt

LOCALE = "ar"

# ─────────────────────────────────────────────────────────────────────────────
# English product/descriptive terms that MUST be translated to Arabic.
# If these appear in Arabic translations, they're untranslated remnants.
# ─────────────────────────────────────────────────────────────────────────────

_UNTRANSLATED_EN = re.compile(
    r'\b(?:'
    # Product types
    r'shampoo|conditioner|serum|mask|scalp|hair\s*care'
    r'|leave[\s-]*in|dry[\s-]*oil|clay[\s-]*mask|body\s*scrub'
    # Descriptive terms
    r'|volumizing|thickening|hydrating|nourishing|revitalizing'
    r'|exfoliating|replenishing|purifying|nurturing|detoxifying'
    r'|anti[\s-]*hair[\s-]*fall|age[\s-]*well|intensive\s+treatment'
    # Range/collection terms
    r'|multivitamin|multivitamins'
    # System/bundle terms
    r'|hair\s+density\s+system|hair\s+stimulation\s+system'
    r'|scalp\s*\+?\s*hair\s+revival|nurture\s+system|age[\s-]*well\s+system'
    # Common nouns/adjectives that must be translated
    r'|description|benefits|ingredients|how\s+to\s+use'
    r'|free\s+of|clinical\s+results|key\s+benefits'
    r')\b',
    re.IGNORECASE,
)

# Words that are ALLOWED in Latin script within Arabic translations
_ALLOWED_LATIN = re.compile(
    r'^(?:'
    r'TARA|Kansa|Wand|Gua|Sha'  # Brand names
    r'|pH|AHA|BHA|NMF|SPF|UV|DNA|RNA|ATP'  # Scientific abbreviations
    r'|ml|mg|mm|cm|kg|g|oz'  # Units
    r'|[A-E]\d*'  # Vitamin letters (A, B3, B5, C, D, E)
    r')$',
    re.IGNORECASE,
)


# ═════════════════════════════════════════════════════════════════════════════
# Phase 1: Fetch
# ═════════════════════════════════════════════════════════════════════════════

def fetch_translations(client, resource_types, locale=LOCALE):
    """Fetch all translatable content with English source and Arabic translations.

    Uses Shopify's translatableResources GraphQL query to get both the source
    content and any existing translations in a single paginated pass.

    Returns:
        (all_fields, resource_counts) where all_fields is a list of dicts:
        [{resource_id, resource_type, key, english, arabic, digest, outdated}, ...]
    """
    query = TRANSLATABLE_RESOURCES_QUERY.replace("%LOCALE%", locale)
    all_fields = []
    resource_counts = {}

    for rtype in resource_types:
        count = 0
        field_count = 0
        cursor = None

        while True:
            try:
                data = client._graphql(query, {
                    "resourceType": rtype,
                    "first": 50,
                    "after": cursor,
                })
            except Exception as e:
                print(f"  ERROR fetching {rtype}: {e}")
                break

            container = data.get("translatableResources", {})
            edges = container.get("edges", [])
            page_info = container.get("pageInfo", {})

            for edge in edges:
                node = edge["node"]
                rid = node["resourceId"]
                translations = {t["key"]: t for t in node.get("translations", [])}
                count += 1

                for field in node.get("translatableContent", []):
                    key = field["key"]
                    english = field.get("value") or ""
                    trans = translations.get(key)
                    arabic = trans["value"] if trans else None
                    outdated = trans.get("outdated", False) if trans else False

                    all_fields.append({
                        "resource_id": rid,
                        "resource_type": rtype,
                        "key": key,
                        "english": english,
                        "arabic": arabic,
                        "digest": field.get("digest", ""),
                        "outdated": outdated,
                    })
                    field_count += 1

            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            time.sleep(0.3)

        resource_counts[rtype] = (count, field_count)
        print(f"  {rtype}: {count} resources, {field_count} fields")

    return all_fields, resource_counts


# ═════════════════════════════════════════════════════════════════════════════
# Phase 2: Classify (basic + enhanced checks)
# ═════════════════════════════════════════════════════════════════════════════

def _extract_checkable_text(value):
    """Extract plain text from a value for language checks."""
    if not value:
        return ""
    if is_rich_text_json(value):
        return extract_text(value) or value
    # Strip HTML tags
    return re.sub(r'<[^>]+>', ' ', value)


def _has_untranslated_english(text):
    """Check if text contains English product terms that should be in Arabic."""
    if not text:
        return []
    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = re.sub(r'\{[^}]*\}', ' ', clean)
    matches = _UNTRANSLATED_EN.findall(clean)
    return [m.strip() for m in matches] if matches else []


def _has_spanish_in_arabic(text):
    """Check if Arabic text contains Spanish remnants in its Latin portions."""
    if not text:
        return False
    # Extract only the Latin-script portions
    latin_parts = re.findall(r'[a-zA-ZÀ-ÿ]+(?:\s+[a-zA-ZÀ-ÿ]+)*', text)
    latin_text = " ".join(latin_parts)
    if not latin_text or len(latin_text) < 15:
        return False
    return is_spanish(latin_text)


def classify_fields(fields):
    """Classify all fields with basic checks + enhanced Arabic quality checks.

    Basic checks (from audit_translations.classify_translation):
      SKIP, MISSING, IDENTICAL, NOT_ARABIC, MIXED_LANGUAGE, CORRUPTED_JSON, OUTDATED, OK

    Enhanced checks (applied to "OK" fields):
      HTML_BLOAT     — Arabic translation contains HTML bloat (Magento artifacts etc.)
      HAS_ENGLISH    — Arabic contains English product terms that should be translated
      HAS_SPANISH    — Arabic contains Spanish text

    Returns list of classified fields (same dicts with added status/detail keys).
    """
    results = []
    stats = {
        "total": 0, "ok": 0, "skip": 0,
        "missing": 0, "identical": 0, "not_arabic": 0,
        "mixed": 0, "corrupted": 0, "outdated": 0,
        "html_bloat": 0, "has_english": 0, "has_spanish": 0,
        "source_spanish": 0,
    }

    for field in fields:
        english = field["english"]
        arabic = field["arabic"]
        key = field["key"]
        outdated = field["outdated"]

        # Basic classification (reuse existing logic)
        status, detail = classify_translation(
            english, arabic, key=key, outdated=outdated,
        )

        if status == "SKIP":
            stats["skip"] += 1
            results.append({**field, "status": "SKIP", "detail": detail})
            continue

        stats["total"] += 1

        # Check 0: Is the English SOURCE actually Spanish?
        # This means review_content.py didn't clean it — flag separately.
        if english and status != "MISSING":
            en_text = _extract_checkable_text(english)
            if en_text and len(en_text) >= 15 and is_spanish(en_text):
                status = "SOURCE_SPANISH"
                detail = "English source is actually Spanish — run review_content.py first"
                stats["source_spanish"] += 1
                results.append({**field, "status": status, "detail": detail})
                continue

        # Enhanced checks for fields that passed basic classification
        if status == "OK":
            ar_text = _extract_checkable_text(arabic) if arabic else ""

            # Check 1: HTML bloat in Arabic translation
            if arabic and has_html_bloat(arabic):
                status, detail = "HTML_BLOAT", "Arabic translation contains HTML bloat"

            # Check 2: Untranslated English product terms
            elif ar_text:
                en_matches = _has_untranslated_english(ar_text)
                if en_matches:
                    preview = ", ".join(en_matches[:3])
                    status = "HAS_ENGLISH"
                    detail = f"untranslated English: {preview}"

            # Check 3: Spanish remnants in Arabic
            if status == "OK" and ar_text and _has_spanish_in_arabic(ar_text):
                status, detail = "HAS_SPANISH", "Arabic contains Spanish text"

        # Update stats
        stat_key = {
            "OK": "ok", "MISSING": "missing", "IDENTICAL": "identical",
            "NOT_ARABIC": "not_arabic", "MIXED_LANGUAGE": "mixed",
            "CORRUPTED_JSON": "corrupted", "OUTDATED": "outdated",
            "HTML_BLOAT": "html_bloat", "HAS_ENGLISH": "has_english",
            "HAS_SPANISH": "has_spanish", "SOURCE_SPANISH": "source_spanish",
        }.get(status, "ok")
        stats[stat_key] += 1

        results.append({**field, "status": status, "detail": detail})

    return results, stats


# ═════════════════════════════════════════════════════════════════════════════
# Phase 3: Semantic correspondence check (Haiku)
# ═════════════════════════════════════════════════════════════════════════════

_SEMANTIC_PROMPT = """\
You are validating Arabic translations for TARA, a luxury scalp-care brand.

For each numbered pair, determine if the Arabic ACCURATELY translates the English.

VALID translations (should PASS):
- Arabic conveys the same meaning as English (doesn't need to be literal)
- Brand names kept in English: TARA, Kansa Wand, Gua Sha
- INCI ingredient names kept in English
- Scientific abbreviations in English: pH, AHA, NMF, UV
- Numbers, units (ml, mg), percentages unchanged
- Arabic may restructure sentences for natural flow

INVALID translations (should FAIL):
- Arabic says something completely different from English
- Important information missing or added
- Arabic is garbled, truncated, or nonsensical
- English product words left untranslated: Shampoo, Conditioner, Serum, Mask
- English range names left untranslated: Date+Multivitamin, Sage+Multivitamin
- Text in a third language (Spanish, French, etc.)

Reply ONLY with a JSON array:
[{"id":1,"pass":true},{"id":2,"pass":false,"reason":"brief reason under 15 words"}]
"""


def run_semantic_check(ok_fields, haiku_client, model="claude-haiku-4-5-20251001",
                       batch_size=15):
    """Batch AI check: does each Arabic translation match its English source?

    Only run on fields that passed basic classification (status == "OK").

    Args:
        ok_fields: List of (index, field_dict) tuples for OK fields.
        haiku_client: anthropic.Anthropic client.
        model: Haiku model for semantic checking.
        batch_size: Pairs per API call (15 is efficient for Haiku).

    Returns:
        dict of {index: {"pass": bool, "reason": str}}
    """
    if not ok_fields:
        return {}

    results = {}
    total_batches = (len(ok_fields) + batch_size - 1) // batch_size
    passed = 0
    failed = 0

    for batch_start in range(0, len(ok_fields), batch_size):
        batch = ok_fields[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1

        if batch_num % 10 == 1 or total_batches <= 20:
            print(f"  Batch {batch_num}/{total_batches}...")

        # Build prompt with numbered pairs
        prompt_parts = [_SEMANTIC_PROMPT, "\n"]
        for j, (idx, field) in enumerate(batch):
            en = field["english"][:400]
            ar = (field["arabic"] or "")[:400]
            # For rich text, show extracted text
            if is_rich_text_json(en):
                en = f"[rich_text] {(extract_text(en) or en)[:350]}"
            if ar and is_rich_text_json(ar):
                ar = f"[rich_text] {(extract_text(ar) or ar)[:350]}"
            prompt_parts.append(f'{j + 1}. EN: {en}\n   AR: {ar}\n\n')

        prompt = "".join(prompt_parts)

        try:
            resp = haiku_client.messages.create(
                model=model,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            answer = resp.content[0].text.strip()

            # Parse JSON from response
            json_match = re.search(r'\[.*\]', answer, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group())
                for item in parsed:
                    pair_offset = item.get("id", 0) - 1  # 1-indexed → 0-indexed
                    if 0 <= pair_offset < len(batch):
                        original_idx = batch[pair_offset][0]
                        is_pass = item.get("pass", True)
                        results[original_idx] = {
                            "pass": is_pass,
                            "reason": item.get("reason", ""),
                        }
                        if is_pass:
                            passed += 1
                        else:
                            failed += 1
            else:
                # Could not parse — mark all as passed (conservative)
                for idx, _ in batch:
                    results[idx] = {"pass": True, "reason": "parse error"}
                    passed += 1

        except Exception as e:
            print(f"    Semantic check error on batch {batch_num}: {e}")
            for idx, _ in batch:
                results[idx] = {"pass": True, "reason": f"API error: {e}"}
                passed += 1

        time.sleep(0.5)

    print(f"  Semantic check complete: {passed} pass, {failed} fail")
    return results


# ═════════════════════════════════════════════════════════════════════════════
# Full Audit Pipeline
# ═════════════════════════════════════════════════════════════════════════════

def run_audit(client, resource_types, haiku_client, haiku_model, skip_semantic=False):
    """Complete audit: fetch → classify → semantic check.

    Returns:
        (all_classified, problems, stats)
        problems = list of fields with status != OK and != SKIP
    """
    # Step 1-2: Fetch English + Arabic
    print("Fetching translations...")
    all_fields, resource_counts = fetch_translations(client, resource_types)

    total_fields = sum(fc for _, fc in resource_counts.values())
    total_resources = sum(rc for rc, _ in resource_counts.values())
    print(f"\nTotal: {total_resources} resources, {total_fields} fields")

    # Step 3a: Basic + enhanced classification
    print(f"\n{'=' * 60}")
    print("CLASSIFICATION")
    print("=" * 60)
    classified, stats = classify_fields(all_fields)

    # Print stats
    for key, val in stats.items():
        if key != "total" and key != "skip" and val > 0:
            print(f"  {key.upper():20s} {val:>5d}")
    print(f"  {'TOTAL':20s} {stats['total']:>5d}")
    print(f"  {'SKIPPED':20s} {stats['skip']:>5d}")

    if stats.get("source_spanish", 0) > 0:
        print(f"\n  WARNING: {stats['source_spanish']} fields have Spanish as the "
              f"English source!")
        print(f"  → Run 'python review_content.py' first to fix the English source.")
        print(f"  → review_arabic.py will translate them directly to Arabic for now.")

    # Step 3b: Semantic correspondence check on OK fields
    if not skip_semantic and stats["ok"] > 0:
        print(f"\n{'=' * 60}")
        print(f"SEMANTIC CORRESPONDENCE CHECK ({haiku_model})")
        print("=" * 60)
        print(f"  Checking {stats['ok']} OK fields...")

        # Collect OK fields with their indices
        ok_fields = [
            (i, f) for i, f in enumerate(classified)
            if f["status"] == "OK"
        ]

        semantic_results = run_semantic_check(
            ok_fields, haiku_client, model=haiku_model,
        )

        # Apply semantic failures
        semantic_failures = 0
        for idx, result in semantic_results.items():
            if not result["pass"]:
                classified[idx]["status"] = "SEMANTIC_MISMATCH"
                classified[idx]["detail"] = result.get("reason", "does not match English")
                stats["ok"] -= 1
                semantic_failures += 1

        if semantic_failures:
            print(f"\n  Semantic failures: {semantic_failures}")
            # Show some examples
            shown = 0
            for idx, result in semantic_results.items():
                if not result["pass"] and shown < 10:
                    f = classified[idx]
                    rid = f["resource_id"].split("/")[-1]
                    en_preview = f["english"][:60]
                    ar_preview = (f["arabic"] or "")[:60]
                    print(f"    {f['resource_type']}/{rid} [{f['key']}]")
                    print(f"      EN: {en_preview}")
                    print(f"      AR: {ar_preview}")
                    print(f"      → {result['reason']}")
                    shown += 1
            if semantic_failures > 10:
                print(f"    ... and {semantic_failures - 10} more")
    elif skip_semantic:
        print("\n  (Semantic check skipped)")

    # Collect all problems
    problems = [f for f in classified if f["status"] not in ("OK", "SKIP")]

    print(f"\n{'=' * 60}")
    print(f"AUDIT SUMMARY")
    print("=" * 60)
    print(f"  Total translatable fields: {stats['total']}")
    print(f"  OK:                        {stats['ok']}")
    print(f"  Problems:                  {len(problems)}")
    if problems:
        # Breakdown by status
        by_status = {}
        for p in problems:
            by_status[p["status"]] = by_status.get(p["status"], 0) + 1
        for s, c in sorted(by_status.items()):
            print(f"    {s}: {c}")

    return classified, problems, stats


# ═════════════════════════════════════════════════════════════════════════════
# Phase 4-6: Fix
# ═════════════════════════════════════════════════════════════════════════════

# Statuses that only need HTML bloat stripping (don't re-translate)
_STRIP_ONLY = frozenset({"HTML_BLOAT"})

# Statuses that need full re-translation
_RETRANSLATE = frozenset({
    "MISSING", "IDENTICAL", "NOT_ARABIC", "MIXED_LANGUAGE",
    "CORRUPTED_JSON", "OUTDATED", "HAS_ENGLISH", "HAS_SPANISH",
    "SOURCE_SPANISH",  # Source is Spanish — translate ES→AR directly
    "SEMANTIC_MISMATCH",
})


def run_fix(client, engine, problems, locale=LOCALE, dry_run=False):
    """Fix all problems: strip HTML bloat and/or re-translate EN→AR.

    For HTML_BLOAT: strips bloat from existing Arabic, uploads.
    For everything else: translates English→Arabic via TranslationEngine, uploads.

    Returns (uploaded, errors, skipped).
    """
    if not problems:
        print("\nNo problems to fix!")
        return 0, 0, 0

    # Separate theme problems that are MISSING (likely blocked by Shopify's
    # ~3,400 translation key limit per locale per theme) from those that
    # already have a translation and can be updated.
    theme_missing = [p for p in problems
                     if p["resource_type"] == "ONLINE_STORE_THEME"
                     and p["status"] == "MISSING"]
    fixable = [p for p in problems
               if not (p["resource_type"] == "ONLINE_STORE_THEME"
                       and p["status"] == "MISSING")]

    bloat_only = [p for p in fixable if p["status"] in _STRIP_ONLY]
    retranslate = [p for p in fixable if p["status"] in _RETRANSLATE]

    print(f"\n{'=' * 60}")
    print(f"FIX PHASE" + (" (DRY RUN)" if dry_run else ""))
    print("=" * 60)
    if theme_missing:
        print(f"  Theme MISSING (skipped): {len(theme_missing)}")
        print(f"    → Shopify limits themes to ~3,400 translation keys per locale.")
        print(f"      Your theme has 4,485 translatable fields — over the cap.")
        print(f"      MISSING keys can't be registered; remove unused sections/")
        print(f"      templates to free up slots, or manage via locales/ar.json.")
    print(f"  HTML bloat strip only: {len(bloat_only)}")
    print(f"  Re-translate EN→AR:   {len(retranslate)}")

    if dry_run:
        print("\n  Planned changes:")
        for p in problems[:30]:
            rid = p["resource_id"].split("/")[-1]
            en_preview = p["english"][:50]
            print(f"  [{p['status']:20s}] {p['resource_type']}/{rid} [{p['key']}]")
            if p.get("detail"):
                print(f"    {p['detail']}")
            if p["status"] in _RETRANSLATE:
                print(f"    EN: {en_preview}")
        if len(problems) > 30:
            print(f"  ... and {len(problems) - 30} more")
        return 0, 0, 0

    uploaded = 0
    errors = 0
    skipped = 0

    # ── Fix HTML bloat (strip only, no re-translation) ──

    if bloat_only:
        print(f"\n  Stripping HTML bloat from {len(bloat_only)} fields...")
        by_resource = {}
        for p in bloat_only:
            by_resource.setdefault(p["resource_id"], []).append(p)

        gids = list(by_resource.keys())
        digest_map = fetch_translatable_resources(client, gids, locale)

        for gid, items in by_resource.items():
            dm = digest_map.get(gid)
            if not dm:
                skipped += len(items)
                continue

            translations_input = []
            for item in items:
                arabic = item["arabic"]
                if not arabic:
                    skipped += 1
                    continue
                cleaned = strip_html_bloat(arabic)
                if cleaned == arabic:
                    skipped += 1
                    continue

                field_info = dm["content"].get(item["key"])
                if not field_info:
                    skipped += 1
                    continue

                translations_input.append({
                    "locale": locale,
                    "key": item["key"],
                    "value": cleaned,
                    "translatableContentDigest": field_info["digest"],
                })

            if translations_input:
                u, e = upload_translations(client, gid, translations_input)
                uploaded += u
                errors += e

        print(f"  HTML bloat: uploaded={uploaded}, errors={errors}, skipped={skipped}")

    # ── Re-translate problems ──

    if retranslate:
        print(f"\n  Translating {len(retranslate)} fields EN→AR...")

        # Build fields for the translation engine
        fields_for_ai = []
        for p in retranslate:
            english = p["english"]
            if not english or not english.strip():
                skipped += 1
                continue
            field_id = f"{p['resource_type']}|{p['resource_id']}|{p['key']}"
            fields_for_ai.append({"id": field_id, "value": english})

        if not fields_for_ai:
            print("  No fields to translate (all have empty English source)")
            return uploaded, errors, skipped

        # Translate via engine (handles rich_text safely)
        t_map = engine.translate_fields(fields_for_ai)
        print(f"  Translated: {len(t_map)} / {len(fields_for_ai)} fields")

        # Post-process: replace English range names with Arabic equivalents
        for fid, value in list(t_map.items()):
            if value and not is_rich_text_json(value):
                t_map[fid] = replace_range_names_ar(value)

        # Group by resource for upload
        by_resource = {}
        for p in retranslate:
            field_id = f"{p['resource_type']}|{p['resource_id']}|{p['key']}"
            if field_id in t_map:
                by_resource.setdefault(p["resource_id"], []).append(
                    (p, t_map[field_id])
                )

        # Fetch fresh digests
        gids = list(by_resource.keys())
        if gids:
            print(f"  Fetching digests for {len(gids)} resources...")
            digest_map = fetch_translatable_resources(client, gids, locale)

            batch_uploaded = 0
            batch_errors = 0
            batch_skipped = 0

            for gid in gids:
                dm = digest_map.get(gid)
                if not dm:
                    items = by_resource.get(gid, [])
                    batch_skipped += len(items)
                    continue

                translations_input = []
                for problem, arabic_value in by_resource[gid]:
                    field_info = dm["content"].get(problem["key"])
                    if not field_info:
                        batch_skipped += 1
                        continue

                    # Validate JSON for rich_text fields
                    if is_rich_text_json(arabic_value):
                        try:
                            json.loads(arabic_value)
                        except json.JSONDecodeError:
                            print(f"    WARNING: Invalid JSON for "
                                  f"{gid} [{problem['key']}], skipping")
                            batch_errors += 1
                            continue

                    translations_input.append({
                        "locale": locale,
                        "key": problem["key"],
                        "value": arabic_value,
                        "translatableContentDigest": field_info["digest"],
                    })

                if translations_input:
                    u, e = upload_translations(client, gid, translations_input)
                    batch_uploaded += u
                    batch_errors += e

                time.sleep(0.3)

            uploaded += batch_uploaded
            errors += batch_errors
            skipped += batch_skipped
            print(f"  Re-translate: uploaded={batch_uploaded}, "
                  f"errors={batch_errors}, skipped={batch_skipped}")

    print(f"\n{'=' * 60}")
    print(f"  FIX TOTALS: uploaded={uploaded}, errors={errors}, skipped={skipped}")
    return uploaded, errors, skipped


# ═════════════════════════════════════════════════════════════════════════════
# Phase 7: Verify
# ═════════════════════════════════════════════════════════════════════════════

def run_verify(client, resource_types, haiku_client, haiku_model,
               skip_semantic=False):
    """Re-audit after fixes to confirm Arabic is clean.

    Runs the full audit pipeline again and compares before/after.
    """
    print(f"\n{'=' * 60}")
    print("VERIFY PHASE (re-auditing)")
    print("=" * 60)

    _, verify_problems, verify_stats = run_audit(
        client, resource_types, haiku_client, haiku_model,
        skip_semantic=skip_semantic,
    )
    return verify_problems, verify_stats


# ═════════════════════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Review & fix Arabic translations on Saudi Shopify store",
    )
    parser.add_argument(
        "--audit", action="store_true",
        help="Audit only — report issues without fixing",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show planned changes without applying",
    )
    parser.add_argument(
        "--type", default=None,
        help="Resource type filter, comma-separated "
             "(e.g. PRODUCT,COLLECTION,METAFIELD,METAOBJECT)",
    )
    parser.add_argument(
        "--skip-semantic", action="store_true",
        help="Skip AI semantic correspondence check (faster, less thorough)",
    )
    parser.add_argument(
        "--model", default="gpt-5-nano",
        help="OpenAI model for EN→AR translation (default: gpt-5-nano)",
    )
    parser.add_argument(
        "--audit-model", default="claude-haiku-4-5-20251001",
        help="Anthropic model for semantic checking (default: claude-haiku-4-5-20251001)",
    )
    parser.add_argument(
        "--reasoning", default="minimal",
        choices=["minimal", "low", "medium", "high"],
        help="Reasoning effort for translation model (default: minimal)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=80,
        help="Fields per translation batch (default: 80)",
    )
    parser.add_argument(
        "--prompt", default=None,
        help="Path to developer prompt file for translation",
    )
    parser.add_argument(
        "--no-verify", action="store_true",
        help="Skip re-audit verification after fixing",
    )
    parser.add_argument(
        "--save-report", metavar="FILE",
        help="Save audit report to JSON file",
    )
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ.get("SAUDI_SHOP_URL")
    access_token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not access_token:
        print("ERROR: Set SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN in .env")
        sys.exit(1)

    client = ShopifyClient(shop_url, access_token)
    haiku_client = anthropic.Anthropic()

    # Resource types
    resource_types = TRANSLATABLE_RESOURCE_TYPES
    if args.type:
        resource_types = [t.strip().upper() for t in args.type.split(",")]

    print("=" * 60)
    print("ARABIC TRANSLATION REVIEW — Saudi Store")
    print(f"  Audit model:       {args.audit_model}")
    print(f"  Translation model: {args.model}")
    print(f"  Semantic check:    {'ON' if not args.skip_semantic else 'OFF'}")
    print("=" * 60)

    # ── Phase 1-3: Audit ──
    classified, problems, stats = run_audit(
        client, resource_types, haiku_client, args.audit_model,
        skip_semantic=args.skip_semantic,
    )

    # Save report if requested
    if args.save_report:
        report = [{
            "resource_type": p["resource_type"],
            "resource_id": p["resource_id"],
            "key": p["key"],
            "status": p["status"],
            "detail": p.get("detail", ""),
            "english": p["english"][:200],
            "arabic": (p["arabic"] or "")[:200],
        } for p in problems]
        with open(args.save_report, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
        print(f"\nReport saved to {args.save_report}")

    if not problems:
        print("\nAll Arabic translations are clean!")
        return

    if args.audit:
        print(f"\nAudit complete. {len(problems)} problems found.")
        print("Run without --audit to fix them.")
        return

    # ── Phase 4-6: Fix ──

    # Initialize translation engine
    prompt_path = args.prompt
    if not prompt_path:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))))
        candidates = [
            os.path.join(project_root, "Arabic",
                         "tara_cached_developer_prompt.txt"),
            os.path.join(project_root, "developer_prompt.txt"),
        ]
        for c in candidates:
            if os.path.exists(c):
                prompt_path = c
                break

    developer_prompt = load_developer_prompt(
        prompt_path or "developer_prompt.txt",
    )
    engine = TranslationEngine(
        developer_prompt,
        model=args.model,
        reasoning_effort=args.reasoning,
        batch_size=args.batch_size,
    )

    uploaded, fix_errors, fix_skipped = run_fix(
        client, engine, problems,
        dry_run=args.dry_run,
    )

    if args.dry_run or args.no_verify:
        return

    # ── Phase 7: Verify ──

    if uploaded > 0:
        print("\nWaiting 2s for Shopify to propagate translations...")
        time.sleep(2)

        verify_problems, verify_stats = run_verify(
            client, resource_types, haiku_client, args.audit_model,
            skip_semantic=args.skip_semantic,
        )

        before = len(problems)
        after = len(verify_problems)
        delta = before - after

        print(f"\n{'=' * 60}")
        print("FINAL SUMMARY")
        print("=" * 60)
        print(f"  Before:    {before} problems")
        print(f"  Uploaded:  {uploaded} fixes")
        print(f"  After:     {after} problems")
        print(f"  Delta:     {delta:+d} ({'improved' if delta > 0 else 'unchanged'})")

        if after > 0:
            remaining = {}
            for p in verify_problems:
                remaining[p["status"]] = remaining.get(p["status"], 0) + 1
            print(f"\n  Remaining problems:")
            for s, c in sorted(remaining.items()):
                print(f"    {s}: {c}")
            print(f"\n  Re-run to fix remaining issues.")
        else:
            print(f"\n  All Arabic translations verified clean!")
        print("=" * 60)
    else:
        print("\nNo uploads made, skipping verification.")


if __name__ == "__main__":
    main()
