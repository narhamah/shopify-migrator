#!/usr/bin/env python3
"""Audit and clean up theme translation keys on the Saudi Shopify store.

Fetches all translatable content for ONLINE_STORE_THEME resources, categorizes
each key as useful or junk, and optionally removes unnecessary translations
to get under Shopify's ~3,400 key-per-locale limit.

The limit is on TOTAL translatable fields, not on registered translations.
Removing existing translations frees up "slots" so the Translations API
can accept new registrations.

Usage:
    python audit_theme_keys.py                  # Audit only — show breakdown
    python audit_theme_keys.py --remove-junk    # Remove unnecessary keys
    python audit_theme_keys.py --dry-run        # Show what would be removed
    python audit_theme_keys.py --dump keys.json # Dump all keys to JSON for review
    python audit_theme_keys.py --translate       # Translate missing Arabic theme keys
    python audit_theme_keys.py --translate --dry-run  # Preview what would be translated
"""

import argparse
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict

from dotenv import load_dotenv

from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.core.graphql_queries import TRANSLATABLE_RESOURCES_QUERY

LOCALE = "ar"

# ─────────────────────────────────────────────────────────────────────────────
# GraphQL mutation to remove translations
# ─────────────────────────────────────────────────────────────────────────────

REMOVE_TRANSLATIONS_MUTATION = """
mutation translationsRemove($resourceId: ID!, $translationKeys: [String!]!, $locales: [String!]!) {
  translationsRemove(resourceId: $resourceId, translationKeys: $translationKeys, locales: $locales) {
    userErrors {
      message
      field
    }
  }
}
"""

# ─────────────────────────────────────────────────────────────────────────────
# Key classification
# ─────────────────────────────────────────────────────────────────────────────


def classify_key(key, value):
    """Classify a theme translation key as 'useful' or 'junk'.

    Returns (category, reason).

    Classification rules are based on actual TARA theme data analysis.
    We aggressively classify non-text content as junk to get under the
    3,400 key limit while keeping every piece of visible text.
    """
    val = (value or "").strip()

    # ── SYSTEM: Shopify-managed strings (auto-translated by Shopify) ────
    # Keys in these namespaces are Shopify platform UI — checkout, customer
    # accounts, etc. — which Shopify auto-translates when Arabic is enabled.
    # Registering custom translations for these wastes key slots.
    _SHOPIFY_PLATFORM_PREFIXES = (
        "shopify.",              # Shopify core (checkout, notices, errors, etc.)
        "customer_accounts.",    # Customer account pages
    )
    if any(key.startswith(p) for p in _SHOPIFY_PLATFORM_PREFIXES):
        return "system", "Shopify platform string (auto-translated)"

    # ── THEME LOCALE: Standard Dawn/theme locale keys ─────────────────
    # These come from the theme's locale JSON files (e.g. ar.json).
    # If the theme ships with Arabic translations for these, removing
    # registered translations is safe (falls back to theme file).
    # If the theme does NOT have Arabic locale files, these must be kept.
    _THEME_LOCALE_PREFIXES = (
        "accessibility.",       # Accessibility labels
        "actions.",             # Button/action labels
        "blocks.",             # Block UI strings
        "blogs.",              # Blog templates
        "contact.",            # Contact form
        "content.",            # Content labels (cart, search, etc.)
        "fields.",             # Form fields
        "gift_cards.",         # Gift card pages
        "placeholders.",       # Placeholder text
        "products.",           # Product template strings
    )
    if any(key.startswith(p) for p in _THEME_LOCALE_PREFIXES):
        return "system", "theme locale string (in ar.json)"

    # ── CUSTOM THEME: Custom section/namespace keys ───────────────────
    # Keys from custom theme sections (tara.*, sections.*, quiz_results.*)
    # are NOT auto-translated by Shopify and NOT in standard theme locale
    # files. These need registered translations to show in Arabic.
    # Classify based on value content (useful vs junk) below.
    _CUSTOM_THEME_PREFIXES = (
        "tara.",               # TARA custom theme namespace
        "sections.",           # Custom section locale keys (plural!)
        "quiz_results.",       # Quiz results page
    )
    # Note: "section." (singular) = merchant theme-editor content,
    #       "sections." (plural) = custom section locale keys.
    # Both are merchant/theme content, not Shopify platform strings.

    # ── MERCHANT CONTENT: section.* (theme editor) + custom theme keys ─
    # Everything below here is merchant-entered or custom theme content.
    # Classify based on the VALUE to determine useful vs junk.

    # ── JUNK: Empty or whitespace-only ──────────────────────────────────
    if not val:
        return "junk", "empty value"

    # ── JUNK: File/image references ─────────────────────────────────────
    if val.startswith("shopify://shop_images/") or val.startswith("gid://"):
        return "junk", "image/file reference"
    if re.match(r"^shopify://", val):
        # shopify:// links (collections, products) — not visible text
        return "junk", "Shopify internal link"

    # ── JUNK: Pure Liquid templates (no static text to translate) ───────
    # Strip HTML tags and check if only Liquid remains
    text_only = re.sub(r"<[^>]+>", "", val).strip()
    if text_only and re.match(r"^(\{\{[^}]+\}\}\s*)+$", text_only):
        return "junk", "pure Liquid template"

    # ── JUNK: CSS dimensions ────────────────────────────────────────────
    if re.match(r"^\d+\s*(px|em|rem|vh|vw|%|fr|pt|cm|mm)$", val, re.I):
        return "junk", "CSS dimension"

    # ── JUNK: Color values ──────────────────────────────────────────────
    if re.match(r"^#[0-9a-fA-F]{3,8}$", val):
        return "junk", "color value"
    if re.match(r"^rgba?\(\d", val):
        return "junk", "color value"

    # ── JUNK: Pure numeric values ───────────────────────────────────────
    if re.match(r"^[\d.,]+$", val):
        return "junk", "pure numeric"

    # ── JUNK: Boolean values ────────────────────────────────────────────
    if val.lower() in ("true", "false"):
        return "junk", "boolean"

    # ── JUNK: URLs (not visible text) ───────────────────────────────────
    if re.match(r"^https?://", val):
        return "junk", "URL"
    if re.match(r"^/(?:collections|products|pages|blogs|cart)/", val):
        return "junk", "internal path"
    if re.match(r"^/pages/", val):
        return "junk", "internal path"

    # ── JUNK: JSON blobs ────────────────────────────────────────────────
    if val.startswith("{") and val.endswith("}"):
        return "junk", "JSON blob"
    if val.startswith("[") and val.endswith("]"):
        return "junk", "JSON array"

    # ── JUNK: Technical identifiers (API keys, form IDs, UUIDs, etc.) ──
    if re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", val, re.I):
        return "junk", "UUID"
    # API keys (mixed alphanumeric, 20+ chars, no natural words)
    if re.match(r"^[A-Za-z0-9_-]{20,}$", val) and not re.search(r"[a-z]{4,}", val):
        return "junk", "API key / technical ID"
    # Region codes like "na1", "eu1"
    if re.match(r"^[a-z]{2}\d$", val):
        return "junk", "region code"
    # Anchor/form-related identifiers
    if "anchor_id:" in key or "form_id:" in key or "portal_id:" in key:
        return "junk", "form/anchor ID"
    # Google Maps API keys and similar
    if ".api_key:" in key or ".worker_url:" in key:
        return "junk", "API key setting"
    # Map coordinates
    if "default_lat:" in key or "default_lng:" in key:
        return "junk", "map coordinate"

    # ── JUNK: Image file extensions in value ────────────────────────────
    if re.match(r".*\.(png|jpg|jpeg|svg|webp|gif|ico|mp4|webm)$", val, re.I):
        return "junk", "media filename"

    # ── JUNK: HTML-wrapped numbers/symbols (e.g., <h2>01</h2>, <p>200+</p>)
    text_content = re.sub(r"<[^>]+>", "", val).strip()
    if text_content and re.match(r"^[\d.,+%°·×\-–—]+$", text_content):
        return "junk", "numeric/symbol in HTML"

    # ── USEFUL: Has actual translatable text ────────────────────────────
    # Contains at least 2 consecutive letters (English or Arabic)
    if re.search(r"[a-zA-Z]{2,}", val):
        return "useful", "translatable text"
    if re.search(r"[\u0600-\u06FF]{2,}", val):
        return "useful", "Arabic text"

    # ── JUNK: Numbers with symbols (e.g., "200+", "100%", "0") ─────────
    if re.match(r"^[\d.,+%]+$", val):
        return "junk", "numeric display value"

    # ── JUNK: Single characters or symbols ──────────────────────────────
    if len(val) <= 2:
        return "junk", "very short value"

    return "review", "unclear"


def fetch_theme_keys(client, locale=LOCALE):
    """Fetch all translatable content for ONLINE_STORE_THEME resources."""
    query = TRANSLATABLE_RESOURCES_QUERY.replace("%LOCALE%", locale)
    all_resources = []
    cursor = None
    page = 0

    print("Fetching ONLINE_STORE_THEME translatable resources...")
    while True:
        try:
            data = client._graphql(query, {
                "resourceType": "ONLINE_STORE_THEME",
                "first": 50,
                "after": cursor,
            })
        except Exception as e:
            print(f"  ERROR: {e}")
            break

        container = data.get("translatableResources", {})
        edges = container.get("edges", [])
        page_info = container.get("pageInfo", {})

        for edge in edges:
            node = edge["node"]
            rid = node["resourceId"]
            translations = {t["key"]: t for t in node.get("translations", [])}

            for field in node.get("translatableContent", []):
                key = field["key"]
                value = field.get("value") or ""
                trans = translations.get(key)
                arabic = trans["value"] if trans else None

                all_resources.append({
                    "resource_id": rid,
                    "key": key,
                    "english": value,
                    "arabic": arabic,
                    "has_translation": trans is not None,
                    "digest": field.get("digest", ""),
                })

        page += 1
        if page % 5 == 0:
            print(f"  ... {len(all_resources)} fields so far (page {page})")

        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        time.sleep(0.3)

    print(f"  Total theme fields: {len(all_resources)}")
    return all_resources


def analyze_keys(fields):
    """Categorize every field and return analysis."""
    categories = {"useful": [], "system": [], "junk": [], "review": []}
    reason_counts = Counter()

    for f in fields:
        cat, reason = classify_key(f["key"], f["english"])
        f["category"] = cat
        f["reason"] = reason
        categories[cat].append(f)
        reason_counts[f"{cat}: {reason}"] += 1

    return categories, reason_counts


def remove_translations(client, fields_to_remove, dry_run=False, locale=LOCALE):
    """Remove Arabic translations for the given fields.

    Groups by resource_id and sends batched translationsRemove mutations.
    """
    by_resource = defaultdict(list)
    for f in fields_to_remove:
        if f["has_translation"]:
            by_resource[f["resource_id"]].append(f["key"])

    total_removed = 0
    total_errors = 0

    for rid, keys in by_resource.items():
        if dry_run:
            print(f"  Would remove {len(keys)} translations from {rid}")
            total_removed += len(keys)
            continue

        # Batch in groups of 50
        for i in range(0, len(keys), 50):
            batch = keys[i:i + 50]
            try:
                result = client._graphql(REMOVE_TRANSLATIONS_MUTATION, {
                    "resourceId": rid,
                    "translationKeys": batch,
                    "locales": [locale],
                })
                user_errors = result.get("translationsRemove", {}).get("userErrors", [])
                if user_errors:
                    for ue in user_errors:
                        print(f"    ERROR: {ue['message']}")
                    total_errors += len(batch)
                else:
                    total_removed += len(batch)
                    print(f"  Removed {len(batch)} keys from {rid}")
            except Exception as e:
                print(f"    ERROR removing from {rid}: {e}")
                total_errors += len(batch)
            time.sleep(0.5)

    return total_removed, total_errors


def print_analysis(categories, reason_counts, fields):
    """Print detailed analysis of theme keys."""
    total = len(fields)
    with_trans = sum(1 for f in fields if f["has_translation"])

    print(f"\n{'=' * 70}")
    print(f"THEME TRANSLATION KEY AUDIT")
    print(f"{'=' * 70}")
    print(f"  Total translatable fields:  {total}")
    print(f"  With Arabic translation:    {with_trans}")
    print(f"  Without translation:        {total - with_trans}")
    print(f"  Shopify limit:              ~3,400")
    print(f"  Over limit by:              ~{max(0, total - 3400)}")

    print(f"\n{'─' * 70}")
    print(f"CLASSIFICATION")
    print(f"{'─' * 70}")
    for cat in ["useful", "system", "junk", "review"]:
        items = categories[cat]
        with_t = sum(1 for f in items if f["has_translation"])
        print(f"  {cat.upper():>8}: {len(items):>5} fields "
              f"({with_t} with existing Arabic translations)")

    removable = len(categories["system"]) + len(categories["junk"])
    remaining = len(categories["useful"]) + len(categories["review"])
    print(f"\n  Removable (system + junk): {removable}")
    print(f"  Remaining after cleanup:  {remaining}")
    if remaining <= 3400:
        print(f"  --> UNDER the 3,400 limit! Removal should unblock translations.")
    else:
        over = remaining - 3400
        print(f"  --> Still {over} over the limit.")

    print(f"\n{'─' * 70}")
    print(f"BREAKDOWN BY REASON")
    print(f"{'─' * 70}")
    for reason, count in reason_counts.most_common():
        print(f"  {count:>5}  {reason}")

    # Show system keys breakdown
    system = categories["system"]
    if system:
        sys_with_t = sum(1 for f in system if f["has_translation"])
        # Group by key prefix (first 2 dotted segments)
        by_prefix = defaultdict(int)
        for f in system:
            parts = f["key"].split(".")
            prefix = ".".join(parts[:2]) if len(parts) >= 2 else parts[0]
            by_prefix[prefix] += 1
        print(f"\n{'─' * 70}")
        print(f"SYSTEM KEYS ({len(system)} total, {sys_with_t} with translations)")
        print(f"  These are auto-translated by Shopify — safe to remove.")
        print(f"{'─' * 70}")
        for prefix, count in sorted(by_prefix.items(), key=lambda x: -x[1]):
            print(f"  {count:>5}  {prefix}.*")

    # Show junk keys that HAVE translations (removal candidates)
    junk_with_trans = [f for f in categories["junk"] if f["has_translation"]]
    if junk_with_trans:
        print(f"\n{'─' * 70}")
        print(f"JUNK WITH EXISTING TRANSLATIONS ({len(junk_with_trans)} — removal targets)")
        print(f"{'─' * 70}")
        by_reason = defaultdict(list)
        for f in junk_with_trans:
            by_reason[f["reason"]].append(f)
        for reason, items in sorted(by_reason.items(), key=lambda x: -len(x[1])):
            print(f"\n  [{reason}] — {len(items)} keys")
            for f in items[:3]:
                val_preview = (f["english"] or "")[:60]
                ar_preview = (f["arabic"] or "")[:40]
                print(f"    key: {f['key'][:70]}")
                print(f"    en:  {val_preview!r}")
                if ar_preview:
                    print(f"    ar:  {ar_preview!r}")
            if len(items) > 3:
                print(f"    ... and {len(items) - 3} more")

    # Show sample of useful keys
    useful = categories["useful"]
    if useful:
        print(f"\n{'─' * 70}")
        print(f"SAMPLE USEFUL KEYS ({len(useful)} total)")
        print(f"{'─' * 70}")
        for f in useful[:10]:
            val_preview = (f["english"] or "")[:60]
            status = "has AR" if f["has_translation"] else "MISSING"
            print(f"  [{status:>7}] {f['key'][:55]}")
            print(f"           {val_preview!r}")

    # Show review keys
    review = categories["review"]
    if review:
        print(f"\n{'─' * 70}")
        print(f"REVIEW ({len(review)} — need manual decision)")
        print(f"{'─' * 70}")
        for f in review:
            val_preview = (f["english"] or "")[:60]
            status = "has AR" if f["has_translation"] else "MISSING"
            print(f"  [{status:>7}] {f['key'][:55]}")
            print(f"           {val_preview!r}")


# ─────────────────────────────────────────────────────────────────────────────
# Duplicate and section analysis
# ─────────────────────────────────────────────────────────────────────────────


def analyze_duplicates(fields):
    """Analyze duplicate English values across theme keys.

    Groups all fields by their English value and shows how many keys share
    the same string. This reveals wasted translation slots — e.g. "Botanical"
    appearing under 13 different section.* keys.

    Returns dict of {english_value: [fields]}.
    """
    from collections import defaultdict

    # Only look at section.* keys (merchant content, not system strings)
    section_fields = [f for f in fields if f["key"].startswith("section.")]

    # Group by normalized English value
    by_value = defaultdict(list)
    for f in section_fields:
        val = (f.get("english") or "").strip()
        if not val:
            continue
        by_value[val].append(f)

    # Sort by duplicate count (highest first)
    duplicates = {v: flds for v, flds in by_value.items() if len(flds) > 1}
    sorted_dupes = sorted(duplicates.items(), key=lambda x: -len(x[1]))

    # Stats
    total_section = len(section_fields)
    unique_values = len(by_value)
    total_duped_keys = sum(len(flds) for _, flds in sorted_dupes)
    wasted_keys = sum(len(flds) - 1 for _, flds in sorted_dupes)

    print(f"\n{'=' * 70}")
    print(f"DUPLICATE ANALYSIS (section.* keys only)")
    print(f"{'=' * 70}")
    print(f"  Total section keys:         {total_section}")
    print(f"  Unique English values:      {unique_values}")
    print(f"  Values appearing 2+ times:  {len(sorted_dupes)}")
    print(f"  Keys consumed by duplicates:{total_duped_keys}")
    print(f"  Wasted slots (excess):      {wasted_keys}")
    print(f"  If deduplicated, section keys would drop from "
          f"{total_section} → {total_section - wasted_keys}")

    if sorted_dupes:
        print(f"\n{'─' * 70}")
        print(f"TOP DUPLICATED STRINGS")
        print(f"{'─' * 70}")
        for val, flds in sorted_dupes[:25]:
            val_preview = val[:55] if len(val) <= 55 else val[:52] + "..."
            has_ar = sum(1 for f in flds if f["has_translation"])
            print(f"  {len(flds):>3}x  {val_preview!r}")
            if has_ar:
                print(f"        ({has_ar} with Arabic translation)")
            # Show which templates they come from
            templates = set()
            for f in flds:
                # Extract template name from key like
                # section.sections/page.json.xxx... or section.template--xxx...
                parts = f["key"].split(".")
                if len(parts) >= 2:
                    templates.add(parts[1][:40])
            if templates:
                print(f"        templates: {', '.join(sorted(templates)[:5])}")
        if len(sorted_dupes) > 25:
            print(f"\n  ... and {len(sorted_dupes) - 25} more duplicated values")

    return sorted_dupes


def analyze_sections(fields):
    """Break down section.* keys by template/section to find the biggest key hogs.

    Helps identify which templates should have their content moved to
    metaobjects/pages to reduce the theme's translatable field count.
    """
    section_fields = [f for f in fields if f["key"].startswith("section.")]
    if not section_fields:
        return

    # Group by template (second segment of the key)
    by_template = defaultdict(list)
    for f in section_fields:
        parts = f["key"].split(".")
        template = parts[1] if len(parts) >= 2 else "unknown"
        by_template[template].append(f)

    sorted_templates = sorted(by_template.items(), key=lambda x: -len(x[1]))

    print(f"\n{'=' * 70}")
    print(f"SECTION KEYS BY TEMPLATE")
    print(f"{'=' * 70}")
    print(f"  Total section.* keys: {len(section_fields)}")
    print(f"  Across {len(by_template)} templates")
    print(f"\n  {'TEMPLATE':<50} {'KEYS':>5}  {'W/AR':>5}  {'JUNK':>5}")
    print(f"  {'─' * 50} {'─' * 5}  {'─' * 5}  {'─' * 5}")

    for template, flds in sorted_templates[:30]:
        with_ar = sum(1 for f in flds if f["has_translation"])
        junk = sum(1 for f in flds
                   if classify_key(f["key"], f["english"])[0] == "junk")
        name = template[:50]
        print(f"  {name:<50} {len(flds):>5}  {with_ar:>5}  {junk:>5}")

    if len(sorted_templates) > 30:
        remaining = sum(len(flds) for _, flds in sorted_templates[30:])
        print(f"  ... {len(sorted_templates) - 30} more templates "
              f"({remaining} keys)")

    # Identify templates with the most "useful" (translatable text) keys
    print(f"\n{'─' * 70}")
    print(f"TEMPLATES WITH MOST TRANSLATABLE TEXT (candidates to move to metaobjects)")
    print(f"{'─' * 70}")
    template_useful = []
    for template, flds in sorted_templates:
        useful = [f for f in flds
                  if classify_key(f["key"], f["english"])[0] == "useful"]
        if useful:
            template_useful.append((template, useful))

    template_useful.sort(key=lambda x: -len(x[1]))
    for template, useful in template_useful[:15]:
        name = template[:50]
        print(f"\n  {name} — {len(useful)} translatable keys")
        # Show sample values
        seen = set()
        for f in useful[:5]:
            val = (f["english"] or "")[:55]
            if val not in seen:
                print(f"    EN: {val!r}")
                seen.add(val)
        if len(useful) > 5:
            print(f"    ... and {len(useful) - 5} more")


def dedup_translations(client, fields, dry_run=False, locale=LOCALE):
    """Remove duplicate Arabic translations, keeping only one per unique value.

    When the same English string has Arabic translations registered under
    multiple section.* keys, this removes translations from all but one key.
    Shopify will fall back to the primary language for unregistered keys,
    but since these are duplicates of text that appears identically in multiple
    templates, the remaining registered translation still covers the value.

    Note: This frees registered translation slots but does NOT reduce the total
    field count. To reduce total fields, move content out of theme sections.

    Returns (removed, errors).
    """
    section_fields = [f for f in fields
                      if f["key"].startswith("section.")
                      and f["has_translation"]
                      and (f.get("english") or "").strip()]

    # Group by English value
    by_value = defaultdict(list)
    for f in section_fields:
        val = f["english"].strip()
        by_value[val].append(f)

    # For each group with 2+ translated copies, keep one, mark rest for removal
    to_remove = []
    for val, flds in by_value.items():
        translated = [f for f in flds if f["has_translation"]]
        if len(translated) > 1:
            # Keep the first one, remove the rest
            for f in translated[1:]:
                to_remove.append(f)

    if not to_remove:
        print("\nNo duplicate translations to remove.")
        return 0, 0

    print(f"\n{'=' * 70}")
    print(f"DEDUP TRANSLATIONS" + (" (DRY RUN)" if dry_run else ""))
    print(f"{'=' * 70}")
    print(f"  Duplicate Arabic translations: {len(to_remove)}")
    print(f"  (keeping 1 per unique English string, removing extras)")

    if dry_run:
        # Show top examples
        examples = defaultdict(int)
        for f in to_remove:
            val = (f["english"] or "")[:50]
            examples[val] += 1
        print(f"\n  Sample strings being deduplicated:")
        for val, count in sorted(examples.items(), key=lambda x: -x[1])[:15]:
            print(f"    {count + 1:>3}x → keep 1, remove {count}: {val!r}")
        print(f"\n  (Dry run — no changes made)")
        return 0, 0

    removed, errors = remove_translations(client, to_remove, dry_run=False,
                                          locale=locale)
    print(f"\n  Removed:  {removed}")
    if errors:
        print(f"  Errors:   {errors}")
    print(f"  Freed {removed} translation slots")

    return removed, errors



def clean_locale_file(client, dry_run=False):
    """Fetch ar.json from the active theme, remove junk entries, and re-upload.

    The ar.json locale file contains Arabic translations for theme locale keys
    (accessibility.*, actions.*, content.*, tara.*, sections.*, etc.).
    Unlike the Translations API, this file has no hard key limit, but bloated
    locale files slow down the theme and can cause issues.

    This function:
    1. Fetches locales/ar.json from the active theme
    2. Walks every key-value pair
    3. Removes entries where the value is junk (identical to English, empty,
       pure Liquid, URLs, CSS, etc.)
    4. Re-uploads the cleaned file
    """
    theme_id = client.get_main_theme_id()
    if not theme_id:
        print("ERROR: No active theme found.")
        return

    # Fetch ar.json
    print(f"\n{'=' * 70}")
    print(f"CLEAN LOCALE FILE (ar.json)" + (" (DRY RUN)" if dry_run else ""))
    print(f"{'=' * 70}")

    try:
        asset = client.get_asset(theme_id, "locales/ar.json")
    except Exception as e:
        print(f"  ERROR fetching locales/ar.json: {e}")
        return

    raw = asset.get("value", "{}")
    locale_data = json.loads(raw)

    # Also fetch en.default.json for comparison
    try:
        en_asset = client.get_asset(theme_id, "locales/en.default.json")
        en_data = json.loads(en_asset.get("value", "{}"))
    except Exception:
        en_data = {}

    # Flatten both locale dicts to dotted key paths
    def flatten(d, prefix=""):
        items = {}
        for k, v in d.items():
            full_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                items.update(flatten(v, full_key))
            else:
                items[full_key] = v
        return items

    ar_flat = flatten(locale_data)
    en_flat = flatten(en_data)

    print(f"  Total ar.json keys: {len(ar_flat)}")
    print(f"  Total en.default.json keys: {len(en_flat)}")

    # Classify each ar.json entry
    junk_keys = []
    identical_keys = []
    useful_keys = []

    for key, ar_val in ar_flat.items():
        ar_str = str(ar_val).strip() if ar_val is not None else ""
        en_str = str(en_flat.get(key, "")).strip()

        # Check if the Arabic value is identical to English (not translated)
        if ar_str and en_str and ar_str == en_str:
            # Some values SHOULD be identical (brand names, numbers, Liquid)
            cat, reason = classify_key(key, en_str)
            if cat == "junk":
                junk_keys.append((key, ar_str, reason))
            elif not re.search(r"[\u0600-\u06FF]", ar_str):
                # No Arabic characters — likely untranslated
                # But skip if it's a brand name, number, or technical value
                if re.search(r"[a-zA-Z]{3,}", ar_str) and cat != "useful":
                    identical_keys.append((key, ar_str, "identical to English"))
            else:
                useful_keys.append(key)
            continue

        # Check if the value itself is junk
        cat, reason = classify_key(key, ar_str)
        if cat == "junk":
            junk_keys.append((key, ar_str, reason))
        else:
            useful_keys.append(key)

    print(f"\n  Useful entries:       {len(useful_keys)}")
    print(f"  Junk entries:         {len(junk_keys)}")
    print(f"  Identical to English: {len(identical_keys)}")

    # Show junk breakdown
    if junk_keys:
        by_reason = defaultdict(list)
        for key, val, reason in junk_keys:
            by_reason[reason].append((key, val))
        print(f"\n{'─' * 70}")
        print(f"JUNK ENTRIES ({len(junk_keys)} — will be removed)")
        print(f"{'─' * 70}")
        for reason, items in sorted(by_reason.items(), key=lambda x: -len(x[1])):
            print(f"\n  [{reason}] — {len(items)} keys")
            for key, val in items[:3]:
                print(f"    {key}")
                print(f"      ar: {str(val)[:60]!r}")
            if len(items) > 3:
                print(f"    ... and {len(items) - 3} more")

    # Show identical-to-English
    if identical_keys:
        print(f"\n{'─' * 70}")
        print(f"IDENTICAL TO ENGLISH ({len(identical_keys)} — will be removed)")
        print(f"{'─' * 70}")
        for key, val, _ in identical_keys[:15]:
            print(f"  {key}")
            print(f"    = {str(val)[:60]!r}")
        if len(identical_keys) > 15:
            print(f"  ... and {len(identical_keys) - 15} more")

    to_remove_keys = set(k for k, _, _ in junk_keys + identical_keys)
    if not to_remove_keys:
        print("\n  ar.json is clean — no junk to remove.")
        return

    print(f"\n  Total keys to remove: {len(to_remove_keys)}")
    print(f"  Keys remaining after: {len(ar_flat) - len(to_remove_keys)}")

    if dry_run:
        print(f"\n  (Dry run — no changes made)")
        return

    # Rebuild locale dict without junk keys
    def remove_keys_from_nested(d, keys_to_remove, prefix=""):
        """Remove dotted keys from a nested dict, pruning empty parents."""
        result = {}
        for k, v in d.items():
            full_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                cleaned = remove_keys_from_nested(v, keys_to_remove, full_key)
                if cleaned:  # Only keep non-empty dicts
                    result[k] = cleaned
            else:
                if full_key not in keys_to_remove:
                    result[k] = v
        return result

    cleaned = remove_keys_from_nested(locale_data, to_remove_keys)
    cleaned_flat = flatten(cleaned)

    print(f"\n  Original keys: {len(ar_flat)}")
    print(f"  Cleaned keys:  {len(cleaned_flat)}")
    print(f"  Removed:       {len(ar_flat) - len(cleaned_flat)}")

    # Upload cleaned ar.json
    cleaned_json = json.dumps(cleaned, indent=2, ensure_ascii=False)
    try:
        client.put_asset(theme_id, "locales/ar.json", cleaned_json)
        print(f"\n  Uploaded cleaned ar.json to theme {theme_id}")
    except Exception as e:
        print(f"\n  ERROR uploading cleaned ar.json: {e}")
        # Save locally as backup
        backup_path = os.path.join("data", "ar_cleaned.json")
        with open(backup_path, "w", encoding="utf-8") as f:
            f.write(cleaned_json)
        print(f"  Saved cleaned version to {backup_path}")


def translate_theme_keys(client, fields, model="gpt-5-nano", dry_run=False,
                         locale=LOCALE):
    """Translate all theme keys that have English text but no Arabic translation.

    Groups fields by resource_id for efficient digest fetching and upload.
    Uses AI to translate short UI strings (buttons, labels, headings).

    Returns (translated, uploaded, errors).
    """
    from tara_migrate.core.graphql_queries import REGISTER_TRANSLATIONS_MUTATION
    from tara_migrate.translation.engine import TranslationEngine

    # Find fields that need translation: have English text, no Arabic
    to_translate = []
    for f in fields:
        english = (f.get("english") or "").strip()
        arabic = (f.get("arabic") or "").strip()
        if not english:
            continue
        # Skip if already has Arabic translation
        if f["has_translation"] and arabic:
            continue
        # Skip junk/non-translatable content
        cat = f.get("category", "")
        if cat == "junk":
            continue
        # Skip pure Liquid/HTML with no translatable text
        text_only = re.sub(r"<[^>]+>", "", english).strip()
        text_only = re.sub(r"\{\{[^}]*\}\}", "", text_only).strip()
        text_only = re.sub(r"\{%[^%]*%\}", "", text_only).strip()
        if not text_only or not re.search(r"[a-zA-Z]{2,}", text_only):
            continue
        to_translate.append(f)

    if not to_translate:
        print("\nAll translatable theme keys already have Arabic translations!")
        return 0, 0, 0

    print(f"\n{'=' * 70}")
    print(f"TRANSLATE THEME KEYS" + (" (DRY RUN)" if dry_run else ""))
    print(f"{'=' * 70}")
    print(f"  Keys needing Arabic translation: {len(to_translate)}")

    # Group by key prefix for context
    by_prefix = Counter()
    for f in to_translate:
        parts = f["key"].split(".")
        prefix = parts[0] if parts else "unknown"
        by_prefix[prefix] += 1
    for prefix, count in by_prefix.most_common(15):
        print(f"    {prefix}: {count}")

    if dry_run:
        print(f"\n  Sample keys to translate:")
        for f in to_translate[:20]:
            en = f["english"][:60]
            print(f"    [{f['key'][:50]}]")
            print(f"      EN: {en}")
        if len(to_translate) > 20:
            print(f"    ... and {len(to_translate) - 20} more")
        return 0, 0, 0

    # Build translation engine with a prompt suited for UI strings
    prompt = (
        "You are translating Shopify theme UI strings from English to Arabic "
        "for a luxury scalp-care brand called TARA.\n"
        "Rules:\n"
        "- Use Modern Standard Arabic suitable for a Gulf audience (Saudi Arabia)\n"
        "- Keep the TARA brand name unchanged\n"
        "- Keep Liquid template tags ({{ }}, {% %}) unchanged\n"
        "- Keep HTML tags unchanged — only translate the text content\n"
        "- Keep placeholders like {{ count }} unchanged\n"
        "- For short UI labels (1-3 words), provide a natural Arabic equivalent\n"
        "- Arabic text should read right-to-left naturally\n"
        "- Use consistent terminology throughout\n"
    )

    engine = TranslationEngine(
        prompt,
        model=model,
        reasoning_effort="minimal",
        batch_size=60,
    )

    # Translate in batches
    batch_fields = []
    for i, f in enumerate(to_translate):
        field_id = f"{f['resource_id']}|{f['key']}"
        batch_fields.append({"id": field_id, "value": f["english"]})

    print(f"\n  Translating {len(batch_fields)} fields via {model}...")
    t_map = engine.translate_fields(batch_fields)
    print(f"  Got {len(t_map)} / {len(batch_fields)} translations")

    if not t_map:
        print("  ERROR: No translations returned!")
        return 0, 0, 0

    # Group by resource_id for upload
    by_resource = defaultdict(list)
    for f in to_translate:
        field_id = f"{f['resource_id']}|{f['key']}"
        arabic = t_map.get(field_id)
        if arabic:
            by_resource[f["resource_id"]].append({
                "key": f["key"],
                "arabic": arabic,
                "digest": f["digest"],
            })

    # Upload translations
    print(f"\n  Uploading to {len(by_resource)} theme resources...")
    total_uploaded = 0
    total_errors = 0
    hit_limit = False

    for rid, items in by_resource.items():
        if hit_limit:
            break

        # Upload in batches of 10 per resource
        for i in range(0, len(items), 10):
            batch = items[i:i + 10]
            translations_input = []
            for item in batch:
                translations_input.append({
                    "locale": locale,
                    "key": item["key"],
                    "value": item["arabic"],
                    "translatableContentDigest": item["digest"],
                })

            try:
                result = client._graphql(REGISTER_TRANSLATIONS_MUTATION, {
                    "resourceId": rid,
                    "translations": translations_input,
                })
                user_errors = result.get("translationsRegister", {}).get(
                    "userErrors", [])
                if user_errors:
                    for ue in user_errors:
                        msg = ue["message"]
                        print(f"    ERROR: {msg}")
                        if "Too many translation keys" in msg:
                            hit_limit = True
                    total_errors += len(batch)
                else:
                    total_uploaded += len(batch)
            except Exception as e:
                print(f"    ERROR uploading to {rid}: {e}")
                total_errors += len(batch)

            time.sleep(0.3)

        if not hit_limit and total_uploaded % 100 < 10:
            print(f"    ... uploaded {total_uploaded} so far")

    print(f"\n  RESULTS:")
    print(f"    Translated: {len(t_map)}")
    print(f"    Uploaded:   {total_uploaded}")
    if total_errors:
        print(f"    Errors:     {total_errors}")
    if hit_limit:
        print(f"    WARNING: Hit Shopify's ~3,400 key limit!")
        print(f"    Run --remove-junk first to free up slots, then --translate again.")

    return len(t_map), total_uploaded, total_errors


def main():
    parser = argparse.ArgumentParser(description="Audit theme translation keys")
    parser.add_argument("--remove-junk", action="store_true",
                        help="Remove ALL junk + system translations (keep only useful)")
    parser.add_argument("--translate", action="store_true",
                        help="Translate missing Arabic theme keys via AI")
    parser.add_argument("--model", default="gpt-5-nano",
                        help="OpenAI model for translation (default: gpt-5-nano)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be removed/translated without doing it")
    parser.add_argument("--dump", metavar="FILE",
                        help="Dump all keys to JSON for manual review")
    parser.add_argument("--analyze-duplicates", action="store_true",
                        help="Show duplicate English values across section keys")
    parser.add_argument("--analyze-sections", action="store_true",
                        help="Show key count per template/section (find key hogs)")
    parser.add_argument("--dedup-translations", action="store_true",
                        help="Remove duplicate Arabic translations (keep 1 per string)")
    parser.add_argument("--full-analysis", action="store_true",
                        help="Run all analyses: audit + duplicates + sections")
    parser.add_argument("--clean-locale", action="store_true",
                        help="Fetch ar.json from theme, remove junk entries, re-upload")
    args = parser.parse_args()

    load_dotenv()
    client = ShopifyClient(
        os.environ["SAUDI_SHOP_URL"],
        os.environ["SAUDI_ACCESS_TOKEN"],
    )

    # Clean ar.json locale file
    if args.clean_locale:
        clean_locale_file(client, dry_run=args.dry_run)
        return

    # Fetch all theme keys
    fields = fetch_theme_keys(client)
    if not fields:
        print("No theme fields found.")
        return

    # Classify
    categories, reason_counts = analyze_keys(fields)

    # Print analysis
    print_analysis(categories, reason_counts, fields)

    # Run duplicate analysis
    if args.analyze_duplicates or args.full_analysis:
        analyze_duplicates(fields)

    # Run section analysis
    if args.analyze_sections or args.full_analysis:
        analyze_sections(fields)

    # Dump to JSON if requested
    if args.dump:
        # Enrich with duplicate info before dumping
        by_value = defaultdict(int)
        for f in fields:
            if f["key"].startswith("section."):
                val = (f.get("english") or "").strip()
                if val:
                    by_value[val] += 1
        for f in fields:
            val = (f.get("english") or "").strip()
            f["duplicate_count"] = by_value.get(val, 0)

        with open(args.dump, "w") as f:
            json.dump(fields, f, indent=2, ensure_ascii=False)
        print(f"\nDumped {len(fields)} keys to {args.dump}")

    # Dedup translations
    if args.dedup_translations:
        dedup_translations(client, fields, dry_run=args.dry_run)
        return

    # Translate missing Arabic theme keys
    if args.translate:
        translate_theme_keys(client, fields, model=args.model,
                             dry_run=args.dry_run)
        return

    # Remove all non-useful translations (junk + system)
    if args.remove_junk:
        to_remove = [f for f in fields
                     if f["has_translation"] and f["category"] != "useful"]

        if not to_remove:
            print("\nNo junk/system translations to remove.")
            return

        registered = sum(1 for f in fields if f["has_translation"])
        useful_count = registered - len(to_remove)

        print(f"\n{'=' * 70}")
        print(f"REMOVE JUNK + SYSTEM TRANSLATIONS"
              + (" (DRY RUN)" if args.dry_run else ""))
        print(f"{'=' * 70}")
        print(f"  Total registered:    {registered}")
        print(f"  To remove:           {len(to_remove)}")
        print(f"  Useful (keeping):    {useful_count}")

        # Breakdown by category
        by_cat = defaultdict(int)
        for f in to_remove:
            by_cat[f["category"]] += 1
        for cat, count in sorted(by_cat.items(), key=lambda x: -x[1]):
            print(f"    {cat}: {count}")

        if args.dry_run:
            print(f"\n  (Dry run — no changes made)")
            return

        removed, errors = remove_translations(client, to_remove, dry_run=False)

        print(f"\n  Removed:   {removed}")
        if errors:
            print(f"  Errors:    {errors}")
        print(f"  Remaining: {useful_count}")
        print(f"  --> {3400 - useful_count} free translation slots available")


if __name__ == "__main__":
    main()
