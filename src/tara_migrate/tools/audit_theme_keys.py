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
    # Only keys starting with "section." are merchant-entered theme content
    # (headings, text blocks, buttons in the theme editor). Everything else
    # is Shopify's built-in locale system — checkout, customer accounts,
    # pagination, attributes, accessibility, blog strings, etc. — which
    # Shopify auto-translates when Arabic is enabled as a store locale.
    # Registering custom translations for these wastes key slots.
    _MERCHANT_PREFIXES = ("section.", "general.")
    if not any(key.startswith(p) for p in _MERCHANT_PREFIXES):
        return "system", "Shopify auto-translated system string"

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
# Tiered removal priority
# ─────────────────────────────────────────────────────────────────────────────
# Shopify limits registered translations to ~3,400 per locale per theme.
# Rather than removing ALL system translations (which may have been customized),
# we remove in tiers — safest first — stopping as soon as we're under the limit.
#
# Tier 1: Junk (images, URLs, CSS, etc.) — definitely not translations
# Tier 2: Shopify checkout strings (shopify.checkout.*) — Shopify's core
#          platform UI, always has reliable built-in Arabic
# Tier 3: Shopify platform strings (shopify.*, customer_accounts.*) —
#          Shopify-managed, built-in translations exist
# Tier 4: Theme locale strings (accessibility.*, actions.*, content.*,
#          blocks.*, etc.) — from theme locale files, may have custom work

def _get_removal_tiers():
    return [
        ("Tier 1 — Junk (images, URLs, CSS, IDs)",
         lambda f: f["category"] == "junk"),
        ("Tier 2 — Shopify checkout (built-in Arabic)",
         lambda f: f["category"] == "system" and f["key"].startswith("shopify.checkout.")),
        ("Tier 3 — Shopify platform strings",
         lambda f: f["category"] == "system" and (
             f["key"].startswith("shopify.") or
             f["key"].startswith("customer_accounts."))),
        ("Tier 4 — Theme locale strings",
         lambda f: f["category"] == "system"),
    ]


def main():
    parser = argparse.ArgumentParser(description="Audit theme translation keys")
    parser.add_argument("--remove-junk", action="store_true",
                        help="Remove junk + minimal system translations to get under limit")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be removed without doing it")
    parser.add_argument("--dump", metavar="FILE",
                        help="Dump all keys to JSON for manual review")
    parser.add_argument("--target", type=int, default=3400,
                        help="Target max registered translations (default: 3400)")
    args = parser.parse_args()

    load_dotenv()
    client = ShopifyClient(
        os.environ["SAUDI_SHOP_URL"],
        os.environ["SAUDI_ACCESS_TOKEN"],
    )

    # Fetch all theme keys
    fields = fetch_theme_keys(client)
    if not fields:
        print("No theme fields found.")
        return

    # Classify
    categories, reason_counts = analyze_keys(fields)

    # Print analysis
    print_analysis(categories, reason_counts, fields)

    # Dump to JSON if requested
    if args.dump:
        with open(args.dump, "w") as f:
            json.dump(fields, f, indent=2, ensure_ascii=False)
        print(f"\nDumped {len(fields)} keys to {args.dump}")

    # Remove translations in tiers until under the limit
    if args.remove_junk or args.dry_run:
        registered = sum(1 for f in fields if f["has_translation"])
        target = args.target
        need_to_remove = registered - target

        if need_to_remove <= 0:
            print(f"\nAlready under the limit ({registered} registered, "
                  f"target {target}). Nothing to remove.")
            return

        print(f"\n{'=' * 70}")
        print(f"REMOVAL PLAN")
        print(f"{'=' * 70}")
        print(f"  Registered translations: {registered}")
        print(f"  Target:                  {target}")
        print(f"  Need to remove:          {need_to_remove}")

        tiers = _get_removal_tiers()
        to_remove = []
        already_selected = set()

        for tier_name, tier_filter in tiers:
            if len(to_remove) >= need_to_remove:
                break

            tier_candidates = [
                f for f in fields
                if f["has_translation"]
                and tier_filter(f)
                and id(f) not in already_selected
            ]

            if not tier_candidates:
                continue

            # Take only as many as needed from this tier
            remaining_needed = need_to_remove - len(to_remove)
            take = tier_candidates[:remaining_needed]
            to_remove.extend(take)
            for f in take:
                already_selected.add(id(f))

            print(f"\n  {tier_name}: {len(take)} translations")
            if len(take) < len(tier_candidates):
                print(f"    (only {len(take)} of {len(tier_candidates)} needed)")
            # Show examples
            for f in take[:3]:
                print(f"    {f['key'][:65]}")
                print(f"      en: {(f['english'] or '')[:50]!r}")
            if len(take) > 3:
                print(f"    ... and {len(take) - 3} more")

        print(f"\n  Total to remove: {len(to_remove)}")
        print(f"  Will remain:     {registered - len(to_remove)}")

        if args.dry_run:
            print(f"\n  (Dry run — no changes made)")
            return

        removed, errors = remove_translations(client, to_remove, dry_run=False)

        print(f"\n  Removed:   {removed}")
        if errors:
            print(f"  Errors:    {errors}")
        final = registered - removed
        print(f"  Remaining: {final}")
        if final <= target:
            print(f"  --> Under the {target} limit! Theme translations should work now.")
        else:
            print(f"  --> Still {final - target} over the limit.")


if __name__ == "__main__":
    main()
