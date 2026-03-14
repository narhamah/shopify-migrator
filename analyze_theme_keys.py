#!/usr/bin/env python3
"""Analyze theme translation keys: full breakdown by source, size, duplicates.

Run after dumping keys:
    PYTHONPATH=src python audit_theme_keys.py --dump data/theme_keys_full.json
    PYTHONPATH=src python analyze_theme_keys.py data/theme_keys_full.json

Or run directly (fetches from Shopify):
    PYTHONPATH=src python analyze_theme_keys.py --fetch
"""

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict


def classify_source(key):
    """Classify a key by its source."""
    if key.startswith("section."):
        return "theme_editor"
    if key.startswith("general."):
        return "theme_locale"
    if key.startswith("shopify.checkout."):
        return "checkout"
    if key.startswith("shopify."):
        return "shopify_system"
    if key.startswith("customer_accounts."):
        return "customer_accounts"

    # Theme locale categories (from Dawn's locales/en.default.json)
    theme_prefixes = (
        "accessibility.", "actions.", "blocks.", "blogs.", "cart.",
        "content.", "contact.", "collection.", "collections.",
        "date.", "filter.", "gift_cards.", "localization.",
        "newsletter.", "notifications.", "pagination.", "passwords.",
        "products.", "search.", "templates.",
    )
    for prefix in theme_prefixes:
        if key.startswith(prefix):
            return "theme_locale"

    # Policies
    if "policy" in key.lower() or key.startswith("SHOP_POLICY"):
        return "policy"

    return "other"


def analyze(fields):
    """Full analysis of theme translation keys."""

    # ── Source breakdown ──
    by_source = defaultdict(list)
    for f in fields:
        source = classify_source(f["key"])
        by_source[source].append(f)

    total = len(fields)
    with_ar = sum(1 for f in fields if f.get("has_translation") or f.get("arabic"))
    total_en_chars = sum(len(f.get("english", "") or "") for f in fields)

    print(f"\n{'=' * 70}")
    print(f"THEME TRANSLATION KEY ANALYSIS")
    print(f"{'=' * 70}")
    print(f"  Total keys:           {total}")
    print(f"  With Arabic:          {with_ar}")
    print(f"  Without Arabic:       {total - with_ar}")
    print(f"  Total EN chars:       {total_en_chars:,}")
    print(f"  Shopify limit:        ~3,400")
    over = total - 3400
    if over > 0:
        print(f"  OVER LIMIT BY:        {over}")
    else:
        print(f"  Under limit by:       {-over}")

    # ── By source ──
    print(f"\n{'─' * 70}")
    print(f"KEYS BY SOURCE")
    print(f"{'─' * 70}")
    source_order = [
        "theme_editor", "theme_locale", "checkout", "shopify_system",
        "customer_accounts", "policy", "other",
    ]
    for source in source_order:
        items = by_source.get(source, [])
        if not items:
            continue
        ar_count = sum(1 for f in items if f.get("has_translation") or f.get("arabic"))
        chars = sum(len(f.get("english", "") or "") for f in items)
        print(f"\n  {source.upper()} ({len(items)} keys, {ar_count} with Arabic, "
              f"{chars:,} EN chars)")

        # Sub-breakdown by prefix
        by_prefix = Counter()
        for f in items:
            parts = f["key"].split(".")
            if source == "theme_editor":
                # section.index.json → section.index.json
                prefix = ".".join(parts[:3]) if len(parts) >= 3 else f["key"]
            elif source in ("checkout", "shopify_system", "customer_accounts"):
                prefix = ".".join(parts[:3]) if len(parts) >= 3 else f["key"]
            else:
                prefix = ".".join(parts[:2]) if len(parts) >= 2 else parts[0]
            by_prefix[prefix] += 1

        for prefix, count in by_prefix.most_common(15):
            print(f"    {count:>4}  {prefix}")
        if len(by_prefix) > 15:
            print(f"    ... and {len(by_prefix) - 15} more prefixes")

    # ── Massive keys (policies, etc.) ──
    print(f"\n{'─' * 70}")
    print(f"LARGEST KEYS (by English character count)")
    print(f"{'─' * 70}")
    sorted_by_size = sorted(fields, key=lambda f: len(f.get("english", "") or ""),
                            reverse=True)
    for f in sorted_by_size[:15]:
        en = f.get("english", "") or ""
        ar = f.get("arabic", "") or ""
        has_ar = "has AR" if (f.get("has_translation") or ar) else "NO AR"
        inline_ar = ""
        # Check if Arabic is baked into the English value
        if re.search(r'[\u0600-\u06FF]{10,}', en):
            inline_ar = " ⚠ INLINE ARABIC IN EN VALUE"
        print(f"  [{has_ar:>6}] {len(en):>6} chars  {f['key'][:55]}{inline_ar}")
        if inline_ar:
            print(f"           → This key has Arabic baked into the 'English' field!")
            print(f"           → Translation registration is WASTED — Arabic is already there.")

    # ── Duplicate detection ──
    print(f"\n{'─' * 70}")
    print(f"DUPLICATE ENGLISH VALUES")
    print(f"{'─' * 70}")
    by_value = defaultdict(list)
    for f in fields:
        en = (f.get("english", "") or "").strip()
        if en and len(en) >= 3:  # Ignore tiny values
            by_value[en].append(f)

    dupes = {v: keys for v, keys in by_value.items() if len(keys) > 1}
    # Sort by most duplicated
    sorted_dupes = sorted(dupes.items(), key=lambda x: len(x[1]), reverse=True)

    total_dupe_keys = sum(len(keys) - 1 for _, keys in sorted_dupes)
    print(f"  Unique values with duplicates: {len(sorted_dupes)}")
    print(f"  Total duplicate keys (could save): {total_dupe_keys}")

    for value, keys in sorted_dupes[:20]:
        val_preview = value[:60]
        print(f"\n  [{len(keys)}x] {val_preview!r}")
        for k in keys[:5]:
            print(f"       {k['key'][:65]}")
        if len(keys) > 5:
            print(f"       ... and {len(keys) - 5} more")

    if len(sorted_dupes) > 20:
        print(f"\n  ... and {len(sorted_dupes) - 20} more duplicate groups")

    # ── Keys with inline Arabic (in the English value) ──
    print(f"\n{'─' * 70}")
    print(f"KEYS WITH ARABIC BAKED INTO ENGLISH VALUE")
    print(f"{'─' * 70}")
    inline_arabic = []
    for f in fields:
        en = f.get("english", "") or ""
        if re.search(r'[\u0600-\u06FF]{10,}', en):
            inline_arabic.append(f)

    print(f"  Count: {len(inline_arabic)}")
    if inline_arabic:
        print(f"  These have Arabic text INSIDE the English source field.")
        print(f"  Registering a separate Arabic translation is redundant.")
        total_inline_chars = sum(len(f.get("english", "") or "") for f in inline_arabic)
        print(f"  Total chars in these keys: {total_inline_chars:,}")
        for f in inline_arabic[:10]:
            en = f.get("english", "") or ""
            print(f"    {f['key'][:55]} ({len(en):,} chars)")

    # ── Empty/useless translations ──
    print(f"\n{'─' * 70}")
    print(f"ARABIC = ENGLISH (identical translations)")
    print(f"{'─' * 70}")
    identical = []
    for f in fields:
        en = (f.get("english", "") or "").strip()
        ar = (f.get("arabic", "") or "").strip()
        if en and ar and en == ar:
            identical.append(f)
    print(f"  Count: {len(identical)}")
    for f in identical[:10]:
        print(f"    {f['key'][:55]}")
        print(f"      value: {(f['english'] or '')[:60]!r}")
    if len(identical) > 10:
        print(f"    ... and {len(identical) - 10} more")

    # ── Summary & recommendations ──
    print(f"\n{'=' * 70}")
    print(f"RECOMMENDATIONS")
    print(f"{'=' * 70}")

    removable = 0

    junk_count = sum(1 for f in fields
                     if not (f.get("english", "") or "").strip()
                     or classify_source(f["key"]) == "other")
    print(f"\n  1. Junk/empty keys:             {junk_count}")
    removable += junk_count

    print(f"  2. Inline-Arabic keys:          {len(inline_arabic)}")
    removable += len(inline_arabic)

    print(f"  3. Identical AR=EN:             {len(identical)}")
    removable += len(identical)

    print(f"  4. Duplicate value keys:        {total_dupe_keys}")
    # Don't double count

    checkout_count = len(by_source.get("checkout", []))
    shopify_count = len(by_source.get("shopify_system", []))
    ca_count = len(by_source.get("customer_accounts", []))
    print(f"  5. Shopify checkout keys:       {checkout_count}")
    print(f"  6. Shopify system keys:         {shopify_count}")
    print(f"  7. Customer accounts keys:      {ca_count}")

    actually_need = len(by_source.get("theme_editor", [])) + len(by_source.get("theme_locale", []))
    print(f"\n  Keys you actually need translations for:")
    print(f"    Theme editor (section.*):    {len(by_source.get('theme_editor', []))}")
    print(f"    Theme locale (UI strings):   {len(by_source.get('theme_locale', []))}")
    print(f"    TOTAL NEEDED:                {actually_need}")

    if actually_need <= 3400:
        print(f"\n  ✓ You can fit all needed keys under the 3,400 limit!")
        print(f"    Remove checkout + shopify system + customer accounts + junk")
        print(f"    Then translate the theme locale strings.")
    else:
        print(f"\n  ✗ Even needed keys alone exceed 3,400.")
        print(f"    Need to deduplicate theme editor content.")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze theme translation keys from JSON dump")
    parser.add_argument("input", nargs="?", default="data/theme_keys_full.json",
                        help="Path to JSON dump from audit_theme_keys.py --dump")
    parser.add_argument("--fetch", action="store_true",
                        help="Fetch directly from Shopify instead of reading JSON")
    args = parser.parse_args()

    if args.fetch:
        from dotenv import load_dotenv
        load_dotenv()
        from tara_migrate.client.shopify_client import ShopifyClient
        from tara_migrate.tools.audit_theme_keys import fetch_theme_keys, analyze_keys
        client = ShopifyClient(
            os.environ["SAUDI_SHOP_URL"],
            os.environ["SAUDI_ACCESS_TOKEN"],
        )
        fields = fetch_theme_keys(client)
        analyze_keys(fields)  # adds category/reason
    else:
        if not os.path.exists(args.input):
            print(f"ERROR: {args.input} not found.")
            print(f"Run first: PYTHONPATH=src python audit_theme_keys.py --dump {args.input}")
            sys.exit(1)
        with open(args.input) as fh:
            fields = json.load(fh)

    analyze(fields)


if __name__ == "__main__":
    main()
