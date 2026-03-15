#!/usr/bin/env python3
"""Crawl the live Arabic storefront, find untranslated English strings,
match them to Shopify theme translation keys, and translate only what's visible.

Shopify does NOT auto-translate checkout or any theme keys — we must handle
everything ourselves. This script ensures we only spend key slots on strings
that actually appear on the site.

Pipeline:
  1. Crawl the /ar site with Playwright → collect all visible English text
  2. Fetch theme translation keys from Shopify API
  3. Match scraped strings to theme keys (exact + normalized + fuzzy)
  4. Translate only matched keys → upload to Shopify
"""

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from urllib.parse import urlparse

from dotenv import load_dotenv

from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.core import config
from tara_migrate.core.language import is_arabic_visible_text
from tara_migrate.tools.audit_theme_keys import (
    fetch_theme_keys,
    analyze_keys,
    classify_key,
    remove_translations,
    REMOVE_TRANSLATIONS_MUTATION,
)

LOCALE = "ar"
DATA_DIR = "data"

# ---------------------------------------------------------------------------
# JS snippets for Playwright
# ---------------------------------------------------------------------------

# Extract ALL visible text from the page, including text inside buttons,
# links, form labels, placeholders, headings, etc.
EXTRACT_ALL_TEXT_JS = """() => {
    const results = [];
    const seen = new Set();

    // Walk the entire DOM tree to find text nodes
    function walk(node) {
        if (node.nodeType === Node.TEXT_NODE) {
            const text = node.textContent.trim();
            if (text && text.length >= 2 && !seen.has(text)) {
                const el = node.parentElement;
                if (!el) return;
                const style = window.getComputedStyle(el);
                if (style.display === 'none' || style.visibility === 'hidden') return;
                if (el.offsetWidth === 0 && el.offsetHeight === 0) return;

                seen.add(text);
                const rect = el.getBoundingClientRect();
                results.push({
                    text: text.substring(0, 500),
                    tag: el.tagName.toLowerCase(),
                    classes: el.className ? el.className.toString().substring(0, 150) : '',
                    id: el.id || '',
                    x: Math.round(rect.x),
                    y: Math.round(rect.y),
                    visible: rect.width > 0 && rect.height > 0,
                });
            }
            return;
        }
        // Skip script, style, noscript
        if (node.nodeType === Node.ELEMENT_NODE) {
            const tag = node.tagName.toLowerCase();
            if (['script', 'style', 'noscript', 'svg', 'path'].includes(tag)) return;
        }
        for (const child of node.childNodes) {
            walk(child);
        }
    }

    walk(document.body);

    // Also grab placeholder text, title attributes, aria-labels
    document.querySelectorAll('[placeholder], [title], [aria-label]').forEach(el => {
        for (const attr of ['placeholder', 'title', 'aria-label']) {
            const val = el.getAttribute(attr);
            if (val && val.length >= 2 && !seen.has(val)) {
                seen.add(val);
                const rect = el.getBoundingClientRect();
                results.push({
                    text: val.substring(0, 500),
                    tag: el.tagName.toLowerCase() + '@' + attr,
                    classes: '',
                    id: '',
                    x: Math.round(rect.x),
                    y: Math.round(rect.y),
                    visible: rect.width > 0 && rect.height > 0,
                });
            }
        }
    });

    return results;
}"""

EXPAND_INTERACTIVE_JS = """() => {
    // Expand accordions
    document.querySelectorAll(
        '[data-accordion], .accordion__trigger, details summary, ' +
        '[aria-expanded="false"], .collapsible-trigger'
    ).forEach(el => {
        try { el.click(); } catch(e) {}
    });

    // Open any closed <details> elements
    document.querySelectorAll('details:not([open])').forEach(el => {
        el.setAttribute('open', '');
    });
}"""

EXTRACT_LINKS_JS = """(args) => {
    const [domain, localePrefix] = args;
    const links = new Set();
    document.querySelectorAll('a[href]').forEach(a => {
        const href = a.href;
        if (!href) return;
        // Include both locale-prefixed and non-prefixed links on same domain
        if (href.includes(domain)) {
            const clean = href.split('#')[0].split('?')[0];
            links.add(clean);
        }
    });
    return [...links];
}"""

ADD_TO_CART_JS = """() => {
    // Try to add first available product to cart for checkout crawling
    const form = document.querySelector('form[action*="/cart/add"]');
    if (form) {
        const data = new FormData(form);
        fetch('/cart/add.js', { method: 'POST', body: data });
        return true;
    }
    return false;
}"""


# ---------------------------------------------------------------------------
# Crawl engine
# ---------------------------------------------------------------------------

def _is_english_text(text):
    """Check if text contains meaningful English that needs translation.

    Returns True for text that has English words and isn't just brand names,
    numbers, codes, or technical identifiers.
    """
    if not text or len(text.strip()) < 2:
        return False

    cleaned = text.strip()

    # Skip pure numbers, currency, measurements
    if re.match(r'^[\d.,+%°×\-–—\s/\\:]+$', cleaned):
        return False

    # Skip pure Arabic text
    if is_arabic_visible_text(cleaned, min_ratio=0.8):
        return False

    # Must contain at least 2 consecutive Latin letters
    if not re.search(r'[a-zA-ZÀ-ÿ]{2,}', cleaned):
        return False

    # Skip if mostly Arabic (allow some English brand names mixed in)
    from tara_migrate.core.language import count_chars
    arabic, latin = count_chars(cleaned)
    total = arabic + latin
    if total > 0 and arabic / total > 0.7:
        return False

    return True


def _normalize_text(text):
    """Normalize text for fuzzy matching: lowercase, collapse whitespace, strip HTML."""
    if not text:
        return ""
    # Strip HTML tags
    t = re.sub(r'<[^>]+>', ' ', text)
    # Strip Liquid tags
    t = re.sub(r'\{\{[^}]*\}\}', '', t)
    t = re.sub(r'\{%[^%]*%\}', '', t)
    # Collapse whitespace
    t = re.sub(r'\s+', ' ', t).strip().lower()
    return t


def crawl_arabic_site(page, base_url, locale_prefix="/ar", max_pages=200,
                      include_checkout=False):
    """Crawl the Arabic site and collect all visible English strings.

    Returns a list of dicts: {text, url, tag, classes, ...}
    """
    visited = set()
    ar_home = base_url.rstrip("/") + locale_prefix
    domain = urlparse(ar_home).netloc

    # Seed URLs — cover all major page types
    to_visit = [
        ar_home,
        ar_home + "/collections",
        ar_home + "/collections/all",
    ]

    all_english_texts = []
    seen_texts = set()  # dedup across pages
    page_count = 0

    print(f"\n  Starting crawl from: {ar_home}")
    print(f"  Max pages: {max_pages}")

    while to_visit and page_count < max_pages:
        url = to_visit.pop(0)
        if url in visited:
            continue

        parsed = urlparse(url)
        if parsed.netloc != domain:
            continue

        # Accept both locale-prefixed and some non-prefixed paths
        # (checkout, cart don't always have /ar prefix)
        path = parsed.path
        has_locale = locale_prefix in path
        is_special = any(p in path for p in ['/cart', '/checkout', '/account',
                                              '/search', '/policies'])
        if not has_locale and not is_special:
            continue

        visited.add(url)
        page_count += 1
        short_path = parsed.path

        print(f"\n  [{page_count:3d}] {short_path}")

        try:
            response = page.goto(url, wait_until="networkidle", timeout=30000)
            if not response or response.status >= 400:
                status = response.status if response else "no response"
                print(f"    HTTP {status} — skipping")
                continue
            time.sleep(1)

            # Expand interactive elements
            page.evaluate(EXPAND_INTERACTIVE_JS)
            time.sleep(0.5)

            # Scroll down to trigger lazy loading
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.5)
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(0.3)

        except Exception as e:
            print(f"    ERROR: {e}")
            continue

        # Extract all text
        texts = page.evaluate(EXTRACT_ALL_TEXT_JS)
        page_english = []

        for t in texts:
            raw = t["text"]
            if raw in seen_texts:
                continue
            if _is_english_text(raw):
                seen_texts.add(raw)
                page_english.append({
                    "text": raw,
                    "url": url,
                    "path": short_path,
                    "tag": t["tag"],
                    "classes": t["classes"],
                    "id": t.get("id", ""),
                })

        if page_english:
            print(f"    {len(page_english)} English strings found:")
            for item in page_english[:5]:
                print(f"      [{item['tag']:10s}] {item['text'][:80]}")
            if len(page_english) > 5:
                print(f"      ... and {len(page_english) - 5} more")
        else:
            print(f"    All text appears Arabic ✓")

        all_english_texts.extend(page_english)

        # Discover more links
        links = page.evaluate(EXTRACT_LINKS_JS, [domain, locale_prefix])
        for link in links:
            if link not in visited:
                to_visit.append(link)

    # If checkout requested, try to reach checkout
    if include_checkout:
        print(f"\n  Attempting checkout crawl...")
        # First visit a product page and add to cart
        product_pages = [url for url in visited if '/products/' in url]
        if product_pages:
            try:
                page.goto(product_pages[0], wait_until="networkidle", timeout=20000)
                time.sleep(1)
                added = page.evaluate(ADD_TO_CART_JS)
                if added:
                    time.sleep(2)
                    # Visit cart and checkout
                    for checkout_path in ['/cart', '/checkout']:
                        checkout_url = f"{base_url.rstrip('/')}{checkout_path}"
                        if checkout_url not in visited:
                            to_visit.insert(0, checkout_url)
                    # Process these remaining URLs
                    while to_visit and page_count < max_pages + 5:
                        url = to_visit.pop(0)
                        if url in visited:
                            continue
                        visited.add(url)
                        page_count += 1
                        print(f"\n  [{page_count:3d}] {urlparse(url).path}")
                        try:
                            response = page.goto(url, wait_until="networkidle",
                                                 timeout=30000)
                            if not response or response.status >= 400:
                                continue
                            time.sleep(1.5)
                            texts = page.evaluate(EXTRACT_ALL_TEXT_JS)
                            for t in texts:
                                raw = t["text"]
                                if raw in seen_texts:
                                    continue
                                if _is_english_text(raw):
                                    seen_texts.add(raw)
                                    all_english_texts.append({
                                        "text": raw,
                                        "url": url,
                                        "path": urlparse(url).path,
                                        "tag": t["tag"],
                                        "classes": t.get("classes", ""),
                                        "id": t.get("id", ""),
                                    })
                        except Exception as e:
                            print(f"    ERROR: {e}")
            except Exception as e:
                print(f"    Checkout crawl error: {e}")

    return all_english_texts, visited


# ---------------------------------------------------------------------------
# Matching: scraped text → theme keys
# ---------------------------------------------------------------------------

def match_scraped_to_keys(scraped_texts, theme_fields):
    """Match scraped English strings to theme translation keys.

    Uses multiple strategies:
    1. Exact match (scraped text == key's English value)
    2. Normalized match (lowercase, stripped HTML/Liquid, collapsed whitespace)
    3. Substring match (scraped text is contained in a key's value, or vice versa)

    Returns:
        matched_keys: list of theme field dicts that matched
        unmatched_texts: list of scraped text dicts that didn't match any key
    """
    # Build lookup indices for theme keys
    exact_map = {}       # exact English value → field
    norm_map = {}        # normalized English value → field
    all_values = []      # (normalized_value, field) for substring matching

    for f in theme_fields:
        en = (f.get("english") or "").strip()
        if not en:
            continue
        # Exact
        if en not in exact_map:
            exact_map[en] = f
        # Normalized
        norm = _normalize_text(en)
        if norm and norm not in norm_map:
            norm_map[norm] = f
        # For substring
        if norm:
            all_values.append((norm, f))

    matched_field_ids = set()  # track by (resource_id, key) to dedup
    matched_keys = []
    unmatched_texts = []

    for item in scraped_texts:
        text = item["text"].strip()
        found = False

        # Strategy 1: Exact match
        if text in exact_map:
            f = exact_map[text]
            fid = (f["resource_id"], f["key"])
            if fid not in matched_field_ids:
                matched_field_ids.add(fid)
                matched_keys.append(f)
            found = True
            continue

        # Strategy 2: Normalized match
        norm_text = _normalize_text(text)
        if norm_text in norm_map:
            f = norm_map[norm_text]
            fid = (f["resource_id"], f["key"])
            if fid not in matched_field_ids:
                matched_field_ids.add(fid)
                matched_keys.append(f)
            found = True
            continue

        # Strategy 3: Substring — scraped text is part of a key value,
        # or key value is part of scraped text
        if norm_text and len(norm_text) >= 4:
            for norm_val, f in all_values:
                if norm_text in norm_val or norm_val in norm_text:
                    fid = (f["resource_id"], f["key"])
                    if fid not in matched_field_ids:
                        matched_field_ids.add(fid)
                        matched_keys.append(f)
                    found = True
                    break  # take first match

        if not found:
            unmatched_texts.append(item)

    return matched_keys, unmatched_texts


# ---------------------------------------------------------------------------
# Translate & upload matched keys
# ---------------------------------------------------------------------------

def translate_and_upload(client, matched_keys, model="gpt-5-nano",
                         dry_run=False):
    """Translate matched theme keys and upload to Shopify.

    Only translates keys that don't already have Arabic translations.
    """
    from tara_migrate.core.graphql_queries import REGISTER_TRANSLATIONS_MUTATION
    from tara_migrate.translation.engine import TranslationEngine

    # Filter to keys needing translation
    to_translate = []
    already_translated = 0
    for f in matched_keys:
        en = (f.get("english") or "").strip()
        ar = (f.get("arabic") or "").strip()
        if not en:
            continue
        if f.get("has_translation") and ar:
            already_translated += 1
            continue
        # Skip non-translatable content
        text_only = re.sub(r'<[^>]+>', '', en).strip()
        text_only = re.sub(r'\{\{[^}]*\}\}', '', text_only).strip()
        text_only = re.sub(r'\{%[^%]*%\}', '', text_only).strip()
        if not text_only or not re.search(r'[a-zA-Z]{2,}', text_only):
            continue
        to_translate.append(f)

    print(f"\n{'=' * 70}")
    print(f"TRANSLATION PLAN" + (" (DRY RUN)" if dry_run else ""))
    print(f"{'=' * 70}")
    print(f"  Matched keys:                {len(matched_keys)}")
    print(f"  Already have Arabic:         {already_translated}")
    print(f"  Need translation:            {len(to_translate)}")

    if not to_translate:
        print("\n  Nothing to translate — all matched keys already have Arabic!")
        return 0, 0, 0

    # Show sample
    print(f"\n  Sample keys to translate:")
    for f in to_translate[:15]:
        en = f["english"][:60]
        print(f"    {f['key'][:50]}")
        print(f"      EN: {en}")
    if len(to_translate) > 15:
        print(f"    ... and {len(to_translate) - 15} more")

    if dry_run:
        return len(to_translate), 0, 0

    # Build translation engine
    prompt = (
        "You are translating Shopify theme UI strings from English to Arabic "
        "for TARA, a luxury scalp-care brand targeting Saudi Arabia.\n"
        "Rules:\n"
        "- Use Modern Standard Arabic suitable for a Gulf audience\n"
        "- Keep the TARA brand name unchanged\n"
        "- Keep product names like 'Kansa Wand', 'Gua Sha' unchanged\n"
        "- Keep Liquid template tags ({{ }}, {% %}) unchanged\n"
        "- Keep HTML tags unchanged — only translate the text content\n"
        "- Keep placeholders like {{ count }} unchanged\n"
        "- For short UI labels (1-3 words), provide a natural Arabic equivalent\n"
        "- Arabic text should read right-to-left naturally\n"
        "- Use consistent terminology throughout\n"
        "- This covers checkout, navigation, product pages, and all storefront text\n"
    )

    engine = TranslationEngine(
        prompt,
        model=model,
        reasoning_effort="minimal",
        batch_size=60,
    )

    # Build field list for engine
    batch_fields = []
    for f in to_translate:
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

    # Upload
    print(f"\n  Uploading to {len(by_resource)} theme resources...")
    total_uploaded = 0
    total_errors = 0

    for rid, items in by_resource.items():
        for i in range(0, len(items), 10):
            batch = items[i:i + 10]
            translations_input = []
            for item in batch:
                translations_input.append({
                    "locale": LOCALE,
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
                            print(f"\n    HIT SHOPIFY KEY LIMIT!")
                            print(f"    Run: python audit_theme_keys.py --remove-junk")
                            print(f"    Then re-run this script.")
                            return len(t_map), total_uploaded, total_errors
                    total_errors += len(batch)
                else:
                    total_uploaded += len(batch)
            except Exception as e:
                print(f"    ERROR uploading to {rid}: {e}")
                total_errors += len(batch)

            time.sleep(0.3)

        if total_uploaded % 50 < 10:
            print(f"    ... uploaded {total_uploaded} so far")

    return len(t_map), total_uploaded, total_errors


# ---------------------------------------------------------------------------
# Remove unmatched translations (free up key slots)
# ---------------------------------------------------------------------------

def remove_unmatched_translations(client, theme_fields, matched_keys,
                                   dry_run=False):
    """Remove Arabic translations for theme keys that are NOT visible on site.

    This frees up key slots for the keys that actually matter.
    Only removes translations that currently exist.
    """
    matched_ids = set()
    for f in matched_keys:
        matched_ids.add((f["resource_id"], f["key"]))

    to_remove = []
    for f in theme_fields:
        if not f.get("has_translation"):
            continue
        fid = (f["resource_id"], f["key"])
        if fid not in matched_ids:
            # This key has a translation but isn't visible on the site
            cat, reason = classify_key(f["key"], f.get("english", ""))
            if cat == "junk":
                to_remove.append(f)
            # For non-junk keys that aren't visible, still remove if they're
            # not useful (system keys, etc.)
            elif cat == "system":
                to_remove.append(f)

    if not to_remove:
        print("\n  No unmatched translations to remove.")
        return 0

    print(f"\n{'=' * 70}")
    print(f"REMOVE UNMATCHED TRANSLATIONS" + (" (DRY RUN)" if dry_run else ""))
    print(f"{'=' * 70}")
    print(f"  Translations to remove (not visible on site): {len(to_remove)}")

    if dry_run:
        for f in to_remove[:10]:
            print(f"    {f['key'][:60]}")
            print(f"      EN: {(f['english'] or '')[:50]}")
        if len(to_remove) > 10:
            print(f"    ... and {len(to_remove) - 10} more")
        return 0

    removed, errors = remove_translations(client, to_remove, dry_run=False)
    print(f"  Removed: {removed}")
    if errors:
        print(f"  Errors:  {errors}")
    return removed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Crawl Arabic site → match to theme keys → translate visible strings only")

    # Crawl options
    parser.add_argument("--base-url", default="https://sa.taraformula.com",
                        help="Store base URL (default: sa.taraformula.com)")
    parser.add_argument("--locale-prefix", default="/ar",
                        help="Locale path prefix (default: /ar)")
    parser.add_argument("--max-pages", type=int, default=200,
                        help="Max pages to crawl (default: 200)")
    parser.add_argument("--include-checkout", action="store_true",
                        help="Attempt to crawl checkout pages")
    parser.add_argument("--headed", action="store_true",
                        help="Run browser visibly (not headless)")

    # Pipeline control
    parser.add_argument("--crawl-only", action="store_true",
                        help="Only crawl and save — don't match or translate")
    parser.add_argument("--skip-crawl", action="store_true",
                        help="Skip crawl, use saved data/crawl_english.json")
    parser.add_argument("--skip-remove", action="store_true",
                        help="Don't remove unmatched translations")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show plan without making changes")

    # Translation options
    parser.add_argument("--model", default="gpt-5-nano",
                        help="Translation model (default: gpt-5-nano)")

    # Data files
    parser.add_argument("--crawl-data", default=None,
                        help="Path to crawl data JSON (default: data/crawl_english.json)")
    parser.add_argument("--keys-data", default=None,
                        help="Path to theme keys JSON (default: data/theme_keys.json)")

    args = parser.parse_args()

    load_dotenv()

    crawl_file = args.crawl_data or os.path.join(DATA_DIR, "crawl_english.json")
    keys_file = args.keys_data or os.path.join(DATA_DIR, "theme_keys.json")

    print("=" * 70)
    print("CRAWL → MATCH → TRANSLATE (visible theme strings only)")
    print("=" * 70)

    # ── Step 1: Crawl ─────────────────────────────────────────────────────
    if args.skip_crawl:
        if not os.path.exists(crawl_file):
            print(f"\nERROR: {crawl_file} not found. Run without --skip-crawl first.")
            sys.exit(1)
        with open(crawl_file, encoding="utf-8") as f:
            crawl_data = json.load(f)
        scraped = crawl_data["english_texts"]
        print(f"\n  Loaded {len(scraped)} English strings from {crawl_file}")
        print(f"  (from {crawl_data.get('pages_visited', '?')} pages)")
    else:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            print("\nERROR: Playwright not installed.")
            print("  pip install playwright")
            print("  playwright install chromium")
            sys.exit(1)

        print(f"\n  STEP 1: Crawling {args.base_url}{args.locale_prefix}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=not args.headed)
            context = browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="ar-SA",
            )
            pw_page = context.new_page()

            scraped, visited = crawl_arabic_site(
                pw_page,
                args.base_url,
                locale_prefix=args.locale_prefix,
                max_pages=args.max_pages,
                include_checkout=args.include_checkout,
            )

            browser.close()

        # Save crawl results
        os.makedirs(DATA_DIR, exist_ok=True)
        crawl_data = {
            "base_url": args.base_url,
            "locale_prefix": args.locale_prefix,
            "pages_visited": len(visited),
            "english_texts": scraped,
            "visited_urls": sorted(visited),
        }
        with open(crawl_file, "w", encoding="utf-8") as f:
            json.dump(crawl_data, f, ensure_ascii=False, indent=2)

        print(f"\n{'─' * 70}")
        print(f"  CRAWL COMPLETE")
        print(f"  Pages visited:      {len(visited)}")
        print(f"  English strings:    {len(scraped)}")
        print(f"  Saved to:           {crawl_file}")

        if args.crawl_only:
            # Show unique texts summary
            unique = set(t["text"] for t in scraped)
            print(f"  Unique strings:     {len(unique)}")
            print(f"\n  Sample English strings found:")
            for text in sorted(unique)[:30]:
                print(f"    {text[:100]}")
            return

    # ── Step 2: Fetch theme keys ──────────────────────────────────────────
    print(f"\n  STEP 2: Fetching theme translation keys from Shopify")

    if os.path.exists(keys_file) and args.skip_crawl:
        # Reuse cached keys if also skipping crawl
        with open(keys_file, encoding="utf-8") as f:
            theme_fields = json.load(f)
        print(f"  Loaded {len(theme_fields)} theme keys from {keys_file}")
    else:
        client = ShopifyClient(
            config.get_dest_shop_url(),
            config.get_dest_access_token(),
        )
        theme_fields = fetch_theme_keys(client)
        # Save for reuse
        with open(keys_file, "w", encoding="utf-8") as f:
            json.dump(theme_fields, f, ensure_ascii=False, indent=2)
        print(f"  Saved {len(theme_fields)} keys to {keys_file}")

    # Classify keys
    categories, reason_counts = analyze_keys(theme_fields)
    total_with_ar = sum(1 for f in theme_fields if f.get("has_translation"))
    print(f"  Total theme fields:     {len(theme_fields)}")
    print(f"  With Arabic already:    {total_with_ar}")
    print(f"  Useful (text):          {len(categories['useful'])}")
    print(f"  System:                 {len(categories['system'])}")
    print(f"  Junk:                   {len(categories['junk'])}")

    # ── Step 3: Match ─────────────────────────────────────────────────────
    print(f"\n  STEP 3: Matching scraped strings to theme keys")

    matched_keys, unmatched_texts = match_scraped_to_keys(scraped, theme_fields)

    print(f"\n{'─' * 70}")
    print(f"  MATCH RESULTS")
    print(f"{'─' * 70}")
    print(f"  Scraped English strings:  {len(scraped)}")
    print(f"  Matched to theme keys:    {len(matched_keys)}")
    print(f"  Unmatched (not in theme): {len(unmatched_texts)}")

    if unmatched_texts:
        print(f"\n  Unmatched strings (may be from product/collection content, not theme):")
        for item in unmatched_texts[:20]:
            print(f"    [{item.get('tag', '?'):10s}] {item['text'][:80]}")
            print(f"      URL: {item.get('path', '?')}")
        if len(unmatched_texts) > 20:
            print(f"    ... and {len(unmatched_texts) - 20} more")

    if matched_keys:
        print(f"\n  Matched theme keys sample:")
        for f in matched_keys[:10]:
            has_ar = "✓ AR" if f.get("has_translation") else "NO AR"
            print(f"    [{has_ar:>5}] {f['key'][:50]}")
            print(f"           EN: {(f['english'] or '')[:60]}")

    # Save match results
    match_file = os.path.join(DATA_DIR, "crawl_matched.json")
    with open(match_file, "w", encoding="utf-8") as f:
        json.dump({
            "matched_keys": [{
                "resource_id": k["resource_id"],
                "key": k["key"],
                "english": k.get("english", ""),
                "has_translation": k.get("has_translation", False),
            } for k in matched_keys],
            "unmatched_texts": unmatched_texts,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n  Match results saved to: {match_file}")

    # Ensure we have a client for remaining steps
    try:
        client
    except NameError:
        client = ShopifyClient(
            config.get_dest_shop_url(),
            config.get_dest_access_token(),
        )

    # ── Step 4: Remove unmatched translations (free slots) ────────────────
    if not args.skip_remove:
        remove_unmatched_translations(client, theme_fields, matched_keys,
                                       dry_run=args.dry_run)

    # ── Step 5: Translate matched keys ────────────────────────────────────
    print(f"\n  STEP 4: Translating matched keys")

    translated, uploaded, errors = translate_and_upload(
        client, matched_keys, model=args.model, dry_run=args.dry_run,
    )

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"PIPELINE COMPLETE" + (" (DRY RUN)" if args.dry_run else ""))
    print(f"{'=' * 70}")
    print(f"  Pages crawled:            {crawl_data.get('pages_visited', '?')}")
    print(f"  English strings found:    {len(scraped)}")
    print(f"  Matched to theme keys:    {len(matched_keys)}")
    print(f"  Translated:               {translated}")
    print(f"  Uploaded to Shopify:      {uploaded}")
    if errors:
        print(f"  Errors:                   {errors}")
    print(f"\n  Unmatched strings: {len(unmatched_texts)}")
    print(f"  (These may be product/collection content — use review_arabic.py for those)")


if __name__ == "__main__":
    main()
