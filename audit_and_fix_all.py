#!/usr/bin/env python3
"""Full audit & fix pipeline: Playwright visual check + API audit + AI re-translation.

Mimics a human reviewing every page of the Arabic store:

  Phase 1 — VISUAL CRAWL (Playwright)
    Browse every page on /ar, expand accordions, extract visible text.
    Detect: raw JSON on page, untranslated English, mixed language.

  Phase 2 — API AUDIT (GraphQL)
    Scan every translatable resource across all types.
    Detect: MISSING, IDENTICAL, CORRUPTED_JSON, MIXED_LANGUAGE, NOT_ARABIC.

  Phase 3 — FIX (AI re-translation + upload)
    For every broken field:
      - Fetch full English source from Shopify
      - Re-translate with proper rich_text handling (extract text nodes → translate → rebuild)
      - Validate JSON output
      - Upload via translationsRegister

  Phase 4 — VERIFY (optional Playwright re-check)
    Re-visit pages that had visual issues, confirm they render correctly.

Usage:
    python audit_and_fix_all.py --dry-run              # Preview what needs fixing
    python audit_and_fix_all.py                         # Full audit + fix → CSV
    python audit_and_fix_all.py --skip-visual           # API-only audit + fix → CSV
    python audit_and_fix_all.py --visual-only           # Playwright check only (no fix)
    python audit_and_fix_all.py --verify                # Re-check previously fixed pages
    python audit_and_fix_all.py --type PRODUCT          # Audit one resource type
    python audit_and_fix_all.py --screenshots           # Save before/after screenshots
    python audit_and_fix_all.py --upload                # Upload via API instead of CSV
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from urllib.parse import urlparse

from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.core.language import (
    count_chars,
    detect_mixed_language,
    has_arabic,
    is_arabic_visible_text,
)
from tara_migrate.core.rich_text import extract_text, is_rich_text_json, sanitize
from tara_migrate.core.shopify_fields import (
    TRANSLATABLE_RESOURCE_TYPES,
    is_skippable_field,
    is_skippable_value,
)
from tara_migrate.translation.engine import TranslationEngine, load_developer_prompt

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ARABIC_LOCALE = "ar"
OUTPUT_DIR = "Arabic"
BASE_URL = "https://sa.taraformula.com"
LOCALE_PREFIX = "/ar"

# Brand terms that are OK in Latin script on Arabic pages
OK_PATTERNS = [
    r"^(tara|TARA|Tara)$",
    r"^(Kansa Wand|Gua Sha|Gua sha)$",
    r"^SAR\s",
    r"^ABG10\+",
    r"^Capixyl",
    r"^Procapil",
    r"^Silverfree",
    r"^INCI",
    r"^pH\s",
    r"^(EUR|SAR|USD)\b",
]

# JSON structure patterns that indicate raw JSON rendered as text
RAW_JSON_PATTERNS = [
    r'"type"\s*:\s*"(root|paragraph|text|list|list-item|heading|link)"',
    r'"children"\s*:\s*\[',
    r'"value"\s*:\s*"[^"]*".*"type"\s*:',
    r'"bold"\s*:\s*true',
    r'"listType"\s*:\s*"(ordered|unordered)"',
]

# GraphQL queries
TRANSLATABLE_RESOURCES_QUERY = """
query($resourceType: TranslatableResourceType!, $first: Int!, $after: String) {
  translatableResources(resourceType: $resourceType, first: $first, after: $after) {
    edges {
      node {
        resourceId
        translatableContent {
          key
          value
          digest
          locale
          type
        }
        translations(locale: "%LOCALE%") {
          key
          value
          outdated
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

REGISTER_TRANSLATIONS_MUTATION = """
mutation translationsRegister($resourceId: ID!, $translations: [TranslationInput!]!) {
  translationsRegister(resourceId: $resourceId, translations: $translations) {
    userErrors {
      message
      field
    }
    translations {
      key
      value
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Playwright JS snippets
# ---------------------------------------------------------------------------

EXTRACT_VISIBLE_TEXT_JS = """() => {
    const results = [];
    const seen = new Set();
    const elements = document.querySelectorAll(
        'h1, h2, h3, h4, h5, h6, p, span, a, button, label, li, td, th, ' +
        'div:not(:has(*:not(br):not(wbr))), ' +
        '[class*="title"], [class*="heading"], [class*="label"], [class*="badge"], ' +
        '[class*="tab"], [class*="accordion"], [class*="btn"], ' +
        '[class*="product__description"], [class*="rte"], [class*="rich-text"]'
    );
    for (const el of elements) {
        const style = window.getComputedStyle(el);
        if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') continue;
        if (el.offsetWidth === 0 && el.offsetHeight === 0) continue;
        let text = '';
        for (const node of el.childNodes) {
            if (node.nodeType === Node.TEXT_NODE) text += node.textContent;
        }
        text = text.trim();
        if (!text && el.children.length === 0) text = el.textContent.trim();
        if (!text || text.length < 2 || seen.has(text)) continue;
        seen.add(text);
        const rect = el.getBoundingClientRect();
        results.push({
            text: text.substring(0, 500),
            tag: el.tagName.toLowerCase(),
            classes: el.className ? el.className.toString().substring(0, 100) : '',
            selector: (() => {
                if (el.id) return '#' + el.id;
                const t = el.tagName.toLowerCase();
                const c = el.className ? '.' + el.className.toString().split(' ').filter(x=>x).slice(0,2).join('.') : '';
                return t + c;
            })(),
            x: Math.round(rect.x),
            y: Math.round(rect.y),
            visible: rect.width > 0 && rect.height > 0 && rect.y < window.innerHeight * 5,
        });
    }
    return results.filter(r => r.visible);
}"""

EXPAND_ACCORDIONS_JS = """() => {
    // Click all accordion triggers, details elements, collapsible sections
    const selectors = [
        'details summary',
        '[data-accordion]',
        '.accordion__trigger',
        '.collapsible__trigger',
        'button[aria-expanded="false"]',
        '[class*="accordion"] button',
        '[class*="accordion"] [role="button"]',
    ];
    selectors.forEach(sel => {
        document.querySelectorAll(sel).forEach(el => {
            try { el.click(); } catch(e) {}
        });
    });
    // Also open all <details> elements
    document.querySelectorAll('details').forEach(d => d.open = true);
}"""

EXTRACT_LINKS_JS = """(args) => {
    const [domain, localePrefix] = args;
    const links = new Set();
    document.querySelectorAll('a[href]').forEach(a => {
        const href = a.href;
        if (href && href.includes(localePrefix) && href.includes(domain)) {
            links.add(href.split('#')[0].split('?')[0]);
        }
    });
    return [...links];
}"""

GET_PAGE_HANDLE_JS = """() => {
    // Extract Shopify resource info from page
    const meta = {};
    // Product page
    const productJson = document.querySelector('[data-product-json], #ProductJson, script[type="application/json"][data-product-id]');
    if (productJson) {
        try {
            const data = JSON.parse(productJson.textContent);
            meta.type = 'product';
            meta.id = data.id;
            meta.handle = data.handle;
        } catch(e) {}
    }
    // Check URL patterns
    const path = window.location.pathname.replace(/^\\/ar/, '');
    const productMatch = path.match(/\\/products\\/([^/?#]+)/);
    const collectionMatch = path.match(/\\/collections\\/([^/?#]+)/);
    const pageMatch = path.match(/\\/pages\\/([^/?#]+)/);
    const blogMatch = path.match(/\\/blogs\\/([^/?#]+)\\/([^/?#]+)/);
    if (productMatch && !meta.handle) {
        meta.type = 'product';
        meta.handle = productMatch[1];
    } else if (collectionMatch && !meta.handle) {
        meta.type = 'collection';
        meta.handle = collectionMatch[1];
    } else if (pageMatch && !meta.handle) {
        meta.type = 'page';
        meta.handle = pageMatch[1];
    } else if (blogMatch && !meta.handle) {
        meta.type = 'article';
        meta.blog = blogMatch[1];
        meta.handle = blogMatch[2];
    }
    return meta;
}"""


# ---------------------------------------------------------------------------
# Phase 1: Playwright Visual Crawl
# ---------------------------------------------------------------------------

def detect_raw_json(text):
    """Detect if visible text contains raw JSON structure from rich_text fields."""
    matches = 0
    for pat in RAW_JSON_PATTERNS:
        if re.search(pat, text):
            matches += 1
    return matches >= 2  # Need at least 2 JSON patterns to flag


def classify_visual_issue(text):
    """Classify a visual text issue.

    Returns: (issue_type, detail)
    - RAW_JSON: Rich text JSON structure rendered as visible text
    - UNTRANSLATED: English/Latin text that should be Arabic
    - MIXED: Mix of Arabic and English/Spanish
    - None: No issue
    """
    if detect_raw_json(text):
        return "RAW_JSON", "raw rich_text JSON visible on page"

    # Check against OK patterns
    for pat in OK_PATTERNS:
        if re.match(pat, text.strip()):
            return None, None

    if is_arabic_visible_text(text, min_ratio=0.4, ok_patterns=OK_PATTERNS):
        return None, None

    # Determine specific issue
    arabic, latin = count_chars(text)
    total = arabic + latin
    if total == 0:
        return None, None

    if arabic == 0:
        return "UNTRANSLATED", f"no Arabic ({latin} Latin chars)"

    is_mixed, lang = detect_mixed_language(text)
    if is_mixed:
        return "MIXED", f"mixed {lang} ({arabic} Arabic, {latin} Latin)"

    if total > 5 and arabic / total < 0.4:
        return "UNTRANSLATED", f"low Arabic ratio ({arabic}/{total})"

    return None, None


def visual_crawl(base_url, locale_prefix, max_pages=100, screenshots_dir=None, headed=False):
    """Crawl the Arabic storefront with Playwright.

    Returns (issues, pages_visited) where issues is a list of
    {url, path, text, tag, selector, issue_type, detail, handle_info}.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  ERROR: Playwright not installed. Run:")
        print("    pip install playwright && playwright install chromium")
        print("  Skipping visual crawl.")
        return [], set()

    ar_home = f"{base_url.rstrip('/')}{locale_prefix}"
    domain = urlparse(ar_home).netloc

    visited = set()
    to_visit = [
        ar_home,
        ar_home + "/collections/all",
        ar_home + "/collections",
    ]
    all_issues = []
    page_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headed)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="ar-SA",
        )
        page = context.new_page()

        while to_visit and page_count < max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue

            parsed = urlparse(url)
            if parsed.netloc != domain or locale_prefix not in parsed.path:
                continue

            visited.add(url)
            page_count += 1
            short_path = parsed.path

            print(f"\n  [{page_count}/{max_pages}] {short_path}")

            try:
                response = page.goto(url, wait_until="networkidle", timeout=30000)
                if not response or response.status >= 400:
                    print(f"    HTTP {response.status if response else 'N/A'}")
                    continue
                time.sleep(1)

                # Expand all accordions
                page.evaluate(EXPAND_ACCORDIONS_JS)
                time.sleep(0.8)

            except Exception as e:
                print(f"    ERROR: {e}")
                continue

            # Get page handle info
            try:
                handle_info = page.evaluate(GET_PAGE_HANDLE_JS)
            except Exception:
                handle_info = {}

            # Screenshot before
            if screenshots_dir:
                fname = short_path.strip("/").replace("/", "_") or "home"
                if len(fname) > 100:
                    fname = fname[:100]
                try:
                    page.screenshot(
                        path=os.path.join(screenshots_dir, f"before_{fname}.png"),
                        full_page=True,
                    )
                except Exception:
                    pass

            # Extract and classify text
            texts = page.evaluate(EXTRACT_VISIBLE_TEXT_JS)
            page_issues = []

            for t in texts:
                issue_type, detail = classify_visual_issue(t["text"])
                if issue_type:
                    page_issues.append({
                        "url": url,
                        "path": short_path,
                        "text": t["text"][:300],
                        "tag": t["tag"],
                        "selector": t["selector"],
                        "classes": t["classes"],
                        "issue_type": issue_type,
                        "detail": detail,
                        "handle_info": handle_info,
                    })

            if page_issues:
                # Group by type for summary
                by_type = {}
                for issue in page_issues:
                    by_type.setdefault(issue["issue_type"], []).append(issue)
                parts = [f"{len(v)} {k}" for k, v in sorted(by_type.items())]
                print(f"    ISSUES: {', '.join(parts)}")
                for issue in page_issues[:3]:
                    preview = issue['text'][:80].replace('\n', ' ')
                    print(f"      [{issue['issue_type']:13s}] {preview}")
                if len(page_issues) > 3:
                    print(f"      ... and {len(page_issues) - 3} more")
            else:
                print(f"    OK")

            all_issues.extend(page_issues)

            # Discover links
            try:
                links = page.evaluate(EXTRACT_LINKS_JS, [domain, locale_prefix])
                for link in links:
                    if link not in visited:
                        to_visit.append(link)
            except Exception:
                pass

        browser.close()

    return all_issues, visited


# ---------------------------------------------------------------------------
# Phase 2: API Audit
# ---------------------------------------------------------------------------

def classify_translation(key, english_value, translated_value, outdated=False):
    """Classify a field's translation status.

    Returns: (status, detail)
    Status: OK, MISSING, IDENTICAL, NOT_ARABIC, MIXED_LANGUAGE,
            CORRUPTED_JSON, OUTDATED, SKIP
    """
    if is_skippable_field(key):
        return "SKIP", "non-translatable field"
    if is_skippable_value(english_value):
        return "SKIP", "non-translatable value"

    if not translated_value:
        return "MISSING", "no translation"

    en_text = english_value
    ar_text = translated_value

    if english_value.strip().startswith("{") and '"type"' in english_value:
        en_extracted = extract_text(english_value)
        ar_extracted = extract_text(translated_value)

        if en_extracted:
            en_text = en_extracted

        if ar_extracted:
            ar_text = ar_extracted
        elif translated_value.strip().startswith("{"):
            try:
                json.loads(translated_value)
            except (json.JSONDecodeError, TypeError):
                return "CORRUPTED_JSON", "invalid JSON in translation"

        # Also check if Arabic translation has raw JSON structure visible
        # (JSON is valid but text nodes contain JSON fragments)
        if ar_extracted and detect_raw_json(ar_extracted):
            return "CORRUPTED_JSON", "JSON structure leaked into translated text"

    # Also detect if any field's Arabic value contains raw JSON structure
    if translated_value and not translated_value.strip().startswith("{"):
        if detect_raw_json(translated_value):
            return "CORRUPTED_JSON", "raw JSON fragments in translation"

    en_clean = re.sub(r"<[^>]+>", " ", en_text)
    ar_clean = re.sub(r"<[^>]+>", " ", ar_text)
    en_clean = re.sub(r"\{[^}]*\}", " ", en_clean).strip()
    ar_clean = re.sub(r"\{[^}]*\}", " ", ar_clean).strip()

    if not en_clean:
        return "SKIP", "structural/CSS-only content"

    if translated_value == english_value:
        ar_chars, _ = count_chars(ar_clean)
        if ar_chars > 0:
            return "OK", "already in target language"
        return "IDENTICAL", "translation identical to source"

    ar_chars, lat_chars = count_chars(ar_clean)
    total = ar_chars + lat_chars

    if total == 0:
        return "OK", "no alpha content"
    if ar_chars == 0:
        return "NOT_ARABIC", "translation has no Arabic characters"

    is_mixed, lang = detect_mixed_language(ar_clean)
    if is_mixed:
        return "MIXED_LANGUAGE", f"significant {lang} text ({lat_chars} Latin / {ar_chars} Arabic)"

    if outdated:
        return "OUTDATED", "translation is outdated"

    return "OK", ""


def api_audit(client, resource_types=None, locale=ARABIC_LOCALE, verbose=False):
    """Scan all translatable resources via GraphQL.

    Returns (problems, stats) where problems is a list of
    {resource_id, resource_type, key, status, detail, english_full, digest}.
    """
    resource_types = resource_types or TRANSLATABLE_RESOURCE_TYPES
    query = TRANSLATABLE_RESOURCES_QUERY.replace("%LOCALE%", locale)

    all_problems = []
    total_stats = {
        "total": 0, "ok": 0, "missing": 0, "identical": 0,
        "not_arabic": 0, "mixed": 0, "corrupted": 0, "outdated": 0, "skip": 0,
    }

    for rtype in resource_types:
        print(f"\n  Scanning {rtype}...")
        problems = []
        stats = dict(total_stats)
        for k in stats:
            stats[k] = 0

        cursor = None
        page_num = 0
        n_resources = 0

        while True:
            page_num += 1
            try:
                data = client._graphql(query, {
                    "resourceType": rtype,
                    "first": 50,
                    "after": cursor,
                })
            except Exception as e:
                print(f"    ERROR on page {page_num}: {e}")
                break

            edges = data["translatableResources"]["edges"]
            page_info = data["translatableResources"]["pageInfo"]
            n_resources += len(edges)

            for edge in edges:
                node = edge["node"]
                resource_id = node["resourceId"]
                translations = {t["key"]: t for t in node["translations"]}

                for field in node["translatableContent"]:
                    key = field["key"]
                    value = field["value"] or ""
                    trans = translations.get(key)
                    ar_value = trans["value"] if trans else None
                    outdated = trans.get("outdated", False) if trans else False

                    status, detail = classify_translation(key, value, ar_value, outdated)

                    if status == "SKIP":
                        stats["skip"] += 1
                        continue

                    stats["total"] += 1
                    stat_key = {
                        "OK": "ok", "MISSING": "missing", "IDENTICAL": "identical",
                        "NOT_ARABIC": "not_arabic", "MIXED_LANGUAGE": "mixed",
                        "CORRUPTED_JSON": "corrupted", "OUTDATED": "outdated",
                    }.get(status, "ok")
                    stats[stat_key] += 1

                    if status != "OK":
                        problems.append({
                            "resource_id": resource_id,
                            "resource_type": rtype,
                            "key": key,
                            "status": status,
                            "detail": detail,
                            "english_full": value,  # Full value, not truncated
                            "arabic_preview": (ar_value or "")[:200],
                            "digest": field["digest"],
                        })

                        if verbose:
                            en_preview = value[:60]
                            if is_rich_text_json(value):
                                extracted = extract_text(value)
                                if extracted:
                                    en_preview = f"[json] {extracted[:55]}"
                            print(f"    [{status:15s}] {resource_id}")
                            print(f"      {key}: {en_preview}")

            if not page_info["hasNextPage"]:
                break
            cursor = page_info["endCursor"]
            time.sleep(0.3)

        all_problems.extend(problems)
        for k in total_stats:
            total_stats[k] += stats[k]

        pct = (stats["ok"] / stats["total"] * 100) if stats["total"] else 100
        n_problems = stats["total"] - stats["ok"]
        print(f"    {n_resources} resources | {stats['total']} fields | "
              f"{stats['ok']} OK ({pct:.0f}%) | {n_problems} problems")
        if n_problems:
            parts = []
            for key, label in [("missing", "missing"), ("identical", "identical"),
                               ("not_arabic", "not_translated"),
                               ("mixed", "mixed_lang"), ("corrupted", "corrupted_json"),
                               ("outdated", "outdated")]:
                if stats[key]:
                    parts.append(f"{label}={stats[key]}")
            print(f"    Breakdown: {', '.join(parts)}")

    return all_problems, total_stats


# ---------------------------------------------------------------------------
# Phase 3: Fix
# ---------------------------------------------------------------------------

def fetch_digests_batch(client, gids, locale):
    """Fetch translatable content + existing translations for a list of GIDs."""
    query = """
    query($resourceIds: [ID!]!, $first: Int!) {
      translatableResourcesByIds(resourceIds: $resourceIds, first: $first) {
        edges {
          node {
            resourceId
            translatableContent {
              key
              value
              digest
              locale
            }
            translations(locale: "%LOCALE%") {
              key
              value
            }
          }
        }
      }
    }
    """.replace("%LOCALE%", locale)

    result = {}
    for i in range(0, len(gids), 10):
        batch = gids[i:i + 10]
        try:
            data = client._graphql(query, {"resourceIds": batch, "first": len(batch)})
            for edge in data.get("translatableResourcesByIds", {}).get("edges", []):
                node = edge["node"]
                result[node["resourceId"]] = {
                    "content": {
                        tc["key"]: {"digest": tc["digest"], "value": tc["value"]}
                        for tc in node["translatableContent"]
                    },
                    "translations": {
                        t["key"]: t["value"]
                        for t in node["translations"]
                    },
                }
        except Exception as e:
            print(f"    Error fetching digests: {e}")
        time.sleep(0.3)
    return result


def upload_translations(client, gid, translations_input):
    """Upload translations one key at a time to avoid rate limits.

    Returns (uploaded, errors).
    """
    total_up = 0
    total_err = 0

    for idx, t in enumerate(translations_input):
        for attempt in range(4):
            try:
                data = client._graphql(REGISTER_TRANSLATIONS_MUTATION, {
                    "resourceId": gid,
                    "translations": [t],
                })
                user_errors = data.get("translationsRegister", {}).get("userErrors", [])

                if user_errors:
                    rate_limited = any(
                        "too many" in (ue.get("message", "") or "").lower()
                        for ue in user_errors
                    )
                    if rate_limited and attempt < 3:
                        wait = 2 ** (attempt + 1)
                        time.sleep(wait)
                        continue
                    for ue in user_errors:
                        print(f"      ERROR {t['key']}: {ue.get('message', ue)}")
                    total_err += 1
                else:
                    total_up += 1
                break

            except Exception as e:
                if attempt < 3:
                    time.sleep(2 ** (attempt + 1))
                else:
                    print(f"      ERROR {t['key']}: {e}")
                    total_err += 1
                    break

        # Brief pause every 10 keys
        if (idx + 1) % 10 == 0:
            time.sleep(0.5)

    return total_up, total_err


def _gid_to_identification(gid):
    """Convert a Shopify GID to CSV identification format.

    gid://shopify/Product/123456 → '123456
    """
    parts = gid.rsplit("/", 1)
    return f"'{parts[-1]}" if len(parts) == 2 else gid


def _gid_to_type(gid):
    """Convert a Shopify GID to CSV Type column.

    gid://shopify/Product/123 → PRODUCT
    gid://shopify/OnlineStoreTheme/123 → ONLINE_STORE_THEME
    gid://shopify/Metafield/123 → METAFIELD
    """
    parts = gid.split("/")
    if len(parts) >= 4:
        shopify_type = parts[3]
        # Convert CamelCase to UPPER_SNAKE
        result = re.sub(r"(?<=[a-z])(?=[A-Z])", "_", shopify_type).upper()
        # Special cases
        type_map = {
            "ONLINE_STORE_THEME": "ONLINE_STORE_THEME",
            "ONLINE_STORE_PAGE": "PAGE",
            "ONLINE_STORE_ARTICLE": "ARTICLE",
            "ONLINE_STORE_BLOG": "BLOG",
            "META_OBJECT": "METAOBJECT",
        }
        return type_map.get(result, result)
    return "UNKNOWN"


def fix_problems(client, engine, problems, locale=ARABIC_LOCALE, dry_run=False,
                 upload=False, csv_out=None):
    """Fix all identified problems by re-translating.

    Default mode: outputs a Shopify-format CSV for manual upload.
    With --upload: pushes directly via API.

    Returns (fixed_count, errors).
    """
    # Filter to fixable problems (skip OUTDATED — those just need source update)
    fixable_statuses = {"MISSING", "IDENTICAL", "NOT_ARABIC", "MIXED_LANGUAGE", "CORRUPTED_JSON"}
    fixable = [p for p in problems if p["status"] in fixable_statuses]

    if not fixable:
        print("\n  No fixable problems found!")
        return 0, 0

    # Group by resource
    by_resource = {}
    for p in fixable:
        rid = p["resource_id"]
        if p["key"] == "handle":
            continue  # skip handle fields
        by_resource.setdefault(rid, []).append(p)

    total_fields = sum(len(v) for v in by_resource.values())
    print(f"\n  Fixable: {total_fields} fields across {len(by_resource)} resources")
    print(f"  Breakdown:")
    status_counts = {}
    for p in fixable:
        status_counts[p["status"]] = status_counts.get(p["status"], 0) + 1
    for s, c in sorted(status_counts.items()):
        print(f"    {s}: {c}")

    # Fetch full English from Shopify API
    gid_list = list(by_resource.keys())
    print(f"\n  Fetching full content for {len(gid_list)} resources...")
    full_data = fetch_digests_batch(client, gid_list, locale)
    print(f"  Got data for {len(full_data)} resources")

    # Build fields for AI translation using full English from API
    fields_for_ai = []
    field_to_problem = {}
    for rid, items in by_resource.items():
        dm = full_data.get(rid, {})
        for item in items:
            field_id = f"{item['resource_type']}|{rid}|{item['key']}"

            # Use full English from API when available
            english = item["english_full"]
            if dm and "content" in dm:
                api_content = dm["content"].get(item["key"])
                if api_content and api_content.get("value"):
                    english = api_content["value"]

            if not english or not english.strip():
                continue

            fields_for_ai.append({"id": field_id, "value": english})
            field_to_problem[field_id] = item

    if not fields_for_ai:
        print("  Nothing to translate!")
        return 0, 0

    print(f"\n  Fields to translate: {len(fields_for_ai)}")

    if dry_run:
        print("\n  DRY RUN — preview of fields to fix:\n")
        for f in fields_for_ai[:25]:
            problem = field_to_problem[f["id"]]
            en_preview = f["value"][:60]
            if is_rich_text_json(f["value"]):
                extracted = extract_text(f["value"])
                if extracted:
                    en_preview = f"[rich_text {len(f['value'])}ch] {extracted[:45]}"
            print(f"    [{problem['status']:13s}] {problem['resource_id']}")
            print(f"      {problem['key']}: {en_preview}")
        if len(fields_for_ai) > 25:
            print(f"\n    ... and {len(fields_for_ai) - 25} more fields")
        return 0, 0

    # Translate (engine handles rich_text decomposition safely)
    print(f"\n  Translating {len(fields_for_ai)} fields...")
    t_map = engine.translate_fields(fields_for_ai)
    print(f"  Translated: {len(t_map)} fields")

    # Build translated rows
    csv_rows = []
    errors = 0

    for rid, items in by_resource.items():
        dm = full_data.get(rid, {})
        for item in items:
            field_id = f"{item['resource_type']}|{rid}|{item['key']}"
            ar_value = t_map.get(field_id)
            if not ar_value:
                continue

            # Get English from API for CSV
            english = item["english_full"]
            if dm and "content" in dm:
                api_content = dm["content"].get(item["key"])
                if api_content and api_content.get("value"):
                    english = api_content["value"]

            # Validate JSON
            stripped = ar_value.strip()
            if stripped.startswith('{"type"') or stripped.startswith("[{"):
                try:
                    parsed = json.loads(ar_value)
                    ar_value = json.dumps(parsed, ensure_ascii=False)
                except json.JSONDecodeError:
                    sanitized = sanitize(ar_value)
                    try:
                        json.loads(sanitized)
                        ar_value = sanitized
                    except (json.JSONDecodeError, TypeError):
                        print(f"      WARNING: Skipping invalid JSON for "
                              f"{rid} {item['key']} ({len(ar_value)} chars)")
                        errors += 1
                        continue

            csv_rows.append({
                "gid": rid,
                "type": _gid_to_type(rid),
                "identification": _gid_to_identification(rid),
                "field": item["key"],
                "english": english,
                "arabic": ar_value,
                "digest": dm.get("content", {}).get(item["key"], {}).get("digest", ""),
            })

    print(f"\n  Prepared {len(csv_rows)} translated rows ({errors} skipped)")

    # ---- CSV output (default) ----
    if not upload:
        csv_path = csv_out or os.path.join(OUTPUT_DIR, "audit_fix_translations.csv")
        os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Type", "Identification", "Field", "Locale",
                "Market", "Status", "Default content", "Translated content",
            ])
            for row in csv_rows:
                writer.writerow([
                    row["type"],
                    row["identification"],
                    row["field"],
                    locale,
                    "",  # Market
                    "",  # Status
                    row["english"],
                    row["arabic"],
                ])

        print(f"\n  CSV written: {csv_path}")
        print(f"  {len(csv_rows)} translations ready for Shopify upload")
        print(f"\n  To upload: Shopify Admin → Settings → Languages → Arabic → Import")
        return len(csv_rows), errors

    # ---- API upload (with --upload flag) ----
    print(f"\n  Uploading {len(csv_rows)} translations via API...")
    uploaded = 0

    # Group by GID for batched upload
    upload_by_gid = {}
    for row in csv_rows:
        upload_by_gid.setdefault(row["gid"], []).append({
            "locale": locale,
            "key": row["field"],
            "value": row["arabic"],
            "translatableContentDigest": row["digest"],
        })

    for gid, translations_input in upload_by_gid.items():
        u, e = upload_translations(client, gid, translations_input)
        uploaded += u
        errors += e
        time.sleep(0.3)

    print(f"\n  Upload complete: uploaded={uploaded}, errors={errors}")
    return uploaded, errors


# ---------------------------------------------------------------------------
# Phase 4: Visual Verify
# ---------------------------------------------------------------------------

def visual_verify(pages_with_issues, base_url, locale_prefix, headed=False):
    """Re-visit pages that had issues and check if they're fixed."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  Playwright not installed — skipping verification.")
        return

    if not pages_with_issues:
        print("  No pages to verify.")
        return

    urls = sorted(set(pages_with_issues))
    print(f"\n  Re-checking {len(urls)} pages...")

    still_broken = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headed)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="ar-SA",
        )
        page = context.new_page()

        for i, url in enumerate(urls):
            path = urlparse(url).path
            print(f"  [{i+1}/{len(urls)}] {path}")

            try:
                page.goto(url, wait_until="networkidle", timeout=30000)
                time.sleep(1)
                page.evaluate(EXPAND_ACCORDIONS_JS)
                time.sleep(0.8)
            except Exception as e:
                print(f"    ERROR: {e}")
                continue

            texts = page.evaluate(EXTRACT_VISIBLE_TEXT_JS)
            issues = []
            for t in texts:
                issue_type, detail = classify_visual_issue(t["text"])
                if issue_type:
                    issues.append({"type": issue_type, "text": t["text"][:80]})

            if issues:
                print(f"    STILL BROKEN: {len(issues)} issues")
                for iss in issues[:3]:
                    print(f"      [{iss['type']}] {iss['text']}")
                still_broken.append({"url": url, "issues": issues})
            else:
                print(f"    FIXED")

        browser.close()

    if still_broken:
        print(f"\n  {len(still_broken)} pages still have issues")
    else:
        print(f"\n  All {len(urls)} pages verified clean!")

    return still_broken


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Full audit & fix: Playwright visual + API audit + AI retranslation")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview what needs fixing without making changes")
    parser.add_argument("--skip-visual", action="store_true",
                        help="Skip Playwright visual crawl (API audit only)")
    parser.add_argument("--visual-only", action="store_true",
                        help="Playwright visual crawl only (no API audit or fix)")
    parser.add_argument("--verify", action="store_true",
                        help="Re-check previously broken pages")
    parser.add_argument("--type", default=None,
                        help="Audit only one resource type (PRODUCT, COLLECTION, etc.)")
    parser.add_argument("--max-pages", type=int, default=100,
                        help="Max pages for Playwright crawl (default: 100)")
    parser.add_argument("--screenshots", action="store_true",
                        help="Save screenshots during visual crawl")
    parser.add_argument("--headed", action="store_true",
                        help="Run browser in visible mode")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show every problem found")
    parser.add_argument("--locale", default=ARABIC_LOCALE,
                        help=f"Target locale (default: {ARABIC_LOCALE})")
    parser.add_argument("--base-url", default=BASE_URL,
                        help=f"Store base URL (default: {BASE_URL})")
    parser.add_argument("--model", default="gpt-5-nano",
                        help="OpenAI model for re-translation (default: gpt-5-nano)")
    parser.add_argument("--reasoning", default="minimal",
                        choices=["minimal", "low", "medium", "high"],
                        help="Reasoning effort (default: minimal)")
    parser.add_argument("--batch-size", type=int, default=80,
                        help="Fields per translation batch (default: 80)")
    parser.add_argument("--csv-out", default=None,
                        help="Output CSV path (default: Arabic/audit_fix_translations.csv)")
    parser.add_argument("--upload", action="store_true",
                        help="Upload via API instead of generating CSV")
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ.get("SAUDI_SHOP_URL")
    token = os.environ.get("SAUDI_ACCESS_TOKEN")

    if not args.visual_only:
        if not shop_url or not token:
            print("ERROR: Set SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN in .env")
            sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=" * 70)
    print("  FULL TRANSLATION AUDIT & FIX")
    print(f"  Store: {args.base_url}")
    print(f"  Locale: {args.locale}")
    mode = "DRY RUN" if args.dry_run else ("API UPLOAD" if args.upload else "CSV OUTPUT")
    print(f"  Mode: {mode}")
    print("=" * 70)

    visual_issues = []
    pages_visited = set()
    api_problems = []
    api_stats = {}

    # ------------------------------------------------------------------
    # Phase 1: Playwright Visual Crawl
    # ------------------------------------------------------------------
    if not args.skip_visual:
        print(f"\n{'=' * 70}")
        print("  PHASE 1: PLAYWRIGHT VISUAL CRAWL")
        print(f"{'=' * 70}")

        screenshots_dir = None
        if args.screenshots:
            screenshots_dir = os.path.join(OUTPUT_DIR, "screenshots")
            os.makedirs(screenshots_dir, exist_ok=True)

        visual_issues, pages_visited = visual_crawl(
            args.base_url, LOCALE_PREFIX,
            max_pages=args.max_pages,
            screenshots_dir=screenshots_dir,
            headed=args.headed,
        )

        # Summary
        if visual_issues:
            by_type = {}
            for iss in visual_issues:
                by_type.setdefault(iss["issue_type"], []).append(iss)
            print(f"\n  Visual crawl: {len(pages_visited)} pages, "
                  f"{len(visual_issues)} issues")
            for t, items in sorted(by_type.items()):
                print(f"    {t}: {len(items)}")
        else:
            print(f"\n  Visual crawl: {len(pages_visited)} pages, no issues!")

        # Save visual report
        visual_report = os.path.join(OUTPUT_DIR, "audit_visual.json")
        with open(visual_report, "w", encoding="utf-8") as f:
            json.dump({
                "pages_visited": len(pages_visited),
                "total_issues": len(visual_issues),
                "issues": visual_issues,
            }, f, ensure_ascii=False, indent=2)
        print(f"  Saved: {visual_report}")

    if args.visual_only:
        print(f"\n{'=' * 70}")
        print("  DONE (visual-only mode)")
        print(f"{'=' * 70}")
        return

    # ------------------------------------------------------------------
    # Phase 2: API Audit
    # ------------------------------------------------------------------
    print(f"\n{'=' * 70}")
    print("  PHASE 2: API AUDIT (all translatable fields)")
    print(f"{'=' * 70}")

    client = ShopifyClient(shop_url, token)

    resource_types = TRANSLATABLE_RESOURCE_TYPES
    if args.type:
        resource_types = [args.type.upper()]

    api_problems, api_stats = api_audit(
        client, resource_types, args.locale, verbose=args.verbose,
    )

    # Summary
    pct = (api_stats["ok"] / api_stats["total"] * 100) if api_stats["total"] else 100
    print(f"\n  API AUDIT SUMMARY:")
    print(f"    Total fields: {api_stats['total']}")
    print(f"    OK:           {api_stats['ok']} ({pct:.0f}%)")
    n_problems = api_stats["total"] - api_stats["ok"]
    print(f"    Problems:     {n_problems}")
    if n_problems:
        print(f"      Missing:        {api_stats['missing']}")
        print(f"      Identical:      {api_stats['identical']}")
        print(f"      Not translated: {api_stats['not_arabic']}")
        print(f"      Mixed language: {api_stats['mixed']}")
        print(f"      Corrupted JSON: {api_stats['corrupted']}")
        print(f"      Outdated:       {api_stats['outdated']}")
    print(f"    Skipped:      {api_stats['skip']} (non-translatable)")

    # Save API audit
    audit_report = os.path.join(OUTPUT_DIR, "audit_api.json")
    with open(audit_report, "w", encoding="utf-8") as f:
        json.dump(api_problems, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {audit_report}")

    # ------------------------------------------------------------------
    # Phase 3: Fix
    # ------------------------------------------------------------------
    if not api_problems:
        print(f"\n{'=' * 70}")
        print("  NO PROBLEMS TO FIX!")
        print(f"{'=' * 70}")
        return

    print(f"\n{'=' * 70}")
    print("  PHASE 3: AI RE-TRANSLATION + UPLOAD")
    print(f"{'=' * 70}")

    # Load translation engine
    prompt_path = None
    script_dir = os.path.dirname(__file__)
    for candidate in [
        os.path.join(script_dir, "Arabic", "tara_cached_developer_prompt.txt"),
        os.path.join(script_dir, "developer_prompt.txt"),
    ]:
        if os.path.exists(candidate):
            prompt_path = candidate
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

    uploaded, errors = fix_problems(
        client, engine, api_problems,
        locale=args.locale,
        dry_run=args.dry_run,
        upload=args.upload,
        csv_out=args.csv_out,
    )

    # ------------------------------------------------------------------
    # Phase 4: Visual Verify (if we had visual issues and made fixes)
    # ------------------------------------------------------------------
    if not args.dry_run and visual_issues and uploaded > 0 and args.verify:
        print(f"\n{'=' * 70}")
        print("  PHASE 4: VISUAL VERIFICATION")
        print(f"{'=' * 70}")

        pages_with_issues = [iss["url"] for iss in visual_issues]
        still_broken = visual_verify(
            pages_with_issues, args.base_url, LOCALE_PREFIX,
            headed=args.headed,
        )
        if still_broken:
            verify_report = os.path.join(OUTPUT_DIR, "audit_verify.json")
            with open(verify_report, "w", encoding="utf-8") as f:
                json.dump(still_broken, f, ensure_ascii=False, indent=2)
            print(f"  Saved: {verify_report}")

    # ------------------------------------------------------------------
    # Final Summary
    # ------------------------------------------------------------------
    print(f"\n{'=' * 70}")
    print("  FINAL SUMMARY")
    print(f"{'=' * 70}")
    if pages_visited:
        print(f"  Pages crawled:      {len(pages_visited)}")
        print(f"  Visual issues:      {len(visual_issues)}")
    print(f"  API fields scanned: {api_stats['total']}")
    print(f"  Problems found:     {api_stats['total'] - api_stats['ok']}")
    if args.dry_run:
        print(f"  (DRY RUN — no changes made)")
    elif args.upload:
        print(f"  Translations uploaded: {uploaded}")
        print(f"  Errors:               {errors}")
    else:
        csv_path = args.csv_out or os.path.join(OUTPUT_DIR, "audit_fix_translations.csv")
        print(f"  CSV rows written:     {uploaded}")
        print(f"  Skipped (bad JSON):   {errors}")
        print(f"  Output:               {csv_path}")
        print(f"\n  Upload: Shopify Admin → Settings → Languages → Arabic → Import")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
