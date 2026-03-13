#!/usr/bin/env python3
"""Visual audit of a translated Shopify storefront using Playwright.

Crawls the public storefront looking for visible non-translated text.
Takes screenshots and generates a JSON report.

Works for any locale — just specify the base URL and locale path prefix.

Prerequisites:
    pip install playwright
    playwright install chromium

Usage:
    python audit_site.py --base-url https://sa.taraformula.com --locale-prefix /ar
    python audit_site.py --base-url https://es.taraformula.com --locale-prefix /es
    python audit_site.py --url https://sa.taraformula.com/ar/products/some-product
    python audit_site.py --screenshots --max-pages 20
    python audit_site.py --json-out output/audit_visual.json
    python audit_site.py --brand-name Tara
"""

import argparse
import json
import os
import re
import sys
import time
from urllib.parse import urlparse

from dotenv import load_dotenv

from tara_migrate.core.language import is_arabic_visible_text


# ---------------------------------------------------------------------------
# Text extraction (runs in browser via Playwright)
# ---------------------------------------------------------------------------

EXTRACT_VISIBLE_TEXT_JS = """() => {
    const results = [];
    const seen = new Set();

    const elements = document.querySelectorAll(
        'h1, h2, h3, h4, h5, h6, p, span, a, button, label, li, td, th, ' +
        'div:not(:has(*:not(br):not(wbr))), ' +
        '[class*="title"], [class*="heading"], [class*="label"], [class*="badge"], ' +
        '[class*="tab"], [class*="accordion"], [class*="btn"]'
    );

    for (const el of elements) {
        const style = window.getComputedStyle(el);
        if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') continue;
        if (el.offsetWidth === 0 && el.offsetHeight === 0) continue;

        let text = '';
        for (const node of el.childNodes) {
            if (node.nodeType === Node.TEXT_NODE) {
                text += node.textContent;
            }
        }
        text = text.trim();

        if (!text && el.children.length === 0) {
            text = el.textContent.trim();
        }

        if (!text || text.length < 2 || seen.has(text)) continue;
        seen.add(text);

        const rect = el.getBoundingClientRect();
        const tag = el.tagName.toLowerCase();
        const classes = el.className ? el.className.toString().substring(0, 100) : '';

        results.push({
            text: text.substring(0, 300),
            tag: tag,
            classes: classes,
            selector: getSelector(el),
            x: Math.round(rect.x),
            y: Math.round(rect.y),
            visible: rect.width > 0 && rect.height > 0 && rect.y < window.innerHeight * 3,
        });
    }

    function getSelector(el) {
        if (el.id) return '#' + el.id;
        const tag = el.tagName.toLowerCase();
        const cls = el.className
            ? '.' + el.className.toString().split(' ').filter(c => c).slice(0, 2).join('.')
            : '';
        return tag + cls;
    }

    return results.filter(r => r.visible);
}"""

EXPAND_ACCORDIONS_JS = """() => {
    document.querySelectorAll('[data-accordion], .accordion__trigger, details summary')
        .forEach(el => el.click());
}"""

EXTRACT_LINKS_JS = """(args) => {
    const [domain, localePrefix] = args;
    const links = new Set();
    document.querySelectorAll('a[href]').forEach(a => {
        const href = a.href;
        if (href && href.includes(localePrefix) && href.includes(domain)) {
            const clean = href.split('#')[0].split('?')[0];
            links.add(clean);
        }
    });
    return [...links];
}"""


# ---------------------------------------------------------------------------
# Crawl engine
# ---------------------------------------------------------------------------

def crawl_site(base_url, locale_prefix, page, max_pages=100,
               screenshots_dir=None, ok_patterns=None, brand_name=None):
    """Crawl the translated site and find untranslated text.

    Args:
        base_url: Full base URL including locale, e.g. https://sa.taraformula.com/ar
        locale_prefix: The locale path prefix, e.g. "/ar"
        page: Playwright Page instance.
        max_pages: Maximum pages to crawl.
        screenshots_dir: Directory to save screenshots (None to skip).
        ok_patterns: Additional regex patterns to whitelist for visible text.
        brand_name: Brand name to whitelist (e.g. "Tara").
    """
    visited = set()
    ar_home = base_url.rstrip("/")
    to_visit = [
        ar_home,
        ar_home + "/collections/all",
        ar_home + "/collections",
    ]
    all_issues = []
    page_count = 0
    domain = urlparse(ar_home).netloc

    # Build brand-specific OK patterns
    extra_patterns = list(ok_patterns or [])
    if brand_name:
        escaped = re.escape(brand_name)
        extra_patterns.append(f"^({escaped}|{escaped.upper()}|{escaped.lower()})$")

    while to_visit and page_count < max_pages:
        url = to_visit.pop(0)
        if url in visited:
            continue

        parsed = urlparse(url)
        if parsed.netloc != domain:
            continue
        if locale_prefix not in parsed.path:
            continue

        visited.add(url)
        page_count += 1
        short_path = parsed.path

        print(f"\n  [{page_count}] {short_path}")

        try:
            response = page.goto(url, wait_until="networkidle", timeout=30000)
            if not response or response.status >= 400:
                print(f"    HTTP {response.status if response else 'no response'}")
                continue
            time.sleep(1)

            # Expand accordions
            page.evaluate(EXPAND_ACCORDIONS_JS)
            time.sleep(0.5)

        except Exception as e:
            print(f"    ERROR: {e}")
            continue

        # Screenshot
        if screenshots_dir:
            fname = short_path.strip("/").replace("/", "_") or "home"
            if len(fname) > 100:
                fname = fname[:100]
            try:
                page.screenshot(
                    path=os.path.join(screenshots_dir, f"{fname}.png"),
                    full_page=True,
                )
            except Exception as e:
                print(f"    Screenshot error: {e}")

        # Extract and analyze text
        texts = page.evaluate(EXTRACT_VISIBLE_TEXT_JS)
        page_issues = []

        for t in texts:
            if not is_arabic_visible_text(t["text"], ok_patterns=extra_patterns):
                page_issues.append({
                    "url": url,
                    "path": short_path,
                    "text": t["text"],
                    "tag": t["tag"],
                    "selector": t["selector"],
                    "classes": t["classes"],
                    "position": f"({t['x']}, {t['y']})",
                })

        if page_issues:
            print(f"    Found {len(page_issues)} untranslated texts:")
            for issue in page_issues[:5]:
                print(f"      [{issue['tag']}] {issue['text'][:80]}")
            if len(page_issues) > 5:
                print(f"      ... and {len(page_issues) - 5} more")
        else:
            print(f"    All text appears translated")

        all_issues.extend(page_issues)

        # Find links
        links = page.evaluate(EXTRACT_LINKS_JS, [domain, locale_prefix])
        for link in links:
            if link not in visited:
                to_visit.append(link)

    return all_issues, visited


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Visual audit of a translated Shopify storefront")
    parser.add_argument("--url", default=None,
                        help="Audit a specific URL")
    parser.add_argument("--base-url", default="https://sa.taraformula.com",
                        help="Base URL of the store (default: sa.taraformula.com)")
    parser.add_argument("--locale-prefix", default="/ar",
                        help="Locale path prefix (default: /ar)")
    parser.add_argument("--max-pages", type=int, default=100,
                        help="Max pages to crawl (default: 100)")
    parser.add_argument("--screenshots", action="store_true",
                        help="Save full-page screenshots")
    parser.add_argument("--screenshots-dir", default=None,
                        help="Screenshots directory (default: <locale>/screenshots)")
    parser.add_argument("--json-out", default=None,
                        help="Output JSON file (default: <locale>/audit_visual.json)")
    parser.add_argument("--headed", action="store_true",
                        help="Run with visible browser")
    parser.add_argument("--brand-name", default=None,
                        help="Brand name to whitelist (e.g. 'Tara')")
    parser.add_argument("--ok-patterns", nargs="*", default=None,
                        help="Additional regex patterns to whitelist")
    args = parser.parse_args()

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: Playwright not installed.")
        print("  pip install playwright")
        print("  playwright install chromium")
        sys.exit(1)

    load_dotenv()

    # Determine output directory from locale prefix
    locale_name = args.locale_prefix.strip("/").capitalize() or "translations"
    screenshots_dir = None
    if args.screenshots:
        screenshots_dir = args.screenshots_dir or os.path.join(locale_name, "screenshots")
        os.makedirs(screenshots_dir, exist_ok=True)

    json_out = args.json_out or os.path.join(locale_name, "audit_visual.json")

    # Determine browser locale from prefix
    locale_map = {
        "/ar": "ar-SA", "/es": "es-ES", "/fr": "fr-FR",
        "/de": "de-DE", "/ja": "ja-JP", "/zh": "zh-CN",
    }
    browser_locale = locale_map.get(args.locale_prefix, "en-US")

    print("=" * 70)
    print(f"  VISUAL TRANSLATION AUDIT ({args.locale_prefix})")
    print("=" * 70)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headed)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale=browser_locale,
        )
        page = context.new_page()

        if args.url:
            print(f"\n  Auditing: {args.url}")
            try:
                page.goto(args.url, wait_until="networkidle", timeout=30000)
                time.sleep(1)
            except Exception as e:
                print(f"  ERROR: {e}")
                browser.close()
                sys.exit(1)

            texts = page.evaluate(EXTRACT_VISIBLE_TEXT_JS)
            issues = []
            for t in texts:
                if not is_arabic_visible_text(t["text"],
                                              ok_patterns=args.ok_patterns):
                    issues.append({
                        "url": args.url, "text": t["text"],
                        "tag": t["tag"], "selector": t["selector"],
                        "classes": t["classes"],
                    })

            if issues:
                print(f"\n  Found {len(issues)} untranslated texts:\n")
                for issue in issues:
                    print(f"    [{issue['tag']:6s}] {issue['text'][:100]}")
            else:
                print("\n  All text appears translated")

            visited = {args.url}
        else:
            start_url = f"{args.base_url.rstrip('/')}{args.locale_prefix}"
            print(f"\n  Starting from: {start_url}")
            print(f"  Max pages: {args.max_pages}")

            issues, visited = crawl_site(
                start_url, args.locale_prefix, page,
                max_pages=args.max_pages,
                screenshots_dir=screenshots_dir,
                ok_patterns=args.ok_patterns,
                brand_name=args.brand_name,
            )

        browser.close()

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  VISUAL AUDIT COMPLETE")
    print(f"  Pages visited: {len(visited)}")
    print(f"  Untranslated texts found: {len(issues)}")
    print(f"{'=' * 70}")

    if issues:
        by_page = {}
        for issue in issues:
            path = urlparse(issue["url"]).path
            if path not in by_page:
                by_page[path] = []
            by_page[path].append(issue)

        print("\n  Issues by page:")
        for path, page_issues in sorted(by_page.items()):
            print(f"    {path}: {len(page_issues)} untranslated texts")

    # Save results
    os.makedirs(os.path.dirname(json_out) or ".", exist_ok=True)
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump({
            "locale_prefix": args.locale_prefix,
            "pages_visited": len(visited),
            "total_issues": len(issues),
            "issues": issues,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n  Results saved to: {json_out}")


if __name__ == "__main__":
    main()
