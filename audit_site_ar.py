#!/usr/bin/env python3
"""Visual audit of Arabic site using Playwright.

Crawls the public Arabic storefront and finds visible non-Arabic text.
Takes screenshots and generates a report.

Prerequisites:
    pip install playwright
    playwright install chromium

Usage:
    python audit_site_ar.py                          # Audit all pages
    python audit_site_ar.py --url https://sa.taraformula.com/ar/products/repairing-hair-mask
    python audit_site_ar.py --screenshots             # Save page screenshots
    python audit_site_ar.py --json-out audit_visual.json
"""

import argparse
import json
import os
import re
import sys
import time
from urllib.parse import urljoin, urlparse

from dotenv import load_dotenv


def _is_arabic_text(text, min_ratio=0.4):
    """Check if visible text is sufficiently Arabic."""
    if not text or not text.strip():
        return True
    cleaned = text.strip()
    # Skip very short text (numbers, symbols, single words)
    if len(cleaned) < 3:
        return True
    # Skip known OK patterns
    ok_patterns = [
        r"^SAR\s", r"^\d+", r"^[A-Z]{2,5}$",  # currency, numbers, codes
        r"^(tara|TARA|Tara)$",  # brand name
        r"^©", r"^@",  # copyright, social
        r"^\+\d", r"^\d+\s?m[lL]",  # phone, measurements
        r"^ABG10\+®", r"^Capixyl™", r"^Procapil®", r"^Silverfree™",  # trademarked ingredients
        r"^INCI", r"^pH\s",  # scientific terms
    ]
    for pat in ok_patterns:
        if re.match(pat, cleaned):
            return True

    arabic = len(re.findall(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", cleaned))
    latin = len(re.findall(r"[a-zA-ZÀ-ÿ]", cleaned))
    total = arabic + latin

    if total == 0:
        return True  # no alpha chars
    if total < 3:
        return True  # too short to judge
    if latin == 0:
        return True

    return arabic / total >= min_ratio


def extract_visible_text(page):
    """Extract all visible text elements from the page."""
    return page.evaluate("""() => {
        const results = [];
        const seen = new Set();

        // Get all text-containing elements
        const elements = document.querySelectorAll(
            'h1, h2, h3, h4, h5, h6, p, span, a, button, label, li, td, th, ' +
            'div:not(:has(*:not(br):not(wbr))), ' +
            '[class*="title"], [class*="heading"], [class*="label"], [class*="badge"], ' +
            '[class*="tab"], [class*="accordion"], [class*="btn"]'
        );

        for (const el of elements) {
            // Skip hidden elements
            const style = window.getComputedStyle(el);
            if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') continue;
            if (el.offsetWidth === 0 && el.offsetHeight === 0) continue;

            // Get direct text content (not children)
            let text = '';
            for (const node of el.childNodes) {
                if (node.nodeType === Node.TEXT_NODE) {
                    text += node.textContent;
                }
            }
            text = text.trim();

            // Also check full textContent for leaf elements
            if (!text && el.children.length === 0) {
                text = el.textContent.trim();
            }

            if (!text || text.length < 2 || seen.has(text)) continue;
            seen.add(text);

            // Get element info
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
            const cls = el.className ? '.' + el.className.toString().split(' ').filter(c => c).slice(0, 2).join('.') : '';
            return tag + cls;
        }

        return results.filter(r => r.visible);
    }""")


def crawl_site(base_url, page, max_pages=100, screenshots_dir=None):
    """Crawl the Arabic site and find untranslated text."""
    visited = set()
    to_visit = [base_url]
    all_issues = []
    page_count = 0

    while to_visit and page_count < max_pages:
        url = to_visit.pop(0)
        if url in visited:
            continue

        # Only visit Arabic pages on the same domain
        parsed = urlparse(url)
        if "/ar" not in parsed.path and not parsed.path.endswith("/ar"):
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
            time.sleep(1)  # let dynamic content load

            # Expand accordions if any
            page.evaluate("""() => {
                document.querySelectorAll('[data-accordion], .accordion__trigger, details summary')
                    .forEach(el => el.click());
            }""")
            time.sleep(0.5)

        except Exception as e:
            print(f"    ERROR: {e}")
            continue

        # Screenshot
        if screenshots_dir:
            fname = short_path.strip("/").replace("/", "_") or "home"
            page.screenshot(path=os.path.join(screenshots_dir, f"{fname}.png"),
                           full_page=True)

        # Extract and analyze text
        texts = extract_visible_text(page)
        page_issues = []

        for t in texts:
            if not _is_arabic_text(t["text"]):
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
            print(f"    ✓ All text appears Arabic")

        all_issues.extend(page_issues)

        # Find links to other Arabic pages
        links = page.evaluate("""(baseUrl) => {
            const links = new Set();
            document.querySelectorAll('a[href]').forEach(a => {
                const href = a.href;
                if (href && href.includes('/ar') && href.startsWith(baseUrl)) {
                    // Skip anchors, query params, external
                    const clean = href.split('#')[0].split('?')[0];
                    if (clean !== baseUrl) links.add(clean);
                }
            });
            return [...links];
        }""", base_url.rstrip("/"))

        for link in links:
            if link not in visited:
                to_visit.append(link)

    return all_issues, visited


def main():
    parser = argparse.ArgumentParser(
        description="Visual audit of Arabic site using Playwright")
    parser.add_argument("--url", default=None,
                        help="Audit a specific URL (default: crawl entire site)")
    parser.add_argument("--base-url", default="https://sa.taraformula.com",
                        help="Base URL of the Arabic store")
    parser.add_argument("--max-pages", type=int, default=100,
                        help="Max pages to crawl (default: 100)")
    parser.add_argument("--screenshots", action="store_true",
                        help="Save full-page screenshots")
    parser.add_argument("--json-out", default="Arabic/audit_visual.json",
                        help="Output file for issues (default: Arabic/audit_visual.json)")
    parser.add_argument("--headless", action="store_true", default=True,
                        help="Run in headless mode (default: True)")
    parser.add_argument("--headed", action="store_true",
                        help="Run with visible browser")
    args = parser.parse_args()

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: Playwright not installed.")
        print("  pip install playwright")
        print("  playwright install chromium")
        sys.exit(1)

    load_dotenv()

    screenshots_dir = None
    if args.screenshots:
        screenshots_dir = os.path.join("Arabic", "screenshots")
        os.makedirs(screenshots_dir, exist_ok=True)

    headless = not args.headed

    print("=" * 70)
    print("  ARABIC SITE VISUAL AUDIT")
    print("=" * 70)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="ar-SA",
        )
        page = context.new_page()

        if args.url:
            # Single page audit
            print(f"\n  Auditing: {args.url}")
            try:
                page.goto(args.url, wait_until="networkidle", timeout=30000)
                time.sleep(1)
            except Exception as e:
                print(f"  ERROR: {e}")
                browser.close()
                sys.exit(1)

            texts = extract_visible_text(page)
            issues = []
            for t in texts:
                if not _is_arabic_text(t["text"]):
                    issues.append({
                        "url": args.url,
                        "text": t["text"],
                        "tag": t["tag"],
                        "selector": t["selector"],
                        "classes": t["classes"],
                    })

            if issues:
                print(f"\n  Found {len(issues)} untranslated texts:\n")
                for issue in issues:
                    print(f"    [{issue['tag']:6s}] {issue['text'][:100]}")
            else:
                print("\n  ✓ All text appears Arabic")

            visited = {args.url}
        else:
            # Full site crawl
            start_url = f"{args.base_url.rstrip('/')}/ar"
            print(f"\n  Starting from: {start_url}")
            print(f"  Max pages: {args.max_pages}")

            issues, visited = crawl_site(
                args.base_url.rstrip("/"), page,
                max_pages=args.max_pages,
                screenshots_dir=screenshots_dir,
            )

        browser.close()

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  VISUAL AUDIT COMPLETE")
    print(f"  Pages visited: {len(visited)}")
    print(f"  Untranslated texts found: {len(issues)}")
    print(f"{'=' * 70}")

    if issues:
        # Group by page
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
    if args.json_out:
        os.makedirs(os.path.dirname(args.json_out) or ".", exist_ok=True)
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump({
                "pages_visited": len(visited),
                "total_issues": len(issues),
                "issues": issues,
            }, f, ensure_ascii=False, indent=2)
        print(f"\n  Results saved to: {args.json_out}")


if __name__ == "__main__":
    main()
