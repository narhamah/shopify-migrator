#!/usr/bin/env python3
"""Review and fix English content on the Saudi Shopify store.

Connects directly to the Saudi store and audits all body_html content
(products, collections, pages, articles) for:
  1. Remaining Spanish text → translates to English via OpenAI
  2. Magento pagebuilder remnants → strips non-Shopify markup

Usage:
    python review_content.py --audit                    # Audit only, no changes
    python review_content.py --dry-run                  # Show planned changes
    python review_content.py                            # Apply fixes
    python review_content.py --type pages               # Only audit pages
    python review_content.py --type articles             # Only audit articles
    python review_content.py --skip-spanish              # Only strip Magento HTML
    python review_content.py --skip-magento              # Only fix Spanish
"""

import argparse
import json
import os
import re
import sys
import time

from dotenv import load_dotenv

from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.tools.patch_spanish import is_spanish

# ─────────────────────────────────────────────────────────────────────────────
# Magento Pagebuilder Detection & Stripping
# ─────────────────────────────────────────────────────────────────────────────

# Attributes that only appear in Magento PageBuilder HTML
_MAGENTO_ATTRS = re.compile(
    r'\s*(?:data-pb-style|data-content-type|data-appearance|data-element'
    r'|data-enable-parallax|data-parallax-speed|data-background-images'
    r'|data-background-type|data-video-loop|data-video-play-only-visible'
    r'|data-video-lazy-load|data-video-fallback-src|data-grid-size'
    r'|data-same-width|data-link-type|data-role|data-price-amount'
    r'|data-price-type|data-price-box|data-product-id|data-product-sku'
    r'|data-post|data-action|data-autoplay|data-autoplay-speed'
    r'|data-infinite-loop|data-show-arrows|data-show-dots'
    r'|data-carousel-mode|data-center-padding)="[^"]*"',
    re.IGNORECASE,
)

# Magento-specific CSS classes
_MAGENTO_CLASSES = re.compile(
    r'pagebuilder-|product-item-|widget-product-|price-container|'
    r'price-final_price|price-wrapper|tocart|towishlist|tocompare|'
    r'yotpo |bottomLine|bottomline-|columnGroup-root|column-root|'
    r'post-blogPostContent|row-contained-|row-root-|row-full-width|'
    r'text-root-|image-root-|image-img-|actions-primary|actions-secondary|'
    r'product-image-container|product-image-wrapper|product-image-photo|'
    r'block-with-products|productsCarousel|mage-',
    re.IGNORECASE,
)

# <style> blocks targeting Magento pagebuilder selectors
_MAGENTO_STYLE_RE = re.compile(
    r'<style[^>]*>(?:[^<]|<(?!/style))*?data-pb-style(?:[^<]|<(?!/style))*?</style>',
    re.IGNORECASE | re.DOTALL,
)

# Generic <style> blocks (often Magento inline styles for product containers)
_INLINE_STYLE_RE = re.compile(
    r'<style[^>]*>(?:[^<]|<(?!/style))*?\.product-image-container(?:[^<]|<(?!/style))*?</style>',
    re.IGNORECASE | re.DOTALL,
)

# <script> blocks (Magento init scripts, inline JS)
_SCRIPT_RE = re.compile(
    r'<script[^>]*>(?:[^<]|<(?!/script))*?</script>',
    re.IGNORECASE | re.DOTALL,
)

# Product carousel blocks (entire <ol class="product-items ..."> ... </ol>)
_PRODUCT_CAROUSEL_RE = re.compile(
    r'<ol\s+class="product-items[^"]*"[^>]*>.*?</ol>',
    re.IGNORECASE | re.DOTALL,
)

# Full Magento product block wrappers
_PRODUCT_BLOCK_RE = re.compile(
    r'<div\s+class="[^"]*productsCarousel[^"]*"[^>]*>.*?</div>\s*(?=</div>|$)',
    re.IGNORECASE | re.DOTALL,
)


def has_magento_remnants(html):
    """Check if HTML contains Magento pagebuilder markup."""
    if not html:
        return False
    return bool(
        _MAGENTO_ATTRS.search(html)
        or _MAGENTO_CLASSES.search(html)
        or _MAGENTO_STYLE_RE.search(html)
        or '<script type="text/x-magento-init">' in html
    )


def strip_magento_html(html):
    """Strip Magento pagebuilder remnants from HTML, keeping visible content.

    Strategy:
    1. Remove <style> blocks with data-pb-style selectors
    2. Remove all <script> blocks
    3. Remove product carousel blocks entirely
    4. Remove Magento data-* attributes from remaining elements
    5. Remove Magento-specific classes
    6. Clean up empty wrappers and whitespace
    """
    if not html:
        return html

    result = html

    # 1. Remove Magento <style> blocks
    result = _MAGENTO_STYLE_RE.sub('', result)
    result = _INLINE_STYLE_RE.sub('', result)

    # 2. Remove ALL <script> blocks
    result = _SCRIPT_RE.sub('', result)

    # 3. Remove product carousel blocks
    result = _PRODUCT_CAROUSEL_RE.sub('', result)

    # 4. Remove Magento data-* attributes
    result = _MAGENTO_ATTRS.sub('', result)

    # 5. Clean up Magento CSS classes from class attributes
    def _clean_classes(m):
        classes = m.group(1)
        # Remove Magento-specific classes
        cleaned = _MAGENTO_CLASSES.sub('', classes).strip()
        # Normalize whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        if not cleaned:
            return ''
        return f'class="{cleaned}"'

    result = re.compile(r'class="([^"]*)"').sub(_clean_classes, result)

    # 6. Remove empty tags left behind (empty divs, spans, figures, etc.)
    # Repeat a few times to handle nested empty tags
    for _ in range(5):
        prev = result
        # Remove empty tags (may contain only whitespace)
        result = re.sub(
            r'<(div|span|figure|ol|ul|li|strong|a|form|button)\b[^>]*>\s*</\1>',
            '', result, flags=re.IGNORECASE | re.DOTALL,
        )
        if result == prev:
            break

    # 7. Normalize whitespace
    result = re.sub(r'\n{3,}', '\n\n', result)
    result = re.sub(r'  +', ' ', result)
    result = result.strip()

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Spanish Content Detection & Translation
# ─────────────────────────────────────────────────────────────────────────────

def extract_visible_text(html):
    """Extract visible text from HTML for language detection."""
    if not html:
        return ""
    # Remove scripts and styles
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    # Remove tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode entities
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&nbsp;', ' ').replace('&#39;', "'").replace('&quot;', '"')
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def has_spanish_content(html):
    """Check if HTML body contains Spanish text."""
    visible = extract_visible_text(html)
    if not visible or len(visible) < 10:
        return False
    return is_spanish(visible)


def translate_spanish_to_english(html, client_openai, model="gpt-4o-mini"):
    """Translate Spanish content in HTML to English, preserving HTML structure.

    Only translates text nodes — HTML tags, attributes, URLs are preserved.
    """
    prompt = (
        "You are translating content for TARA, a luxury scalp-care brand.\n"
        "Translate the Spanish text in this HTML to English.\n"
        "RULES:\n"
        "- Preserve ALL HTML tags, attributes, URLs, and structure exactly\n"
        "- Only translate visible Spanish text to English\n"
        "- Keep brand names unchanged: TARA, Kansa Wand, Gua Sha\n"
        "- Keep product range names in English: Onion + Peptides, "
        "Rosemary + Peptides, Black Garlic + Ceramides, etc.\n"
        "- Keep INCI/scientific names unchanged\n"
        "- Use professional, direct tone (no hype, no fluff)\n"
        "- If text is already in English, return it unchanged\n"
        "- Return ONLY the HTML, no explanations\n\n"
        f"{html}"
    )
    try:
        resp = client_openai.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=16384,
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"    Translation error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Resource Fetching
# ─────────────────────────────────────────────────────────────────────────────

def fetch_all_resources(client):
    """Fetch all content resources from the store."""
    resources = {}

    print("Fetching products...")
    products = client.get_products()
    resources["products"] = [
        {"id": p["id"], "title": p.get("title", ""), "handle": p.get("handle", ""),
         "body_html": p.get("body_html", ""), "type": "product"}
        for p in products
    ]
    print(f"  {len(resources['products'])} products")

    print("Fetching collections...")
    collections = client.get_collections()
    resources["collections"] = [
        {"id": c["id"], "title": c.get("title", ""), "handle": c.get("handle", ""),
         "body_html": c.get("body_html", ""), "type": "collection"}
        for c in collections
    ]
    print(f"  {len(resources['collections'])} collections")

    print("Fetching pages...")
    pages = client.get_pages()
    resources["pages"] = [
        {"id": p["id"], "title": p.get("title", ""), "handle": p.get("handle", ""),
         "body_html": p.get("body_html", ""), "type": "page"}
        for p in pages
    ]
    print(f"  {len(resources['pages'])} pages")

    print("Fetching articles...")
    blogs = client.get_blogs()
    articles = []
    for blog in blogs:
        blog_articles = client.get_articles(blog["id"])
        for a in blog_articles:
            articles.append({
                "id": a["id"], "blog_id": blog["id"],
                "title": a.get("title", ""), "handle": a.get("handle", ""),
                "body_html": a.get("body_html", ""), "type": "article",
            })
    resources["articles"] = articles
    print(f"  {len(resources['articles'])} articles")

    return resources


# ─────────────────────────────────────────────────────────────────────────────
# Audit & Fix
# ─────────────────────────────────────────────────────────────────────────────

def audit_content(resources, skip_spanish=False, skip_magento=False):
    """Audit all resources for Spanish content and Magento remnants.

    Returns a list of findings: [{resource, issue, detail}, ...]
    """
    findings = []

    for resource_type, items in resources.items():
        for item in items:
            body = item.get("body_html", "")
            if not body:
                continue

            label = f"{resource_type}/{item['handle']} (id={item['id']})"

            if not skip_magento and has_magento_remnants(body):
                findings.append({
                    "resource_type": resource_type,
                    "item": item,
                    "issue": "magento",
                    "label": label,
                })

            if not skip_spanish and has_spanish_content(body):
                findings.append({
                    "resource_type": resource_type,
                    "item": item,
                    "issue": "spanish",
                    "label": label,
                })

    return findings


def apply_fixes(client, findings, dry_run=False, model="gpt-4o-mini"):
    """Apply fixes for all findings.

    Magento stripping is done locally (no AI needed).
    Spanish translation uses OpenAI.
    """
    if not findings:
        print("\nNo issues found — content is clean!")
        return

    # Group findings by resource to avoid double-updating
    by_resource = {}
    for f in findings:
        key = (f["resource_type"], f["item"]["id"])
        if key not in by_resource:
            by_resource[key] = {"item": f["item"], "issues": [], "resource_type": f["resource_type"]}
        by_resource[key]["issues"].append(f["issue"])

    openai_client = None
    fixed = 0
    failed = 0

    for (rtype, rid), info in by_resource.items():
        item = info["item"]
        issues = info["issues"]
        label = f"{rtype}/{item['handle']}"
        body = item["body_html"]

        print(f"\n  {label} — issues: {', '.join(issues)}")

        # Step 1: Strip Magento first (deterministic, no AI)
        if "magento" in issues:
            before_len = len(body)
            body = strip_magento_html(body)
            after_len = len(body)
            reduction = before_len - after_len
            pct = (reduction / before_len * 100) if before_len else 0
            print(f"    Stripped Magento: {before_len:,} → {after_len:,} chars ({pct:.0f}% reduction)")

        # Step 2: Translate Spanish (needs AI)
        if "spanish" in issues:
            # Re-check after Magento stripping (some Spanish may have been in Magento blocks)
            if has_spanish_content(body):
                if openai_client is None:
                    import openai
                    openai_client = openai.OpenAI()
                print(f"    Translating Spanish → English...")
                translated = translate_spanish_to_english(body, openai_client, model=model)
                if translated:
                    body = translated
                    print(f"    Translated OK ({len(body):,} chars)")
                else:
                    print(f"    Translation FAILED — skipping")
                    failed += 1
                    continue
            else:
                print(f"    Spanish was in Magento blocks (already stripped)")

        if dry_run:
            # Show a preview of the cleaned content
            visible = extract_visible_text(body)
            preview = visible[:200] + "..." if len(visible) > 200 else visible
            print(f"    [DRY RUN] Would update. Preview: {preview}")
            fixed += 1
            continue

        # Apply the update via Shopify REST API
        try:
            if rtype == "products":
                client.update_product(rid, {"body_html": body})
            elif rtype == "collections":
                # Determine if custom or smart
                try:
                    client._request("PUT", f"custom_collections/{rid}.json",
                                    json={"custom_collection": {"id": rid, "body_html": body}})
                except Exception:
                    client._request("PUT", f"smart_collections/{rid}.json",
                                    json={"smart_collection": {"id": rid, "body_html": body}})
            elif rtype == "pages":
                client._request("PUT", f"pages/{rid}.json",
                                json={"page": {"id": rid, "body_html": body}})
            elif rtype == "articles":
                blog_id = item.get("blog_id")
                client._request("PUT", f"blogs/{blog_id}/articles/{rid}.json",
                                json={"article": {"id": rid, "body_html": body}})
            print(f"    Updated on Shopify ✓")
            fixed += 1
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"    Update FAILED: {e}")
            failed += 1

    print(f"\n{'='*60}")
    print(f"Fixed: {fixed}  Failed: {failed}  Total: {len(by_resource)}")
    if dry_run:
        print("(Dry run — no changes were made)")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Review & fix English content on Saudi Shopify store")
    parser.add_argument("--audit", action="store_true",
                        help="Audit only — report issues without fixing")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show planned changes without applying")
    parser.add_argument("--type", choices=["products", "collections", "pages", "articles"],
                        help="Only audit a specific resource type")
    parser.add_argument("--skip-spanish", action="store_true",
                        help="Skip Spanish detection (only strip Magento)")
    parser.add_argument("--skip-magento", action="store_true",
                        help="Skip Magento stripping (only fix Spanish)")
    parser.add_argument("--model", default="gpt-4o-mini",
                        help="OpenAI model for Spanish→English translation (default: gpt-4o-mini)")
    parser.add_argument("--save-report", metavar="FILE",
                        help="Save audit report to JSON file")
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ.get("SAUDI_SHOP_URL")
    access_token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not access_token:
        print("ERROR: Set SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN in .env")
        sys.exit(1)

    client = ShopifyClient(shop_url, access_token)

    print("=" * 60)
    print("CONTENT REVIEWER — Saudi Store English Content")
    print("=" * 60)

    # Fetch resources
    resources = fetch_all_resources(client)

    # Filter by type if requested
    if args.type:
        resources = {args.type: resources.get(args.type, [])}

    # Audit
    print(f"\n{'='*60}")
    print("AUDIT RESULTS")
    print("=" * 60)

    findings = audit_content(resources,
                             skip_spanish=args.skip_spanish,
                             skip_magento=args.skip_magento)

    # Group for display
    magento_findings = [f for f in findings if f["issue"] == "magento"]
    spanish_findings = [f for f in findings if f["issue"] == "spanish"]

    if magento_findings:
        print(f"\nMagento pagebuilder remnants: {len(magento_findings)}")
        for f in magento_findings:
            body = f["item"]["body_html"]
            visible = extract_visible_text(body)
            print(f"  - {f['label']} ({len(body):,} chars, "
                  f"{len(visible):,} visible)")

    if spanish_findings:
        print(f"\nSpanish content detected: {len(spanish_findings)}")
        for f in spanish_findings:
            visible = extract_visible_text(f["item"]["body_html"])
            preview = visible[:100] + "..." if len(visible) > 100 else visible
            print(f"  - {f['label']}")
            print(f"    {preview}")

    if not findings:
        print("\nAll content is clean — no issues found!")
        return

    print(f"\nTotal issues: {len(findings)} "
          f"(Magento: {len(magento_findings)}, Spanish: {len(spanish_findings)})")

    # Save report if requested
    if args.save_report:
        report = [{
            "resource_type": f["resource_type"],
            "id": f["item"]["id"],
            "handle": f["item"]["handle"],
            "title": f["item"]["title"],
            "issue": f["issue"],
            "body_html_length": len(f["item"].get("body_html", "")),
        } for f in findings]
        with open(args.save_report, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
        print(f"\nReport saved to {args.save_report}")

    if args.audit:
        return

    # Fix
    print(f"\n{'='*60}")
    print("APPLYING FIXES" + (" (DRY RUN)" if args.dry_run else ""))
    print("=" * 60)

    apply_fixes(client, findings, dry_run=args.dry_run, model=args.model)


if __name__ == "__main__":
    main()
