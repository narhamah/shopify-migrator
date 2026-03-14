#!/usr/bin/env python3
"""Review and fix English content on the Saudi Shopify store.

Connects directly to the Saudi store and audits ALL content for:
  1. Remaining Spanish text → translates to English via OpenAI
  2. Magento pagebuilder remnants → strips non-Shopify markup

Content checked:
  - body_html on products, collections, pages, articles
  - titles on products, collections, pages, articles
  - product metafields (tagline, short_description, accordion content, SEO tags)
  - article metafields (blog_summary, hero_caption, short_title)
  - metaobjects: benefit, faq_entry, ingredient, blog_author fields

Usage:
    python review_content.py --audit                    # Audit only, no changes
    python review_content.py --dry-run                  # Show planned changes
    python review_content.py                            # Apply fixes
    python review_content.py --type pages               # Only audit pages
    python review_content.py --type metaobjects          # Only audit metaobjects
    python review_content.py --skip-spanish              # Only strip Magento HTML
    python review_content.py --skip-magento              # Only fix Spanish
"""

import argparse
import json
import os
import re
import sys
import time

import anthropic
from dotenv import load_dotenv

from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.tools.patch_spanish import is_spanish
from tara_migrate.translation.translator import (
    ARTICLE_TRANSLATABLE_METAFIELDS,
    METAOBJECT_TRANSLATABLE_FIELDS,
    PRODUCT_TRANSLATABLE_METAFIELDS,
)

# Global audit model client (lazy-initialized)
_audit_client = None
_audit_model = None


def _get_audit_client():
    """Lazy-initialize the Anthropic client for audit."""
    global _audit_client
    if _audit_client is None:
        _audit_client = anthropic.Anthropic()
    return _audit_client


def _ai_is_spanish(text, model="claude-haiku-4-5-20251001"):
    """Use Claude to detect if text contains Spanish content.

    Returns True if the text contains Spanish that should be translated.
    Falls back to regex detection if the API call fails.
    """
    if not text or len(text) < 10:
        return False

    # Quick regex pre-filter: skip text that's clearly not Spanish
    # (pure ASCII with no Spanish markers at all)
    if not is_spanish(text) and not re.search(r'[áéíóúñü¿¡]', text, re.IGNORECASE):
        return False

    try:
        client = _get_audit_client()
        resp = client.messages.create(
            model=model,
            max_tokens=10,
            messages=[{"role": "user", "content": (
                "Is the following text in Spanish (or does it contain Spanish that "
                "should be translated to English)? Ignore brand names like TARA, "
                "Kansa Wand, Gua Sha, and INCI/scientific names.\n"
                "Reply ONLY with YES or NO.\n\n"
                f"{text[:2000]}"
            )}],
        )
        answer = resp.content[0].text.strip().upper()
        return answer.startswith("YES")
    except Exception as e:
        print(f"    Audit model error, falling back to regex: {e}")
        return is_spanish(text)

# Metafield types that contain translatable text
TEXT_METAFIELD_TYPES = {
    "single_line_text_field",
    "multi_line_text_field",
    "rich_text_field",
}

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


def extract_text_from_rich_text_json(value):
    """Extract visible text from a rich_text_field JSON value."""
    if not value:
        return ""
    try:
        data = json.loads(value) if isinstance(value, str) else value
    except (json.JSONDecodeError, TypeError):
        return value if isinstance(value, str) else ""

    texts = []

    def _walk(node):
        if isinstance(node, str):
            texts.append(node)
            return
        if isinstance(node, dict):
            if "value" in node and isinstance(node["value"], str):
                texts.append(node["value"])
            for child in node.get("children", []):
                _walk(child)
        if isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(data)
    return " ".join(texts).strip()


def has_spanish_text(text):
    """Check if plain text contains Spanish using AI audit model."""
    if not text or len(text) < 10:
        return False
    if _audit_model:
        return _ai_is_spanish(text, model=_audit_model)
    return is_spanish(text)


def has_spanish_content(html):
    """Check if HTML body contains Spanish text using AI audit model."""
    visible = extract_visible_text(html)
    if not visible or len(visible) < 10:
        return False
    if _audit_model:
        return _ai_is_spanish(visible, model=_audit_model)
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


def translate_plain_text(text, client_openai, model="gpt-4o-mini"):
    """Translate a plain Spanish text string to English."""
    prompt = (
        "Translate this Spanish text to English for TARA, a luxury scalp-care brand.\n"
        "RULES:\n"
        "- Keep brand names unchanged: TARA, Kansa Wand, Gua Sha\n"
        "- Keep INCI/scientific names unchanged\n"
        "- Use professional, direct tone\n"
        "- Return ONLY the translation, no explanations\n\n"
        f"{text}"
    )
    try:
        resp = client_openai.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=4096,
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"    Translation error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Resource Fetching
# ─────────────────────────────────────────────────────────────────────────────

def _extract_text_for_check(value, mf_type):
    """Extract checkable text from a metafield value based on its type."""
    if not value:
        return ""
    if mf_type == "rich_text_field":
        return extract_text_from_rich_text_json(value)
    return value


def _fetch_metafields_for_resource(client, resource_type, resource_id, translatable_keys):
    """Fetch metafields for a resource, returning only translatable text fields."""
    metafields = client.get_metafields(resource_type, resource_id)
    result = []
    for mf in metafields:
        ns = mf.get("namespace", "")
        key = mf.get("key", "")
        full_key = f"{ns}.{key}"
        mf_type = mf.get("type", "")
        if full_key in translatable_keys and mf_type in TEXT_METAFIELD_TYPES:
            result.append({
                "id": mf["id"],
                "key": full_key,
                "value": mf.get("value", ""),
                "type": mf_type,
                "namespace": ns,
                "bare_key": key,
            })
    return result


def fetch_all_resources(client):
    """Fetch all content resources from the store, including metafields and metaobjects."""
    resources = {}

    # --- Products (body_html + title + metafields) ---
    print("Fetching products...")
    products = client.get_products()
    product_items = []
    for p in products:
        item = {
            "id": p["id"], "title": p.get("title", ""), "handle": p.get("handle", ""),
            "body_html": p.get("body_html", ""), "type": "product",
        }
        # Fetch metafields
        mfs = _fetch_metafields_for_resource(client, "products", p["id"],
                                             PRODUCT_TRANSLATABLE_METAFIELDS)
        item["metafields"] = mfs
        product_items.append(item)
    resources["products"] = product_items
    mf_count = sum(len(p["metafields"]) for p in product_items)
    print(f"  {len(product_items)} products ({mf_count} text metafields)")

    # --- Collections (body_html + title) ---
    print("Fetching collections...")
    collections = client.get_collections()
    resources["collections"] = [
        {"id": c["id"], "title": c.get("title", ""), "handle": c.get("handle", ""),
         "body_html": c.get("body_html", ""), "type": "collection", "metafields": []}
        for c in collections
    ]
    print(f"  {len(resources['collections'])} collections")

    # --- Pages (body_html + title) ---
    print("Fetching pages...")
    pages = client.get_pages()
    resources["pages"] = [
        {"id": p["id"], "title": p.get("title", ""), "handle": p.get("handle", ""),
         "body_html": p.get("body_html", ""), "type": "page", "metafields": []}
        for p in pages
    ]
    print(f"  {len(resources['pages'])} pages")

    # --- Articles (body_html + title + metafields) ---
    print("Fetching articles...")
    blogs = client.get_blogs()
    articles = []
    for blog in blogs:
        blog_articles = client.get_articles(blog["id"])
        for a in blog_articles:
            item = {
                "id": a["id"], "blog_id": blog["id"],
                "title": a.get("title", ""), "handle": a.get("handle", ""),
                "body_html": a.get("body_html", ""), "type": "article",
            }
            mfs = _fetch_metafields_for_resource(client, "articles", a["id"],
                                                 ARTICLE_TRANSLATABLE_METAFIELDS)
            item["metafields"] = mfs
            articles.append(item)
    resources["articles"] = articles
    mf_count = sum(len(a["metafields"]) for a in articles)
    print(f"  {len(articles)} articles ({mf_count} text metafields)")

    # --- Metaobjects ---
    print("Fetching metaobjects...")
    all_metaobjects = []
    for mo_type, translatable_fields in METAOBJECT_TRANSLATABLE_FIELDS.items():
        try:
            metaobjects = client.get_metaobjects(mo_type)
        except Exception as e:
            print(f"  Warning: Could not fetch {mo_type}: {e}")
            continue
        for mo in metaobjects:
            text_fields = []
            for field in mo.get("fields", []):
                if field["key"] in translatable_fields and field.get("value"):
                    text_fields.append({
                        "key": field["key"],
                        "value": field["value"],
                        "type": field.get("type", "single_line_text_field"),
                    })
            all_metaobjects.append({
                "id": mo["id"],
                "handle": mo.get("handle", ""),
                "title": mo.get("handle", ""),  # metaobjects use handle as label
                "type": "metaobject",
                "metaobject_type": mo_type,
                "body_html": "",
                "text_fields": text_fields,
                "metafields": [],
            })
        print(f"  {len(metaobjects)} {mo_type} metaobjects")
    resources["metaobjects"] = all_metaobjects

    return resources


# ─────────────────────────────────────────────────────────────────────────────
# Audit & Fix
# ─────────────────────────────────────────────────────────────────────────────

def audit_content(resources, skip_spanish=False, skip_magento=False):
    """Audit all resources for Spanish content and Magento remnants.

    Checks:
      - body_html for Magento remnants and Spanish text
      - title for Spanish text
      - metafield values for Spanish text and Magento remnants (rich_text)
      - metaobject text_fields for Spanish text

    Returns a list of findings: [{resource_type, item, issue, field, label}, ...]
    """
    findings = []

    for resource_type, items in resources.items():
        for item in items:
            handle = item.get("handle", item.get("id", "?"))
            item_id = item.get("id", "?")
            base_label = f"{resource_type}/{handle} (id={item_id})"

            # --- Check body_html ---
            body = item.get("body_html", "")
            if body:
                if not skip_magento and has_magento_remnants(body):
                    findings.append({
                        "resource_type": resource_type,
                        "item": item,
                        "issue": "magento",
                        "field": "body_html",
                        "label": f"{base_label} [body_html]",
                    })

                if not skip_spanish and has_spanish_content(body):
                    findings.append({
                        "resource_type": resource_type,
                        "item": item,
                        "issue": "spanish",
                        "field": "body_html",
                        "label": f"{base_label} [body_html]",
                    })

            # --- Check title ---
            title = item.get("title", "")
            if title and not skip_spanish and has_spanish_text(title):
                findings.append({
                    "resource_type": resource_type,
                    "item": item,
                    "issue": "spanish",
                    "field": "title",
                    "label": f"{base_label} [title]",
                })

            # --- Check metafields (products, articles) ---
            for mf in item.get("metafields", []):
                mf_value = mf.get("value", "")
                if not mf_value:
                    continue
                mf_type = mf.get("type", "")
                text = _extract_text_for_check(mf_value, mf_type)

                if not skip_magento and mf_type == "rich_text_field" and has_magento_remnants(mf_value):
                    findings.append({
                        "resource_type": resource_type,
                        "item": item,
                        "issue": "magento",
                        "field": f"metafield:{mf['key']}",
                        "label": f"{base_label} [{mf['key']}]",
                        "metafield": mf,
                    })

                if not skip_spanish and has_spanish_text(text):
                    findings.append({
                        "resource_type": resource_type,
                        "item": item,
                        "issue": "spanish",
                        "field": f"metafield:{mf['key']}",
                        "label": f"{base_label} [{mf['key']}]",
                        "metafield": mf,
                    })

            # --- Check metaobject text_fields ---
            for tf in item.get("text_fields", []):
                tf_value = tf.get("value", "")
                if not tf_value:
                    continue
                tf_type = tf.get("type", "")
                text = _extract_text_for_check(tf_value, tf_type)

                if not skip_magento and tf_type == "rich_text_field" and has_magento_remnants(tf_value):
                    findings.append({
                        "resource_type": resource_type,
                        "item": item,
                        "issue": "magento",
                        "field": f"text_field:{tf['key']}",
                        "label": f"{base_label} [{tf['key']}]",
                        "text_field": tf,
                    })

                if not skip_spanish and has_spanish_text(text):
                    findings.append({
                        "resource_type": resource_type,
                        "item": item,
                        "issue": "spanish",
                        "field": f"text_field:{tf['key']}",
                        "label": f"{base_label} [{tf['key']}]",
                        "text_field": tf,
                    })

    return findings


def apply_fixes(client, findings, dry_run=False, model="gpt-4o-mini"):
    """Apply fixes for all findings.

    Handles body_html, titles, metafields, and metaobject fields.
    Magento stripping is done locally (no AI needed).
    Spanish translation uses OpenAI.
    """
    if not findings:
        print("\nNo issues found — content is clean!")
        return

    # Group findings by (resource_type, id, field) to avoid double-updating
    by_target = {}
    for f in findings:
        key = (f["resource_type"], f["item"]["id"], f["field"])
        if key not in by_target:
            by_target[key] = {
                "item": f["item"],
                "issues": [],
                "resource_type": f["resource_type"],
                "field": f["field"],
                "finding": f,
            }
        by_target[key]["issues"].append(f["issue"])

    openai_client = None
    fixed = 0
    failed = 0

    for (rtype, rid, field), info in by_target.items():
        item = info["item"]
        issues = info["issues"]
        finding = info["finding"]
        label = f"{rtype}/{item.get('handle', rid)} [{field}]"

        print(f"\n  {label} — issues: {', '.join(issues)}")

        # ── body_html fixes ──
        if field == "body_html":
            body = item["body_html"]

            if "magento" in issues:
                before_len = len(body)
                body = strip_magento_html(body)
                after_len = len(body)
                reduction = before_len - after_len
                pct = (reduction / before_len * 100) if before_len else 0
                print(f"    Stripped Magento: {before_len:,} -> {after_len:,} chars ({pct:.0f}% reduction)")

            if "spanish" in issues:
                if has_spanish_content(body):
                    if openai_client is None:
                        import openai
                        openai_client = openai.OpenAI()
                    print(f"    Translating Spanish -> English...")
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
                visible = extract_visible_text(body)
                preview = visible[:200] + "..." if len(visible) > 200 else visible
                print(f"    [DRY RUN] Would update. Preview: {preview}")
                fixed += 1
                continue

            try:
                if rtype == "products":
                    client.update_product(rid, {"body_html": body})
                elif rtype == "collections":
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
                print(f"    Updated on Shopify")
                fixed += 1
                time.sleep(0.5)
            except Exception as e:
                print(f"    Update FAILED: {e}")
                failed += 1

        # ── title fixes ──
        elif field == "title":
            title = item["title"]
            if "spanish" in issues:
                if openai_client is None:
                    import openai
                    openai_client = openai.OpenAI()
                print(f"    Translating title: {title}")
                translated = translate_plain_text(title, openai_client, model=model)
                if not translated:
                    print(f"    Translation FAILED — skipping")
                    failed += 1
                    continue
                print(f"    -> {translated}")

                if dry_run:
                    print(f"    [DRY RUN] Would update title")
                    fixed += 1
                    continue

                try:
                    if rtype == "products":
                        client.update_product(rid, {"title": translated})
                    elif rtype == "collections":
                        try:
                            client._request("PUT", f"custom_collections/{rid}.json",
                                            json={"custom_collection": {"id": rid, "title": translated}})
                        except Exception:
                            client._request("PUT", f"smart_collections/{rid}.json",
                                            json={"smart_collection": {"id": rid, "title": translated}})
                    elif rtype == "pages":
                        client._request("PUT", f"pages/{rid}.json",
                                        json={"page": {"id": rid, "title": translated}})
                    elif rtype == "articles":
                        blog_id = item.get("blog_id")
                        client._request("PUT", f"blogs/{blog_id}/articles/{rid}.json",
                                        json={"article": {"id": rid, "title": translated}})
                    print(f"    Updated on Shopify")
                    fixed += 1
                    time.sleep(0.5)
                except Exception as e:
                    print(f"    Update FAILED: {e}")
                    failed += 1

        # ── metafield fixes ──
        elif field.startswith("metafield:"):
            mf = finding.get("metafield", {})
            mf_value = mf.get("value", "")
            mf_type = mf.get("type", "")
            mf_key = mf.get("key", "")

            if "magento" in issues and mf_type == "rich_text_field":
                mf_value = strip_magento_html(mf_value)
                print(f"    Stripped Magento from {mf_key}")

            if "spanish" in issues:
                text = _extract_text_for_check(mf_value, mf_type)
                if has_spanish_text(text):
                    if openai_client is None:
                        import openai
                        openai_client = openai.OpenAI()
                    print(f"    Translating {mf_key}...")
                    if mf_type == "rich_text_field":
                        translated = translate_spanish_to_english(mf_value, openai_client, model=model)
                    else:
                        translated = translate_plain_text(mf_value, openai_client, model=model)
                    if translated:
                        mf_value = translated
                        print(f"    Translated OK")
                    else:
                        print(f"    Translation FAILED — skipping")
                        failed += 1
                        continue

            if dry_run:
                preview = mf_value[:200] + "..." if len(mf_value) > 200 else mf_value
                print(f"    [DRY RUN] Would update {mf_key}. Preview: {preview}")
                fixed += 1
                continue

            try:
                gid_type = "Product" if rtype == "products" else "Article"
                client.set_metafields([{
                    "ownerId": f"gid://shopify/{gid_type}/{rid}",
                    "namespace": mf.get("namespace", ""),
                    "key": mf.get("bare_key", ""),
                    "value": mf_value,
                    "type": mf_type,
                }])
                print(f"    Updated {mf_key} on Shopify")
                fixed += 1
                time.sleep(0.5)
            except Exception as e:
                print(f"    Update FAILED: {e}")
                failed += 1

        # ── metaobject text_field fixes ──
        elif field.startswith("text_field:"):
            tf = finding.get("text_field", {})
            tf_value = tf.get("value", "")
            tf_type = tf.get("type", "")
            tf_key = tf.get("key", "")

            if "magento" in issues and tf_type == "rich_text_field":
                tf_value = strip_magento_html(tf_value)
                print(f"    Stripped Magento from {tf_key}")

            if "spanish" in issues:
                text = _extract_text_for_check(tf_value, tf_type)
                if has_spanish_text(text):
                    if openai_client is None:
                        import openai
                        openai_client = openai.OpenAI()
                    print(f"    Translating {tf_key}...")
                    if tf_type == "rich_text_field":
                        translated = translate_spanish_to_english(tf_value, openai_client, model=model)
                    else:
                        translated = translate_plain_text(tf_value, openai_client, model=model)
                    if translated:
                        tf_value = translated
                        print(f"    Translated OK")
                    else:
                        print(f"    Translation FAILED — skipping")
                        failed += 1
                        continue

            if dry_run:
                preview = tf_value[:200] + "..." if len(tf_value) > 200 else tf_value
                print(f"    [DRY RUN] Would update {tf_key}. Preview: {preview}")
                fixed += 1
                continue

            try:
                # Update metaobject field via GraphQL
                mo_id = item["id"]  # GID
                client.update_metaobject(mo_id, [{"key": tf_key, "value": tf_value}])
                print(f"    Updated {tf_key} on Shopify")
                fixed += 1
                time.sleep(0.5)
            except Exception as e:
                print(f"    Update FAILED: {e}")
                failed += 1

    print(f"\n{'='*60}")
    print(f"Fixed: {fixed}  Failed: {failed}  Total: {len(by_target)}")
    if dry_run:
        print("(Dry run — no changes were made)")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Review & fix ALL English content on Saudi Shopify store")
    parser.add_argument("--audit", action="store_true",
                        help="Audit only — report issues without fixing")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show planned changes without applying")
    parser.add_argument("--type",
                        choices=["products", "collections", "pages", "articles", "metaobjects"],
                        help="Only audit a specific resource type")
    parser.add_argument("--skip-spanish", action="store_true",
                        help="Skip Spanish detection (only strip Magento)")
    parser.add_argument("--skip-magento", action="store_true",
                        help="Skip Magento stripping (only fix Spanish)")
    parser.add_argument("--model", default="gpt-5o-mini",
                        help="OpenAI model for Spanish->English translation (default: gpt-5o-mini)")
    parser.add_argument("--audit-model", default="claude-haiku-4-5-20251001",
                        help="Anthropic model for Spanish detection audit (default: claude-haiku-4-5-20251001)")
    parser.add_argument("--save-report", metavar="FILE",
                        help="Save audit report to JSON file")
    args = parser.parse_args()

    load_dotenv()
    shop_url = os.environ.get("SAUDI_SHOP_URL")
    access_token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not access_token:
        print("ERROR: Set SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN in .env")
        sys.exit(1)

    # Set audit model globally so has_spanish_text/has_spanish_content use it
    global _audit_model
    _audit_model = args.audit_model

    client = ShopifyClient(shop_url, access_token)

    print("=" * 60)
    print("CONTENT REVIEWER — Saudi Store (Full Coverage)")
    print(f"  Audit model:       {args.audit_model}")
    print(f"  Translation model: {args.model}")
    print("=" * 60)

    # Fetch resources
    resources = fetch_all_resources(client)

    # Filter by type if requested
    if args.type:
        resources = {args.type: resources.get(args.type, [])}

    # Summary
    total_items = sum(len(v) for v in resources.values())
    total_mfs = sum(
        len(item.get("metafields", []))
        for items in resources.values() for item in items
    )
    total_tfs = sum(
        len(item.get("text_fields", []))
        for items in resources.values() for item in items
    )
    print(f"\nTotal: {total_items} resources, {total_mfs} metafields, {total_tfs} metaobject fields")

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
            print(f"  - {f['label']}")

    if spanish_findings:
        print(f"\nSpanish content detected: {len(spanish_findings)}")
        for f in spanish_findings:
            # Show a preview of the Spanish text
            field = f["field"]
            if field == "body_html":
                visible = extract_visible_text(f["item"]["body_html"])
            elif field == "title":
                visible = f["item"]["title"]
            elif field.startswith("metafield:"):
                mf = f.get("metafield", {})
                visible = _extract_text_for_check(mf.get("value", ""), mf.get("type", ""))
            elif field.startswith("text_field:"):
                tf = f.get("text_field", {})
                visible = _extract_text_for_check(tf.get("value", ""), tf.get("type", ""))
            else:
                visible = ""
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
            "handle": f["item"].get("handle", ""),
            "title": f["item"].get("title", ""),
            "issue": f["issue"],
            "field": f["field"],
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
