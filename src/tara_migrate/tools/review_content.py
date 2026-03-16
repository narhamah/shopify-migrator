#!/usr/bin/env python3
"""Review and fix English content on the Saudi Shopify store.

Connects directly to the destination store and audits ALL content for:
  1. Remaining Spanish text → translates to English via OpenAI
  2. HTML bloat → strips all unnecessary HTML (styles, scripts, data-*, junk attrs)

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
    python review_content.py --skip-spanish              # Only strip HTML bloat
    python review_content.py --skip-html-cleanup         # Only fix Spanish
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
from tara_migrate.core import config
from tara_migrate.core.rich_text import extract_text_nodes, rebuild, is_rich_text_json
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
            max_tokens=8192,
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
# HTML Bloat Detection & Stripping — DOM Parser Approach
# ─────────────────────────────────────────────────────────────────────────────
# HTML is a deterministic markup language. Parse it as a tree, keep only
# semantic content and attributes referenced by the Shopify theme's CSS.
# Drop everything else (scripts, styles, data-* attrs, dead classes/IDs,
# event handlers, empty wrappers).
#
# The theme's class/ID selectors (from fetch_theme_selectors()) are ground
# truth for what belongs. When theme data isn't available, fall back to
# stripping known Magento class patterns.

from html.parser import HTMLParser

# Void elements that don't have closing tags
_VOID_ELEMENTS = frozenset({
    'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input',
    'link', 'meta', 'param', 'source', 'track', 'wbr',
})

# Magento-specific CSS classes (blacklist fallback when no theme data)
_MAGENTO_CLASSES = re.compile(
    r'pagebuilder-|product-items?|product-item-|widget-product-|price-container|'
    r'price-final_price|price-wrapper|tocart|towishlist|tocompare|'
    r'yotpo |bottomLine|bottomline-|columnGroup-root|column-root|'
    r'post-blogPostContent|row-contained-|row-root-|row-full-width|'
    r'text-root-|image-root-|image-img-|actions-primary|actions-secondary|'
    r'product-image-container|product-image-wrapper|product-image-photo|'
    r'block-with-products|productsCarousel|mage-|left-image|text-wrapper',
    re.IGNORECASE,
)

# CSS class selector extractor (matches .classname in CSS)
_CSS_CLASS_SELECTOR_RE = re.compile(r'\.([a-zA-Z_][a-zA-Z0-9_-]*)')

# CSS ID selector extractor (matches #idname in CSS)
_CSS_ID_SELECTOR_RE = re.compile(r'#([a-zA-Z_][a-zA-Z0-9_-]*)')

# Non-breaking spaces used as layout hacks (3+ in a row)
_NBSP_RE = re.compile(r'(?:&nbsp;\s*){3,}')


# ── DOM Node Types ──

class _TextNode:
    __slots__ = ('text',)
    def __init__(self, text):
        self.text = text

class _CommentNode:
    __slots__ = ('text',)
    def __init__(self, text):
        self.text = text

class _ElementNode:
    __slots__ = ('tag', 'attrs', 'children')
    def __init__(self, tag, attrs=None):
        self.tag = tag.lower()
        self.attrs = dict(attrs) if attrs else {}
        self.children = []


class _TreeBuilder(HTMLParser):
    """Parse HTML into a lightweight DOM tree using stdlib html.parser."""

    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.root = _ElementNode('__root__')
        self._stack = [self.root]
        self._skip_content = False  # True when inside <script>/<style>

    @property
    def _current(self):
        return self._stack[-1]

    def handle_starttag(self, tag, attrs):
        tag_lower = tag.lower()
        node = _ElementNode(tag_lower, attrs)
        self._current.children.append(node)
        if tag_lower in ('script', 'style'):
            self._skip_content = True
            self._stack.append(node)
        elif tag_lower not in _VOID_ELEMENTS:
            self._stack.append(node)

    def handle_endtag(self, tag):
        tag_lower = tag.lower()
        if tag_lower in _VOID_ELEMENTS:
            return
        # Walk up stack to find matching open tag (lenient for malformed HTML)
        for i in range(len(self._stack) - 1, 0, -1):
            if self._stack[i].tag == tag_lower:
                self._stack[i:] = []
                break
        self._skip_content = any(
            n.tag in ('script', 'style') for n in self._stack[1:]
        )

    def handle_startendtag(self, tag, attrs):
        node = _ElementNode(tag.lower(), attrs)
        self._current.children.append(node)

    def handle_data(self, data):
        if not self._skip_content:
            self._current.children.append(_TextNode(data))
        else:
            # Store script/style content as text inside the element
            self._current.children.append(_TextNode(data))

    def handle_entityref(self, name):
        self._current.children.append(_TextNode(f'&{name};'))

    def handle_charref(self, name):
        self._current.children.append(_TextNode(f'&#{name};'))

    def handle_comment(self, data):
        if not self._skip_content:
            self._current.children.append(_CommentNode(data))


def _parse_html(html):
    """Parse HTML string into a DOM tree. Returns the root _ElementNode."""
    builder = _TreeBuilder()
    builder.feed(html)
    return builder.root


def _collect_anchor_targets(node):
    """Collect all href="#id" anchor targets in the tree."""
    targets = set()
    if isinstance(node, _ElementNode):
        href = node.attrs.get('href', '')
        if href.startswith('#') and len(href) > 1:
            targets.add(href[1:])
        for child in node.children:
            targets.update(_collect_anchor_targets(child))
    return targets


def _clean_tree(node, theme_classes, theme_ids, anchor_targets):
    """Walk the tree and clean each element in-place. Returns True to keep, False to remove."""
    if isinstance(node, (_TextNode, _CommentNode)):
        return True

    # Remove <script> and <style> elements entirely
    if node.tag in ('script', 'style'):
        return False

    # Clean attributes
    cleaned_attrs = {}
    for attr, val in node.attrs.items():
        # Strip ALL data-* attributes
        if attr.startswith('data-'):
            continue
        # Strip event handlers (on*)
        if attr.startswith('on') and attr[2:].isalpha():
            continue
        # Filter classes
        if attr == 'class' and val:
            classes = val.split()
            if theme_classes is not None:
                # Whitelist mode: keep only classes in theme CSS
                classes = [c for c in classes if c in theme_classes]
            else:
                # Blacklist mode: strip known Magento classes
                classes = [c for c in classes if not _MAGENTO_CLASSES.match(c)]
            if classes:
                cleaned_attrs['class'] = ' '.join(classes)
            continue
        # Filter IDs
        if attr == 'id' and val:
            if theme_ids is not None:
                if val not in theme_ids and val not in anchor_targets:
                    continue
            elif val not in anchor_targets:
                # Without theme data, keep IDs that are anchor targets
                # Also keep all IDs (we can't know if they're needed)
                pass
            cleaned_attrs[attr] = val
            continue
        cleaned_attrs[attr] = val if val is not None else ''
    node.attrs = cleaned_attrs

    # Recurse into children
    node.children = [
        child for child in node.children
        if _clean_tree(child, theme_classes, theme_ids, anchor_targets)
    ]

    return True


def _collapse_wrappers(node):
    """Collapse empty wrapper divs/spans: no attrs, no direct text, one child element → unwrap."""
    if not isinstance(node, _ElementNode):
        return node

    # Recurse first (bottom-up)
    node.children = [_collapse_wrappers(child) for child in node.children]

    # Collapsible: div/span with no attributes, no text children, exactly one element child
    if node.tag in ('div', 'span') and not node.attrs:
        element_children = [c for c in node.children if isinstance(c, _ElementNode)]
        text_children = [c for c in node.children if isinstance(c, _TextNode) and c.text.strip()]
        if len(element_children) == 1 and not text_children:
            return element_children[0]

    return node


def _remove_empty(node):
    """Remove empty non-void elements (no text, no children, no meaningful attrs)."""
    if not isinstance(node, _ElementNode):
        return node

    node.children = [_remove_empty(c) for c in node.children]
    node.children = [
        c for c in node.children
        if not isinstance(c, _ElementNode)
        or c.tag in _VOID_ELEMENTS
        or c.children
        or c.attrs.get('src')
        or c.attrs.get('href')
    ]
    return node


def _serialize(node):
    """Serialize a DOM tree back to an HTML string."""
    if isinstance(node, _TextNode):
        return node.text
    if isinstance(node, _CommentNode):
        return f'<!--{node.text}-->'

    parts = []
    if node.tag != '__root__':
        attr_str = ''
        if node.attrs:
            attr_parts = []
            for k, v in node.attrs.items():
                if v == '':
                    attr_parts.append(k)
                else:
                    # Escape quotes in attribute values
                    escaped = v.replace('&', '&amp;').replace('"', '&quot;')
                    attr_parts.append(f'{k}="{escaped}"')
            attr_str = ' ' + ' '.join(attr_parts)
        parts.append(f'<{node.tag}{attr_str}>')
        if node.tag in _VOID_ELEMENTS:
            return parts[0]

    for child in node.children:
        parts.append(_serialize(child))

    if node.tag != '__root__':
        parts.append(f'</{node.tag}>')

    return ''.join(parts)


def parse_and_clean_html(html, theme_classes=None, theme_ids=None):
    """Parse HTML into a DOM tree, strip bloat, serialize back to clean HTML.

    This is the correct approach: HTML is a deterministic markup language.
    Parse it, walk the tree, keep semantic content and theme-referenced
    attributes, drop everything else.

    Args:
        html: Raw HTML string (body_html from Shopify).
        theme_classes: Set of CSS class names from the active theme (whitelist).
            If None, falls back to stripping known Magento class patterns.
        theme_ids: Set of CSS ID names from the active theme.
            If None, keeps all IDs.
    """
    if not html:
        return html

    # Parse
    root = _parse_html(html)

    # Collect anchor targets before cleaning (needed for ID filtering)
    anchor_targets = _collect_anchor_targets(root)

    # Clean (strip scripts, styles, data-*, event handlers, dead classes/IDs)
    _clean_tree(root, theme_classes, theme_ids, anchor_targets)

    # Collapse empty wrappers (multiple passes for deeply nested)
    for _ in range(10):
        prev_root = root
        root = _collapse_wrappers(root)
        if root is prev_root:
            break

    # Remove empty elements
    root = _remove_empty(root)

    # Serialize
    result = _serialize(root)

    # Final whitespace normalization
    result = _NBSP_RE.sub(' ', result)
    result = re.sub(r'\n{3,}', '\n\n', result)
    result = re.sub(r'  +', ' ', result)
    result = result.strip()

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Public API (used by review_arabic.py and internally)
# ─────────────────────────────────────────────────────────────────────────────

def has_html_bloat(html):
    """Check if HTML contains bloat by parsing and comparing.

    Parses the HTML, cleans it, and compares to the original.
    If the cleaned output differs, there is bloat.
    """
    if not html:
        return False
    cleaned = parse_and_clean_html(html)
    # Normalize whitespace for comparison
    def _normalize(s):
        return re.sub(r'\s+', ' ', s).strip()
    return _normalize(html) != _normalize(cleaned)


def strip_html_bloat(html, theme_classes=None, theme_ids=None):
    """Strip HTML bloat by parsing the DOM and keeping only clean content.

    Delegates to parse_and_clean_html() — the correct approach for
    processing structured markup.
    """
    if not html:
        return html
    return parse_and_clean_html(html, theme_classes, theme_ids)


# ─────────────────────────────────────────────────────────────────────────────
# Theme-Aware CSS Validation
# ─────────────────────────────────────────────────────────────────────────────

def fetch_theme_selectors(client):
    """Fetch the active theme's CSS and extract all class/ID selectors.

    Returns (class_set, id_set) — sets of strings the theme actually uses.
    Any class or ID NOT in these sets is dead weight and safe to strip.
    """
    theme_id = client.get_main_theme_id()
    if not theme_id:
        print("  WARNING: Could not find active theme — skipping CSS analysis")
        return None, None

    print(f"  Theme ID: {theme_id}")

    # List all assets and find CSS files
    assets = client.list_assets(theme_id)
    css_keys = [a["key"] for a in assets
                if a.get("key", "").endswith(".css") or a.get("key", "").endswith(".css.liquid")]

    if not css_keys:
        print("  WARNING: No CSS files found in theme")
        return None, None

    print(f"  Found {len(css_keys)} CSS files")

    all_classes = set()
    all_ids = set()

    for key in css_keys:
        try:
            asset = client.get_asset(theme_id, key)
            css_content = asset.get("value", "")
            if not css_content:
                continue

            # Extract class selectors
            classes = _CSS_CLASS_SELECTOR_RE.findall(css_content)
            all_classes.update(classes)

            # Extract ID selectors
            ids = _CSS_ID_SELECTOR_RE.findall(css_content)
            all_ids.update(ids)

        except Exception as e:
            print(f"    Could not read {key}: {e}")

    print(f"  Extracted {len(all_classes)} class selectors, {len(all_ids)} ID selectors from theme CSS")
    return all_classes, all_ids


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
        "- CRITICAL: Translate EVERY Spanish word to English. Zero Spanish "
        "words may remain in the output. There are NO exceptions.\n"
        "  'ácido salicílico' is Spanish → translate to 'salicylic acid'\n"
        "  'extracto' is Spanish → translate to 'extract'\n"
        "  'carbón activado' is Spanish → translate to 'activated charcoal'\n"
        "  'cuero cabelludo' is Spanish → translate to 'scalp'\n"
        "  These are NOT INCI names. INCI names only appear in dedicated "
        "ingredient lists (comma-separated, e.g. 'AQUA, SODIUM LAURETH "
        "SULFATE, SALICYLIC ACID'). Spanish words in a product description "
        "are NEVER INCI — always translate them.\n"
        "- Use professional, direct tone (no hype, no fluff)\n"
        "- If text is already in English, return it unchanged\n"
        "- Return ONLY the HTML, no explanations\n\n"
        f"{html}"
    )
    try:
        kwargs = dict(
            model=model,
            messages=[{"role": "user", "content": prompt}],
        )
        if not model.startswith("gpt-5"):
            kwargs["temperature"] = 0.2
        resp = client_openai.chat.completions.create(**kwargs)
        result = resp.choices[0].message.content.strip()
        # Strip markdown code block wrappers if the model added them
        if result.startswith("```"):
            result = re.sub(r"^```\w*\n?", "", result)
        if result.endswith("```"):
            result = result[:-3]
        return result.strip()
    except Exception as e:
        print(f"    Translation error: {e}")
        return None


def _slugify(text):
    """Convert text to a Shopify-compatible handle (lowercase, hyphen-separated)."""
    import unicodedata
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def translate_plain_text(text, client_openai, model="gpt-4o-mini"):
    """Translate a plain Spanish text string to English."""
    prompt = (
        "Translate this Spanish text to English for TARA, a luxury scalp-care brand.\n"
        "RULES:\n"
        "- Keep brand names unchanged: TARA, Kansa Wand, Gua Sha\n"
        "- CRITICAL: Translate EVERY Spanish word to English. Zero Spanish "
        "words may remain. There are NO exceptions.\n"
        "  'ácido salicílico' → 'salicylic acid' (this is Spanish, NOT INCI)\n"
        "  'extracto' → 'extract'\n"
        "  'carbón activado' → 'activated charcoal'\n"
        "  INCI names only appear in dedicated ingredient lists "
        "(comma-separated, e.g. 'AQUA, SALICYLIC ACID'). "
        "Spanish words in descriptions are NEVER INCI.\n"
        "- Use professional, direct tone\n"
        "- Return ONLY the translation, no explanations\n\n"
        f"{text}"
    )
    try:
        kwargs = dict(
            model=model,
            messages=[{"role": "user", "content": prompt}],
        )
        if not model.startswith("gpt-5"):
            kwargs["temperature"] = 0.2
        resp = client_openai.chat.completions.create(**kwargs)
        result = resp.choices[0].message.content.strip()
        # Strip markdown code block wrappers if the model added them
        if result.startswith("```"):
            result = re.sub(r"^```\w*\n?", "", result)
        if result.endswith("```"):
            result = result[:-3]
        return result.strip()
    except Exception as e:
        print(f"    Translation error: {e}")
        return None


def _translate_rich_text_json(json_str, client_openai, model="gpt-4o-mini"):
    """Translate text nodes inside rich_text JSON, preserving the JSON structure."""
    try:
        texts, parsed = extract_text_nodes(json_str)
        if not texts:
            return json_str
        translations = {}
        for path, text_value in texts:
            translated = translate_plain_text(text_value, client_openai, model=model)
            if translated:
                translations[tuple(path)] = translated
            else:
                translations[tuple(path)] = text_value  # keep original on failure
        return rebuild(parsed, translations)
    except Exception as e:
        print(f"    Rich text translation error: {e}")
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


_SEO_TRANSLATABLE_QUERY = """\
query($resourceType: TranslatableResourceType!, $first: Int!, $after: String) {
  translatableResources(resourceType: $resourceType, first: $first, after: $after) {
    edges {
      node {
        resourceId
        translatableContent {
          key
          value
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

# SEO field keys in Shopify's Translations API
_SEO_KEYS = {"meta_title", "meta_description"}


def _fetch_seo_fields(client, resource_type):
    """Fetch SEO title/description via the Translations API.

    The REST metafields endpoint and GraphQL seo{} field do NOT reliably
    return global.title_tag/description_tag. The Translations API is the
    only reliable source for these values.

    Args:
        client: ShopifyClient instance
        resource_type: Shopify translatable resource type
            (e.g. "PRODUCT", "COLLECTION", "PAGE", "ARTICLE")

    Returns dict of {numeric_id: {title_tag: str, description_tag: str}}.
    """
    seo_map = {}
    cursor = None
    while True:
        data = client._graphql(_SEO_TRANSLATABLE_QUERY, {
            "resourceType": resource_type,
            "first": 50,
            "after": cursor,
        })
        container = data.get("translatableResources", {})
        for edge in container.get("edges", []):
            node = edge["node"]
            gid = node["resourceId"]
            numeric_id = int(gid.split("/")[-1])
            for field in node.get("translatableContent", []):
                key = field["key"]
                value = field.get("value") or ""
                if key == "meta_title" and value:
                    seo_map.setdefault(numeric_id, {})["title_tag"] = value
                elif key == "meta_description" and value:
                    seo_map.setdefault(numeric_id, {})["description_tag"] = value
        page_info = container.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        time.sleep(0.3)
    return seo_map


def _fetch_product_seo_fields(client):
    """Fetch SEO fields for products. Convenience wrapper."""
    return _fetch_seo_fields(client, "PRODUCT")


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


def _inject_seo_fields(item, seo_map):
    """Inject SEO title_tag/description_tag as synthetic metafield entries.

    Returns the number of SEO fields injected.
    """
    injected = 0
    seo = seo_map.get(item["id"])
    if not seo:
        return 0
    mfs = item.get("metafields", [])
    existing_keys = {m["key"] for m in mfs}
    if "global.title_tag" not in existing_keys and seo.get("title_tag"):
        mfs.append({
            "id": f"seo-title-{item['id']}",
            "key": "global.title_tag",
            "value": seo["title_tag"],
            "type": "single_line_text_field",
            "namespace": "global",
            "bare_key": "title_tag",
        })
        injected += 1
    if "global.description_tag" not in existing_keys and seo.get("description_tag"):
        mfs.append({
            "id": f"seo-desc-{item['id']}",
            "key": "global.description_tag",
            "value": seo["description_tag"],
            "type": "single_line_text_field",
            "namespace": "global",
            "bare_key": "description_tag",
        })
        injected += 1
    item["metafields"] = mfs
    return injected


def fetch_all_resources(client):
    """Fetch all content resources from the store, including metafields and metaobjects."""
    resources = {}

    # --- Products (body_html + title + metafields + SEO) ---
    print("Fetching products...")
    products = client.get_products()

    # Fetch SEO fields via GraphQL (global.title_tag/description_tag are NOT
    # returned by the REST metafields endpoint — they're system metafields)
    seo_map = _fetch_product_seo_fields(client)

    product_items = []
    seo_injected = 0
    for p in products:
        item = {
            "id": p["id"], "title": p.get("title", ""), "handle": p.get("handle", ""),
            "body_html": p.get("body_html", ""), "type": "product",
        }
        mfs = _fetch_metafields_for_resource(client, "products", p["id"],
                                             PRODUCT_TRANSLATABLE_METAFIELDS)
        item["metafields"] = mfs
        seo_injected += _inject_seo_fields(item, seo_map)
        product_items.append(item)
    resources["products"] = product_items
    mf_count = sum(len(p["metafields"]) for p in product_items)
    print(f"  {len(product_items)} products ({mf_count} text metafields, {seo_injected} SEO fields via GraphQL)")

    # --- Collections (body_html + title + SEO) ---
    print("Fetching collections...")
    collections = client.get_collections()
    collection_seo_map = _fetch_seo_fields(client, "COLLECTION")
    collection_items = []
    collection_seo_injected = 0
    for c in collections:
        item = {
            "id": c["id"], "title": c.get("title", ""), "handle": c.get("handle", ""),
            "body_html": c.get("body_html", ""), "type": "collection", "metafields": [],
        }
        collection_seo_injected += _inject_seo_fields(item, collection_seo_map)
        collection_items.append(item)
    resources["collections"] = collection_items
    print(f"  {len(collection_items)} collections ({collection_seo_injected} SEO fields via GraphQL)")

    # --- Pages (body_html + title + SEO) ---
    print("Fetching pages...")
    pages = client.get_pages()
    page_seo_map = _fetch_seo_fields(client, "PAGE")
    page_items = []
    page_seo_injected = 0
    for p in pages:
        item = {
            "id": p["id"], "title": p.get("title", ""), "handle": p.get("handle", ""),
            "body_html": p.get("body_html", ""), "type": "page", "metafields": [],
        }
        page_seo_injected += _inject_seo_fields(item, page_seo_map)
        page_items.append(item)
    resources["pages"] = page_items
    print(f"  {len(page_items)} pages ({page_seo_injected} SEO fields via GraphQL)")

    # --- Articles (body_html + title + metafields + SEO) ---
    print("Fetching articles...")
    blogs = client.get_blogs()
    article_seo_map = _fetch_seo_fields(client, "ARTICLE")
    articles = []
    article_seo_injected = 0
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
            article_seo_injected += _inject_seo_fields(item, article_seo_map)
            articles.append(item)
    resources["articles"] = articles
    mf_count = sum(len(a["metafields"]) for a in articles)
    print(f"  {len(articles)} articles ({mf_count} text metafields, {article_seo_injected} SEO fields via GraphQL)")

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

def audit_content(resources, skip_spanish=False, skip_html_cleanup=False):
    """Audit all resources for Spanish content and HTML bloat.

    Checks:
      - body_html for HTML bloat (styles, scripts, data-*, junk attrs) and Spanish text
      - title for Spanish text
      - metafield values for Spanish text and HTML bloat (rich_text)
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
                if not skip_html_cleanup and has_html_bloat(body):
                    findings.append({
                        "resource_type": resource_type,
                        "item": item,
                        "issue": "html_bloat",
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

                if not skip_html_cleanup and mf_type == "rich_text_field" and has_html_bloat(mf_value):
                    findings.append({
                        "resource_type": resource_type,
                        "item": item,
                        "issue": "html_bloat",
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

                if not skip_html_cleanup and tf_type == "rich_text_field" and has_html_bloat(tf_value):
                    findings.append({
                        "resource_type": resource_type,
                        "item": item,
                        "issue": "html_bloat",
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


# ─────────────────────────────────────────────────────────────────────────────
# AI Bloat Scanner — Sonnet 4.6 pattern learning
# ─────────────────────────────────────────────────────────────────────────────

_BLOAT_SCAN_PROMPT = """\
Analyze this HTML content from a Shopify store. Identify ANY unnecessary HTML bloat that should be stripped, including but not limited to:

- Inline style attributes (style="...")
- data-* attributes from any framework (Magento, React, Vue, etc.)
- <style> or <script> blocks
- HTML comments
- Empty wrapper elements (div/span with no semantic purpose)
- Redundant class attributes from CSS frameworks or page builders
- width/height attributes on non-img elements
- Non-semantic attributes (id, role, tabindex, aria-* on non-interactive elements)
- &nbsp; used for spacing/layout
- Any other HTML that adds no value for Shopify content display

For each bloat pattern found, report:
1. PATTERN: A short name for the pattern
2. EXAMPLE: The actual HTML snippet showing the bloat
3. SUGGESTION: How it should be cleaned

If the HTML is clean and has no bloat, respond with just: CLEAN

HTML to analyze:
"""

_BLOAT_CLEAN_PROMPT = """\
Clean this HTML for a Shopify store product/page body_html field.
Strip ALL unnecessary bloat while preserving the visible content and semantic structure.

REMOVE:
- All <style> and <script> blocks
- All HTML comments
- All data-* attributes
- All inline style attributes (style="...")
- All id, role, tabindex, aria-*, onclick/on* attributes
- All width/height attributes (except on <img> tags)
- Empty wrapper elements (div, span with no content or purpose)
- Redundant CSS class attributes from page builders or frameworks
- &nbsp; used for spacing (replace with normal spaces)
- Any non-semantic wrapper divs/spans

PRESERVE:
- All visible text content
- Semantic HTML elements: <h1>-<h6>, <p>, <ul>, <ol>, <li>, <a>, <img>, <table>, <blockquote>
- <img> src, alt, width, height attributes
- <a> href attributes
- Content structure and hierarchy

Return ONLY the cleaned HTML. No explanations, no markdown code blocks.

HTML to clean:
"""


def ai_clean_html(html, model="claude-sonnet-4-6"):
    """Use Sonnet 4.6 to clean HTML, stripping all bloat while preserving content.

    This is a fallback for HTML patterns our regex rules don't catch.
    Returns cleaned HTML, or None on failure.
    """
    if not html or len(html) < 20:
        return html

    try:
        client = _get_audit_client()
        resp = client.messages.create(
            model=model,
            max_tokens=8192,
            messages=[{"role": "user", "content": _BLOAT_CLEAN_PROMPT + html}],
        )
        cleaned = resp.content[0].text.strip()
        # Strip markdown code block wrappers if the model added them
        if cleaned.startswith("```html"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        return cleaned.strip()
    except Exception as e:
        print(f"    AI clean error: {e}")
        return None


def ai_scan_for_bloat(resources, debug_file="data/html_bloat_debug.jsonl",
                      model="claude-sonnet-4-6"):
    """Use Sonnet 4.6 to scan all body_html and rich_text fields for HTML bloat patterns.

    Logs findings to a JSONL debug file for later analysis and pattern building.
    This is a learning step — it finds bloat our regex rules might miss.
    """
    client = _get_audit_client()
    scanned = 0
    bloat_found = 0

    # Ensure data directory exists
    debug_dir = os.path.dirname(debug_file)
    if debug_dir:
        os.makedirs(debug_dir, exist_ok=True)

    with open(debug_file, "w", encoding="utf-8") as fh:
        for resource_type, items in resources.items():
            for item in items:
                handle = item.get("handle", item.get("id", "?"))
                item_id = item.get("id", "?")

                # Collect all HTML content from this resource
                html_fields = {}

                body = item.get("body_html", "")
                if body and len(body) > 20:
                    html_fields["body_html"] = body

                # Rich text metafields
                for mf in item.get("metafields", []):
                    if mf.get("type") == "rich_text_field" and mf.get("value"):
                        html_fields[f"metafield:{mf['key']}"] = mf["value"]

                # Rich text metaobject fields
                for tf in item.get("text_fields", []):
                    if tf.get("type") == "rich_text_field" and tf.get("value"):
                        html_fields[f"text_field:{tf['key']}"] = tf["value"]

                if not html_fields:
                    continue

                for field_name, html_content in html_fields.items():
                    scanned += 1
                    # Truncate very long HTML to avoid token limits
                    html_sample = html_content[:4000]

                    try:
                        resp = client.messages.create(
                            model=model,
                            max_tokens=8192,
                            messages=[{"role": "user", "content": (
                                _BLOAT_SCAN_PROMPT + html_sample
                            )}],
                        )
                        answer = resp.content[0].text.strip()

                        if answer.upper() != "CLEAN":
                            bloat_found += 1
                            # Also get Sonnet's cleaned version
                            cleaned = ai_clean_html(html_content, model=model)
                            # First strip with our regex rules for comparison
                            regex_cleaned = strip_html_bloat(html_content)
                            entry = {
                                "resource_type": resource_type,
                                "id": str(item_id),
                                "handle": handle,
                                "field": field_name,
                                "html_length": len(html_content),
                                "html_sample": html_content[:2000],
                                "ai_analysis": answer,
                                "regex_detected": has_html_bloat(html_content),
                                "regex_cleaned_length": len(regex_cleaned),
                                "ai_cleaned_length": len(cleaned) if cleaned else None,
                                "ai_cleaned_sample": cleaned[:2000] if cleaned else None,
                            }
                            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
                            print(f"  BLOAT: {resource_type}/{handle} [{field_name}] "
                                  f"({len(html_content):,} → regex:{len(regex_cleaned):,} "
                                  f"/ ai:{len(cleaned):,} chars)" if cleaned else
                                  f"  BLOAT: {resource_type}/{handle} [{field_name}] "
                                  f"({len(html_content):,} chars, AI clean failed)")

                    except Exception as e:
                        print(f"  Scan error for {resource_type}/{handle} [{field_name}]: {e}")

    print(f"\nAI bloat scan complete: {scanned} fields scanned, "
          f"{bloat_found} with bloat detected")
    if bloat_found > 0:
        print(f"Debug log: {debug_file}")

    return debug_file


def apply_fixes(client, findings, dry_run=False, model="gpt-4o-mini", ai_clean=False,
                theme_classes=None, theme_ids=None):
    """Apply fixes for all findings.

    Handles body_html, titles, metafields, and metaobject fields.
    HTML bloat stripping uses regex (default) or Sonnet 4.6 (--ai-clean).
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

            if "html_bloat" in issues:
                before_len = len(body)
                if ai_clean:
                    cleaned = ai_clean_html(body)
                    if cleaned:
                        body = cleaned
                    else:
                        print(f"    AI clean failed, falling back to regex")
                        body = strip_html_bloat(body, theme_classes, theme_ids)
                else:
                    body = strip_html_bloat(body, theme_classes, theme_ids)
                after_len = len(body)
                reduction = before_len - after_len
                pct = (reduction / before_len * 100) if before_len else 0
                method = "AI" if ai_clean else "regex"
                print(f"    Stripped HTML bloat ({method}): {before_len:,} -> {after_len:,} chars ({pct:.0f}% reduction)")

            if "spanish" in issues:
                # Always translate if audit flagged as Spanish — don't re-check
                # (re-checking with Haiku can give different answers, causing loops)
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
                    if rtype == "metaobjects":
                        new_handle = _slugify(translated)
                        print(f"    [DRY RUN] Would update handle: {item['handle']} -> {new_handle}")
                    else:
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
                    elif rtype == "metaobjects":
                        # For metaobjects, "title" is the handle — slugify and update
                        new_handle = _slugify(translated)
                        mo_id = item["id"]  # GID
                        client.update_metaobject(mo_id, [], handle=new_handle)
                        print(f"    Handle: {item['handle']} -> {new_handle}")
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

            if "html_bloat" in issues and mf_type == "rich_text_field":
                if ai_clean:
                    cleaned = ai_clean_html(mf_value)
                    mf_value = cleaned if cleaned else strip_html_bloat(mf_value, theme_classes, theme_ids)
                else:
                    mf_value = strip_html_bloat(mf_value, theme_classes, theme_ids)
                method = "AI" if ai_clean else "regex"
                print(f"    Stripped HTML bloat from {mf_key} ({method})")

            if "spanish" in issues:
                # Always translate if audit flagged — don't re-check with Haiku
                if openai_client is None:
                    import openai
                    openai_client = openai.OpenAI()
                print(f"    Translating {mf_key}...")
                if mf_type == "rich_text_field" and is_rich_text_json(mf_value):
                    translated = _translate_rich_text_json(mf_value, openai_client, model=model)
                elif mf_type == "rich_text_field":
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
                _GID_TYPE_MAP = {
                    "products": "Product",
                    "collections": "Collection",
                    "pages": "Page",
                    "articles": "Article",
                }
                gid_type = _GID_TYPE_MAP.get(rtype, "Product")
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

            if "html_bloat" in issues and tf_type == "rich_text_field":
                if ai_clean:
                    cleaned = ai_clean_html(tf_value)
                    tf_value = cleaned if cleaned else strip_html_bloat(tf_value, theme_classes, theme_ids)
                else:
                    tf_value = strip_html_bloat(tf_value, theme_classes, theme_ids)
                method = "AI" if ai_clean else "regex"
                print(f"    Stripped HTML bloat from {tf_key} ({method})")

            if "spanish" in issues:
                # Always translate if audit flagged — don't re-check with Haiku
                if openai_client is None:
                    import openai
                    openai_client = openai.OpenAI()
                print(f"    Translating {tf_key}...")
                if tf_type == "rich_text_field" and is_rich_text_json(tf_value):
                    translated = _translate_rich_text_json(tf_value, openai_client, model=model)
                elif tf_type == "rich_text_field":
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
        description="Review & fix ALL English content on destination Shopify store")
    parser.add_argument("--audit", action="store_true",
                        help="Audit only — report issues without fixing")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show planned changes without applying")
    parser.add_argument("--type",
                        choices=["products", "collections", "pages", "articles", "metaobjects"],
                        help="Only audit a specific resource type")
    parser.add_argument("--skip-spanish", action="store_true",
                        help="Skip Spanish detection (only strip HTML bloat)")
    parser.add_argument("--skip-html-cleanup", action="store_true",
                        help="Skip HTML cleanup (only fix Spanish)")
    parser.add_argument("--model", default="gpt-4o-mini",
                        help="OpenAI model for Spanish->English translation (default: gpt-4o-mini)")
    parser.add_argument("--audit-model", default="claude-haiku-4-5-20251001",
                        help="Anthropic model for Spanish detection audit (default: claude-haiku-4-5-20251001)")
    parser.add_argument("--ai-clean", action="store_true",
                        help="Use Sonnet 4.6 to clean HTML (instead of regex-only stripping)")
    parser.add_argument("--scan-bloat", action="store_true",
                        help="Run AI bloat scan (Sonnet 4.6) and log patterns to debug file")
    parser.add_argument("--scan-bloat-file", default="data/html_bloat_debug.jsonl",
                        help="Debug file for AI bloat scan (default: data/html_bloat_debug.jsonl)")
    parser.add_argument("--save-report", metavar="FILE",
                        help="Save audit report to JSON file")
    args = parser.parse_args()

    load_dotenv()
    shop_url = config.get_dest_shop_url()
    access_token = config.get_dest_access_token()
    if not shop_url or not access_token:
        print("ERROR: Set DEST_SHOP_URL and DEST_ACCESS_TOKEN in .env")
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
                             skip_html_cleanup=args.skip_html_cleanup)

    # Group for display
    bloat_findings = [f for f in findings if f["issue"] == "html_bloat"]
    spanish_findings = [f for f in findings if f["issue"] == "spanish"]

    if bloat_findings:
        print(f"\nHTML bloat detected: {len(bloat_findings)}")
        for f in bloat_findings:
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
          f"(HTML bloat: {len(bloat_findings)}, Spanish: {len(spanish_findings)})")

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

    # AI bloat scan (pattern learning step)
    if args.scan_bloat:
        print(f"\n{'='*60}")
        print("AI BLOAT SCAN (Sonnet 4.6 — pattern learning)")
        print("=" * 60)
        debug_file = ai_scan_for_bloat(resources, debug_file=args.scan_bloat_file)
        print(f"\nDone. Review {debug_file} and feed it back for pattern analysis.")

    if args.audit:
        return

    # Fetch theme CSS for class/ID validation
    print(f"\n{'='*60}")
    print("FETCHING THEME CSS (for automated class/ID validation)")
    print("=" * 60)
    theme_classes, theme_ids = fetch_theme_selectors(client)
    if theme_classes is not None:
        print(f"  Theme-aware mode: classes not in theme CSS will be stripped automatically")
    else:
        print(f"  Fingerprint-only mode: only known Magento patterns will be stripped")

    # Fix
    print(f"\n{'='*60}")
    print("APPLYING FIXES" + (" (DRY RUN)" if args.dry_run else ""))
    print("=" * 60)

    apply_fixes(client, findings, dry_run=args.dry_run, model=args.model,
                ai_clean=args.ai_clean,
                theme_classes=theme_classes, theme_ids=theme_ids)


if __name__ == "__main__":
    main()
