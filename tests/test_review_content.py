"""Tests for review_content — HTML bloat stripping, Spanish detection, SEO fields, and full-coverage audit."""

import pytest
from unittest.mock import MagicMock, patch

from tara_migrate.tools.review_content import (
    has_html_bloat,
    strip_html_bloat,
    parse_and_clean_html,
    has_spanish_content,
    has_spanish_text,
    extract_visible_text,
    extract_text_from_rich_text_json,
    audit_content,
    _extract_text_for_check,
    _fetch_product_seo_fields,
)


# ─────────────────────────────────────────────────────────────────────────────
# HTML Bloat Detection
# ─────────────────────────────────────────────────────────────────────────────

class TestHasHtmlBloat:
    def test_clean_html(self):
        assert not has_html_bloat("<p>Hello world</p>")

    def test_empty(self):
        assert not has_html_bloat("")
        assert not has_html_bloat(None)

    def test_data_pb_style(self):
        html = '<div data-pb-style="ABC123">content</div>'
        assert has_html_bloat(html)

    def test_data_content_type(self):
        html = '<div data-content-type="row" data-appearance="full-width">x</div>'
        assert has_html_bloat(html)

    def test_pagebuilder_class(self):
        html = '<div class="pagebuilder-column">x</div>'
        assert has_html_bloat(html)

    def test_product_carousel_class(self):
        html = '<div class="productsCarousel">x</div>'
        assert has_html_bloat(html)

    def test_magento_init_script(self):
        html = '<script type="text/x-magento-init">{"foo":"bar"}</script>'
        assert has_html_bloat(html)

    def test_product_item_class(self):
        html = '<div class="product-item-info">x</div>'
        assert has_html_bloat(html)

    def test_event_handlers(self):
        html = '<div onclick="alert(1)">x</div>'
        assert has_html_bloat(html)

    # ── Legitimate Shopify HTML should NOT be flagged ──

    def test_inline_style_not_flagged(self):
        """Inline styles could be intentional formatting."""
        html = '<p style="color: red;">Hello</p>'
        assert not has_html_bloat(html)

    def test_html_comments_not_flagged(self):
        html = '<p>Hello</p><!-- section marker --><p>World</p>'
        assert not has_html_bloat(html)

    def test_id_aria_not_flagged(self):
        html = '<div id="faq" role="region" aria-label="FAQ"><p>Content</p></div>'
        assert not has_html_bloat(html)

    def test_all_data_attrs_flagged(self):
        """All data-* attributes are bloat — Shopify body_html doesn't use them."""
        html = '<div data-section-id="123"><p>Content</p></div>'
        assert has_html_bloat(html)

    def test_all_style_blocks_flagged(self):
        """All <style> blocks are bloat — theme CSS handles styling."""
        html = '<style>.custom { color: red; }</style><p>Content</p>'
        assert has_html_bloat(html)


# ─────────────────────────────────────────────────────────────────────────────
# HTML Bloat Stripping
# ─────────────────────────────────────────────────────────────────────────────

class TestStripHtmlBloat:
    def test_clean_html_unchanged(self):
        html = "<p>Hello <strong>world</strong></p>"
        assert strip_html_bloat(html) == html

    def test_empty(self):
        assert strip_html_bloat("") == ""
        assert strip_html_bloat(None) is None

    def test_removes_magento_style_blocks(self):
        html = (
            '<style>#html-body [data-pb-style=ABC]{display:flex}</style>'
            '<p>Keep this</p>'
        )
        result = strip_html_bloat(html)
        assert "<style" not in result
        assert "<p>Keep this</p>" in result

    def test_strips_orphan_style_blocks(self):
        """<style> blocks whose selectors don't match any HTML are orphaned — strip."""
        html = (
            '<style>.custom-layout { display: grid; }</style>'
            '<p>Content</p>'
        )
        result = strip_html_bloat(html)
        assert "<style" not in result
        assert "<p>Content</p>" in result

    def test_strips_all_style_blocks(self):
        """All <style> blocks are stripped — theme CSS handles styling."""
        html = (
            '<style>.custom-layout { display: grid; }</style>'
            '<div class="custom-layout"><p>Content</p></div>'
        )
        result = strip_html_bloat(html)
        assert "<style" not in result
        assert "<p>Content</p>" in result

    def test_removes_script_blocks(self):
        html = (
            '<p>Hello</p>'
            '<script type="text/javascript">var x = 1;</script>'
            '<p>World</p>'
        )
        result = strip_html_bloat(html)
        assert "<script" not in result
        assert "<p>Hello</p>" in result
        assert "<p>World</p>" in result

    def test_removes_magento_data_attributes(self):
        html = '<div data-content-type="row" data-appearance="full-width" data-element="main"><p>Keep</p></div>'
        result = strip_html_bloat(html)
        assert "data-content-type" not in result
        assert "data-appearance" not in result
        assert "<p>Keep</p>" in result

    def test_strips_all_data_attributes(self):
        """All data-* attributes are stripped — body_html doesn't need them."""
        html = '<div data-section-id="123" data-shopify="true"><p>Content</p></div>'
        result = strip_html_bloat(html)
        assert 'data-section-id' not in result
        assert 'data-shopify' not in result
        assert "Content" in result

    def test_removes_magento_classes(self):
        html = '<div class="pagebuilder-column myclass"><p>Keep</p></div>'
        result = strip_html_bloat(html)
        assert "pagebuilder-column" not in result
        # Custom class is preserved
        assert "myclass" in result

    def test_removes_product_carousel(self):
        html = (
            '<h2>Our Products</h2>'
            '<ol class="product-items widget-product-carousel">'
            '<li class="product-item"><div>Product 1</div></li>'
            '</ol>'
            '<p>Footer</p>'
        )
        result = strip_html_bloat(html)
        assert "product-items" not in result
        assert "<h2>Our Products</h2>" in result
        assert "<p>Footer</p>" in result

    def test_removes_magento_product_image_styles(self):
        html = (
            '<p>Content</p>'
            '<style>.product-image-container-278 { width: 132px; }</style>'
            '<p>More</p>'
        )
        result = strip_html_bloat(html)
        assert "product-image-container" not in result
        assert "<p>Content</p>" in result

    def test_preserves_inline_style_attributes(self):
        """Inline style attributes could be intentional merchant formatting."""
        html = '<p style="color: red; font-size: 14px;">Hello</p>'
        result = strip_html_bloat(html)
        assert 'style=' in result
        assert "Hello" in result

    def test_preserves_html_comments(self):
        """HTML comments could be Shopify section markers — preserve them."""
        html = '<p>Before</p><!-- section --><p>After</p>'
        result = strip_html_bloat(html)
        assert "Before" in result
        assert "After" in result

    def test_preserves_id_role_aria_attributes(self):
        """id, role, aria-* are legitimate for accessibility and anchors."""
        html = '<div id="faq" role="region" aria-label="FAQ"><p>Content</p></div>'
        result = strip_html_bloat(html)
        assert 'id="faq"' in result
        assert 'role="region"' in result
        assert 'aria-label="FAQ"' in result

    def test_removes_event_handlers(self):
        """Event handlers are a security risk — always strip."""
        html = '<div onclick="alert(1)" onload="init()"><p>Content</p></div>'
        result = strip_html_bloat(html)
        assert 'onclick=' not in result
        assert 'onload=' not in result
        assert "Content" in result

    # ── Theme-aware stripping ──

    def test_theme_aware_strips_dead_classes(self):
        """Classes not in theme CSS are dead weight — strip them."""
        html = '<div class="magento-leftover fancy-thing"><p>Content</p></div>'
        theme_classes = {"fancy-thing"}  # only fancy-thing is in the theme CSS
        result = strip_html_bloat(html, theme_classes=theme_classes)
        assert "fancy-thing" in result
        assert "magento-leftover" not in result

    def test_theme_aware_strips_dead_ids(self):
        """IDs not in theme CSS and not used as anchors — strip them."""
        html = '<div id="pb-row-123"><p>Content</p></div>'
        theme_ids = set()  # no IDs in theme CSS
        result = strip_html_bloat(html, theme_ids=theme_ids)
        assert 'id=' not in result
        assert "Content" in result

    def test_theme_aware_preserves_anchor_ids(self):
        """IDs used as anchor targets within the HTML are preserved."""
        html = '<a href="#faq">Go to FAQ</a><div id="faq"><p>FAQ here</p></div>'
        theme_ids = set()  # not in CSS, but used as anchor
        result = strip_html_bloat(html, theme_ids=theme_ids)
        assert 'id="faq"' in result

    def test_without_theme_data_preserves_all_classes(self):
        """Without theme CSS data, all non-Magento classes are preserved."""
        html = '<div class="unknown-class"><p>Content</p></div>'
        result = strip_html_bloat(html)  # no theme_classes passed
        assert "unknown-class" in result

    def test_preserves_semantic_html(self):
        """Headings, paragraphs, images should survive."""
        html = (
            '<div data-content-type="row" data-appearance="default">'
            '<h3>Brush the Right Way</h3>'
            '<p>Use a wide-tooth comb for thin hair.</p>'
            '<img src="https://example.com/img.jpg" alt="Hair">'
            '</div>'
        )
        result = strip_html_bloat(html)
        assert "<h3>Brush the Right Way</h3>" in result
        assert "Use a wide-tooth comb" in result
        assert '<img src="https://example.com/img.jpg"' in result

    def test_significant_size_reduction(self):
        """Bloated HTML is typically 10-100x larger than visible content."""
        # Simulate a typical bloated block
        magento_html = (
            '<style>#html-body [data-pb-style=X]{display:flex}</style>'
            '<div data-content-type="row" data-appearance="full-width" '
            'data-enable-parallax="0" data-parallax-speed="0.5" '
            'data-background-images="{}" data-element="main" data-pb-style="X">'
            '<div class="pagebuilder-column-group" data-content-type="column-group">'
            '<div class="pagebuilder-column" data-content-type="column">'
            '<h2 data-content-type="heading">Healthy Hair</h2>'
            '<div class="text-desktop" data-content-type="text">'
            '<p>Discover our formulas.</p>'
            '</div>'
            '</div></div></div>'
            '<script type="text/x-magento-init">{"foo":"bar"}</script>'
        )
        result = strip_html_bloat(magento_html)
        assert len(result) < len(magento_html)
        assert "Healthy Hair" in result
        assert "Discover our formulas" in result
        assert "data-pb-style" not in result
        assert "<script" not in result

    def test_full_article_body(self):
        """Test with article body_html similar to user's actual data."""
        html = (
            '<div class="post-blogPostContent-K2i max-w-[886px] mx-auto">'
            '<div class="row-contained-Oxp row-root-L38 desktopMaxW">'
            '<div><div class="text-root-aN9">'
            '<p>Brushing thin hair can be frustrating.</p>'
            '</div></div></div></div>'
        )
        result = strip_html_bloat(html)
        assert "Brushing thin hair can be frustrating" in result
        # Magento classes should be gone
        assert "post-blogPostContent" not in result
        assert "row-contained" not in result
        assert "text-root" not in result


# ─────────────────────────────────────────────────────────────────────────────
# DOM Parser — parse_and_clean_html
# ─────────────────────────────────────────────────────────────────────────────

class TestParseAndCleanHtml:
    def test_clean_html_passthrough(self):
        html = "<p>Hello <strong>world</strong></p>"
        result = parse_and_clean_html(html)
        assert "Hello" in result
        assert "<strong>world</strong>" in result

    def test_empty(self):
        assert parse_and_clean_html("") == ""
        assert parse_and_clean_html(None) is None

    def test_strips_all_data_attrs(self):
        html = '<div data-section-id="123" data-custom="x"><p>Content</p></div>'
        result = parse_and_clean_html(html)
        assert "data-" not in result
        assert "Content" in result

    def test_strips_all_style_blocks(self):
        html = '<style>.foo { color: red; }</style><p>Content</p>'
        result = parse_and_clean_html(html)
        assert "<style" not in result
        assert "Content" in result

    def test_strips_script_blocks(self):
        html = '<p>Before</p><script>alert(1)</script><p>After</p>'
        result = parse_and_clean_html(html)
        assert "<script" not in result
        assert "Before" in result
        assert "After" in result

    def test_strips_event_handlers(self):
        html = '<div onclick="alert(1)" onmouseover="x()"><p>Content</p></div>'
        result = parse_and_clean_html(html)
        assert "onclick" not in result
        assert "onmouseover" not in result

    def test_collapses_empty_wrappers(self):
        """Deeply nested empty divs collapse to just the content."""
        html = '<div><div><div><p>Text</p></div></div></div>'
        result = parse_and_clean_html(html)
        assert result == "<p>Text</p>"

    def test_preserves_semantic_elements(self):
        html = (
            '<h1>Title</h1><h2>Sub</h2><p>Para</p>'
            '<ul><li>Item</li></ul>'
            '<a href="https://example.com">Link</a>'
            '<img src="img.jpg" alt="Alt">'
            '<blockquote>Quote</blockquote>'
            '<table><tr><td>Cell</td></tr></table>'
        )
        result = parse_and_clean_html(html)
        assert "<h1>Title</h1>" in result
        assert "<h2>Sub</h2>" in result
        assert "<p>Para</p>" in result
        assert "<li>Item</li>" in result
        assert 'href="https://example.com"' in result
        assert 'src="img.jpg"' in result
        assert "<blockquote>Quote</blockquote>" in result
        assert "<td>Cell</td>" in result

    def test_preserves_inline_style_attrs(self):
        html = '<p style="color: red;">Hello</p>'
        result = parse_and_clean_html(html)
        assert 'style=' in result
        assert "Hello" in result

    def test_preserves_accessibility_attrs(self):
        html = '<div id="faq" role="region" aria-label="FAQ"><p>Content</p></div>'
        result = parse_and_clean_html(html)
        assert 'role="region"' in result
        assert 'aria-label="FAQ"' in result

    def test_preserves_html_comments(self):
        html = '<p>Before</p><!-- section marker --><p>After</p>'
        result = parse_and_clean_html(html)
        assert "<!-- section marker -->" in result

    def test_preserves_anchor_target_ids(self):
        html = '<a href="#faq">Go</a><div id="faq"><p>FAQ</p></div>'
        result = parse_and_clean_html(html)
        assert 'id="faq"' in result

    def test_void_elements(self):
        html = '<p>Before<br>After</p><hr><img src="x.jpg">'
        result = parse_and_clean_html(html)
        assert "<br>" in result
        assert "<hr>" in result
        assert "<img" in result

    def test_entities_preserved(self):
        html = '<p>A &amp; B &lt; C</p>'
        result = parse_and_clean_html(html)
        assert "&amp;" in result
        assert "&lt;" in result

    def test_idempotent(self):
        html = '<h1>Title</h1><p>Content with <strong>bold</strong></p>'
        first = parse_and_clean_html(html)
        second = parse_and_clean_html(first)
        assert first == second

    def test_theme_classes_whitelist(self):
        html = '<div class="rte hero-banner unknown"><p>Content</p></div>'
        result = parse_and_clean_html(html, theme_classes={"rte", "hero-banner"})
        assert "rte" in result
        assert "hero-banner" in result
        assert "unknown" not in result

    def test_theme_ids_whitelist(self):
        html = '<div id="pb-junk"><p>Content</p></div>'
        result = parse_and_clean_html(html, theme_ids=set())
        assert 'id=' not in result

    def test_real_magento_pagebuilder_bloat(self):
        """Regression: real Magento PageBuilder HTML from a collection body_html."""
        bloated = (
            '<div><div><div><div> <div> '
            '<h1>Award-Winning Haircare</h1> '
            '<div><p>We combine proven botanical extracts.</p></div> '
            '</div> </div></div></div></div> '
            '<style>#html-body [data-pb-style=PUM06FI],'
            '#html-body [data-pb-style=S4YVP9U]'
            '{background-position:left top}</style>'
            '<div data-content-type="row" data-appearance="contained" '
            'data-element="main">'
            '<div data-enable-parallax="0" data-parallax-speed="0.5" '
            'data-background-images="{}" data-pb-style="PUM06FI">'
            '<div class="pagebuilder-column-group" data-content-type="column-group">'
            '<h1 data-content-type="heading" data-pb-style="YQ4D9A4">'
            'العناية بالشعر</h1> '
            '<div data-content-type="text" data-pb-style="OIUIHC7">'
            '<p>نمزج مستخلصات نباتية</p>'
            '</div></div></div></div>'
        )
        result = parse_and_clean_html(bloated)
        # Bloat gone
        assert "data-pb-style" not in result
        assert "data-content-type" not in result
        assert "<style" not in result
        assert "pagebuilder" not in result
        # Content preserved
        assert "Award-Winning Haircare" in result
        assert "We combine proven botanical extracts." in result
        assert "العناية بالشعر" in result
        assert "نمزج مستخلصات نباتية" in result
        # Much smaller
        assert len(result) < len(bloated) // 2

    def test_malformed_html(self):
        """Unclosed tags should be handled gracefully."""
        html = '<div><p>Paragraph<div>Nested</div>'
        result = parse_and_clean_html(html)
        assert "Paragraph" in result
        assert "Nested" in result


# ─────────────────────────────────────────────────────────────────────────────
# Spanish Detection
# ─────────────────────────────────────────────────────────────────────────────

class TestHasSpanishContent:
    def test_english_content(self):
        assert not has_spanish_content("<p>Stop hair loss at roots.</p>")

    def test_spanish_content(self):
        assert has_spanish_content(
            "<p>Detn la cada desde la raz. Cebolla + Pptidos.</p>"
        )

    def test_spanish_domain_words(self):
        assert has_spanish_content(
            "<p>Champ fortalecedor con extracto de romero para el cuero cabelludo.</p>"
        )

    def test_mixed_spanish_english(self):
        # Spanish function words in otherwise English context
        assert has_spanish_content(
            "<p>Srum para el cuero cabelludo con pptidos avanzados.</p>"
        )

    def test_empty(self):
        assert not has_spanish_content("")
        assert not has_spanish_content(None)

    def test_short_text(self):
        assert not has_spanish_content("<p>OK</p>")

    def test_html_with_spanish_in_tags(self):
        """Spanish in HTML attributes shouldn't trigger detection."""
        html = '<img alt="imagen" src="https://example.com/img.jpg">'
        # Very short visible text, should not trigger
        assert not has_spanish_content(html)


class TestHasSpanishText:
    def test_plain_english(self):
        assert not has_spanish_text("Stop hair loss at the root")

    def test_plain_spanish(self):
        assert has_spanish_text("Champ fortalecedor con extracto de romero")

    def test_short_text(self):
        assert not has_spanish_text("OK")

    def test_empty(self):
        assert not has_spanish_text("")
        assert not has_spanish_text(None)


# ─────────────────────────────────────────────────────────────────────────────
# Extract Visible Text
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractVisibleText:
    def test_basic(self):
        assert extract_visible_text("<p>Hello <b>world</b></p>") == "Hello world"

    def test_script_removed(self):
        html = '<p>Hello</p><script>alert(1)</script><p>World</p>'
        assert "alert" not in extract_visible_text(html)
        assert "Hello" in extract_visible_text(html)

    def test_style_removed(self):
        html = '<style>.x{color:red}</style><p>Content</p>'
        result = extract_visible_text(html)
        assert "color" not in result
        assert "Content" in result

    def test_entities(self):
        assert "&" in extract_visible_text("<p>A &amp; B</p>")


# ─────────────────────────────────────────────────────────────────────────────
# Rich Text JSON Extraction
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractTextFromRichTextJson:
    def test_simple_rich_text(self):
        rt = '{"type":"root","children":[{"type":"paragraph","children":[{"type":"text","value":"Hello world"}]}]}'
        assert "Hello world" in extract_text_from_rich_text_json(rt)

    def test_empty(self):
        assert extract_text_from_rich_text_json("") == ""
        assert extract_text_from_rich_text_json(None) == ""

    def test_plain_string_fallback(self):
        assert extract_text_from_rich_text_json("not json") == "not json"

    def test_nested_children(self):
        rt = '{"type":"root","children":[{"type":"paragraph","children":[{"type":"text","value":"First"},{"type":"text","value":"Second"}]}]}'
        result = extract_text_from_rich_text_json(rt)
        assert "First" in result
        assert "Second" in result


class TestExtractTextForCheck:
    def test_plain_text(self):
        assert _extract_text_for_check("Hello", "single_line_text_field") == "Hello"

    def test_rich_text(self):
        rt = '{"type":"root","children":[{"type":"paragraph","children":[{"type":"text","value":"Content"}]}]}'
        result = _extract_text_for_check(rt, "rich_text_field")
        assert "Content" in result

    def test_empty(self):
        assert _extract_text_for_check("", "single_line_text_field") == ""


# ─────────────────────────────────────────────────────────────────────────────
# Audit — Full Coverage
# ─────────────────────────────────────────────────────────────────────────────

class TestAuditContent:
    def test_clean_content(self):
        resources = {
            "products": [{"id": 1, "handle": "test", "title": "Test",
                          "body_html": "<p>Clean English content</p>", "type": "product",
                          "metafields": []}]
        }
        findings = audit_content(resources)
        assert len(findings) == 0

    def test_finds_html_bloat(self):
        resources = {
            "pages": [{"id": 1, "handle": "test", "title": "Test",
                       "body_html": '<div data-pb-style="X">content</div>', "type": "page",
                       "metafields": []}]
        }
        findings = audit_content(resources)
        assert len(findings) == 1
        assert findings[0]["issue"] == "html_bloat"
        assert findings[0]["field"] == "body_html"

    def test_finds_spanish(self):
        resources = {
            "articles": [{"id": 1, "handle": "test", "title": "Test",
                          "body_html": "<p>Champ fortalecedor con extracto de romero para el cuero cabelludo.</p>",
                          "type": "article", "metafields": []}]
        }
        findings = audit_content(resources)
        assert len(findings) == 1
        assert findings[0]["issue"] == "spanish"
        assert findings[0]["field"] == "body_html"

    def test_skip_spanish_flag(self):
        resources = {
            "articles": [{"id": 1, "handle": "test", "title": "Test",
                          "body_html": "<p>Champ fortalecedor con extracto de romero.</p>",
                          "type": "article", "metafields": []}]
        }
        findings = audit_content(resources, skip_spanish=True)
        assert len(findings) == 0

    def test_skip_html_cleanup_flag(self):
        resources = {
            "pages": [{"id": 1, "handle": "test", "title": "Test",
                       "body_html": '<div data-pb-style="X">content</div>', "type": "page",
                       "metafields": []}]
        }
        findings = audit_content(resources, skip_html_cleanup=True)
        assert len(findings) == 0

    def test_both_issues(self):
        resources = {
            "pages": [{"id": 1, "handle": "test", "title": "Test",
                       "body_html": '<div data-pb-style="X">Champ fortalecedor para el cuero cabelludo</div>',
                       "type": "page", "metafields": []}]
        }
        findings = audit_content(resources)
        assert len(findings) == 2
        issues = {f["issue"] for f in findings}
        assert "html_bloat" in issues
        assert "spanish" in issues

    def test_empty_body(self):
        resources = {
            "products": [{"id": 1, "handle": "test", "title": "Test",
                          "body_html": "", "type": "product", "metafields": []}]
        }
        findings = audit_content(resources)
        assert len(findings) == 0

    # ── Title audit ──

    def test_finds_spanish_title(self):
        resources = {
            "products": [{"id": 1, "handle": "test",
                          "title": "Champ fortalecedor con extracto de romero",
                          "body_html": "<p>English body</p>", "type": "product",
                          "metafields": []}]
        }
        findings = audit_content(resources)
        assert len(findings) == 1
        assert findings[0]["issue"] == "spanish"
        assert findings[0]["field"] == "title"

    def test_english_title_clean(self):
        resources = {
            "products": [{"id": 1, "handle": "test",
                          "title": "Strengthening Shampoo",
                          "body_html": "", "type": "product",
                          "metafields": []}]
        }
        findings = audit_content(resources)
        assert len(findings) == 0

    # ── Metafield audit ──

    def test_finds_spanish_in_metafield(self):
        resources = {
            "products": [{"id": 1, "handle": "test", "title": "Test",
                          "body_html": "", "type": "product",
                          "metafields": [{
                              "id": 100, "key": "custom.tagline",
                              "value": "Fortalece tu cuero cabelludo desde la raiz",
                              "type": "single_line_text_field",
                              "namespace": "custom", "bare_key": "tagline",
                          }]}]
        }
        findings = audit_content(resources)
        assert len(findings) == 1
        assert findings[0]["issue"] == "spanish"
        assert findings[0]["field"] == "metafield:custom.tagline"

    def test_clean_metafield(self):
        resources = {
            "products": [{"id": 1, "handle": "test", "title": "Test",
                          "body_html": "", "type": "product",
                          "metafields": [{
                              "id": 100, "key": "custom.tagline",
                              "value": "Strengthen your scalp from the root",
                              "type": "single_line_text_field",
                              "namespace": "custom", "bare_key": "tagline",
                          }]}]
        }
        findings = audit_content(resources)
        assert len(findings) == 0

    def test_finds_html_bloat_in_rich_text_metafield(self):
        resources = {
            "products": [{"id": 1, "handle": "test", "title": "Test",
                          "body_html": "", "type": "product",
                          "metafields": [{
                              "id": 100, "key": "custom.key_benefits_content",
                              "value": '<div data-pb-style="X">Benefits here</div>',
                              "type": "rich_text_field",
                              "namespace": "custom", "bare_key": "key_benefits_content",
                          }]}]
        }
        findings = audit_content(resources)
        assert len(findings) == 1
        assert findings[0]["issue"] == "html_bloat"
        assert findings[0]["field"] == "metafield:custom.key_benefits_content"

    # ── Metaobject audit ──

    def test_finds_spanish_in_metaobject_field(self):
        resources = {
            "metaobjects": [{
                "id": "gid://shopify/Metaobject/123", "handle": "benefit-1",
                "title": "benefit-1", "type": "metaobject",
                "metaobject_type": "benefit",
                "body_html": "", "metafields": [],
                "text_fields": [{
                    "key": "title",
                    "value": "Fortalece tu cuero cabelludo",
                    "type": "single_line_text_field",
                }],
            }]
        }
        findings = audit_content(resources)
        assert len(findings) == 1
        assert findings[0]["issue"] == "spanish"
        assert findings[0]["field"] == "text_field:title"

    def test_clean_metaobject_fields(self):
        resources = {
            "metaobjects": [{
                "id": "gid://shopify/Metaobject/123", "handle": "benefit-1",
                "title": "benefit-1", "type": "metaobject",
                "metaobject_type": "benefit",
                "body_html": "", "metafields": [],
                "text_fields": [{
                    "key": "title",
                    "value": "Strengthen your scalp",
                    "type": "single_line_text_field",
                }],
            }]
        }
        findings = audit_content(resources)
        assert len(findings) == 0

    # ── Combined: multiple field types with issues ──

    def test_multiple_issues_across_fields(self):
        """A product with Spanish title, bloated body, and Spanish metafield."""
        resources = {
            "products": [{"id": 1, "handle": "test",
                          "title": "Champ fortalecedor con extracto de romero",
                          "body_html": '<div data-pb-style="X">English body</div>',
                          "type": "product",
                          "metafields": [{
                              "id": 100, "key": "custom.tagline",
                              "value": "Fortalece tu cuero cabelludo desde la raiz",
                              "type": "single_line_text_field",
                              "namespace": "custom", "bare_key": "tagline",
                          }]}]
        }
        findings = audit_content(resources)
        assert len(findings) == 3
        fields = {f["field"] for f in findings}
        assert "body_html" in fields
        assert "title" in fields
        assert "metafield:custom.tagline" in fields


# ─────────────────────────────────────────────────────────────────────────────
# SEO Field Fetching via Translations API
# ─────────────────────────────────────────────────────────────────────────────

class TestFetchProductSeoFields:
    """Test _fetch_product_seo_fields which uses the Translations API."""

    def _make_translatable_response(self, products_data, has_next=False, cursor="c1"):
        """Build a mock translatableResources GraphQL response.

        products_data: list of (gid, [(key, value), ...])
        """
        edges = []
        for gid, fields in products_data:
            content = [{"key": k, "value": v} for k, v in fields]
            edges.append({"node": {"resourceId": gid, "translatableContent": content}})
        return {
            "translatableResources": {
                "edges": edges,
                "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
            }
        }

    def test_returns_meta_title_and_description(self):
        client = MagicMock()
        client._graphql.return_value = self._make_translatable_response([
            ("gid://shopify/Product/111", [
                ("title", "My Product"),
                ("body_html", "<p>Body</p>"),
                ("meta_title", "SEO Title | TARA"),
                ("meta_description", "SEO description for product"),
            ]),
        ])
        result = _fetch_product_seo_fields(client)
        assert 111 in result
        assert result[111]["title_tag"] == "SEO Title | TARA"
        assert result[111]["description_tag"] == "SEO description for product"

    def test_skips_empty_seo_values(self):
        client = MagicMock()
        client._graphql.return_value = self._make_translatable_response([
            ("gid://shopify/Product/222", [
                ("title", "Product"),
                ("meta_title", ""),
                ("meta_description", ""),
            ]),
        ])
        result = _fetch_product_seo_fields(client)
        assert 222 not in result

    def test_skips_null_seo_values(self):
        client = MagicMock()
        client._graphql.return_value = self._make_translatable_response([
            ("gid://shopify/Product/333", [
                ("title", "Product"),
                ("meta_title", None),
                ("meta_description", None),
            ]),
        ])
        result = _fetch_product_seo_fields(client)
        assert 333 not in result

    def test_only_meta_title_present(self):
        client = MagicMock()
        client._graphql.return_value = self._make_translatable_response([
            ("gid://shopify/Product/444", [
                ("title", "Product"),
                ("meta_title", "Just a title"),
            ]),
        ])
        result = _fetch_product_seo_fields(client)
        assert 444 in result
        assert result[444]["title_tag"] == "Just a title"
        assert "description_tag" not in result[444]

    def test_only_meta_description_present(self):
        client = MagicMock()
        client._graphql.return_value = self._make_translatable_response([
            ("gid://shopify/Product/555", [
                ("title", "Product"),
                ("meta_description", "Desc only"),
            ]),
        ])
        result = _fetch_product_seo_fields(client)
        assert 555 in result
        assert result[555]["description_tag"] == "Desc only"
        assert "title_tag" not in result[555]

    def test_multiple_products(self):
        client = MagicMock()
        client._graphql.return_value = self._make_translatable_response([
            ("gid://shopify/Product/111", [
                ("meta_title", "Title 1"),
            ]),
            ("gid://shopify/Product/222", [
                ("meta_title", "Title 2"),
                ("meta_description", "Desc 2"),
            ]),
            ("gid://shopify/Product/333", [
                ("title", "No SEO"),
            ]),
        ])
        result = _fetch_product_seo_fields(client)
        assert len(result) == 2
        assert 111 in result
        assert 222 in result
        assert 333 not in result

    def test_spanish_meta_title_detected(self):
        """Spanish SEO titles from the Translations API are captured."""
        client = MagicMock()
        client._graphql.return_value = self._make_translatable_response([
            ("gid://shopify/Product/999", [
                ("title", "Hair Strength System"),
                ("meta_title", "Rutina Reparadora y Fortalecedora con Ajo Negro y Ceramidas | TARA"),
                ("meta_description", "Desc"),
            ]),
        ])
        result = _fetch_product_seo_fields(client)
        assert 999 in result
        assert "Rutina Reparadora" in result[999]["title_tag"]

    def test_pagination(self):
        """Fetches across multiple pages."""
        client = MagicMock()
        # Page 1: has next
        page1 = self._make_translatable_response(
            [("gid://shopify/Product/111", [("meta_title", "Title 1")])],
            has_next=True, cursor="cursor_1",
        )
        # Page 2: no next
        page2 = self._make_translatable_response(
            [("gid://shopify/Product/222", [("meta_title", "Title 2")])],
            has_next=False,
        )
        client._graphql.side_effect = [page1, page2]
        result = _fetch_product_seo_fields(client)
        assert len(result) == 2
        assert 111 in result
        assert 222 in result
        assert client._graphql.call_count == 2

    def test_empty_response(self):
        client = MagicMock()
        client._graphql.return_value = {
            "translatableResources": {
                "edges": [],
                "pageInfo": {"hasNextPage": False},
            }
        }
        result = _fetch_product_seo_fields(client)
        assert result == {}

    def test_ignores_non_seo_keys(self):
        """Only meta_title and meta_description are extracted."""
        client = MagicMock()
        client._graphql.return_value = self._make_translatable_response([
            ("gid://shopify/Product/111", [
                ("title", "Product Title"),
                ("body_html", "<p>Body</p>"),
                ("handle", "product-handle"),
            ]),
        ])
        result = _fetch_product_seo_fields(client)
        assert 111 not in result

    # ── SEO injection into audit pipeline ──

    def test_seo_fields_audited_for_spanish(self):
        """SEO fields injected into resources are audited for Spanish content."""
        resources = {
            "products": [{
                "id": 999, "handle": "test", "title": "Test Product",
                "body_html": "<p>Clean English</p>", "type": "product",
                "metafields": [{
                    "id": "seo-title-999",
                    "key": "global.title_tag",
                    "value": "Acondicionador Hidratante Fresa + NMF | TARA",
                    "type": "single_line_text_field",
                    "namespace": "global",
                    "bare_key": "title_tag",
                }],
            }],
        }
        findings = audit_content(resources)
        assert len(findings) == 1
        assert findings[0]["issue"] == "spanish"
        assert findings[0]["field"] == "metafield:global.title_tag"

    def test_english_seo_field_not_flagged(self):
        """English SEO fields should not be flagged."""
        resources = {
            "products": [{
                "id": 999, "handle": "test", "title": "Test Product",
                "body_html": "", "type": "product",
                "metafields": [{
                    "id": "seo-title-999",
                    "key": "global.title_tag",
                    "value": "Rejuvenating Scalp Serum | TARA",
                    "type": "single_line_text_field",
                    "namespace": "global",
                    "bare_key": "title_tag",
                }],
            }],
        }
        findings = audit_content(resources)
        assert len(findings) == 0

    def test_multiple_spanish_seo_fields(self):
        """Multiple products with Spanish SEO fields are all caught."""
        resources = {
            "products": [
                {
                    "id": 1, "handle": "p1", "title": "Product 1",
                    "body_html": "", "type": "product",
                    "metafields": [{
                        "id": "seo-title-1",
                        "key": "global.title_tag",
                        "value": "Acondicionador Suavizante y Nutritivo | TARA",
                        "type": "single_line_text_field",
                        "namespace": "global", "bare_key": "title_tag",
                    }],
                },
                {
                    "id": 2, "handle": "p2", "title": "Product 2",
                    "body_html": "", "type": "product",
                    "metafields": [{
                        "id": "seo-title-2",
                        "key": "global.title_tag",
                        "value": "Rutina Reparadora y Fortalecedora con Ajo Negro | TARA",
                        "type": "single_line_text_field",
                        "namespace": "global", "bare_key": "title_tag",
                    }],
                },
            ],
        }
        findings = audit_content(resources)
        assert len(findings) == 2
        assert all(f["issue"] == "spanish" for f in findings)
