"""Tests for review_content — Magento stripping, Spanish detection, and full-coverage audit."""

import pytest

from tara_migrate.tools.review_content import (
    has_magento_remnants,
    strip_magento_html,
    has_spanish_content,
    has_spanish_text,
    extract_visible_text,
    extract_text_from_rich_text_json,
    audit_content,
    _extract_text_for_check,
)


# ─────────────────────────────────────────────────────────────────────────────
# Magento Detection
# ─────────────────────────────────────────────────────────────────────────────

class TestHasMagentoRemnants:
    def test_clean_html(self):
        assert not has_magento_remnants("<p>Hello world</p>")

    def test_empty(self):
        assert not has_magento_remnants("")
        assert not has_magento_remnants(None)

    def test_data_pb_style(self):
        html = '<div data-pb-style="ABC123">content</div>'
        assert has_magento_remnants(html)

    def test_data_content_type(self):
        html = '<div data-content-type="row" data-appearance="full-width">x</div>'
        assert has_magento_remnants(html)

    def test_pagebuilder_class(self):
        html = '<div class="pagebuilder-column">x</div>'
        assert has_magento_remnants(html)

    def test_product_carousel_class(self):
        html = '<div class="productsCarousel">x</div>'
        assert has_magento_remnants(html)

    def test_magento_init_script(self):
        html = '<script type="text/x-magento-init">{"foo":"bar"}</script>'
        assert has_magento_remnants(html)

    def test_product_item_class(self):
        html = '<div class="product-item-info">x</div>'
        assert has_magento_remnants(html)


# ─────────────────────────────────────────────────────────────────────────────
# Magento Stripping
# ─────────────────────────────────────────────────────────────────────────────

class TestStripMagentoHtml:
    def test_clean_html_unchanged(self):
        html = "<p>Hello <strong>world</strong></p>"
        assert strip_magento_html(html) == html

    def test_empty(self):
        assert strip_magento_html("") == ""
        assert strip_magento_html(None) is None

    def test_removes_magento_style_blocks(self):
        html = (
            '<style>#html-body [data-pb-style=ABC]{display:flex}</style>'
            '<p>Keep this</p>'
        )
        result = strip_magento_html(html)
        assert "data-pb-style" not in result
        assert "<p>Keep this</p>" in result

    def test_removes_script_blocks(self):
        html = (
            '<p>Hello</p>'
            '<script type="text/javascript">var x = 1;</script>'
            '<p>World</p>'
        )
        result = strip_magento_html(html)
        assert "<script" not in result
        assert "<p>Hello</p>" in result
        assert "<p>World</p>" in result

    def test_removes_magento_data_attributes(self):
        html = '<div data-content-type="row" data-appearance="full-width" data-element="main"><p>Keep</p></div>'
        result = strip_magento_html(html)
        assert "data-content-type" not in result
        assert "data-appearance" not in result
        assert "<p>Keep</p>" in result

    def test_removes_magento_classes(self):
        html = '<div class="pagebuilder-column myclass"><p>Keep</p></div>'
        result = strip_magento_html(html)
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
        result = strip_magento_html(html)
        assert "product-items" not in result
        assert "<h2>Our Products</h2>" in result
        assert "<p>Footer</p>" in result

    def test_removes_product_image_styles(self):
        html = (
            '<p>Content</p>'
            '<style>.product-image-container-278 { width: 132px; }</style>'
            '<p>More</p>'
        )
        result = strip_magento_html(html)
        assert "product-image-container" not in result
        assert "<p>Content</p>" in result

    def test_preserves_semantic_html(self):
        """Headings, paragraphs, images should survive."""
        html = (
            '<div data-content-type="row" data-appearance="default">'
            '<h3>Brush the Right Way</h3>'
            '<p>Use a wide-tooth comb for thin hair.</p>'
            '<img src="https://example.com/img.jpg" alt="Hair">'
            '</div>'
        )
        result = strip_magento_html(html)
        assert "<h3>Brush the Right Way</h3>" in result
        assert "Use a wide-tooth comb" in result
        assert '<img src="https://example.com/img.jpg"' in result

    def test_significant_size_reduction(self):
        """Magento HTML is typically 10-100x larger than visible content."""
        # Simulate a typical Magento pagebuilder block
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
        result = strip_magento_html(magento_html)
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
        result = strip_magento_html(html)
        assert "Brushing thin hair can be frustrating" in result
        # Magento classes should be gone
        assert "post-blogPostContent" not in result
        assert "row-contained" not in result
        assert "text-root" not in result


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

    def test_finds_magento(self):
        resources = {
            "pages": [{"id": 1, "handle": "test", "title": "Test",
                       "body_html": '<div data-pb-style="X">content</div>', "type": "page",
                       "metafields": []}]
        }
        findings = audit_content(resources)
        assert len(findings) == 1
        assert findings[0]["issue"] == "magento"
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

    def test_skip_magento_flag(self):
        resources = {
            "pages": [{"id": 1, "handle": "test", "title": "Test",
                       "body_html": '<div data-pb-style="X">content</div>', "type": "page",
                       "metafields": []}]
        }
        findings = audit_content(resources, skip_magento=True)
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
        assert issues == {"magento", "spanish"}

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

    def test_finds_magento_in_rich_text_metafield(self):
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
        assert findings[0]["issue"] == "magento"
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
        """A product with Spanish title, Magento body, and Spanish metafield."""
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
