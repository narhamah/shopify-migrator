"""Tests for review_content — Magento stripping and Spanish detection."""

import pytest

from tara_migrate.tools.review_content import (
    has_magento_remnants,
    strip_magento_html,
    has_spanish_content,
    extract_visible_text,
    audit_content,
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
            "<p>Detén la caída desde la raíz. Cebolla + Péptidos.</p>"
        )

    def test_spanish_domain_words(self):
        assert has_spanish_content(
            "<p>Champú fortalecedor con extracto de romero para el cuero cabelludo.</p>"
        )

    def test_mixed_spanish_english(self):
        # Spanish function words in otherwise English context
        assert has_spanish_content(
            "<p>Sérum para el cuero cabelludo con péptidos avanzados.</p>"
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
# Audit
# ─────────────────────────────────────────────────────────────────────────────

class TestAuditContent:
    def test_clean_content(self):
        resources = {
            "products": [{"id": 1, "handle": "test", "title": "Test",
                          "body_html": "<p>Clean English content</p>", "type": "product"}]
        }
        findings = audit_content(resources)
        assert len(findings) == 0

    def test_finds_magento(self):
        resources = {
            "pages": [{"id": 1, "handle": "test", "title": "Test",
                       "body_html": '<div data-pb-style="X">content</div>', "type": "page"}]
        }
        findings = audit_content(resources)
        assert len(findings) == 1
        assert findings[0]["issue"] == "magento"

    def test_finds_spanish(self):
        resources = {
            "articles": [{"id": 1, "handle": "test", "title": "Test",
                          "body_html": "<p>Champú fortalecedor con extracto de romero para el cuero cabelludo.</p>",
                          "type": "article"}]
        }
        findings = audit_content(resources)
        assert len(findings) == 1
        assert findings[0]["issue"] == "spanish"

    def test_skip_spanish_flag(self):
        resources = {
            "articles": [{"id": 1, "handle": "test", "title": "Test",
                          "body_html": "<p>Champú fortalecedor con extracto de romero.</p>",
                          "type": "article"}]
        }
        findings = audit_content(resources, skip_spanish=True)
        assert len(findings) == 0

    def test_skip_magento_flag(self):
        resources = {
            "pages": [{"id": 1, "handle": "test", "title": "Test",
                       "body_html": '<div data-pb-style="X">content</div>', "type": "page"}]
        }
        findings = audit_content(resources, skip_magento=True)
        assert len(findings) == 0

    def test_both_issues(self):
        resources = {
            "pages": [{"id": 1, "handle": "test", "title": "Test",
                       "body_html": '<div data-pb-style="X">Champú fortalecedor para el cuero cabelludo</div>',
                       "type": "page"}]
        }
        findings = audit_content(resources)
        assert len(findings) == 2
        issues = {f["issue"] for f in findings}
        assert issues == {"magento", "spanish"}

    def test_empty_body(self):
        resources = {
            "products": [{"id": 1, "handle": "test", "title": "Test",
                          "body_html": "", "type": "product"}]
        }
        findings = audit_content(resources)
        assert len(findings) == 0
