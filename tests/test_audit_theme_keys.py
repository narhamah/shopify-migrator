"""Tests for audit_theme_keys — key classification, JSON parsing,
hardcoded string extraction, and template JSON extraction."""

import json
import os
import textwrap

import pytest

from tara_migrate.tools.audit_theme_keys import (
    _parse_json_with_comments,
    _is_non_text,
    _normalize_for_lookup,
    _find_in_lookup,
    classify_key,
    extract_hardcoded_strings,
    extract_template_json_strings,
)


# ─────────────────────────────────────────────────────────────────────────────
# _parse_json_with_comments
# ─────────────────────────────────────────────────────────────────────────────

class TestParseJsonWithComments:
    def test_plain_json(self):
        data = _parse_json_with_comments('{"a": 1}')
        assert data == {"a": 1}

    def test_block_comment_header(self):
        text = """\
/*
 * Auto-generated file — do not edit.
 */
{
  "greeting": "مرحبا"
}"""
        data = _parse_json_with_comments(text)
        assert data["greeting"] == "مرحبا"

    def test_multiple_block_comments(self):
        text = '/* comment 1 */{"key": /* inline */ "value"}'
        # inline comment removal should work too
        data = _parse_json_with_comments(text)
        assert data["key"] == "value"

    def test_empty_after_comment(self):
        data = _parse_json_with_comments("/* only comment */ {}")
        assert data == {}

    def test_strict_false_allows_control_chars(self):
        text = '{"desc": "line1\\tline2"}'
        data = _parse_json_with_comments(text)
        assert "line1" in data["desc"]


# ─────────────────────────────────────────────────────────────────────────────
# classify_key
# ─────────────────────────────────────────────────────────────────────────────

class TestClassifyKey:
    # ── Shopify platform keys ──
    def test_shopify_platform(self):
        cat, _ = classify_key("shopify.checkout.title", "Checkout")
        assert cat == "shopify_platform"

    def test_customer_accounts(self):
        cat, _ = classify_key("customer_accounts.login.title", "Log in")
        assert cat == "shopify_platform"

    # ── Theme locale keys ──
    def test_theme_locale_prefix(self):
        cat, _ = classify_key("accessibility.skip_to_content", "Skip to content")
        assert cat == "theme_locale"

    def test_products_locale(self):
        cat, _ = classify_key("products.product.add_to_cart", "Add to cart")
        assert cat == "theme_locale"

    # ── Junk keys ──
    def test_empty_value(self):
        cat, _ = classify_key("section.x.settings.text", "")
        assert cat == "junk"

    def test_image_ref(self):
        cat, _ = classify_key("section.x.settings.image", "shopify://shop_images/hero.jpg")
        assert cat == "junk"

    def test_url(self):
        cat, _ = classify_key("section.x.settings.link", "https://example.com")
        assert cat == "junk"

    def test_color_hex(self):
        cat, _ = classify_key("section.x.settings.color", "#ff0000")
        assert cat == "junk"

    def test_numeric(self):
        cat, _ = classify_key("section.x.settings.count", "42")
        assert cat == "junk"

    def test_boolean(self):
        cat, _ = classify_key("section.x.settings.flag", "true")
        assert cat == "junk"

    def test_json_blob(self):
        cat, _ = classify_key("section.x.settings.data", '{"reviewCount": 5}')
        assert cat == "junk"

    def test_uuid(self):
        cat, _ = classify_key("section.x.settings.id", "a1b2c3d4-e5f6-7890-abcd-ef1234567890")
        assert cat == "junk"

    def test_css_dimension(self):
        cat, _ = classify_key("section.x.settings.width", "16px")
        assert cat == "junk"

    def test_media_filename(self):
        cat, _ = classify_key("section.x.settings.bg", "hero-banner.jpg")
        assert cat == "junk"

    def test_internal_path(self):
        cat, _ = classify_key("section.x.settings.link", "/collections/all")
        assert cat == "junk"

    def test_html_wrapped_numbers(self):
        cat, _ = classify_key("section.x.settings.stat", "<h2>01</h2>")
        assert cat == "junk"

    # ── Useful keys ──
    def test_translatable_text(self):
        cat, _ = classify_key("section.x.settings.heading", "Sulfate Free")
        assert cat == "useful"

    def test_arabic_text(self):
        cat, _ = classify_key("section.x.settings.heading", "خالٍ من الكبريتات")
        assert cat == "useful"

    def test_html_with_text(self):
        cat, _ = classify_key("section.x.settings.text", "<h2>Our Story</h2>")
        assert cat == "useful"

    def test_mixed_text(self):
        cat, _ = classify_key("section.x.settings.heading", "Free Shipping on orders over 200 SAR")
        assert cat == "useful"


# ─────────────────────────────────────────────────────────────────────────────
# extract_hardcoded_strings
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractHardcodedStrings:
    def _make_theme(self, tmp_path, files):
        """Create a mini theme directory with the given files."""
        for rel_path, content in files.items():
            fpath = tmp_path / rel_path
            fpath.parent.mkdir(parents=True, exist_ok=True)
            fpath.write_text(content, encoding="utf-8")
        return str(tmp_path)

    def test_finds_hardcoded_tag_text(self, tmp_path):
        theme = self._make_theme(tmp_path, {
            "sections/test.liquid": textwrap.dedent("""\
                <h2>Our Amazing Products</h2>
                <p>{{ 'products.title' | t }}</p>
            """),
        })
        results = extract_hardcoded_strings(theme)
        english_texts = [r["english"] for r in results]
        assert any("Our Amazing Products" in t for t in english_texts)
        # The | t usage should NOT be detected
        assert not any("products.title" in t for t in english_texts)

    def test_finds_hardcoded_attributes(self, tmp_path):
        theme = self._make_theme(tmp_path, {
            "sections/quiz.liquid": textwrap.dedent("""\
                <input placeholder="Enter your email" aria-label="Email field">
            """),
        })
        results = extract_hardcoded_strings(theme)
        english_texts = [r["english"] for r in results]
        assert any("Enter your email" in t for t in english_texts)
        assert any("Email field" in t for t in english_texts)

    def test_skips_liquid_translated(self, tmp_path):
        theme = self._make_theme(tmp_path, {
            "sections/ok.liquid": textwrap.dedent("""\
                <h2>{{ 'sections.heading' | t }}</h2>
                <button>{{ 'actions.submit' | t }}</button>
            """),
        })
        results = extract_hardcoded_strings(theme)
        assert len(results) == 0

    def test_skips_brand_names(self, tmp_path):
        theme = self._make_theme(tmp_path, {
            "sections/brand.liquid": textwrap.dedent("""\
                <span>TARA</span>
                <span>Shopify</span>
            """),
        })
        results = extract_hardcoded_strings(theme)
        assert len(results) == 0

    def test_skips_urls_and_numbers(self, tmp_path):
        theme = self._make_theme(tmp_path, {
            "sections/data.liquid": textwrap.dedent("""\
                <a href="https://example.com">{{ link_text }}</a>
                <span>12345</span>
            """),
        })
        results = extract_hardcoded_strings(theme)
        assert len(results) == 0

    def test_skips_schema_block(self, tmp_path):
        theme = self._make_theme(tmp_path, {
            "sections/with_schema.liquid": textwrap.dedent("""\
                <h2>{{ section.settings.heading }}</h2>
                {% schema %}
                {
                  "name": "Test Section",
                  "presets": [{
                    "name": "Test",
                    "settings": {
                      "heading": "Hello World"
                    }
                  }]
                }
                {% endschema %}
            """),
        })
        results = extract_hardcoded_strings(theme)
        # "Hello World" in schema preset should be found
        preset_results = [r for r in results if r["type"] == "schema_preset"]
        assert any("Hello World" in r["english"] for r in preset_results)

    def test_skips_schema_locale_refs(self, tmp_path):
        theme = self._make_theme(tmp_path, {
            "sections/ref.liquid": textwrap.dedent("""\
                {% schema %}
                {
                  "name": "t:names.header",
                  "presets": [{
                    "name": "t:names.header",
                    "blocks": [{"type": "text", "settings": {"text": "t:html_defaults.heading"}}]
                  }]
                }
                {% endschema %}
            """),
        })
        results = extract_hardcoded_strings(theme)
        assert len(results) == 0

    def test_ignores_non_section_dirs(self, tmp_path):
        """Only scans sections/, snippets/, blocks/, layout/."""
        theme = self._make_theme(tmp_path, {
            "templates/index.json": '{"sections": {}}',
            "config/settings.json": '{"current": {}}',
        })
        results = extract_hardcoded_strings(theme)
        assert len(results) == 0


# ─────────────────────────────────────────────────────────────────────────────
# extract_template_json_strings
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractTemplateJsonStrings:
    def _make_theme(self, tmp_path, files):
        for rel_path, content in files.items():
            fpath = tmp_path / rel_path
            fpath.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(content, dict):
                fpath.write_text(json.dumps(content), encoding="utf-8")
            else:
                fpath.write_text(content, encoding="utf-8")
        return str(tmp_path)

    def test_extracts_text_settings(self, tmp_path):
        template = {
            "sections": {
                "main": {
                    "type": "hero",
                    "settings": {
                        "heading": "Welcome to TARA",
                        "text": "<p>Luxury scalp care</p>",
                    },
                }
            }
        }
        theme = self._make_theme(tmp_path, {"templates/index.json": template})
        results = extract_template_json_strings(theme)
        texts = [r["english"] for r in results]
        assert "Welcome to TARA" in texts
        assert "<p>Luxury scalp care</p>" in texts

    def test_extracts_block_settings(self, tmp_path):
        template = {
            "sections": {
                "hero": {
                    "type": "hero",
                    "settings": {},
                    "blocks": {
                        "btn_1": {
                            "type": "button",
                            "settings": {
                                "label": "Shop Now",
                                "link": "/collections/all",
                            },
                        }
                    },
                }
            }
        }
        theme = self._make_theme(tmp_path, {"templates/index.json": template})
        results = extract_template_json_strings(theme)
        texts = [r["english"] for r in results]
        assert "Shop Now" in texts
        # link is not a text key, should not be extracted
        assert "/collections/all" not in texts

    def test_skips_non_text_keys(self, tmp_path):
        template = {
            "sections": {
                "main": {
                    "type": "image",
                    "settings": {
                        "image": "shopify://shop_images/hero.jpg",
                        "color_scheme": "scheme-1",
                        "padding_top": "40",
                    },
                }
            }
        }
        theme = self._make_theme(tmp_path, {"templates/index.json": template})
        results = extract_template_json_strings(theme)
        assert len(results) == 0

    def test_skips_liquid_only_values(self, tmp_path):
        template = {
            "sections": {
                "main": {
                    "type": "text",
                    "settings": {
                        "text": "{{ product.title }}",
                    },
                }
            }
        }
        theme = self._make_theme(tmp_path, {"templates/index.json": template})
        results = extract_template_json_strings(theme)
        assert len(results) == 0

    def test_handles_comment_header_json(self, tmp_path):
        content = """\
/*
 * Auto-generated
 */
{
  "sections": {
    "main": {
      "type": "text",
      "settings": {
        "heading": "Page not found"
      }
    }
  }
}"""
        theme = self._make_theme(tmp_path, {"templates/404.json": content})
        results = extract_template_json_strings(theme)
        assert any("Page not found" in r["english"] for r in results)

    def test_empty_templates_dir(self, tmp_path):
        theme = self._make_theme(tmp_path, {"templates/.gitkeep": ""})
        results = extract_template_json_strings(theme)
        assert results == []


class TestIsNonText:
    """Tests for _is_non_text — identifying non-translatable values."""

    def test_shopify_image_ref(self):
        assert _is_non_text("shopify://shop_images/hero.jpg") is True

    def test_gid_ref(self):
        assert _is_non_text("gid://shopify/Collection/12345") is True

    def test_url(self):
        assert _is_non_text("https://example.com/page") is True

    def test_internal_path(self):
        assert _is_non_text("/collections/all") is True

    def test_hex_color(self):
        assert _is_non_text("#1a2b3c") is True

    def test_rgba_color(self):
        assert _is_non_text("rgba(0,0,0,0.5)") is True

    def test_number(self):
        assert _is_non_text("12345") is True

    def test_css_dimension(self):
        assert _is_non_text("48px") is True

    def test_boolean(self):
        assert _is_non_text("true") is True
        assert _is_non_text("False") is True

    def test_json_blob(self):
        assert _is_non_text('{"key": "value"}') is True

    def test_real_text(self):
        assert _is_non_text("Add to cart") is False

    def test_html_text(self):
        assert _is_non_text("<p>Free shipping on all orders</p>") is False

    def test_short_label(self):
        assert _is_non_text("Sold out") is False


class TestNormalizeForLookup:
    """Tests for _normalize_for_lookup."""

    def test_strips_html(self):
        assert _normalize_for_lookup("<p>Hello</p>") == "hello"

    def test_strips_liquid(self):
        assert _normalize_for_lookup("{{ count }} items") == "items"

    def test_strips_ruby_placeholders(self):
        assert _normalize_for_lookup("%{count} items") == "items"

    def test_collapses_whitespace(self):
        assert _normalize_for_lookup("  hello   world  ") == "hello world"


class TestFindInLookup:
    """Tests for _find_in_lookup — matching keys to crawled text."""

    def setup_method(self):
        self.site_texts = {
            "Add to cart": ["/products/kansa"],
            "Free shipping": ["/", "/collections/all"],
            "Sold out": ["/products/serum"],
            "Your Personalized Routine": ["/pages/quiz-results"],
        }
        self.norm_index = {}
        for text in self.site_texts:
            norm = _normalize_for_lookup(text)
            if norm:
                self.norm_index[norm] = text

    def test_exact_match(self):
        result = _find_in_lookup("Add to cart", self.site_texts, self.norm_index)
        assert result is not None
        assert result["type"] == "exact"

    def test_normalized_match(self):
        # Case difference
        result = _find_in_lookup("FREE SHIPPING", self.site_texts, self.norm_index)
        assert result is not None
        assert result["type"] == "normalized"

    def test_substring_match(self):
        result = _find_in_lookup("Personalized Routine",
                                  self.site_texts, self.norm_index)
        assert result is not None
        assert result["type"] == "substring"

    def test_no_match(self):
        result = _find_in_lookup("Something not on site",
                                  self.site_texts, self.norm_index)
        assert result is None

    def test_short_text_no_false_positive(self):
        # Very short strings shouldn't substring-match randomly
        result = _find_in_lookup("to", self.site_texts, self.norm_index)
        assert result is None
