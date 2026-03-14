"""Tests for shopify_fields — field/value classification for translation pipelines."""

import pytest

from tara_migrate.core.shopify_fields import (
    SKIP_FIELD_PATTERNS,
    TEXT_METAFIELD_TYPES,
    TRANSLATABLE_RESOURCE_TYPES,
    is_skippable_field,
    is_skippable_value,
)


# ─────────────────────────────────────────────────────────────────────────────
# SKIP_FIELD_PATTERNS — is_skippable_field()
# ─────────────────────────────────────────────────────────────────────────────

class TestIsSkippableFieldOriginalPatterns:
    """Original non-translatable patterns (image, icon, link, etc.)."""

    def test_image_suffix(self):
        assert is_skippable_field("sections.header.settings.logo.image")

    def test_image_numbered(self):
        assert is_skippable_field("sections.hero.settings.banner.image_1")
        assert is_skippable_field("sections.hero.settings.banner.image_3")

    def test_icon_colon(self):
        assert is_skippable_field("sections.benefits.blocks.abc.icon:material")

    def test_link_suffix(self):
        assert is_skippable_field("sections.footer.blocks.abc.link")

    def test_url_suffix(self):
        assert is_skippable_field("sections.hero.settings.video_url")
        assert is_skippable_field("settings.custom_css_url")

    def test_logo(self):
        assert is_skippable_field("sections.header.settings.logo")
        assert is_skippable_field("sections.header.settings.logo_width")

    def test_logo_requires_dot_prefix(self):
        """Pattern is '\\.logo' — 'footer_logo' has underscore, not dot, before 'logo'."""
        assert not is_skippable_field("sections.footer.settings.footer_logo")

    def test_favicon(self):
        assert is_skippable_field("sections.header.settings.favicon")

    def test_google_maps(self):
        assert is_skippable_field("sections.contact.settings.google_maps")
        assert is_skippable_field("settings.google_maps_key")

    def test_form_id(self):
        assert is_skippable_field("sections.contact.settings.form_id")

    def test_portal_id(self):
        assert is_skippable_field("sections.contact.settings.portal_id")

    def test_anchor_id(self):
        assert is_skippable_field("sections.faq.settings.anchor_id")

    def test_worker_url(self):
        assert is_skippable_field("settings.worker_url")

    def test_default_lat(self):
        assert is_skippable_field("sections.map.settings.default_lat")

    def test_default_lng(self):
        assert is_skippable_field("sections.map.settings.default_lng")

    def test_max_height(self):
        assert is_skippable_field("sections.hero.settings.max_height")

    def test_max_width(self):
        assert is_skippable_field("sections.hero.settings.max_width")


class TestIsSkippableFieldThemePatterns:
    """Theme-specific non-translatable patterns."""

    def test_color_scheme(self):
        assert is_skippable_field("color_schemes.scheme_1.settings.background")
        assert is_skippable_field("sections.header.settings.color_scheme")

    def test_color_suffix(self):
        assert is_skippable_field("sections.header.settings.text.color")
        assert is_skippable_field("settings.brand.color")

    def test_colors_suffix(self):
        assert is_skippable_field("sections.header.settings.accent.colors")

    def test_gradient(self):
        assert is_skippable_field("settings.button_gradient_1")
        assert is_skippable_field("sections.hero.settings.background_gradient")

    def test_shadow_dot(self):
        assert is_skippable_field("sections.card.settings.shadow_horizontal")
        assert is_skippable_field("sections.card.settings.shadow_vertical")

    def test_shadow_opacity(self):
        assert is_skippable_field("sections.card.settings.shadow_opacity")

    def test_opacity(self):
        assert is_skippable_field("sections.overlay.settings.opacity")

    def test_card_style(self):
        assert is_skippable_field("sections.collection.settings.card_style")

    def test_badge_position(self):
        assert is_skippable_field("sections.product.settings.badge_position")

    def test_crop_position(self):
        assert is_skippable_field("sections.gallery.settings.crop_position")

    def test_section_width(self):
        assert is_skippable_field("sections.hero.settings.section_width")

    def test_column_count(self):
        assert is_skippable_field("sections.grid.settings.column_count")

    def test_row_count(self):
        assert is_skippable_field("sections.grid.settings.row_count")

    def test_social_facebook(self):
        assert is_skippable_field("settings.social_facebook")

    def test_social_twitter(self):
        assert is_skippable_field("settings.social_twitter")

    def test_social_pinterest(self):
        assert is_skippable_field("settings.social_pinterest")

    def test_social_instagram(self):
        assert is_skippable_field("settings.social_instagram")

    def test_social_tiktok(self):
        assert is_skippable_field("settings.social_tiktok")

    def test_social_tumblr(self):
        assert is_skippable_field("settings.social_tumblr")

    def test_social_snapchat(self):
        assert is_skippable_field("settings.social_snapchat")

    def test_social_youtube(self):
        assert is_skippable_field("settings.social_youtube")


class TestIsSkippableFieldTranslatableMustNotSkip:
    """Fields that contain translatable content must NOT be skipped."""

    def test_title(self):
        assert not is_skippable_field("title")
        assert not is_skippable_field("sections.hero.settings.title")

    def test_body_html(self):
        assert not is_skippable_field("body_html")
        assert not is_skippable_field("sections.page.settings.body_html")

    def test_description(self):
        assert not is_skippable_field("description")
        assert not is_skippable_field("meta_description")

    def test_key_benefits_heading(self):
        assert not is_skippable_field("sections.product.blocks.abc.key_benefits_heading")

    def test_tagline(self):
        assert not is_skippable_field("custom.tagline")

    def test_question(self):
        assert not is_skippable_field("custom.question")

    def test_answer(self):
        assert not is_skippable_field("custom.answer")

    def test_name(self):
        assert not is_skippable_field("name")

    def test_meta_title(self):
        assert not is_skippable_field("meta_title")

    def test_meta_description(self):
        assert not is_skippable_field("meta_description")

    def test_heading(self):
        assert not is_skippable_field("sections.hero.settings.heading")

    def test_subheading(self):
        assert not is_skippable_field("sections.hero.settings.subheading")

    def test_button_text(self):
        assert not is_skippable_field("sections.hero.settings.button_text")

    def test_text(self):
        assert not is_skippable_field("sections.rich_text.blocks.abc.text")


class TestIsSkippableFieldEdgeCases:
    """Edge cases: partial matches, case sensitivity."""

    def test_partial_match_image_in_middle(self):
        """'image' pattern requires end-of-string — mid-string should not match."""
        assert not is_skippable_field("sections.image_gallery.settings.title")

    def test_color_mid_word_not_matched(self):
        """'.color' requires a dot prefix — 'multicolor' should not match."""
        assert not is_skippable_field("sections.hero.settings.multicolor")

    def test_social_facebook_with_suffix_not_matched(self):
        """social_facebook$ requires end-of-string."""
        assert not is_skippable_field("settings.social_facebook_alt_text")

    def test_social_youtube_with_suffix_not_matched(self):
        assert not is_skippable_field("settings.social_youtube_description")

    def test_case_sensitivity_image(self):
        """Patterns are lowercase — uppercase should NOT match (regex is case-sensitive)."""
        assert not is_skippable_field("sections.hero.settings.IMAGE")

    def test_case_sensitivity_color_scheme(self):
        assert not is_skippable_field("sections.hero.settings.Color_Scheme")

    def test_empty_key(self):
        assert not is_skippable_field("")

    def test_link_mid_string_not_matched(self):
        """'.link$' requires end-of-string — 'linked_text' is translatable."""
        assert not is_skippable_field("sections.hero.settings.linked_text")

    def test_opacity_mid_word_not_matched(self):
        """'.opacity$' requires dot prefix — 'opacity_text' should not match pattern."""
        assert not is_skippable_field("opacity_text")

    def test_shadow_without_dot_prefix(self):
        """'.shadow_' requires dot — bare 'shadow_x' should not match."""
        assert not is_skippable_field("shadow_x")


# ─────────────────────────────────────────────────────────────────────────────
# is_skippable_value()
# ─────────────────────────────────────────────────────────────────────────────

class TestIsSkippableValueEmptyNoneWhitespace:
    def test_none(self):
        assert is_skippable_value(None)

    def test_empty_string(self):
        assert is_skippable_value("")

    def test_whitespace_only(self):
        assert is_skippable_value("   ")
        assert is_skippable_value("\t\n")


class TestIsSkippableValueURLs:
    def test_shopify_url(self):
        assert is_skippable_value("shopify://products/kansa-wand")

    def test_http_url(self):
        assert is_skippable_value("http://example.com/image.jpg")

    def test_https_url(self):
        assert is_skippable_value("https://cdn.shopify.com/files/image.png")

    def test_slash_path(self):
        assert is_skippable_value("/collections/all")

    def test_slash_root(self):
        assert is_skippable_value("/")


class TestIsSkippableValueGIDs:
    def test_gid(self):
        assert is_skippable_value("gid://shopify/Product/12345")

    def test_gid_metaobject(self):
        assert is_skippable_value("gid://shopify/Metaobject/99999")


class TestIsSkippableValuePureNumbers:
    def test_integer(self):
        assert is_skippable_value("42")

    def test_zero(self):
        assert is_skippable_value("0")

    def test_decimal(self):
        assert is_skippable_value("3.14")

    def test_negative_integer(self):
        assert is_skippable_value("-7")

    def test_negative_decimal(self):
        assert is_skippable_value("-0.5")

    def test_large_number(self):
        assert is_skippable_value("1000000")

    def test_number_with_leading_whitespace(self):
        assert is_skippable_value("  123  ")


class TestIsSkippableValueHexStrings:
    def test_8_char_hex(self):
        assert is_skippable_value("abcdef01")

    def test_long_hex(self):
        assert is_skippable_value("1234567890abcdef")

    def test_short_hex_not_matched(self):
        """Less than 8 hex chars should not be treated as a hex ID."""
        assert not is_skippable_value("abcdef0")

    def test_hex_with_uppercase_not_matched(self):
        """Pattern uses [0-9a-f] (lowercase) — uppercase should not match."""
        assert not is_skippable_value("ABCDEF01")


class TestIsSkippableValueJSONArraysOfIDs:
    def test_gid_array(self):
        assert is_skippable_value('["gid://shopify/Product/1", "gid://shopify/Product/2"]')

    def test_numeric_id_array(self):
        assert is_skippable_value('["12345", "67890"]')

    def test_mixed_gid_numeric(self):
        assert is_skippable_value('["gid://shopify/Product/1", "999"]')

    def test_array_of_text_not_skipped(self):
        """Arrays containing translatable text should NOT be skipped."""
        assert not is_skippable_value('["hello", "world"]')

    def test_empty_array(self):
        """Empty JSON array — all() returns True for empty iterables."""
        assert is_skippable_value("[]")

    def test_malformed_json_array(self):
        assert not is_skippable_value("[not valid json")


class TestIsSkippableValueJSONConfigObjects:
    def test_review_count_config(self):
        assert is_skippable_value('{"reviewCount": 5, "widget": "stars"}')

    def test_json_object_without_review_count(self):
        assert not is_skippable_value('{"title": "Hello", "body": "World"}')

    def test_review_count_case_sensitive(self):
        """Must be exact 'reviewCount', not 'ReviewCount'."""
        assert not is_skippable_value('{"ReviewCount": 5}')


class TestIsSkippableValueCSSHexColors:
    def test_three_char_hex(self):
        assert is_skippable_value("#fff")

    def test_six_char_hex(self):
        assert is_skippable_value("#1a2b3c")

    def test_eight_char_hex_with_alpha(self):
        assert is_skippable_value("#1a2b3cff")

    def test_uppercase_hex_color(self):
        assert is_skippable_value("#AABBCC")

    def test_mixed_case_hex_color(self):
        assert is_skippable_value("#AaBbCc")

    def test_four_char_hex(self):
        assert is_skippable_value("#abcd")

    def test_invalid_hex_color_too_long(self):
        assert not is_skippable_value("#1a2b3c4d5")

    def test_hash_text_not_color(self):
        assert not is_skippable_value("#trending")


class TestIsSkippableValueCSSRGBAHSLA:
    def test_rgba(self):
        assert is_skippable_value("rgba(255, 0, 0, 0.5)")

    def test_rgb(self):
        assert is_skippable_value("rgb(100, 200, 50)")

    def test_hsla(self):
        assert is_skippable_value("hsla(120, 100%, 50%, 0.8)")

    def test_hsl(self):
        assert is_skippable_value("hsl(240, 50%, 50%)")


class TestIsSkippableValueBooleans:
    def test_true_lowercase(self):
        assert is_skippable_value("true")

    def test_false_lowercase(self):
        assert is_skippable_value("false")

    def test_true_titlecase(self):
        assert is_skippable_value("True")

    def test_false_uppercase(self):
        assert is_skippable_value("FALSE")

    def test_mixed_case_true(self):
        assert is_skippable_value("TrUe")


class TestIsSkippableValueCSSDimensions:
    def test_px(self):
        assert is_skippable_value("16px")

    def test_rem(self):
        assert is_skippable_value("1.5rem")

    def test_percent(self):
        assert is_skippable_value("100%")

    def test_vh(self):
        assert is_skippable_value("50vh")

    def test_em(self):
        assert is_skippable_value("2em")

    def test_vw(self):
        assert is_skippable_value("80vw")

    def test_negative_px(self):
        assert is_skippable_value("-10px")

    def test_zero_px(self):
        assert is_skippable_value("0px")

    def test_decimal_em(self):
        assert is_skippable_value("0.75em")


class TestIsSkippableValueTranslatableMustNotSkip:
    """Values that contain real translatable content must NOT be skipped."""

    def test_add_to_cart(self):
        assert not is_skippable_value("Add to cart")

    def test_search(self):
        assert not is_skippable_value("Search")

    def test_subscribe(self):
        assert not is_skippable_value("Subscribe")

    def test_free_shipping(self):
        assert not is_skippable_value("Free shipping over $50")

    def test_regular_sentence(self):
        assert not is_skippable_value("Discover our luxury scalp-care collection.")

    def test_product_name(self):
        assert not is_skippable_value("Kansa Wand")

    def test_arabic_text(self):
        assert not is_skippable_value("\u0623\u0636\u0641 \u0625\u0644\u0649 \u0627\u0644\u0633\u0644\u0629")

    def test_short_word(self):
        assert not is_skippable_value("Sale")

    def test_sentence_with_numbers(self):
        assert not is_skippable_value("Buy 2, get 1 free")

    def test_html_content(self):
        assert not is_skippable_value("<p>Welcome to our store</p>")

    def test_brand_name(self):
        assert not is_skippable_value("TARA")

    def test_text_starting_with_hash_word(self):
        """A hashtag-like string that is not a hex color."""
        assert not is_skippable_value("#ShopNow and save")

    def test_single_word(self):
        assert not is_skippable_value("Submit")

    def test_text_with_dimension_like_substring(self):
        """Text containing 'px' as part of a word should not be skipped."""
        assert not is_skippable_value("Experience luxury")


# ─────────────────────────────────────────────────────────────────────────────
# TRANSLATABLE_RESOURCE_TYPES
# ─────────────────────────────────────────────────────────────────────────────

class TestTranslatableResourceTypes:
    def test_contains_product(self):
        assert "PRODUCT" in TRANSLATABLE_RESOURCE_TYPES

    def test_contains_collection(self):
        assert "COLLECTION" in TRANSLATABLE_RESOURCE_TYPES

    def test_contains_metafield(self):
        assert "METAFIELD" in TRANSLATABLE_RESOURCE_TYPES

    def test_contains_metaobject(self):
        assert "METAOBJECT" in TRANSLATABLE_RESOURCE_TYPES

    def test_contains_online_store_theme(self):
        assert "ONLINE_STORE_THEME" in TRANSLATABLE_RESOURCE_TYPES

    def test_contains_page(self):
        assert "PAGE" in TRANSLATABLE_RESOURCE_TYPES

    def test_contains_blog(self):
        assert "BLOG" in TRANSLATABLE_RESOURCE_TYPES

    def test_contains_article(self):
        assert "ARTICLE" in TRANSLATABLE_RESOURCE_TYPES

    def test_exactly_eight_types(self):
        assert len(TRANSLATABLE_RESOURCE_TYPES) == 8

    def test_is_list(self):
        assert isinstance(TRANSLATABLE_RESOURCE_TYPES, list)


# ─────────────────────────────────────────────────────────────────────────────
# TEXT_METAFIELD_TYPES
# ─────────────────────────────────────────────────────────────────────────────

class TestTextMetafieldTypes:
    def test_contains_single_line(self):
        assert "single_line_text_field" in TEXT_METAFIELD_TYPES

    def test_contains_multi_line(self):
        assert "multi_line_text_field" in TEXT_METAFIELD_TYPES

    def test_contains_rich_text(self):
        assert "rich_text_field" in TEXT_METAFIELD_TYPES

    def test_exactly_three_types(self):
        assert len(TEXT_METAFIELD_TYPES) == 3

    def test_is_set(self):
        assert isinstance(TEXT_METAFIELD_TYPES, set)

    def test_does_not_contain_number(self):
        assert "number_integer" not in TEXT_METAFIELD_TYPES

    def test_does_not_contain_json(self):
        assert "json" not in TEXT_METAFIELD_TYPES

    def test_does_not_contain_url(self):
        assert "url" not in TEXT_METAFIELD_TYPES
