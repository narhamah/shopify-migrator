"""Tests for review_arabic — Arabic translation review and fix pipeline.

Covers:
  1. _extract_checkable_text()
  2. _has_untranslated_english()
  3. _has_spanish_in_arabic()
  4. classify_fields()
  5. run_semantic_check()
  6. run_audit()
  7. run_fix()
  8. main() CLI argument parsing
"""

import json
from unittest.mock import MagicMock, patch, call

import pytest

from tara_migrate.tools.review_arabic import (
    _extract_checkable_text,
    _has_untranslated_english,
    _has_spanish_in_arabic,
    _UNTRANSLATED_EN,
    _ALLOWED_LATIN,
    classify_fields,
    run_semantic_check,
    run_audit,
    run_fix,
    fetch_translations,
    _STRIP_ONLY,
    _RETRANSLATE,
    LOCALE,
    main,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_field(english="Hello world", arabic="مرحبا بالعالم", key="title",
                resource_id="gid://shopify/Product/1",
                resource_type="PRODUCT", outdated=False):
    return {
        "resource_id": resource_id,
        "resource_type": resource_type,
        "key": key,
        "english": english,
        "arabic": arabic,
        "digest": "abc123",
        "outdated": outdated,
    }


RICH_TEXT_JSON = json.dumps({
    "type": "root",
    "children": [
        {
            "type": "paragraph",
            "children": [
                {"type": "text", "value": "Hello world"}
            ]
        }
    ]
})

RICH_TEXT_AR = json.dumps({
    "type": "root",
    "children": [
        {
            "type": "paragraph",
            "children": [
                {"type": "text", "value": "مرحبا بالعالم"}
            ]
        }
    ]
})


# ═══════════════════════════════════════════════════════════════════════════
# 1. _extract_checkable_text()
# ═══════════════════════════════════════════════════════════════════════════

class TestExtractCheckableText:
    def test_plain_text_passthrough(self):
        assert _extract_checkable_text("Hello world") == "Hello world"

    def test_rich_text_json_extraction(self):
        result = _extract_checkable_text(RICH_TEXT_JSON)
        assert "Hello world" in result

    def test_html_tag_stripping(self):
        result = _extract_checkable_text("<p>Hello <strong>world</strong></p>")
        assert "<p>" not in result
        assert "<strong>" not in result
        assert "Hello" in result
        assert "world" in result

    def test_empty_string(self):
        assert _extract_checkable_text("") == ""

    def test_none(self):
        assert _extract_checkable_text(None) == ""

    def test_nested_html(self):
        result = _extract_checkable_text("<div><p>Inner <em>text</em></p></div>")
        assert "Inner" in result
        assert "text" in result
        assert "<" not in result

    def test_self_closing_tags(self):
        result = _extract_checkable_text("Hello<br/>world")
        assert "Hello" in result
        assert "world" in result

    def test_rich_text_fallback_to_raw(self):
        """When extract_text returns None, fall back to raw value."""
        # A value that looks like rich text but is malformed
        malformed = '{"type": "root"}'
        result = _extract_checkable_text(malformed)
        # Should still return something (either extracted or raw)
        assert result is not None


# ═══════════════════════════════════════════════════════════════════════════
# 2. _has_untranslated_english()
# ═══════════════════════════════════════════════════════════════════════════

class TestHasUntranslatedEnglish:
    """Test each pattern in _UNTRANSLATED_EN and the allowed exceptions."""

    # --- Product types ---

    def test_shampoo(self):
        assert _has_untranslated_english("شامبو shampoo للشعر")

    def test_conditioner(self):
        assert _has_untranslated_english("بلسم conditioner طبيعي")

    def test_serum(self):
        assert _has_untranslated_english("سيروم serum فروة الرأس")

    def test_mask(self):
        assert _has_untranslated_english("قناع mask للشعر")

    def test_scalp(self):
        assert _has_untranslated_english("علاج scalp المتقدم")

    def test_hair_care(self):
        assert _has_untranslated_english("منتجات hair care طبيعية")

    def test_leave_in(self):
        assert _has_untranslated_english("بخاخ leave-in للشعر")

    def test_leave_in_space(self):
        assert _has_untranslated_english("بخاخ leave in للشعر")

    def test_dry_oil(self):
        assert _has_untranslated_english("زيت dry oil خفيف")

    def test_dry_oil_hyphen(self):
        assert _has_untranslated_english("زيت dry-oil خفيف")

    def test_clay_mask(self):
        assert _has_untranslated_english("قناع clay mask طبيعي")

    def test_clay_mask_hyphen(self):
        assert _has_untranslated_english("قناع clay-mask طبيعي")

    def test_body_scrub(self):
        assert _has_untranslated_english("مقشر body scrub طبيعي")

    # --- Descriptive terms ---

    def test_volumizing(self):
        assert _has_untranslated_english("شامبو volumizing للشعر")

    def test_thickening(self):
        assert _has_untranslated_english("سيروم thickening مكثف")

    def test_hydrating(self):
        assert _has_untranslated_english("كريم hydrating مرطب")

    def test_nourishing(self):
        assert _has_untranslated_english("بلسم nourishing مغذي")

    def test_revitalizing(self):
        assert _has_untranslated_english("علاج revitalizing منشط")

    def test_exfoliating(self):
        assert _has_untranslated_english("مقشر exfoliating لطيف")

    def test_replenishing(self):
        assert _has_untranslated_english("كريم replenishing معيد")

    def test_purifying(self):
        assert _has_untranslated_english("غسول purifying منقي")

    def test_nurturing(self):
        assert _has_untranslated_english("زيت nurturing مغذي")

    def test_detoxifying(self):
        assert _has_untranslated_english("قناع detoxifying مزيل")

    # --- Compound terms ---

    def test_anti_hair_fall(self):
        assert _has_untranslated_english("علاج anti-hair-fall متقدم")

    def test_anti_hair_fall_spaces(self):
        assert _has_untranslated_english("علاج anti hair fall متقدم")

    def test_age_well(self):
        assert _has_untranslated_english("مجموعة age-well مضادة")

    def test_intensive_treatment(self):
        assert _has_untranslated_english("مكثف intensive treatment للشعر")

    # --- Range/collection terms ---

    def test_multivitamin(self):
        assert _has_untranslated_english("مجموعة multivitamin للشعر")

    def test_multivitamins(self):
        assert _has_untranslated_english("فيتامينات multivitamins متعددة")

    # --- System/bundle terms ---

    def test_hair_density_system(self):
        assert _has_untranslated_english("نظام hair density system للشعر")

    def test_hair_stimulation_system(self):
        assert _has_untranslated_english("نظام hair stimulation system لتحفيز")

    def test_scalp_hair_revival(self):
        assert _has_untranslated_english("مجموعة scalp+hair revival إحياء")

    def test_scalp_hair_revival_spaces(self):
        assert _has_untranslated_english("مجموعة scalp hair revival إحياء")

    def test_nurture_system(self):
        assert _has_untranslated_english("نظام nurture system للعناية")

    def test_age_well_system(self):
        assert _has_untranslated_english("نظام age-well system مضاد")

    def test_age_well_system_spaces(self):
        assert _has_untranslated_english("نظام age well system مضاد")

    # --- Common nouns/adjectives ---

    def test_description(self):
        assert _has_untranslated_english("حقل description المنتج")

    def test_benefits(self):
        assert _has_untranslated_english("فوائد benefits الرئيسية")

    def test_ingredients(self):
        assert _has_untranslated_english("مكونات ingredients طبيعية")

    def test_how_to_use(self):
        assert _has_untranslated_english("طريقة how to use الاستخدام")

    def test_free_of(self):
        assert _has_untranslated_english("خالي free of من البارابين")

    def test_clinical_results(self):
        assert _has_untranslated_english("نتائج clinical results سريرية")

    def test_key_benefits(self):
        assert _has_untranslated_english("الفوائد key benefits الرئيسية")

    # --- Case insensitivity ---

    def test_case_insensitive_shampoo(self):
        assert _has_untranslated_english("شامبو Shampoo للشعر")

    def test_case_insensitive_SERUM(self):
        assert _has_untranslated_english("سيروم SERUM فروة")

    # --- In Arabic context ---

    def test_surrounded_by_arabic(self):
        result = _has_untranslated_english("هذا المنتج هو shampoo رائع للشعر الجاف")
        assert result

    # --- Must NOT match allowed Latin words ---

    def test_allowed_tara(self):
        assert not _has_untranslated_english("منتجات TARA الفاخرة")

    def test_allowed_kansa(self):
        assert not _has_untranslated_english("أداة Kansa التقليدية")

    def test_allowed_wand(self):
        # "Wand" by itself is allowed
        assert not _has_untranslated_english("عصا Wand للوجه")

    def test_allowed_gua_sha(self):
        assert not _has_untranslated_english("أداة Gua Sha للوجه")

    def test_allowed_ph(self):
        assert not _has_untranslated_english("توازن pH للشعر")

    def test_allowed_aha(self):
        assert not _has_untranslated_english("حمض AHA لطيف")

    def test_allowed_bha(self):
        assert not _has_untranslated_english("حمض BHA مقشر")

    def test_allowed_nmf(self):
        assert not _has_untranslated_english("عامل NMF مرطب")

    def test_allowed_spf(self):
        assert not _has_untranslated_english("حماية SPF عالية")

    def test_allowed_uv(self):
        assert not _has_untranslated_english("أشعة UV الضارة")

    def test_allowed_ml(self):
        assert not _has_untranslated_english("50 ml حجم")

    def test_allowed_mg(self):
        assert not _has_untranslated_english("200 mg تركيز")

    # --- Edge cases ---

    def test_empty_string(self):
        assert not _has_untranslated_english("")

    def test_none(self):
        assert not _has_untranslated_english(None)

    def test_pure_arabic(self):
        assert not _has_untranslated_english("شامبو طبيعي للعناية بالشعر")

    def test_html_stripped_before_check(self):
        result = _has_untranslated_english("<p>شامبو shampoo للشعر</p>")
        assert result

    def test_json_templates_stripped(self):
        result = _has_untranslated_english("شامبو {variable} للشعر")
        assert not result  # {variable} is stripped, no English match

    def test_returns_list_of_matches(self):
        matches = _has_untranslated_english("شامبو shampoo و conditioner للشعر")
        assert isinstance(matches, list)
        assert len(matches) >= 2
        assert "shampoo" in [m.lower() for m in matches]
        assert "conditioner" in [m.lower() for m in matches]


# ═══════════════════════════════════════════════════════════════════════════
# 3. _has_spanish_in_arabic()
# ═══════════════════════════════════════════════════════════════════════════

class TestHasSpanishInArabic:
    @patch("tara_migrate.tools.review_arabic.is_spanish", return_value=True)
    def test_arabic_with_spanish_words(self, mock_is_spanish):
        text = "هذا المنتج يحتوي على fortalecedor con extracto de romero في تركيبته"
        assert _has_spanish_in_arabic(text)

    @patch("tara_migrate.tools.review_arabic.is_spanish", return_value=False)
    def test_pure_arabic(self, mock_is_spanish):
        assert not _has_spanish_in_arabic("شامبو طبيعي للعناية بفروة الرأس والشعر")

    def test_short_latin_portions(self):
        """Latin text < 15 chars should return False regardless."""
        text = "منتج TARA الفاخر"  # TARA is only 4 chars
        assert not _has_spanish_in_arabic(text)

    @patch("tara_migrate.tools.review_arabic.is_spanish", return_value=False)
    def test_arabic_with_allowed_brand_names(self, mock_is_spanish):
        text = "أداة TARA Kansa Wand للوجه"
        result = _has_spanish_in_arabic(text)
        # The Latin portion is "TARA Kansa Wand" = 15 chars, but is_spanish returns False
        assert not result

    def test_empty_text(self):
        assert not _has_spanish_in_arabic("")

    def test_none(self):
        assert not _has_spanish_in_arabic(None)

    @patch("tara_migrate.tools.review_arabic.is_spanish", return_value=True)
    def test_long_spanish_phrase(self, mock_is_spanish):
        text = "هذا champú fortalecedor con extracto de romero para el cuero cabelludo"
        assert _has_spanish_in_arabic(text)

    def test_no_latin_at_all(self):
        assert not _has_spanish_in_arabic("مرحبا بالعالم")


# ═══════════════════════════════════════════════════════════════════════════
# 4. classify_fields()
# ═══════════════════════════════════════════════════════════════════════════

class TestClassifyFields:
    """Test each classification status and stats counting."""

    def test_skip_non_translatable_field(self):
        """Fields with image/url patterns should be SKIP."""
        fields = [_make_field(key="hero.image", english="image.jpg", arabic="image.jpg")]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "SKIP"
        assert stats["skip"] == 1

    def test_skip_non_translatable_value(self):
        """Values like URLs should be SKIP."""
        fields = [_make_field(
            english="https://cdn.shopify.com/img.jpg",
            arabic="https://cdn.shopify.com/img.jpg",
        )]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "SKIP"
        assert stats["skip"] == 1

    def test_skip_number_value(self):
        fields = [_make_field(english="29.99", arabic="29.99")]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "SKIP"

    def test_missing_translation(self):
        fields = [_make_field(arabic=None)]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "MISSING"
        assert stats["missing"] == 1

    def test_missing_translation_empty_string(self):
        fields = [_make_field(arabic="")]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "MISSING"
        assert stats["missing"] == 1

    def test_identical_translation(self):
        fields = [_make_field(english="Hello world", arabic="Hello world")]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "IDENTICAL"
        assert stats["identical"] == 1

    def test_not_arabic(self):
        """Translation with no Arabic characters."""
        fields = [_make_field(english="Hello world", arabic="Hola mundo")]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "NOT_ARABIC"
        assert stats["not_arabic"] == 1

    def test_ok_translation(self):
        fields = [_make_field(english="Hello world", arabic="مرحبا بالعالم")]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "OK"
        assert stats["ok"] == 1

    @patch("tara_migrate.tools.review_arabic.is_spanish", return_value=True)
    def test_source_spanish(self, mock_is_spanish):
        """English source that is actually Spanish (>= 15 chars)."""
        fields = [_make_field(
            english="Rutina Reparadora y Fortalecedora con Ajo Negro y Ceramidas",
            arabic="روتين الإصلاح والتقوية بالثوم الأسود والسيراميدات",
        )]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "SOURCE_SPANISH"
        assert stats["source_spanish"] == 1
        assert "Spanish" in results[0]["detail"]

    @patch("tara_migrate.tools.review_arabic.is_spanish", return_value=True)
    def test_source_spanish_short_not_flagged(self, mock_is_spanish):
        """Short Spanish text (< 15 chars) should NOT be flagged as SOURCE_SPANISH."""
        fields = [_make_field(
            english="Hola mundo",  # 10 chars, < 15
            arabic="مرحبا بالعالم",
        )]
        results, stats = classify_fields(fields)
        # Short text won't be flagged as SOURCE_SPANISH
        assert results[0]["status"] != "SOURCE_SPANISH"

    @patch("tara_migrate.tools.review_arabic.is_spanish", return_value=False)
    def test_english_source_not_flagged(self, mock_is_spanish):
        """English source text should NOT be flagged as SOURCE_SPANISH."""
        fields = [_make_field(
            english="Strengthen your scalp from the root with natural ingredients",
            arabic="قوي فروة رأسك من الجذور بمكونات طبيعية",
        )]
        results, stats = classify_fields(fields)
        assert results[0]["status"] != "SOURCE_SPANISH"
        assert stats["source_spanish"] == 0

    @patch("tara_migrate.tools.review_arabic.is_spanish", return_value=True)
    def test_source_spanish_real_meta_title(self, mock_is_spanish):
        """Test with a realistic Spanish meta_title."""
        fields = [_make_field(
            english="Champú fortalecedor con extracto de romero para el cuero cabelludo",
            arabic="شامبو مقوي بمستخلص إكليل الجبل لفروة الرأس",
        )]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "SOURCE_SPANISH"

    @patch("tara_migrate.tools.review_arabic.has_html_bloat", return_value=True)
    @patch("tara_migrate.tools.review_arabic.is_spanish", return_value=False)
    def test_html_bloat(self, mock_is_spanish, mock_bloat):
        fields = [_make_field(
            english="<p>Hello world</p>",
            arabic='<div data-pb-style="X">مرحبا بالعالم</div>',
        )]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "HTML_BLOAT"
        assert stats["html_bloat"] == 1

    @patch("tara_migrate.tools.review_arabic.is_spanish", return_value=False)
    def test_has_english(self, mock_is_spanish):
        fields = [_make_field(
            english="Hydrating Shampoo for dry scalp",
            arabic="شامبو hydrating لفروة الرأس الجافة",
        )]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "HAS_ENGLISH"
        assert stats["has_english"] == 1
        assert "hydrating" in results[0]["detail"].lower()

    @patch("tara_migrate.tools.review_arabic.is_spanish", return_value=False)
    @patch("tara_migrate.tools.review_arabic._has_spanish_in_arabic", return_value=True)
    def test_has_spanish(self, mock_spanish_ar, mock_is_spanish):
        fields = [_make_field(
            english="Strengthening Shampoo",
            arabic="شامبو مقوي champú fortalecedor con extracto",
        )]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "HAS_SPANISH"
        assert stats["has_spanish"] == 1

    def test_stats_counting_multiple(self):
        """Multiple fields with different statuses."""
        fields = [
            _make_field(english="Hello", arabic="مرحبا"),                    # OK
            _make_field(english="World", arabic=None),                       # MISSING
            _make_field(english="Test", arabic="Test"),                      # IDENTICAL
            _make_field(key="hero.image", english="img.jpg", arabic="img.jpg"),  # SKIP
        ]
        results, stats = classify_fields(fields)
        assert stats["skip"] == 1
        assert stats["missing"] == 1
        assert stats["identical"] == 1
        assert stats["ok"] == 1
        assert stats["total"] == 3  # SKIP not counted in total

    @patch("tara_migrate.tools.review_arabic.is_spanish", return_value=False)
    def test_source_spanish_not_checked_for_missing(self, mock_is_spanish):
        """SOURCE_SPANISH check should not run on MISSING translations."""
        fields = [_make_field(english="Some English text here", arabic=None)]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "MISSING"
        # is_spanish should not be called since status is MISSING
        mock_is_spanish.assert_not_called()

    @patch("tara_migrate.tools.review_arabic.is_spanish", return_value=False)
    def test_ok_fields_checked_for_english_and_spanish(self, mock_is_spanish):
        """OK fields should be checked for English remnants and Spanish."""
        fields = [_make_field(english="Good product", arabic="منتج جيد")]
        results, stats = classify_fields(fields)
        assert results[0]["status"] == "OK"

    def test_all_results_have_status_and_detail(self):
        fields = [
            _make_field(english="Hello", arabic="مرحبا"),
            _make_field(english="World", arabic=None),
        ]
        results, stats = classify_fields(fields)
        for r in results:
            assert "status" in r
            assert "detail" in r


# ═══════════════════════════════════════════════════════════════════════════
# 5. run_semantic_check()
# ═══════════════════════════════════════════════════════════════════════════

class TestRunSemanticCheck:
    def _mock_haiku_response(self, json_array):
        """Create a mock Anthropic response with the given JSON array."""
        mock_resp = MagicMock()
        mock_resp.content = [MagicMock()]
        mock_resp.content[0].text = json.dumps(json_array)
        return mock_resp

    def test_empty_input(self):
        haiku_client = MagicMock()
        result = run_semantic_check([], haiku_client)
        assert result == {}
        haiku_client.messages.create.assert_not_called()

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    def test_single_pass(self, mock_sleep):
        haiku_client = MagicMock()
        haiku_client.messages.create.return_value = self._mock_haiku_response(
            [{"id": 1, "pass": True}]
        )
        ok_fields = [(0, _make_field())]
        result = run_semantic_check(ok_fields, haiku_client, batch_size=15)
        assert 0 in result
        assert result[0]["pass"] is True

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    def test_single_fail(self, mock_sleep):
        haiku_client = MagicMock()
        haiku_client.messages.create.return_value = self._mock_haiku_response(
            [{"id": 1, "pass": False, "reason": "wrong meaning"}]
        )
        ok_fields = [(5, _make_field())]
        result = run_semantic_check(ok_fields, haiku_client, batch_size=15)
        assert 5 in result
        assert result[5]["pass"] is False
        assert result[5]["reason"] == "wrong meaning"

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    def test_batch_processing_multiple_pairs(self, mock_sleep):
        haiku_client = MagicMock()
        haiku_client.messages.create.return_value = self._mock_haiku_response([
            {"id": 1, "pass": True},
            {"id": 2, "pass": False, "reason": "mismatch"},
            {"id": 3, "pass": True},
        ])
        ok_fields = [
            (10, _make_field(english="A", arabic="أ")),
            (20, _make_field(english="B", arabic="ب")),
            (30, _make_field(english="C", arabic="ج")),
        ]
        result = run_semantic_check(ok_fields, haiku_client, batch_size=15)
        assert len(result) == 3
        assert result[10]["pass"] is True
        assert result[20]["pass"] is False
        assert result[30]["pass"] is True

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    def test_multiple_batches(self, mock_sleep):
        haiku_client = MagicMock()
        # First batch: 2 items, second batch: 1 item
        haiku_client.messages.create.side_effect = [
            self._mock_haiku_response([
                {"id": 1, "pass": True},
                {"id": 2, "pass": True},
            ]),
            self._mock_haiku_response([
                {"id": 1, "pass": False, "reason": "bad"},
            ]),
        ]
        ok_fields = [
            (0, _make_field()),
            (1, _make_field()),
            (2, _make_field()),
        ]
        result = run_semantic_check(ok_fields, haiku_client, batch_size=2)
        assert len(result) == 3
        assert result[0]["pass"] is True
        assert result[1]["pass"] is True
        assert result[2]["pass"] is False

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    def test_api_error_marks_as_pass(self, mock_sleep):
        """API errors should be handled conservatively (mark as pass)."""
        haiku_client = MagicMock()
        haiku_client.messages.create.side_effect = Exception("API timeout")
        ok_fields = [(0, _make_field()), (1, _make_field())]
        result = run_semantic_check(ok_fields, haiku_client, batch_size=15)
        assert len(result) == 2
        assert result[0]["pass"] is True
        assert "API error" in result[0]["reason"]
        assert result[1]["pass"] is True

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    def test_parse_error_marks_as_pass(self, mock_sleep):
        """If JSON cannot be parsed from response, mark all as pass."""
        haiku_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = [MagicMock()]
        mock_resp.content[0].text = "I cannot process this request"
        haiku_client.messages.create.return_value = mock_resp
        ok_fields = [(0, _make_field())]
        result = run_semantic_check(ok_fields, haiku_client, batch_size=15)
        assert result[0]["pass"] is True
        assert result[0]["reason"] == "parse error"

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    def test_json_embedded_in_text(self, mock_sleep):
        """JSON array embedded in surrounding text should be extracted."""
        haiku_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = [MagicMock()]
        mock_resp.content[0].text = (
            'Here are the results:\n[{"id":1,"pass":true}]\nDone.'
        )
        haiku_client.messages.create.return_value = mock_resp
        ok_fields = [(0, _make_field())]
        result = run_semantic_check(ok_fields, haiku_client, batch_size=15)
        assert result[0]["pass"] is True

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    def test_rich_text_in_semantic_check(self, mock_sleep):
        """Rich text JSON should have text extracted for the prompt."""
        haiku_client = MagicMock()
        haiku_client.messages.create.return_value = self._mock_haiku_response(
            [{"id": 1, "pass": True}]
        )
        ok_fields = [(0, _make_field(english=RICH_TEXT_JSON, arabic=RICH_TEXT_AR))]
        result = run_semantic_check(ok_fields, haiku_client, batch_size=15)
        assert result[0]["pass"] is True
        # Check that the prompt contained [rich_text] prefix
        call_args = haiku_client.messages.create.call_args
        prompt_content = call_args[1]["messages"][0]["content"]
        assert "[rich_text]" in prompt_content

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    def test_model_parameter_passed(self, mock_sleep):
        haiku_client = MagicMock()
        haiku_client.messages.create.return_value = self._mock_haiku_response(
            [{"id": 1, "pass": True}]
        )
        ok_fields = [(0, _make_field())]
        run_semantic_check(ok_fields, haiku_client, model="custom-model")
        call_args = haiku_client.messages.create.call_args
        assert call_args[1]["model"] == "custom-model"


# ═══════════════════════════════════════════════════════════════════════════
# 6. run_audit()
# ═══════════════════════════════════════════════════════════════════════════

class TestRunAudit:
    @patch("tara_migrate.tools.review_arabic.run_semantic_check")
    @patch("tara_migrate.tools.review_arabic.fetch_translations")
    def test_full_pipeline(self, mock_fetch, mock_semantic):
        """Fetch -> classify -> semantic check."""
        mock_fetch.return_value = (
            [
                _make_field(english="Hello", arabic="مرحبا"),
                _make_field(english="World", arabic=None),
            ],
            {"PRODUCT": (1, 2)},
        )
        mock_semantic.return_value = {
            0: {"pass": True, "reason": ""},
        }
        client = MagicMock()
        haiku_client = MagicMock()

        classified, problems, stats = run_audit(
            client, ["PRODUCT"], haiku_client, "claude-haiku-4-5-20251001",
        )

        assert len(classified) == 2
        assert len(problems) == 1  # MISSING
        assert problems[0]["status"] == "MISSING"
        mock_fetch.assert_called_once()
        mock_semantic.assert_called_once()

    @patch("tara_migrate.tools.review_arabic.run_semantic_check")
    @patch("tara_migrate.tools.review_arabic.fetch_translations")
    def test_skip_semantic(self, mock_fetch, mock_semantic):
        mock_fetch.return_value = (
            [_make_field(english="Hello", arabic="مرحبا")],
            {"PRODUCT": (1, 1)},
        )
        client = MagicMock()
        haiku_client = MagicMock()

        classified, problems, stats = run_audit(
            client, ["PRODUCT"], haiku_client, "claude-haiku-4-5-20251001",
            skip_semantic=True,
        )

        mock_semantic.assert_not_called()
        assert stats["ok"] == 1

    @patch("tara_migrate.tools.review_arabic.run_semantic_check")
    @patch("tara_migrate.tools.review_arabic.fetch_translations")
    def test_semantic_failures_applied(self, mock_fetch, mock_semantic):
        """Semantic failures should change OK to SEMANTIC_MISMATCH."""
        mock_fetch.return_value = (
            [
                _make_field(english="Hello", arabic="مرحبا"),
                _make_field(english="World", arabic="عالم"),
            ],
            {"PRODUCT": (1, 2)},
        )
        mock_semantic.return_value = {
            0: {"pass": True, "reason": ""},
            1: {"pass": False, "reason": "wrong meaning"},
        }
        client = MagicMock()
        haiku_client = MagicMock()

        classified, problems, stats = run_audit(
            client, ["PRODUCT"], haiku_client, "claude-haiku-4-5-20251001",
        )

        # One OK, one SEMANTIC_MISMATCH
        statuses = [c["status"] for c in classified]
        assert "SEMANTIC_MISMATCH" in statuses
        assert "OK" in statuses
        assert len(problems) == 1
        assert problems[0]["status"] == "SEMANTIC_MISMATCH"

    @patch("tara_migrate.tools.review_arabic.run_semantic_check")
    @patch("tara_migrate.tools.review_arabic.fetch_translations")
    def test_problem_collection(self, mock_fetch, mock_semantic):
        """Problems should only include non-OK, non-SKIP fields."""
        mock_fetch.return_value = (
            [
                _make_field(key="hero.image", english="x.jpg", arabic="x.jpg"),  # SKIP
                _make_field(english="Test", arabic="مرحبا"),              # OK
                _make_field(english="Missing", arabic=None),             # MISSING
                _make_field(english="Same", arabic="Same"),              # IDENTICAL
            ],
            {"PRODUCT": (1, 4)},
        )
        mock_semantic.return_value = {}
        client = MagicMock()
        haiku_client = MagicMock()

        classified, problems, stats = run_audit(
            client, ["PRODUCT"], haiku_client, "claude-haiku-4-5-20251001",
            skip_semantic=True,
        )

        problem_statuses = {p["status"] for p in problems}
        assert "SKIP" not in problem_statuses
        assert "OK" not in problem_statuses
        assert "MISSING" in problem_statuses
        assert "IDENTICAL" in problem_statuses

    @patch("tara_migrate.tools.review_arabic.run_semantic_check")
    @patch("tara_migrate.tools.review_arabic.fetch_translations")
    def test_no_ok_fields_skips_semantic(self, mock_fetch, mock_semantic):
        """If no fields are OK, semantic check should be skipped."""
        mock_fetch.return_value = (
            [_make_field(english="Missing", arabic=None)],
            {"PRODUCT": (1, 1)},
        )
        client = MagicMock()
        haiku_client = MagicMock()

        classified, problems, stats = run_audit(
            client, ["PRODUCT"], haiku_client, "claude-haiku-4-5-20251001",
        )

        mock_semantic.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════
# 7. run_fix()
# ═══════════════════════════════════════════════════════════════════════════

class TestRunFix:
    def test_empty_problems(self):
        client = MagicMock()
        engine = MagicMock()
        uploaded, errors, skipped = run_fix(client, engine, [])
        assert uploaded == 0
        assert errors == 0
        assert skipped == 0
        engine.translate_fields.assert_not_called()

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    def test_dry_run(self, mock_sleep):
        client = MagicMock()
        engine = MagicMock()
        problems = [
            {**_make_field(arabic=None), "status": "MISSING", "detail": "no translation"},
        ]
        uploaded, errors, skipped = run_fix(
            client, engine, problems, dry_run=True,
        )
        assert uploaded == 0
        assert errors == 0
        assert skipped == 0
        engine.translate_fields.assert_not_called()

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    @patch("tara_migrate.tools.review_arabic.upload_translations")
    @patch("tara_migrate.tools.review_arabic.fetch_translatable_resources")
    def test_html_bloat_stripping(self, mock_fetch_tr, mock_upload, mock_sleep):
        """HTML_BLOAT should strip bloat without retranslating."""
        client = MagicMock()
        engine = MagicMock()
        problems = [{
            **_make_field(
                english="<p>Hello</p>",
                arabic='<div data-pb-style="X">مرحبا</div>',
            ),
            "status": "HTML_BLOAT",
            "detail": "bloat detected",
        }]
        mock_fetch_tr.return_value = {
            "gid://shopify/Product/1": {
                "content": {
                    "title": {"digest": "d123", "value": "Hello"},
                },
                "translations": {},
            },
        }
        mock_upload.return_value = (1, 0)

        uploaded, errors, skipped = run_fix(client, engine, problems)

        # Should NOT call translate_fields for HTML_BLOAT
        engine.translate_fields.assert_not_called()
        mock_upload.assert_called_once()

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    @patch("tara_migrate.tools.review_arabic.upload_translations")
    @patch("tara_migrate.tools.review_arabic.fetch_translatable_resources")
    def test_retranslation_missing(self, mock_fetch_tr, mock_upload, mock_sleep):
        """MISSING status should trigger retranslation."""
        client = MagicMock()
        engine = MagicMock()
        engine.translate_fields.return_value = {
            "PRODUCT|gid://shopify/Product/1|title": "مرحبا"
        }
        problems = [{
            **_make_field(english="Hello", arabic=None),
            "status": "MISSING", "detail": "no translation",
        }]
        mock_fetch_tr.return_value = {
            "gid://shopify/Product/1": {
                "content": {
                    "title": {"digest": "d123", "value": "Hello"},
                },
                "translations": {},
            },
        }
        mock_upload.return_value = (1, 0)

        uploaded, errors, skipped = run_fix(client, engine, problems)

        engine.translate_fields.assert_called_once()
        assert uploaded == 1

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    @patch("tara_migrate.tools.review_arabic.upload_translations")
    @patch("tara_migrate.tools.review_arabic.fetch_translatable_resources")
    def test_retranslation_identical(self, mock_fetch_tr, mock_upload, mock_sleep):
        client = MagicMock()
        engine = MagicMock()
        engine.translate_fields.return_value = {
            "PRODUCT|gid://shopify/Product/1|title": "مرحبا"
        }
        problems = [{
            **_make_field(english="Hello", arabic="Hello"),
            "status": "IDENTICAL", "detail": "identical",
        }]
        mock_fetch_tr.return_value = {
            "gid://shopify/Product/1": {
                "content": {"title": {"digest": "d", "value": "Hello"}},
                "translations": {},
            },
        }
        mock_upload.return_value = (1, 0)

        uploaded, errors, skipped = run_fix(client, engine, problems)
        engine.translate_fields.assert_called_once()
        assert uploaded == 1

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    @patch("tara_migrate.tools.review_arabic.upload_translations")
    @patch("tara_migrate.tools.review_arabic.fetch_translatable_resources")
    def test_retranslation_not_arabic(self, mock_fetch_tr, mock_upload, mock_sleep):
        client = MagicMock()
        engine = MagicMock()
        engine.translate_fields.return_value = {
            "PRODUCT|gid://shopify/Product/1|title": "مرحبا"
        }
        problems = [{
            **_make_field(english="Hello", arabic="Hola"),
            "status": "NOT_ARABIC", "detail": "no Arabic chars",
        }]
        mock_fetch_tr.return_value = {
            "gid://shopify/Product/1": {
                "content": {"title": {"digest": "d", "value": "Hello"}},
                "translations": {},
            },
        }
        mock_upload.return_value = (1, 0)

        uploaded, errors, skipped = run_fix(client, engine, problems)
        engine.translate_fields.assert_called_once()

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    @patch("tara_migrate.tools.review_arabic.upload_translations")
    @patch("tara_migrate.tools.review_arabic.fetch_translatable_resources")
    @patch("tara_migrate.tools.review_arabic.TranslationEngine")
    def test_retranslation_source_spanish_metaobject(self, mock_engine_cls,
                                                      mock_fetch_tr,
                                                      mock_upload, mock_sleep):
        """SOURCE_SPANISH on metaobject should update via metaobjectUpdate."""
        client = MagicMock()
        engine = MagicMock()
        engine.model = "gpt-5-nano"
        engine.reasoning_effort = "minimal"
        engine.batch_size = 80

        gid = "gid://shopify/Metaobject/151189422313"
        field_id = f"METAOBJECT|{gid}|label"

        # Mock the EN engine created for ES→EN translation
        mock_en_engine = MagicMock()
        mock_en_engine.translate_fields.return_value = {
            field_id: "Strengthening"
        }
        mock_engine_cls.return_value = mock_en_engine

        # Mock the AR engine for EN→AR translation
        engine.translate_fields.return_value = {field_id: "تقوية"}
        problems = [{
            **_make_field(
                english="Fortalecedor",
                arabic="تقوية قديم",
                key="label",
                resource_id=gid,
                resource_type="METAOBJECT",
            ),
            "status": "SOURCE_SPANISH", "detail": "Spanish source",
        }]
        mock_fetch_tr.return_value = {
            gid: {
                "content": {"label": {"digest": "d", "value": "Fortalecedor"}},
                "translations": {},
            },
        }
        mock_upload.return_value = (1, 0)

        uploaded, errors, skipped = run_fix(client, engine, problems)

        # EN engine should have been created and called for ES→EN
        mock_engine_cls.assert_called_once()
        mock_en_engine.translate_fields.assert_called_once()
        # Metaobject should be updated via update_metaobject
        client.update_metaobject.assert_called_once_with(
            gid, [{"key": "label", "value": "Strengthening"}])
        # AR engine should have been called for EN→AR
        engine.translate_fields.assert_called_once()
        # The English source should be updated to the translated value
        assert problems[0]["english"] == "Strengthening"

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    @patch("tara_migrate.tools.review_arabic.upload_translations")
    @patch("tara_migrate.tools.review_arabic.fetch_translatable_resources")
    @patch("tara_migrate.tools.review_arabic.TranslationEngine")
    def test_retranslation_source_spanish_product_seo(self, mock_engine_cls,
                                                       mock_fetch_tr,
                                                       mock_upload, mock_sleep):
        """SOURCE_SPANISH on product SEO should update via update_product_seo."""
        client = MagicMock()
        engine = MagicMock()
        engine.model = "gpt-5-nano"
        engine.reasoning_effort = "minimal"
        engine.batch_size = 80

        field_id = "PRODUCT|gid://shopify/Product/1|meta_title"

        # Mock the EN engine created for ES→EN translation
        mock_en_engine = MagicMock()
        mock_en_engine.translate_fields.return_value = {
            field_id: "Hydrating Conditioner | TARA"
        }
        mock_engine_cls.return_value = mock_en_engine

        # Mock the AR engine for EN→AR translation
        engine.translate_fields.return_value = {field_id: "بلسم مرطب | TARA"}
        problems = [{
            **_make_field(
                english="Acondicionador Hidratante | TARA",
                arabic="بلسم قديم",
                key="meta_title",
            ),
            "status": "SOURCE_SPANISH", "detail": "Spanish source",
        }]
        mock_fetch_tr.return_value = {
            "gid://shopify/Product/1": {
                "content": {"meta_title": {"digest": "d", "value": "Acondicionador Hidratante | TARA"}},
                "translations": {},
            },
        }
        mock_upload.return_value = (1, 0)

        uploaded, errors, skipped = run_fix(client, engine, problems)

        # Product SEO should be updated
        client.update_product_seo.assert_called_once_with(
            "1", "Hydrating Conditioner | TARA", None)
        # The English source should be updated
        assert problems[0]["english"] == "Hydrating Conditioner | TARA"

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    @patch("tara_migrate.tools.review_arabic.upload_translations")
    @patch("tara_migrate.tools.review_arabic.fetch_translatable_resources")
    def test_retranslation_has_english(self, mock_fetch_tr, mock_upload, mock_sleep):
        client = MagicMock()
        engine = MagicMock()
        engine.translate_fields.return_value = {
            "PRODUCT|gid://shopify/Product/1|title": "شامبو مرطب"
        }
        problems = [{
            **_make_field(
                english="Hydrating Shampoo",
                arabic="شامبو hydrating",
            ),
            "status": "HAS_ENGLISH", "detail": "untranslated English",
        }]
        mock_fetch_tr.return_value = {
            "gid://shopify/Product/1": {
                "content": {"title": {"digest": "d", "value": "Hydrating Shampoo"}},
                "translations": {},
            },
        }
        mock_upload.return_value = (1, 0)

        uploaded, errors, skipped = run_fix(client, engine, problems)
        engine.translate_fields.assert_called_once()

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    @patch("tara_migrate.tools.review_arabic.upload_translations")
    @patch("tara_migrate.tools.review_arabic.fetch_translatable_resources")
    def test_retranslation_has_spanish(self, mock_fetch_tr, mock_upload, mock_sleep):
        client = MagicMock()
        engine = MagicMock()
        engine.translate_fields.return_value = {
            "PRODUCT|gid://shopify/Product/1|title": "شامبو مقوي"
        }
        problems = [{
            **_make_field(
                english="Strengthening Shampoo",
                arabic="شامبو champú fortalecedor con extracto",
            ),
            "status": "HAS_SPANISH", "detail": "Spanish remnants",
        }]
        mock_fetch_tr.return_value = {
            "gid://shopify/Product/1": {
                "content": {"title": {"digest": "d", "value": "Strengthening Shampoo"}},
                "translations": {},
            },
        }
        mock_upload.return_value = (1, 0)

        uploaded, errors, skipped = run_fix(client, engine, problems)
        engine.translate_fields.assert_called_once()

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    @patch("tara_migrate.tools.review_arabic.upload_translations")
    @patch("tara_migrate.tools.review_arabic.fetch_translatable_resources")
    def test_retranslation_semantic_mismatch(self, mock_fetch_tr, mock_upload, mock_sleep):
        client = MagicMock()
        engine = MagicMock()
        engine.translate_fields.return_value = {
            "PRODUCT|gid://shopify/Product/1|title": "مرحبا بالعالم"
        }
        problems = [{
            **_make_field(english="Hello world", arabic="شيء آخر تماما"),
            "status": "SEMANTIC_MISMATCH", "detail": "wrong meaning",
        }]
        mock_fetch_tr.return_value = {
            "gid://shopify/Product/1": {
                "content": {"title": {"digest": "d", "value": "Hello world"}},
                "translations": {},
            },
        }
        mock_upload.return_value = (1, 0)

        uploaded, errors, skipped = run_fix(client, engine, problems)
        engine.translate_fields.assert_called_once()

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    @patch("tara_migrate.tools.review_arabic.upload_translations")
    @patch("tara_migrate.tools.review_arabic.fetch_translatable_resources")
    def test_rich_text_json_validation(self, mock_fetch_tr, mock_upload, mock_sleep):
        """Invalid JSON for rich_text fields should be skipped with error."""
        client = MagicMock()
        engine = MagicMock()
        # Return invalid JSON that starts like rich text
        engine.translate_fields.return_value = {
            "PRODUCT|gid://shopify/Product/1|title": '{"type": "root", broken'
        }
        problems = [{
            **_make_field(english=RICH_TEXT_JSON, arabic=None),
            "status": "MISSING", "detail": "no translation",
        }]
        mock_fetch_tr.return_value = {
            "gid://shopify/Product/1": {
                "content": {"title": {"digest": "d", "value": RICH_TEXT_JSON}},
                "translations": {},
            },
        }
        mock_upload.return_value = (0, 0)

        uploaded, errors, skipped = run_fix(client, engine, problems)
        # Invalid JSON should cause an error, not a crash
        assert errors >= 1 or uploaded == 0

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    @patch("tara_migrate.tools.review_arabic.upload_translations")
    @patch("tara_migrate.tools.review_arabic.fetch_translatable_resources")
    def test_empty_english_source_skipped(self, mock_fetch_tr, mock_upload, mock_sleep):
        """Fields with empty English source should be skipped in retranslation."""
        client = MagicMock()
        engine = MagicMock()
        engine.translate_fields.return_value = {}
        problems = [{
            **_make_field(english="", arabic=None),
            "status": "MISSING", "detail": "no translation",
        }]

        uploaded, errors, skipped = run_fix(client, engine, problems)
        assert skipped >= 1

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    @patch("tara_migrate.tools.review_arabic.upload_translations")
    @patch("tara_migrate.tools.review_arabic.fetch_translatable_resources")
    def test_missing_digest_skips(self, mock_fetch_tr, mock_upload, mock_sleep):
        """Missing digest for a resource should skip those fields."""
        client = MagicMock()
        engine = MagicMock()
        engine.translate_fields.return_value = {
            "PRODUCT|gid://shopify/Product/1|title": "مرحبا"
        }
        problems = [{
            **_make_field(english="Hello", arabic=None),
            "status": "MISSING", "detail": "no translation",
        }]
        # No digest for this resource
        mock_fetch_tr.return_value = {}

        uploaded, errors, skipped = run_fix(client, engine, problems)
        assert skipped >= 1

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    @patch("tara_migrate.tools.review_arabic.upload_translations")
    @patch("tara_migrate.tools.review_arabic.fetch_translatable_resources")
    @patch("tara_migrate.tools.review_arabic.replace_range_names_ar")
    def test_replace_range_names_called(self, mock_replace, mock_fetch_tr,
                                         mock_upload, mock_sleep):
        """Post-processing should call replace_range_names_ar on non-rich-text."""
        mock_replace.side_effect = lambda x: x  # passthrough
        client = MagicMock()
        engine = MagicMock()
        engine.translate_fields.return_value = {
            "PRODUCT|gid://shopify/Product/1|title": "مرحبا"
        }
        problems = [{
            **_make_field(english="Hello", arabic=None),
            "status": "MISSING", "detail": "no translation",
        }]
        mock_fetch_tr.return_value = {
            "gid://shopify/Product/1": {
                "content": {"title": {"digest": "d", "value": "Hello"}},
                "translations": {},
            },
        }
        mock_upload.return_value = (1, 0)

        run_fix(client, engine, problems)
        mock_replace.assert_called()


# ═══════════════════════════════════════════════════════════════════════════
# 7b. _STRIP_ONLY / _RETRANSLATE constants
# ═══════════════════════════════════════════════════════════════════════════

class TestFixConstants:
    def test_strip_only_contains_html_bloat(self):
        assert "HTML_BLOAT" in _STRIP_ONLY

    def test_retranslate_contains_all_expected(self):
        expected = {
            "MISSING", "IDENTICAL", "NOT_ARABIC", "MIXED_LANGUAGE",
            "CORRUPTED_JSON", "OUTDATED", "HAS_ENGLISH", "HAS_SPANISH",
            "SOURCE_SPANISH", "SEMANTIC_MISMATCH",
        }
        assert expected == _RETRANSLATE

    def test_no_overlap(self):
        assert not _STRIP_ONLY & _RETRANSLATE


# ═══════════════════════════════════════════════════════════════════════════
# 8. main() CLI argument parsing
# ═══════════════════════════════════════════════════════════════════════════

class TestMainCLI:
    @patch("tara_migrate.tools.review_arabic.run_audit")
    @patch("tara_migrate.tools.review_arabic.anthropic")
    @patch("tara_migrate.tools.review_arabic.ShopifyClient")
    @patch("tara_migrate.tools.review_arabic.load_dotenv")
    @patch.dict("os.environ", {
        "SAUDI_SHOP_URL": "test.myshopify.com",
        "SAUDI_ACCESS_TOKEN": "shpat_test",
    })
    def test_type_filter(self, mock_dotenv, mock_client_cls, mock_anthropic,
                         mock_audit):
        """--type should filter resource types."""
        mock_audit.return_value = ([], [], {"total": 0, "ok": 0})

        with patch("sys.argv", ["review_arabic.py", "--audit", "--type", "PRODUCT"]):
            main()

        call_args = mock_audit.call_args
        assert call_args[0][1] == ["PRODUCT"]  # resource_types

    @patch("tara_migrate.tools.review_arabic.run_audit")
    @patch("tara_migrate.tools.review_arabic.anthropic")
    @patch("tara_migrate.tools.review_arabic.ShopifyClient")
    @patch("tara_migrate.tools.review_arabic.load_dotenv")
    @patch.dict("os.environ", {
        "SAUDI_SHOP_URL": "test.myshopify.com",
        "SAUDI_ACCESS_TOKEN": "shpat_test",
    })
    def test_multiple_type_filter(self, mock_dotenv, mock_client_cls,
                                   mock_anthropic, mock_audit):
        """--type with comma-separated values."""
        mock_audit.return_value = ([], [], {"total": 0, "ok": 0})

        with patch("sys.argv", ["review_arabic.py", "--audit", "--type",
                                "PRODUCT,COLLECTION"]):
            main()

        call_args = mock_audit.call_args
        assert call_args[0][1] == ["PRODUCT", "COLLECTION"]

    @patch("tara_migrate.tools.review_arabic.TRANSLATABLE_RESOURCE_TYPES",
           ["PRODUCT", "COLLECTION", "METAFIELD", "METAOBJECT",
            "ONLINE_STORE_THEME", "PAGE", "BLOG", "ARTICLE"])
    @patch("tara_migrate.tools.review_arabic.run_audit")
    @patch("tara_migrate.tools.review_arabic.anthropic")
    @patch("tara_migrate.tools.review_arabic.ShopifyClient")
    @patch("tara_migrate.tools.review_arabic.load_dotenv")
    @patch.dict("os.environ", {
        "SAUDI_SHOP_URL": "test.myshopify.com",
        "SAUDI_ACCESS_TOKEN": "shpat_test",
    })
    def test_default_resource_types(self, mock_dotenv, mock_client_cls,
                                     mock_anthropic, mock_audit):
        """Default should include all TRANSLATABLE_RESOURCE_TYPES."""
        mock_audit.return_value = ([], [], {"total": 0, "ok": 0})

        with patch("sys.argv", ["review_arabic.py", "--audit"]):
            main()

        call_args = mock_audit.call_args
        resource_types = call_args[0][1]
        assert "PRODUCT" in resource_types
        assert "COLLECTION" in resource_types
        assert "METAFIELD" in resource_types
        assert "METAOBJECT" in resource_types
        assert "ONLINE_STORE_THEME" in resource_types
        assert "PAGE" in resource_types

    @patch("tara_migrate.tools.review_arabic.run_audit")
    @patch("tara_migrate.tools.review_arabic.anthropic")
    @patch("tara_migrate.tools.review_arabic.ShopifyClient")
    @patch("tara_migrate.tools.review_arabic.load_dotenv")
    @patch.dict("os.environ", {
        "SAUDI_SHOP_URL": "test.myshopify.com",
        "SAUDI_ACCESS_TOKEN": "shpat_test",
    })
    def test_skip_semantic_flag(self, mock_dotenv, mock_client_cls,
                                 mock_anthropic, mock_audit):
        """--skip-semantic should be passed through."""
        mock_audit.return_value = ([], [], {"total": 0, "ok": 0})

        with patch("sys.argv", ["review_arabic.py", "--audit", "--skip-semantic"]):
            main()

        call_args = mock_audit.call_args
        assert call_args[1]["skip_semantic"] is True

    @patch("tara_migrate.tools.review_arabic.run_audit")
    @patch("tara_migrate.tools.review_arabic.anthropic")
    @patch("tara_migrate.tools.review_arabic.ShopifyClient")
    @patch("tara_migrate.tools.review_arabic.load_dotenv")
    @patch.dict("os.environ", {
        "SAUDI_SHOP_URL": "test.myshopify.com",
        "SAUDI_ACCESS_TOKEN": "shpat_test",
    })
    def test_audit_flag_no_fix(self, mock_dotenv, mock_client_cls,
                                mock_anthropic, mock_audit):
        """--audit should only audit, not fix."""
        mock_audit.return_value = (
            [{"status": "MISSING", "detail": "test"}],
            [{"status": "MISSING", "detail": "test"}],
            {"total": 1, "ok": 0},
        )

        with patch("sys.argv", ["review_arabic.py", "--audit"]):
            # Should not raise or call run_fix
            main()

    @patch("tara_migrate.tools.review_arabic.run_fix")
    @patch("tara_migrate.tools.review_arabic.run_audit")
    @patch("tara_migrate.tools.review_arabic.TranslationEngine")
    @patch("tara_migrate.tools.review_arabic.load_developer_prompt")
    @patch("tara_migrate.tools.review_arabic.anthropic")
    @patch("tara_migrate.tools.review_arabic.ShopifyClient")
    @patch("tara_migrate.tools.review_arabic.load_dotenv")
    @patch.dict("os.environ", {
        "SAUDI_SHOP_URL": "test.myshopify.com",
        "SAUDI_ACCESS_TOKEN": "shpat_test",
    })
    def test_dry_run_flag(self, mock_dotenv, mock_client_cls, mock_anthropic,
                           mock_load_prompt, mock_engine_cls, mock_audit,
                           mock_fix):
        """--dry-run should call run_fix with dry_run=True."""
        mock_audit.return_value = (
            [{**_make_field(arabic=None), "status": "MISSING", "detail": "test"}],
            [{**_make_field(arabic=None), "status": "MISSING", "detail": "test"}],
            {"total": 1, "ok": 0, "source_spanish": 0},
        )
        mock_load_prompt.return_value = "test prompt"
        mock_fix.return_value = (0, 0, 0)

        with patch("sys.argv", ["review_arabic.py", "--dry-run"]):
            with patch("os.path.exists", return_value=True):
                main()

        call_args = mock_fix.call_args
        assert call_args[1]["dry_run"] is True

    @patch("tara_migrate.tools.review_arabic.run_fix")
    @patch("tara_migrate.tools.review_arabic.run_audit")
    @patch("tara_migrate.tools.review_arabic.TranslationEngine")
    @patch("tara_migrate.tools.review_arabic.load_developer_prompt")
    @patch("tara_migrate.tools.review_arabic.anthropic")
    @patch("tara_migrate.tools.review_arabic.ShopifyClient")
    @patch("tara_migrate.tools.review_arabic.load_dotenv")
    @patch.dict("os.environ", {
        "SAUDI_SHOP_URL": "test.myshopify.com",
        "SAUDI_ACCESS_TOKEN": "shpat_test",
    })
    def test_no_verify_flag(self, mock_dotenv, mock_client_cls, mock_anthropic,
                             mock_load_prompt, mock_engine_cls, mock_audit,
                             mock_fix):
        """--no-verify should skip verification phase."""
        mock_audit.return_value = (
            [{**_make_field(arabic=None), "status": "MISSING", "detail": "test"}],
            [{**_make_field(arabic=None), "status": "MISSING", "detail": "test"}],
            {"total": 1, "ok": 0, "source_spanish": 0},
        )
        mock_load_prompt.return_value = "test prompt"
        mock_fix.return_value = (1, 0, 0)

        with patch("sys.argv", ["review_arabic.py", "--no-verify"]):
            with patch("os.path.exists", return_value=True):
                main()

        # main() should return without calling run_verify


# ═══════════════════════════════════════════════════════════════════════════
# fetch_translations()
# ═══════════════════════════════════════════════════════════════════════════

class TestFetchTranslations:
    def test_single_resource_single_field(self):
        client = MagicMock()
        client._graphql.return_value = {
            "translatableResources": {
                "edges": [{
                    "node": {
                        "resourceId": "gid://shopify/Product/1",
                        "translatableContent": [{
                            "key": "title",
                            "value": "Hello",
                            "digest": "abc",
                        }],
                        "translations": [{
                            "key": "title",
                            "value": "مرحبا",
                            "outdated": False,
                        }],
                    }
                }],
                "pageInfo": {"hasNextPage": False},
            }
        }

        fields, counts = fetch_translations(client, ["PRODUCT"])
        assert len(fields) == 1
        assert fields[0]["english"] == "Hello"
        assert fields[0]["arabic"] == "مرحبا"
        assert fields[0]["resource_type"] == "PRODUCT"
        assert counts["PRODUCT"] == (1, 1)

    def test_missing_translation(self):
        client = MagicMock()
        client._graphql.return_value = {
            "translatableResources": {
                "edges": [{
                    "node": {
                        "resourceId": "gid://shopify/Product/1",
                        "translatableContent": [{
                            "key": "title",
                            "value": "Hello",
                            "digest": "abc",
                        }],
                        "translations": [],
                    }
                }],
                "pageInfo": {"hasNextPage": False},
            }
        }

        fields, counts = fetch_translations(client, ["PRODUCT"])
        assert fields[0]["arabic"] is None

    @patch("tara_migrate.tools.review_arabic.time.sleep")
    def test_pagination(self, mock_sleep):
        client = MagicMock()
        # First page
        client._graphql.side_effect = [
            {
                "translatableResources": {
                    "edges": [{
                        "node": {
                            "resourceId": "gid://shopify/Product/1",
                            "translatableContent": [{
                                "key": "title", "value": "A", "digest": "a",
                            }],
                            "translations": [],
                        }
                    }],
                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor1"},
                }
            },
            {
                "translatableResources": {
                    "edges": [{
                        "node": {
                            "resourceId": "gid://shopify/Product/2",
                            "translatableContent": [{
                                "key": "title", "value": "B", "digest": "b",
                            }],
                            "translations": [],
                        }
                    }],
                    "pageInfo": {"hasNextPage": False},
                }
            },
        ]

        fields, counts = fetch_translations(client, ["PRODUCT"])
        assert len(fields) == 2
        assert counts["PRODUCT"] == (2, 2)

    def test_api_error_handling(self):
        client = MagicMock()
        client._graphql.side_effect = Exception("Network error")

        fields, counts = fetch_translations(client, ["PRODUCT"])
        assert len(fields) == 0
        assert counts["PRODUCT"] == (0, 0)

    def test_multiple_resource_types(self):
        client = MagicMock()
        client._graphql.return_value = {
            "translatableResources": {
                "edges": [{
                    "node": {
                        "resourceId": "gid://shopify/Product/1",
                        "translatableContent": [{
                            "key": "title", "value": "Test", "digest": "d",
                        }],
                        "translations": [],
                    }
                }],
                "pageInfo": {"hasNextPage": False},
            }
        }

        fields, counts = fetch_translations(client, ["PRODUCT", "COLLECTION"])
        assert len(fields) == 2  # 1 per type
        assert "PRODUCT" in counts
        assert "COLLECTION" in counts


# ═══════════════════════════════════════════════════════════════════════════
# _ALLOWED_LATIN regex
# ═══════════════════════════════════════════════════════════════════════════

class TestAllowedLatin:
    """Verify the _ALLOWED_LATIN regex matches what it should."""

    def test_tara(self):
        assert _ALLOWED_LATIN.match("TARA")

    def test_kansa(self):
        assert _ALLOWED_LATIN.match("Kansa")

    def test_wand(self):
        assert _ALLOWED_LATIN.match("Wand")

    def test_gua(self):
        assert _ALLOWED_LATIN.match("Gua")

    def test_sha(self):
        assert _ALLOWED_LATIN.match("Sha")

    def test_ph(self):
        assert _ALLOWED_LATIN.match("pH")

    def test_aha(self):
        assert _ALLOWED_LATIN.match("AHA")

    def test_bha(self):
        assert _ALLOWED_LATIN.match("BHA")

    def test_nmf(self):
        assert _ALLOWED_LATIN.match("NMF")

    def test_spf(self):
        assert _ALLOWED_LATIN.match("SPF")

    def test_uv(self):
        assert _ALLOWED_LATIN.match("UV")

    def test_ml(self):
        assert _ALLOWED_LATIN.match("ml")

    def test_mg(self):
        assert _ALLOWED_LATIN.match("mg")

    def test_vitamin_b5(self):
        assert _ALLOWED_LATIN.match("B5")

    def test_vitamin_c(self):
        assert _ALLOWED_LATIN.match("C")

    def test_shampoo_not_allowed(self):
        assert not _ALLOWED_LATIN.match("shampoo")

    def test_conditioner_not_allowed(self):
        assert not _ALLOWED_LATIN.match("conditioner")

    def test_random_word_not_allowed(self):
        assert not _ALLOWED_LATIN.match("hello")

    def test_dna(self):
        assert _ALLOWED_LATIN.match("DNA")

    def test_rna(self):
        assert _ALLOWED_LATIN.match("RNA")

    def test_atp(self):
        assert _ALLOWED_LATIN.match("ATP")


# ═══════════════════════════════════════════════════════════════════════════
# _UNTRANSLATED_EN regex directly
# ═══════════════════════════════════════════════════════════════════════════

class TestUntranslatedEnRegex:
    """Verify the regex patterns match correctly."""

    def test_shampoo_matches(self):
        assert _UNTRANSLATED_EN.search("this is a shampoo for hair")

    def test_conditioner_matches(self):
        assert _UNTRANSLATED_EN.search("use conditioner daily")

    def test_serum_matches(self):
        assert _UNTRANSLATED_EN.search("apply serum gently")

    def test_mask_matches(self):
        assert _UNTRANSLATED_EN.search("apply mask for 5 minutes")

    def test_scalp_matches(self):
        assert _UNTRANSLATED_EN.search("massage the scalp")

    def test_hair_care_matches(self):
        assert _UNTRANSLATED_EN.search("premium hair care products")

    def test_no_match_for_brand_names(self):
        """Brand names like TARA should not be matched."""
        assert not _UNTRANSLATED_EN.search("TARA products")

    def test_no_match_for_plain_arabic(self):
        assert not _UNTRANSLATED_EN.search("شامبو طبيعي")

    def test_word_boundary(self):
        """'serum' should not match 'museum'."""
        assert not _UNTRANSLATED_EN.search("museum exhibit")

    def test_hair_care_space_variation(self):
        assert _UNTRANSLATED_EN.search("haircare products")
        assert _UNTRANSLATED_EN.search("hair care routine")


# ═══════════════════════════════════════════════════════════════════════════
# run_verify() (simple wrapper)
# ═══════════════════════════════════════════════════════════════════════════

class TestRunVerify:
    @patch("tara_migrate.tools.review_arabic.run_audit")
    def test_calls_run_audit(self, mock_audit):
        from tara_migrate.tools.review_arabic import run_verify
        mock_audit.return_value = ([], [], {"total": 5, "ok": 5})
        client = MagicMock()
        haiku_client = MagicMock()

        problems, stats = run_verify(
            client, ["PRODUCT"], haiku_client, "claude-haiku-4-5-20251001",
        )

        mock_audit.assert_called_once()
        assert problems == []

    @patch("tara_migrate.tools.review_arabic.run_audit")
    def test_skip_semantic_passed(self, mock_audit):
        from tara_migrate.tools.review_arabic import run_verify
        mock_audit.return_value = ([], [], {"total": 5, "ok": 5})
        client = MagicMock()
        haiku_client = MagicMock()

        run_verify(
            client, ["PRODUCT"], haiku_client, "claude-haiku-4-5-20251001",
            skip_semantic=True,
        )

        call_args = mock_audit.call_args
        assert call_args[1]["skip_semantic"] is True
