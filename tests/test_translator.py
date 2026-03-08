"""Tests for translator.py — Translator class and constants."""
import json
import os
from unittest.mock import MagicMock, patch

import pytest

# Patch _load_tov before importing translator module
_MOCK_TOV = "Mock tone of voice content"


@pytest.fixture(autouse=True)
def _patch_tov_files(monkeypatch):
    """Prevent translator from reading real tov files at import."""
    pass  # Files exist in the repo, so import works normally


from translator import (
    Translator,
    SYSTEM_PROMPT,
    TRANSLATABLE_FIELD_TYPES,
    METAOBJECT_TRANSLATABLE_FIELDS,
    PRODUCT_TRANSLATABLE_METAFIELDS,
    ARTICLE_TRANSLATABLE_METAFIELDS,
    TARA_TONE_EN,
    TARA_TONE_AR,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_system_prompt_contains_tara(self):
        assert "TARA" in SYSTEM_PROMPT

    def test_system_prompt_contains_rules(self):
        assert "TRANSLATION RULES" in SYSTEM_PROMPT

    def test_translatable_field_types(self):
        assert "single_line_text_field" in TRANSLATABLE_FIELD_TYPES
        assert "multi_line_text_field" in TRANSLATABLE_FIELD_TYPES
        assert "rich_text_field" in TRANSLATABLE_FIELD_TYPES

    def test_metaobject_translatable_fields(self):
        assert "benefit" in METAOBJECT_TRANSLATABLE_FIELDS
        assert "title" in METAOBJECT_TRANSLATABLE_FIELDS["benefit"]
        assert "faq_entry" in METAOBJECT_TRANSLATABLE_FIELDS
        assert "blog_author" in METAOBJECT_TRANSLATABLE_FIELDS
        assert "ingredient" in METAOBJECT_TRANSLATABLE_FIELDS

    def test_product_translatable_metafields(self):
        assert "custom.tagline" in PRODUCT_TRANSLATABLE_METAFIELDS
        assert "custom.short_description" in PRODUCT_TRANSLATABLE_METAFIELDS
        assert "custom.key_benefits_heading" in PRODUCT_TRANSLATABLE_METAFIELDS
        assert len(PRODUCT_TRANSLATABLE_METAFIELDS) == 17

    def test_article_translatable_metafields(self):
        assert "custom.blog_summary" in ARTICLE_TRANSLATABLE_METAFIELDS
        assert "custom.hero_caption" in ARTICLE_TRANSLATABLE_METAFIELDS
        assert "custom.short_title" in ARTICLE_TRANSLATABLE_METAFIELDS
        assert len(ARTICLE_TRANSLATABLE_METAFIELDS) == 3

    def test_tov_files_loaded(self):
        assert len(TARA_TONE_EN) > 100
        assert len(TARA_TONE_AR) > 100


# ---------------------------------------------------------------------------
# Translator.translate
# ---------------------------------------------------------------------------

class TestTranslate:
    def _make_translator(self):
        with patch("translator.OpenAI") as MockOpenAI:
            mock_client = MagicMock()
            MockOpenAI.return_value = mock_client
            t = Translator("fake-key")
            return t, mock_client

    def _set_response(self, mock_client, text):
        mock_choice = MagicMock()
        mock_choice.message.content = text
        mock_client.chat.completions.create.return_value = MagicMock(choices=[mock_choice])

    def test_translate_text(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Hello World")
        result = t.translate("Hola Mundo", "Spanish", "English")
        assert result == "Hello World"

    def test_translate_empty_string(self):
        t, mc = self._make_translator()
        assert t.translate("", "Spanish", "English") == ""

    def test_translate_none(self):
        t, mc = self._make_translator()
        assert t.translate(None, "Spanish", "English") is None

    def test_translate_whitespace_only(self):
        t, mc = self._make_translator()
        assert t.translate("   ", "Spanish", "English") == "   "

    def test_translate_html_only(self):
        t, mc = self._make_translator()
        result = t.translate("<br><hr>", "Spanish", "English")
        assert result == "<br><hr>"

    def test_translate_html_with_text(self):
        t, mc = self._make_translator()
        self._set_response(mc, "<p>Hello</p>")
        result = t.translate("<p>Hola</p>", "Spanish", "English")
        assert result == "<p>Hello</p>"

    def test_model_is_o3(self):
        t, mc = self._make_translator()
        assert t.model == "o3"


# ---------------------------------------------------------------------------
# Translator.translate_rich_text
# ---------------------------------------------------------------------------

class TestTranslateRichText:
    def _make_translator(self):
        with patch("translator.OpenAI") as MockOpenAI:
            mock_client = MagicMock()
            MockOpenAI.return_value = mock_client
            t = Translator("fake-key")
            return t, mock_client

    def _set_response(self, mock_client, text):
        mock_choice = MagicMock()
        mock_choice.message.content = text
        mock_client.chat.completions.create.return_value = MagicMock(choices=[mock_choice])

    def test_rich_text_translation(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Hello")
        rich_json = json.dumps({"type": "root", "children": [{"type": "text", "value": "Hola"}]})
        result = t.translate_rich_text(rich_json, "Spanish", "English")
        parsed = json.loads(result)
        assert parsed["children"][0]["value"] == "Hello"

    def test_rich_text_nested_children(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Hello")
        rich_json = json.dumps({
            "type": "root",
            "children": [
                {"type": "paragraph", "children": [{"type": "text", "value": "Hola"}]}
            ]
        })
        result = t.translate_rich_text(rich_json, "Spanish", "English")
        parsed = json.loads(result)
        assert parsed["children"][0]["children"][0]["value"] == "Hello"

    def test_rich_text_empty(self):
        t, mc = self._make_translator()
        assert t.translate_rich_text("", "Spanish", "English") == ""

    def test_rich_text_none(self):
        t, mc = self._make_translator()
        assert t.translate_rich_text(None, "Spanish", "English") is None

    def test_rich_text_whitespace(self):
        t, mc = self._make_translator()
        assert t.translate_rich_text("  ", "Spanish", "English") == "  "

    def test_rich_text_invalid_json(self):
        t, mc = self._make_translator()
        self._set_response(mc, "translated text")
        result = t.translate_rich_text("not json {{{", "Spanish", "English")
        assert result == "translated text"

    def test_rich_text_no_children(self):
        t, mc = self._make_translator()
        rich_json = json.dumps({"type": "root"})
        result = t.translate_rich_text(rich_json, "Spanish", "English")
        parsed = json.loads(result)
        assert parsed["type"] == "root"

    def test_rich_text_non_text_nodes(self):
        t, mc = self._make_translator()
        rich_json = json.dumps({
            "type": "root",
            "children": [{"type": "heading", "value": "keep this"}]
        })
        result = t.translate_rich_text(rich_json, "Spanish", "English")
        parsed = json.loads(result)
        assert parsed["children"][0]["value"] == "keep this"


# ---------------------------------------------------------------------------
# Translator.translate_product
# ---------------------------------------------------------------------------

class TestTranslateProduct:
    def _make_translator(self):
        with patch("translator.OpenAI") as MockOpenAI:
            mock_client = MagicMock()
            MockOpenAI.return_value = mock_client
            t = Translator("fake-key")
            return t, mock_client

    def _set_response(self, mock_client, text):
        mock_choice = MagicMock()
        mock_choice.message.content = text
        mock_client.chat.completions.create.return_value = MagicMock(choices=[mock_choice])

    def test_basic_product(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Translated")
        product = {"id": 1, "title": "Producto", "body_html": "<p>Desc</p>",
                    "product_type": "Tipo", "tags": "tag1, tag2"}
        result = t.translate_product(product, "Spanish", "English")
        assert result["title"] == "Translated"
        assert result["body_html"] == "Translated"
        assert result["product_type"] == "Translated"
        assert result["tags"] == "Translated"

    def test_product_with_tags_list(self):
        t, mc = self._make_translator()
        self._set_response(mc, "t1, t2")
        product = {"id": 1, "title": "P", "body_html": "", "product_type": "",
                    "tags": ["tag1", "tag2"]}
        result = t.translate_product(product, "Spanish", "English")
        assert "t1" in result["tags"]

    def test_product_no_tags(self):
        t, mc = self._make_translator()
        self._set_response(mc, "X")
        product = {"id": 1, "title": "P", "body_html": "", "product_type": ""}
        result = t.translate_product(product, "Spanish", "English")
        assert "tags" not in result or result.get("tags") is None or result.get("tags") == ""

    def test_product_with_variants(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Translated")
        product = {
            "id": 1, "title": "P", "body_html": "", "product_type": "",
            "variants": [
                {"title": "Large", "option1": "Grande", "option2": "Rojo", "option3": None},
                {"title": "Default Title", "option1": None},
            ],
        }
        result = t.translate_product(product, "Spanish", "English")
        assert len(result["variants"]) == 2
        assert result["variants"][0]["title"] == "Translated"
        assert result["variants"][1]["title"] == "Default Title"  # Not translated

    def test_product_with_options(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Translated")
        product = {
            "id": 1, "title": "P", "body_html": "", "product_type": "",
            "options": [{"name": "Tamaño", "values": ["Pequeño", "Grande"]}],
        }
        result = t.translate_product(product, "Spanish", "English")
        assert result["options"][0]["name"] == "Translated"
        assert len(result["options"][0]["values"]) == 2

    def test_product_with_metafields(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Translated")
        product = {
            "id": 1, "title": "P", "body_html": "", "product_type": "",
            "metafields": [
                {"namespace": "custom", "key": "tagline", "value": "Lujo", "type": "single_line_text_field"},
                {"namespace": "custom", "key": "ingredients", "value": '["gid://1"]', "type": "list.metaobject_reference"},
            ],
        }
        result = t.translate_product(product, "Spanish", "English")
        # tagline should be translated, ingredients (reference) should not
        tagline_mf = [mf for mf in result["metafields"] if mf["key"] == "tagline"][0]
        assert tagline_mf["value"] == "Translated"
        ingr_mf = [mf for mf in result["metafields"] if mf["key"] == "ingredients"][0]
        assert ingr_mf["value"] == '["gid://1"]'  # Unchanged

    def test_product_rich_text_metafield(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Translated")
        product = {
            "id": 1, "title": "P", "body_html": "", "product_type": "",
            "metafields": [
                {"namespace": "custom", "key": "key_benefits_content",
                 "value": '{"type":"root","children":[{"type":"text","value":"Hola"}]}',
                 "type": "rich_text_field"},
            ],
        }
        result = t.translate_product(product, "Spanish", "English")
        mf = result["metafields"][0]
        parsed = json.loads(mf["value"])
        assert parsed["children"][0]["value"] == "Translated"

    def test_product_no_metafields(self):
        t, mc = self._make_translator()
        self._set_response(mc, "T")
        product = {"id": 1, "title": "P", "body_html": "", "product_type": ""}
        result = t.translate_product(product, "Spanish", "English")
        assert "metafields" not in result

    def test_product_no_variants(self):
        t, mc = self._make_translator()
        self._set_response(mc, "T")
        product = {"id": 1, "title": "P", "body_html": "", "product_type": ""}
        result = t.translate_product(product, "Spanish", "English")
        assert "variants" not in result

    def test_product_no_options(self):
        t, mc = self._make_translator()
        self._set_response(mc, "T")
        product = {"id": 1, "title": "P", "body_html": "", "product_type": ""}
        result = t.translate_product(product, "Spanish", "English")
        assert "options" not in result


# ---------------------------------------------------------------------------
# Translator.translate_page / collection / article
# ---------------------------------------------------------------------------

class TestTranslatePage:
    def _make_translator(self):
        with patch("translator.OpenAI") as MockOpenAI:
            mock_client = MagicMock()
            MockOpenAI.return_value = mock_client
            t = Translator("fake-key")
            return t, mock_client

    def _set_response(self, mock_client, text):
        mock_choice = MagicMock()
        mock_choice.message.content = text
        mock_client.chat.completions.create.return_value = MagicMock(choices=[mock_choice])

    def test_translate_page(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Translated")
        page = {"id": 1, "title": "Titulo", "body_html": "<p>Cuerpo</p>"}
        result = t.translate_page(page, "Spanish", "English")
        assert result["title"] == "Translated"
        assert result["body_html"] == "Translated"

    def test_translate_collection(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Translated")
        coll = {"id": 1, "title": "Coleccion", "body_html": "<p>Desc</p>"}
        result = t.translate_collection(coll, "Spanish", "English")
        assert result["title"] == "Translated"

    def test_translate_article(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Translated")
        article = {"id": 1, "title": "Articulo", "body_html": "<p>Cuerpo</p>",
                    "summary_html": "<p>Resumen</p>", "tags": "etiqueta1"}
        result = t.translate_article(article, "Spanish", "English")
        assert result["title"] == "Translated"
        assert result["summary_html"] == "Translated"
        assert result["tags"] == "Translated"

    def test_translate_article_tags_list(self):
        t, mc = self._make_translator()
        self._set_response(mc, "t1, t2")
        article = {"id": 1, "title": "A", "body_html": "", "summary_html": "",
                    "tags": ["t1", "t2"]}
        result = t.translate_article(article, "Spanish", "English")
        assert "t1" in result["tags"]

    def test_translate_article_no_tags(self):
        t, mc = self._make_translator()
        self._set_response(mc, "T")
        article = {"id": 1, "title": "A", "body_html": "", "summary_html": ""}
        result = t.translate_article(article, "Spanish", "English")
        assert "tags" not in result or result.get("tags") in (None, "")

    def test_translate_article_with_metafields(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Translated")
        article = {
            "id": 1, "title": "A", "body_html": "", "summary_html": "",
            "metafields": [
                {"namespace": "custom", "key": "blog_summary", "value": "Resumen", "type": "single_line_text_field"},
                {"namespace": "custom", "key": "author", "value": "gid://1", "type": "metaobject_reference"},
            ],
        }
        result = t.translate_article(article, "Spanish", "English")
        summary_mf = [mf for mf in result["metafields"] if mf["key"] == "blog_summary"][0]
        assert summary_mf["value"] == "Translated"
        author_mf = [mf for mf in result["metafields"] if mf["key"] == "author"][0]
        assert author_mf["value"] == "gid://1"  # Not translated

    def test_translate_article_no_metafields(self):
        t, mc = self._make_translator()
        self._set_response(mc, "T")
        article = {"id": 1, "title": "A", "body_html": "", "summary_html": ""}
        result = t.translate_article(article, "Spanish", "English")
        assert "metafields" not in result


# ---------------------------------------------------------------------------
# Translator.translate_metaobject
# ---------------------------------------------------------------------------

class TestTranslateMetaobject:
    def _make_translator(self):
        with patch("translator.OpenAI") as MockOpenAI:
            mock_client = MagicMock()
            MockOpenAI.return_value = mock_client
            t = Translator("fake-key")
            return t, mock_client

    def _set_response(self, mock_client, text):
        mock_choice = MagicMock()
        mock_choice.message.content = text
        mock_client.chat.completions.create.return_value = MagicMock(choices=[mock_choice])

    def test_benefit_translation(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Translated")
        mo = {
            "type": "benefit",
            "fields": [
                {"key": "title", "value": "Brillo", "type": "single_line_text_field"},
                {"key": "icon_label", "value": "brillo", "type": "single_line_text_field"},
            ],
        }
        result = t.translate_metaobject(mo, "Spanish", "English")
        assert result["fields"][0]["value"] == "Translated"
        assert result["fields"][1]["value"] == "Translated"

    def test_non_translatable_field(self):
        t, mc = self._make_translator()
        mo = {
            "type": "ingredient",
            "fields": [
                {"key": "is_hero", "value": "true", "type": "boolean"},
                {"key": "sort_order", "value": "1", "type": "number_integer"},
            ],
        }
        result = t.translate_metaobject(mo, "Spanish", "English")
        assert result["fields"][0]["value"] == "true"
        assert result["fields"][1]["value"] == "1"

    def test_rich_text_field_metaobject(self):
        t, mc = self._make_translator()
        self._set_response(mc, "Translated")
        mo = {
            "type": "faq_entry",
            "fields": [
                {"key": "answer", "value": '{"type":"root","children":[{"type":"text","value":"Así"}]}',
                 "type": "rich_text_field"},
            ],
        }
        result = t.translate_metaobject(mo, "Spanish", "English")
        parsed = json.loads(result["fields"][0]["value"])
        assert parsed["children"][0]["value"] == "Translated"

    def test_unknown_type_no_translation(self):
        t, mc = self._make_translator()
        mo = {
            "type": "unknown_type",
            "fields": [{"key": "name", "value": "should not translate", "type": "single_line_text_field"}],
        }
        result = t.translate_metaobject(mo, "Spanish", "English")
        assert result["fields"][0]["value"] == "should not translate"

    def test_empty_value_not_translated(self):
        t, mc = self._make_translator()
        mo = {
            "type": "benefit",
            "fields": [{"key": "title", "value": "", "type": "single_line_text_field"}],
        }
        result = t.translate_metaobject(mo, "Spanish", "English")
        assert result["fields"][0]["value"] == ""

    def test_none_value_not_translated(self):
        t, mc = self._make_translator()
        mo = {
            "type": "benefit",
            "fields": [{"key": "title", "value": None, "type": "single_line_text_field"}],
        }
        result = t.translate_metaobject(mo, "Spanish", "English")
        assert result["fields"][0]["value"] is None
