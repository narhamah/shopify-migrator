"""Tests for translate_to_english.py."""
import json
import os
from unittest.mock import MagicMock, patch

import pytest

from translate_to_english import load_json, save_json, load_or_init, main


class TestLoadJson:
    def test_load_valid_json(self, tmp_path):
        f = tmp_path / "data.json"
        f.write_text(json.dumps([1, 2, 3]))
        result = load_json(str(f))
        assert result == [1, 2, 3]


class TestSaveJson:
    def test_save_json(self, tmp_path):
        f = str(tmp_path / "out.json")
        save_json({"a": 1}, f)
        with open(f) as fh:
            assert json.load(fh) == {"a": 1}


class TestLoadOrInit:
    def test_existing_file(self, tmp_path):
        f = tmp_path / "data.json"
        f.write_text(json.dumps([1, 2]))
        result = load_or_init(str(f))
        assert result == [1, 2]

    def test_nonexistent_file(self, tmp_path):
        result = load_or_init(str(tmp_path / "nope.json"))
        assert result == []


class TestResumeLogic:
    """Test the resume logic at the unit level."""

    def test_skip_already_translated_products(self):
        existing_ids = {1}
        products = [{"id": 1, "title": "P1"}, {"id": 2, "title": "P2"}]
        to_translate = [p for p in products if p["id"] not in existing_ids]
        assert len(to_translate) == 1
        assert to_translate[0]["id"] == 2

    def test_skip_already_translated_metaobjects(self):
        existing_handles = {"shine"}
        objects = [{"handle": "shine"}, {"handle": "glow"}]
        to_translate = [o for o in objects if o["handle"] not in existing_handles]
        assert len(to_translate) == 1
        assert to_translate[0]["handle"] == "glow"


class TestMainIntegration:
    @patch("translate_to_english.load_dotenv")
    @patch("translate_to_english.Translator")
    def test_full_pipeline(self, MockTranslator, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        mock_translator = MagicMock()
        MockTranslator.return_value = mock_translator

        mock_translator.translate_product.side_effect = lambda p, s, t: {**p, "title": "EN"}
        mock_translator.translate_collection.side_effect = lambda c, s, t: {**c, "title": "EN"}
        mock_translator.translate_page.side_effect = lambda p, s, t: {**p, "title": "EN"}
        mock_translator.translate_article.side_effect = lambda a, s, t: {**a, "title": "EN"}
        mock_translator.translate_blog.side_effect = lambda b, s, t: {**b, "title": "EN"}
        mock_translator.translate_metaobject.side_effect = lambda m, s, t: m

        # Set up input data
        input_dir = tmp_path / "data" / "spain_export"
        input_dir.mkdir(parents=True)

        (input_dir / "products.json").write_text(json.dumps([{"id": 1, "title": "Prod"}]))
        (input_dir / "collections.json").write_text(json.dumps([{"id": 2, "title": "Coll"}]))
        (input_dir / "pages.json").write_text(json.dumps([{"id": 3, "title": "Page"}]))
        (input_dir / "articles.json").write_text(json.dumps([{"id": 4, "title": "Art"}]))
        (input_dir / "blogs.json").write_text(json.dumps([{"id": 5, "title": "Blog"}]))
        (input_dir / "metaobjects.json").write_text(json.dumps({
            "benefit": {
                "definition": {"type": "benefit"},
                "objects": [{"handle": "shine", "type": "benefit", "fields": []}],
            }
        }))
        (input_dir / "metaobject_definitions.json").write_text(json.dumps([{"type": "benefit"}]))

        os.environ["OPENAI_API_KEY"] = "fake"
        try:
            main()
        finally:
            del os.environ["OPENAI_API_KEY"]

        mock_translator.translate_product.assert_called_once()
        mock_translator.translate_collection.assert_called_once()
        mock_translator.translate_page.assert_called_once()
        mock_translator.translate_article.assert_called_once()
        mock_translator.translate_metaobject.assert_called_once()

        # Verify output files were created
        output_dir = tmp_path / "data" / "english"
        assert (output_dir / "products.json").exists()
        assert (output_dir / "blogs.json").exists()

    @patch("translate_to_english.load_dotenv")
    @patch("translate_to_english.Translator")
    def test_resume_skips_existing(self, MockTranslator, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        mock_translator = MagicMock()
        MockTranslator.return_value = mock_translator
        mock_translator.translate_product.side_effect = lambda p, s, t: {**p, "title": "EN"}
        mock_translator.translate_collection.side_effect = lambda c, s, t: c
        mock_translator.translate_page.side_effect = lambda p, s, t: p
        mock_translator.translate_article.side_effect = lambda a, s, t: a
        mock_translator.translate_metaobject.side_effect = lambda m, s, t: m

        input_dir = tmp_path / "data" / "spain_export"
        input_dir.mkdir(parents=True)
        output_dir = tmp_path / "data" / "english"
        output_dir.mkdir(parents=True)

        (input_dir / "products.json").write_text(json.dumps([{"id": 1, "title": "P1"}, {"id": 2, "title": "P2"}]))
        (input_dir / "collections.json").write_text(json.dumps([]))
        (input_dir / "pages.json").write_text(json.dumps([]))
        (input_dir / "articles.json").write_text(json.dumps([]))
        (input_dir / "blogs.json").write_text(json.dumps([]))

        # Already translated product 1
        (output_dir / "products.json").write_text(json.dumps([{"id": 1, "title": "EN-P1"}]))

        os.environ["OPENAI_API_KEY"] = "fake"
        try:
            main()
        finally:
            del os.environ["OPENAI_API_KEY"]

        # Only product 2 should be translated
        assert mock_translator.translate_product.call_count == 1
        call_args = mock_translator.translate_product.call_args
        assert call_args[0][0]["id"] == 2
