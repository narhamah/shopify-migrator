"""Tests for translate_to_arabic.py."""
import json
import os
from unittest.mock import MagicMock, patch

import pytest

from translate_to_arabic import load_json, save_json, load_or_init, main


class TestLoadJson:
    def test_load_valid_json(self, tmp_path):
        f = tmp_path / "data.json"
        f.write_text(json.dumps({"key": "val"}))
        result = load_json(str(f))
        assert result == {"key": "val"}


class TestSaveJson:
    def test_save_json(self, tmp_path):
        f = str(tmp_path / "out.json")
        save_json([1, 2], f)
        with open(f) as fh:
            assert json.load(fh) == [1, 2]


class TestLoadOrInit:
    def test_existing_file(self, tmp_path):
        f = tmp_path / "data.json"
        f.write_text(json.dumps([{"id": 1}]))
        result = load_or_init(str(f))
        assert result == [{"id": 1}]

    def test_nonexistent_file(self):
        result = load_or_init("/tmp/nonexistent_12345.json")
        assert result == []


class TestResumeLogic:
    def test_skip_already_translated(self):
        existing_ids = {1, 2}
        products = [{"id": 1}, {"id": 2}, {"id": 3}]
        to_translate = [p for p in products if p["id"] not in existing_ids]
        assert len(to_translate) == 1
        assert to_translate[0]["id"] == 3

    def test_metaobject_resume_by_handle(self):
        existing_handles = {"shine", "glow"}
        objects = [{"handle": "shine"}, {"handle": "glow"}, {"handle": "new"}]
        to_translate = [o for o in objects if o["handle"] not in existing_handles]
        assert len(to_translate) == 1
        assert to_translate[0]["handle"] == "new"


class TestMainIntegration:
    @patch("translate_to_arabic.load_dotenv")
    @patch("translate_to_arabic.Translator")
    def test_full_pipeline(self, MockTranslator, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        mock_translator = MagicMock()
        MockTranslator.return_value = mock_translator
        mock_translator.translate_product.side_effect = lambda p, s, t: {**p, "title": "AR"}
        mock_translator.translate_collection.side_effect = lambda c, s, t: {**c, "title": "AR"}
        mock_translator.translate_page.side_effect = lambda p, s, t: {**p, "title": "AR"}
        mock_translator.translate_article.side_effect = lambda a, s, t: {**a, "title": "AR"}
        mock_translator.translate_blog.side_effect = lambda b, s, t: {**b, "title": "AR"}
        mock_translator.translate_metaobject.side_effect = lambda m, s, t: m

        input_dir = tmp_path / "data" / "english"
        input_dir.mkdir(parents=True)

        (input_dir / "products.json").write_text(json.dumps([{"id": 1, "title": "Prod"}]))
        (input_dir / "collections.json").write_text(json.dumps([{"id": 2, "title": "Coll"}]))
        (input_dir / "pages.json").write_text(json.dumps([{"id": 3, "title": "Page"}]))
        (input_dir / "articles.json").write_text(json.dumps([{"id": 4, "title": "Art"}]))
        (input_dir / "blogs.json").write_text(json.dumps([{"id": 5, "title": "Blog"}]))

        os.environ["OPENAI_API_KEY"] = "fake"
        try:
            main()
        finally:
            del os.environ["OPENAI_API_KEY"]

        mock_translator.translate_product.assert_called_once()
        mock_translator.translate_collection.assert_called_once()
        mock_translator.translate_page.assert_called_once()
        mock_translator.translate_article.assert_called_once()

        output_dir = tmp_path / "data" / "arabic"
        assert (output_dir / "products.json").exists()
        assert (output_dir / "blogs.json").exists()

    @patch("translate_to_arabic.load_dotenv")
    @patch("translate_to_arabic.Translator")
    def test_empty_inputs(self, MockTranslator, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        mock_translator = MagicMock()
        MockTranslator.return_value = mock_translator

        input_dir = tmp_path / "data" / "english"
        input_dir.mkdir(parents=True)

        for name in ["products.json", "collections.json", "pages.json", "articles.json", "blogs.json"]:
            (input_dir / name).write_text(json.dumps([]))

        os.environ["OPENAI_API_KEY"] = "fake"
        try:
            main()
        finally:
            del os.environ["OPENAI_API_KEY"]

        mock_translator.translate_product.assert_not_called()

    @patch("translate_to_arabic.load_dotenv")
    @patch("translate_to_arabic.Translator")
    def test_with_metaobjects(self, MockTranslator, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        mock_translator = MagicMock()
        MockTranslator.return_value = mock_translator
        mock_translator.translate_product.side_effect = lambda p, s, t: {**p, "title": "AR"}
        mock_translator.translate_collection.side_effect = lambda c, s, t: c
        mock_translator.translate_page.side_effect = lambda p, s, t: p
        mock_translator.translate_article.side_effect = lambda a, s, t: a
        mock_translator.translate_metaobject.side_effect = lambda m, s, t: {**m, "_ar": True}

        input_dir = tmp_path / "data" / "english"
        input_dir.mkdir(parents=True)

        (input_dir / "products.json").write_text(json.dumps([{"id": 1, "title": "Prod"}]))
        (input_dir / "collections.json").write_text(json.dumps([]))
        (input_dir / "pages.json").write_text(json.dumps([]))
        (input_dir / "articles.json").write_text(json.dumps([]))
        (input_dir / "blogs.json").write_text(json.dumps([]))
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

        mock_translator.translate_metaobject.assert_called_once()

    @patch("translate_to_arabic.load_dotenv")
    @patch("translate_to_arabic.Translator")
    def test_resume_skips_existing(self, MockTranslator, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        mock_translator = MagicMock()
        MockTranslator.return_value = mock_translator
        mock_translator.translate_product.side_effect = lambda p, s, t: {**p, "title": "AR"}
        mock_translator.translate_collection.side_effect = lambda c, s, t: c
        mock_translator.translate_page.side_effect = lambda p, s, t: p
        mock_translator.translate_article.side_effect = lambda a, s, t: a

        input_dir = tmp_path / "data" / "english"
        input_dir.mkdir(parents=True)
        output_dir = tmp_path / "data" / "arabic"
        output_dir.mkdir(parents=True)

        (input_dir / "products.json").write_text(json.dumps([{"id": 1, "title": "P1"}, {"id": 2, "title": "P2"}]))
        (input_dir / "collections.json").write_text(json.dumps([]))
        (input_dir / "pages.json").write_text(json.dumps([]))
        (input_dir / "articles.json").write_text(json.dumps([]))
        (input_dir / "blogs.json").write_text(json.dumps([]))
        (output_dir / "products.json").write_text(json.dumps([{"id": 1, "title": "AR-P1"}]))

        os.environ["OPENAI_API_KEY"] = "fake"
        try:
            main()
        finally:
            del os.environ["OPENAI_API_KEY"]

        assert mock_translator.translate_product.call_count == 1
        assert mock_translator.translate_product.call_args[0][0]["id"] == 2
