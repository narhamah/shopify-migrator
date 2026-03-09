"""Tests for migrate_assets.py."""
import json
import os
from unittest.mock import MagicMock, patch

import pytest

from migrate_assets import extract_file_url_from_gid, upload_optimized, main
from utils import load_json, save_json, METAOBJECT_FILE_FIELDS, ARTICLE_FILE_METAFIELDS
from tests.conftest import make_metaobjects_data, make_article, make_id_map


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_metaobject_file_fields(self):
        assert "blog_author" in METAOBJECT_FILE_FIELDS
        assert "avatar" in METAOBJECT_FILE_FIELDS["blog_author"]
        assert "ingredient" in METAOBJECT_FILE_FIELDS
        assert set(METAOBJECT_FILE_FIELDS["ingredient"]) == {"image", "icon", "science_images"}

    def test_article_file_metafields(self):
        assert "custom.listing_image" in ARTICLE_FILE_METAFIELDS
        assert "custom.hero_image" in ARTICLE_FILE_METAFIELDS
        assert len(ARTICLE_FILE_METAFIELDS) == 2


# ---------------------------------------------------------------------------
# load_json / save_json
# ---------------------------------------------------------------------------

class TestLoadJson:
    def test_existing(self, tmp_path):
        f = tmp_path / "data.json"
        f.write_text(json.dumps({"a": 1}))
        assert load_json(str(f)) == {"a": 1}

    def test_missing(self, tmp_path):
        assert load_json(str(tmp_path / "nope.json")) == []

    def test_missing_non_json(self, tmp_path):
        assert load_json(str(tmp_path / "nope.txt")) == {}


class TestSaveJson:
    def test_save(self, tmp_path):
        f = str(tmp_path / "out.json")
        save_json({"a": 1}, f)
        with open(f) as fh:
            assert json.load(fh) == {"a": 1}


# ---------------------------------------------------------------------------
# extract_file_url_from_gid
# ---------------------------------------------------------------------------

class TestExtractFileUrlFromGid:
    def test_media_image(self):
        mc = MagicMock()
        mc.get_file_by_id.return_value = {
            "id": "gid://1", "image": {"url": "https://cdn.shopify.com/img.jpg"}, "url": None
        }
        result = extract_file_url_from_gid(mc, "gid://shopify/MediaImage/1")
        assert result == "https://cdn.shopify.com/img.jpg"

    def test_generic_file(self):
        mc = MagicMock()
        mc.get_file_by_id.return_value = {"id": "gid://1", "url": "https://cdn.shopify.com/file.pdf"}
        result = extract_file_url_from_gid(mc, "gid://shopify/GenericFile/1")
        assert result == "https://cdn.shopify.com/file.pdf"

    def test_empty_gid(self):
        mc = MagicMock()
        assert extract_file_url_from_gid(mc, "") is None
        assert extract_file_url_from_gid(mc, None) is None

    def test_non_gid_string(self):
        mc = MagicMock()
        assert extract_file_url_from_gid(mc, "not-a-gid") is None

    def test_node_not_found(self):
        mc = MagicMock()
        mc.get_file_by_id.return_value = None
        assert extract_file_url_from_gid(mc, "gid://shopify/MediaImage/999") is None

    def test_api_error(self):
        mc = MagicMock()
        mc.get_file_by_id.side_effect = Exception("API error")
        assert extract_file_url_from_gid(mc, "gid://shopify/MediaImage/1") is None

    def test_node_no_url(self):
        mc = MagicMock()
        mc.get_file_by_id.return_value = {"id": "gid://1"}
        assert extract_file_url_from_gid(mc, "gid://shopify/MediaImage/1") is None

    def test_empty_image_dict(self):
        mc = MagicMock()
        mc.get_file_by_id.return_value = {"id": "gid://1", "image": {}}
        assert extract_file_url_from_gid(mc, "gid://shopify/MediaImage/1") is None


# ---------------------------------------------------------------------------
# Helper to set up data dirs
# ---------------------------------------------------------------------------

def _setup_asset_data(base_path, metaobjects=None, articles=None, id_map=None):
    data_dir = base_path / "data"
    data_dir.mkdir(exist_ok=True)
    en_dir = data_dir / "english"
    en_dir.mkdir(exist_ok=True)

    if metaobjects is not None:
        (en_dir / "metaobjects.json").write_text(json.dumps(metaobjects))
    if articles is not None:
        (en_dir / "articles.json").write_text(json.dumps(articles))

    if id_map is None:
        id_map = make_id_map()
    (data_dir / "id_map.json").write_text(json.dumps(id_map))


# ---------------------------------------------------------------------------
# main() — integration tests
# ---------------------------------------------------------------------------

class TestMainMetaobjectFiles:
    @patch("migrate_assets.load_dotenv")
    @patch("migrate_assets.ShopifyClient")
    @patch("migrate_assets.time.sleep")
    @patch("migrate_assets.download_and_optimize")
    def test_uploads_metaobject_files(self, mock_download, mock_sleep, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        source_mc = MagicMock()
        dest_mc = MagicMock()
        MockClient.side_effect = [source_mc, dest_mc]

        source_mc.get_file_by_id.return_value = {
            "id": "gid://src/1", "image": {"url": "https://cdn.shopify.com/img.jpg"}
        }
        mock_download.return_value = (b"webp-bytes", "img.webp", "image/webp")
        dest_mc.upload_file_bytes.return_value = "gid://shopify/MediaImage/NEW_1"
        dest_mc.update_metaobject.return_value = {"id": "gid://dest/1"}

        metaobjects = {
            "ingredient": {
                "definition": {"type": "ingredient"},
                "objects": [{
                    "id": "gid://shopify/Metaobject/400",
                    "handle": "argan-oil",
                    "type": "ingredient",
                    "fields": [
                        {"key": "image", "value": "gid://shopify/MediaImage/10", "type": "file_reference"},
                        {"key": "icon", "value": "gid://shopify/MediaImage/11", "type": "file_reference"},
                        {"key": "name", "value": "Argan Oil", "type": "single_line_text_field"},
                    ],
                }],
            },
        }
        id_map = {
            "metaobjects_ingredient": {"gid://shopify/Metaobject/400": "gid://shopify/Metaobject/800"},
            "articles": {},
        }
        _setup_asset_data(tmp_path, metaobjects=metaobjects, articles=[], id_map=id_map)

        os.environ.update({
            "SPAIN_SHOP_URL": "spain.myshopify.com", "SPAIN_ACCESS_TOKEN": "tok",
            "SAUDI_SHOP_URL": "saudi.myshopify.com", "SAUDI_ACCESS_TOKEN": "tok",
        })
        try:
            main()
        finally:
            for k in ["SPAIN_SHOP_URL", "SPAIN_ACCESS_TOKEN", "SAUDI_SHOP_URL", "SAUDI_ACCESS_TOKEN"]:
                del os.environ[k]

        assert dest_mc.upload_file_bytes.call_count == 2
        dest_mc.update_metaobject.assert_called_once()


class TestMainArticleFiles:
    @patch("migrate_assets.load_dotenv")
    @patch("migrate_assets.ShopifyClient")
    @patch("migrate_assets.time.sleep")
    @patch("migrate_assets.download_and_optimize")
    def test_uploads_article_file_metafields(self, mock_download, mock_sleep, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        source_mc = MagicMock()
        dest_mc = MagicMock()
        MockClient.side_effect = [source_mc, dest_mc]

        source_mc.get_file_by_id.return_value = {
            "id": "gid://src/1", "image": {"url": "https://cdn.shopify.com/listing.jpg"}
        }
        mock_download.return_value = (b"webp-bytes", "listing.webp", "image/webp")
        dest_mc.upload_file_bytes.return_value = "gid://shopify/MediaImage/NEW_1"
        dest_mc.set_metafields.return_value = []

        articles = [{
            "id": 5001, "handle": "test-article", "title": "Test",
            "metafields": [
                {"namespace": "custom", "key": "listing_image",
                 "value": "gid://shopify/MediaImage/77", "type": "file_reference"},
            ],
        }]
        _setup_asset_data(tmp_path, articles=articles, id_map={"articles": {"5001": 9005}})

        os.environ.update({
            "SPAIN_SHOP_URL": "spain.myshopify.com", "SPAIN_ACCESS_TOKEN": "tok",
            "SAUDI_SHOP_URL": "saudi.myshopify.com", "SAUDI_ACCESS_TOKEN": "tok",
        })
        try:
            main()
        finally:
            for k in ["SPAIN_SHOP_URL", "SPAIN_ACCESS_TOKEN", "SAUDI_SHOP_URL", "SAUDI_ACCESS_TOKEN"]:
                del os.environ[k]

        dest_mc.upload_file_bytes.assert_called_once()
        dest_mc.set_metafields.assert_called_once()


class TestFileMapResume:
    def test_single_file_in_map(self):
        file_map = {"gid://shopify/MediaImage/10": "gid://shopify/MediaImage/NEW_10"}
        assert file_map["gid://shopify/MediaImage/10"] == "gid://shopify/MediaImage/NEW_10"

    def test_list_file_in_map(self):
        file_map = {"gid://shopify/MediaImage/12": "gid://shopify/MediaImage/NEW_12"}
        source_gids = json.loads('["gid://shopify/MediaImage/12"]')
        dest_gids = [file_map[gid] for gid in source_gids if gid in file_map]
        assert dest_gids == ["gid://shopify/MediaImage/NEW_12"]


class TestMainListFileReference:
    @patch("migrate_assets.load_dotenv")
    @patch("migrate_assets.ShopifyClient")
    @patch("migrate_assets.time.sleep")
    @patch("migrate_assets.download_and_optimize")
    def test_uploads_list_file_references(self, mock_download, mock_sleep, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        source_mc = MagicMock()
        dest_mc = MagicMock()
        MockClient.side_effect = [source_mc, dest_mc]

        source_mc.get_file_by_id.return_value = {
            "id": "gid://src/1", "image": {"url": "https://cdn.shopify.com/sci.jpg"}
        }
        mock_download.return_value = (b"webp-bytes", "sci.webp", "image/webp")
        dest_mc.upload_file_bytes.return_value = "gid://shopify/MediaImage/NEW_SCI"
        dest_mc.update_metaobject.return_value = {"id": "gid://dest/1"}

        metaobjects = {
            "ingredient": {
                "definition": {"type": "ingredient"},
                "objects": [{
                    "id": "gid://shopify/Metaobject/400",
                    "handle": "argan-oil",
                    "type": "ingredient",
                    "fields": [
                        {"key": "science_images",
                         "value": '["gid://shopify/MediaImage/12", "gid://shopify/MediaImage/13"]',
                         "type": "list.file_reference"},
                    ],
                }],
            },
        }
        id_map = {
            "metaobjects_ingredient": {"gid://shopify/Metaobject/400": "gid://shopify/Metaobject/800"},
            "articles": {},
        }
        _setup_asset_data(tmp_path, metaobjects=metaobjects, articles=[], id_map=id_map)

        os.environ.update({
            "SPAIN_SHOP_URL": "spain.myshopify.com", "SPAIN_ACCESS_TOKEN": "tok",
            "SAUDI_SHOP_URL": "saudi.myshopify.com", "SAUDI_ACCESS_TOKEN": "tok",
        })
        try:
            main()
        finally:
            for k in ["SPAIN_SHOP_URL", "SPAIN_ACCESS_TOKEN", "SAUDI_SHOP_URL", "SAUDI_ACCESS_TOKEN"]:
                del os.environ[k]

        assert dest_mc.upload_file_bytes.call_count == 2
        dest_mc.update_metaobject.assert_called_once()


class TestMainUploadErrors:
    @patch("migrate_assets.load_dotenv")
    @patch("migrate_assets.ShopifyClient")
    @patch("migrate_assets.time.sleep")
    @patch("migrate_assets.download_and_optimize")
    def test_upload_error_continues(self, mock_download, mock_sleep, MockClient, mock_dotenv, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)

        source_mc = MagicMock()
        dest_mc = MagicMock()
        MockClient.side_effect = [source_mc, dest_mc]

        source_mc.get_file_by_id.return_value = {
            "id": "gid://src/1", "image": {"url": "https://cdn.shopify.com/img.jpg"}
        }
        mock_download.return_value = (b"webp-bytes", "img.webp", "image/webp")
        dest_mc.upload_file_bytes.side_effect = Exception("Upload failed")

        metaobjects = {
            "ingredient": {
                "definition": {"type": "ingredient"},
                "objects": [{
                    "id": "gid://shopify/Metaobject/400",
                    "handle": "argan-oil",
                    "type": "ingredient",
                    "fields": [
                        {"key": "image", "value": "gid://shopify/MediaImage/10", "type": "file_reference"},
                    ],
                }],
            },
        }
        id_map = {
            "metaobjects_ingredient": {"gid://shopify/Metaobject/400": "gid://shopify/Metaobject/800"},
            "articles": {},
        }
        _setup_asset_data(tmp_path, metaobjects=metaobjects, articles=[], id_map=id_map)

        os.environ.update({
            "SPAIN_SHOP_URL": "spain.myshopify.com", "SPAIN_ACCESS_TOKEN": "tok",
            "SAUDI_SHOP_URL": "saudi.myshopify.com", "SAUDI_ACCESS_TOKEN": "tok",
        })
        try:
            main()
        finally:
            for k in ["SPAIN_SHOP_URL", "SPAIN_ACCESS_TOKEN", "SAUDI_SHOP_URL", "SAUDI_ACCESS_TOKEN"]:
                del os.environ[k]

        captured = capsys.readouterr()
        assert "upload error" in captured.out.lower()

    @patch("migrate_assets.load_dotenv")
    @patch("migrate_assets.ShopifyClient")
    @patch("migrate_assets.time.sleep")
    def test_file_url_not_found_skips(self, mock_sleep, MockClient, mock_dotenv, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)

        source_mc = MagicMock()
        dest_mc = MagicMock()
        MockClient.side_effect = [source_mc, dest_mc]

        source_mc.get_file_by_id.return_value = None  # File not found

        metaobjects = {
            "ingredient": {
                "definition": {"type": "ingredient"},
                "objects": [{
                    "id": "gid://shopify/Metaobject/400",
                    "handle": "argan-oil",
                    "type": "ingredient",
                    "fields": [
                        {"key": "image", "value": "gid://shopify/MediaImage/10", "type": "file_reference"},
                    ],
                }],
            },
        }
        id_map = {
            "metaobjects_ingredient": {"gid://shopify/Metaobject/400": "gid://shopify/Metaobject/800"},
            "articles": {},
        }
        _setup_asset_data(tmp_path, metaobjects=metaobjects, articles=[], id_map=id_map)

        os.environ.update({
            "SPAIN_SHOP_URL": "spain.myshopify.com", "SPAIN_ACCESS_TOKEN": "tok",
            "SAUDI_SHOP_URL": "saudi.myshopify.com", "SAUDI_ACCESS_TOKEN": "tok",
        })
        try:
            main()
        finally:
            for k in ["SPAIN_SHOP_URL", "SPAIN_ACCESS_TOKEN", "SAUDI_SHOP_URL", "SAUDI_ACCESS_TOKEN"]:
                del os.environ[k]

        captured = capsys.readouterr()
        assert "could not get url" in captured.out.lower()
        dest_mc.upload_file_bytes.assert_not_called()


class TestMainBlogAuthorFiles:
    @patch("migrate_assets.load_dotenv")
    @patch("migrate_assets.ShopifyClient")
    @patch("migrate_assets.time.sleep")
    @patch("migrate_assets.download_and_optimize")
    def test_uploads_blog_author_avatar(self, mock_download, mock_sleep, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        source_mc = MagicMock()
        dest_mc = MagicMock()
        MockClient.side_effect = [source_mc, dest_mc]

        source_mc.get_file_by_id.return_value = {
            "id": "gid://src/1", "image": {"url": "https://cdn.shopify.com/avatar.jpg"}
        }
        mock_download.return_value = (b"webp-bytes", "avatar.webp", "image/webp")
        dest_mc.upload_file_bytes.return_value = "gid://shopify/MediaImage/NEW_AV"
        dest_mc.update_metaobject.return_value = {"id": "gid://dest/1"}

        metaobjects = {
            "blog_author": {
                "definition": {"type": "blog_author"},
                "objects": [{
                    "id": "gid://shopify/Metaobject/300",
                    "handle": "jane",
                    "type": "blog_author",
                    "fields": [
                        {"key": "avatar", "value": "gid://shopify/MediaImage/20", "type": "file_reference"},
                        {"key": "name", "value": "Jane", "type": "single_line_text_field"},
                    ],
                }],
            },
        }
        id_map = {
            "metaobjects_blog_author": {"gid://shopify/Metaobject/300": "gid://shopify/Metaobject/700"},
            "articles": {},
        }
        _setup_asset_data(tmp_path, metaobjects=metaobjects, articles=[], id_map=id_map)

        os.environ.update({
            "SPAIN_SHOP_URL": "spain.myshopify.com", "SPAIN_ACCESS_TOKEN": "tok",
            "SAUDI_SHOP_URL": "saudi.myshopify.com", "SAUDI_ACCESS_TOKEN": "tok",
        })
        try:
            main()
        finally:
            for k in ["SPAIN_SHOP_URL", "SPAIN_ACCESS_TOKEN", "SAUDI_SHOP_URL", "SAUDI_ACCESS_TOKEN"]:
                del os.environ[k]

        dest_mc.upload_file_bytes.assert_called_once()
        dest_mc.update_metaobject.assert_called_once()


class TestMissingDestId:
    def test_no_dest_id(self):
        id_map = {"metaobjects_ingredient": {}}
        assert id_map.get("metaobjects_ingredient", {}).get("gid://shopify/Metaobject/400") is None


class TestListFileReference:
    def test_list_file_detection(self):
        assert "list." in "list.file_reference"

    def test_single_file_detection(self):
        assert "list." not in "file_reference"


class TestArticleNoMetafields:
    @patch("migrate_assets.load_dotenv")
    @patch("migrate_assets.ShopifyClient")
    @patch("migrate_assets.time.sleep")
    def test_no_file_metafields(self, mock_sleep, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        source_mc = MagicMock()
        dest_mc = MagicMock()
        MockClient.side_effect = [source_mc, dest_mc]

        articles = [{
            "id": 5001, "handle": "test-article", "title": "Test",
            "metafields": [
                {"namespace": "custom", "key": "blog_summary",
                 "value": "Summary", "type": "single_line_text_field"},
            ],
        }]
        _setup_asset_data(tmp_path, articles=articles, id_map={"articles": {"5001": 9005}})

        os.environ.update({
            "SPAIN_SHOP_URL": "spain.myshopify.com", "SPAIN_ACCESS_TOKEN": "tok",
            "SAUDI_SHOP_URL": "saudi.myshopify.com", "SAUDI_ACCESS_TOKEN": "tok",
        })
        try:
            main()
        finally:
            for k in ["SPAIN_SHOP_URL", "SPAIN_ACCESS_TOKEN", "SAUDI_SHOP_URL", "SAUDI_ACCESS_TOKEN"]:
                del os.environ[k]

        dest_mc.upload_file_bytes.assert_not_called()
        dest_mc.set_metafields.assert_not_called()
