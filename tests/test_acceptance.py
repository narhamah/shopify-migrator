"""Tests for the acceptance gate (audit.acceptance)."""
import os
from unittest.mock import MagicMock

import pytest

from tara_migrate.audit.acceptance import (
    check_id_map,
    check_locales,
    check_manifest,
    check_products_have_images,
    run_acceptance,
)


class TestChecks:
    def test_manifest_completed(self):
        assert check_manifest({"status": "completed", "phases": {"p1": {"status": "completed"}}}).passed

    def test_manifest_failed_phase(self):
        c = check_manifest({"status": "failed", "phases": {"p3": {"status": "failed"}}})
        assert not c.passed
        assert "p3" in c.detail

    def test_manifest_missing(self):
        assert not check_manifest({}).passed

    def test_id_map_populated(self):
        assert check_id_map({"products": {"1": 2}, "collections": {"3": 4}}).passed

    def test_id_map_empty_collections(self):
        assert not check_id_map({"products": {"1": 2}, "collections": {}}).passed

    def test_products_have_images(self):
        products = [{"handle": "a", "images": [{"src": "x"}]}, {"handle": "b", "images": [{"src": "y"}]}]
        assert check_products_have_images(products).passed

    def test_products_missing_images(self):
        products = [{"handle": "a", "images": [{"src": "x"}]}, {"handle": "b", "images": []}]
        c = check_products_have_images(products)
        assert not c.passed
        assert "b" in c.detail

    def test_no_products_fails(self):
        assert not check_products_have_images([]).passed

    def test_locales_ok(self):
        locales = [{"locale": "en", "published": True}, {"locale": "ar", "published": True}]
        assert check_locales(locales, ["en", "ar"]).passed

    def test_locale_unpublished(self):
        locales = [{"locale": "ar", "published": False}]
        c = check_locales(locales, ["ar"])
        assert not c.passed
        assert "unpublished" in c.detail


class TestRunAcceptance:
    def test_all_pass(self):
        client = MagicMock()
        client.get_products.return_value = [{"handle": "a", "images": [{"src": "x"}]}]
        client.get_locales.return_value = [{"locale": "ar", "published": True}]
        result = run_acceptance(
            client,
            manifest_data={"status": "completed", "phases": {}},
            id_map={"products": {"1": 2}, "collections": {"3": 4}},
            expected_locales=["ar"],
        )
        assert result.ok
        assert "PASS" in result.summary()

    def test_fails_when_product_missing_images(self):
        client = MagicMock()
        client.get_products.return_value = [{"handle": "a", "images": []}]
        result = run_acceptance(
            client,
            manifest_data={"status": "completed", "phases": {}},
            id_map={"products": {"1": 2}, "collections": {"3": 4}},
        )
        assert not result.ok

    def test_product_fetch_error_is_a_failed_check(self):
        client = MagicMock()
        client.get_products.side_effect = Exception("boom")
        result = run_acceptance(
            client,
            manifest_data={"status": "completed", "phases": {}},
            id_map={"products": {"1": 2}, "collections": {"3": 4}},
        )
        assert not result.ok


@pytest.mark.acceptance
def test_live_acceptance_gate():
    """Live gate against a real destination store. Skipped unless configured."""
    if not os.environ.get("DEST_SHOP_URL") or not os.environ.get("DEST_ACCESS_TOKEN"):
        pytest.skip("set DEST_SHOP_URL/DEST_ACCESS_TOKEN to run the live acceptance gate")
    from tara_migrate.client import ShopifyClient
    from tara_migrate.core import config, load_json

    client = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())
    manifest = load_json(config.get_progress_file("run_manifest.json"), default={})
    id_map = load_json(config.get_id_map_file(), default={})
    result = run_acceptance(client, manifest, id_map)
    assert result.ok, result.summary()
