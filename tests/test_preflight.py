"""Tests for core.config validation and core.preflight."""
from unittest.mock import MagicMock, patch

import pytest

from tara_migrate.core import config
from tara_migrate.core.preflight import PreflightError, run_preflight


@pytest.fixture
def store_env(monkeypatch):
    monkeypatch.setenv("SOURCE_SHOP_URL", "src.myshopify.com")
    monkeypatch.setenv("SOURCE_ACCESS_TOKEN", "shpat_src")
    monkeypatch.setenv("DEST_SHOP_URL", "dst.myshopify.com")
    monkeypatch.setenv("DEST_ACCESS_TOKEN", "shpat_dst")
    # ensure no legacy magento bleed-through
    monkeypatch.delenv("MAGENTO_SITE_URL", raising=False)
    monkeypatch.delenv("MAGENTO_STORE_CODE", raising=False)


# --- config validation ---

class TestMissingRequired:
    def test_all_missing(self, monkeypatch):
        for v in ("SOURCE_SHOP_URL", "SOURCE_ACCESS_TOKEN", "DEST_SHOP_URL", "DEST_ACCESS_TOKEN",
                  "SPAIN_SHOP_URL", "SPAIN_ACCESS_TOKEN", "SAUDI_SHOP_URL", "SAUDI_ACCESS_TOKEN"):
            monkeypatch.delenv(v, raising=False)
        problems = config.missing_required()
        assert len(problems) == 4

    def test_legacy_aliases_satisfy(self, monkeypatch):
        for v in ("SOURCE_SHOP_URL", "SOURCE_ACCESS_TOKEN", "DEST_SHOP_URL", "DEST_ACCESS_TOKEN"):
            monkeypatch.delenv(v, raising=False)
        monkeypatch.setenv("SPAIN_SHOP_URL", "src.myshopify.com")
        monkeypatch.setenv("SPAIN_ACCESS_TOKEN", "tok")
        monkeypatch.setenv("SAUDI_SHOP_URL", "dst.myshopify.com")
        monkeypatch.setenv("SAUDI_ACCESS_TOKEN", "tok")
        assert config.missing_required() == []

    def test_magento_required(self, store_env):
        problems = config.missing_required(require_magento=True)
        assert any("MAGENTO_SITE_URL" in p for p in problems)
        assert any("MAGENTO_STORE_CODE" in p for p in problems)

    def test_env_missing_raises_config_error(self, monkeypatch):
        monkeypatch.delenv("DEST_SHOP_URL", raising=False)
        monkeypatch.delenv("SAUDI_SHOP_URL", raising=False)
        with pytest.raises(config.ConfigError, match="DEST_SHOP_URL"):
            config.get_dest_shop_url()


# --- preflight ---

class TestRunPreflight:
    def test_missing_config_raises(self, monkeypatch):
        for v in ("SOURCE_SHOP_URL", "SOURCE_ACCESS_TOKEN", "DEST_SHOP_URL", "DEST_ACCESS_TOKEN",
                  "SPAIN_SHOP_URL", "SPAIN_ACCESS_TOKEN", "SAUDI_SHOP_URL", "SAUDI_ACCESS_TOKEN"):
            monkeypatch.delenv(v, raising=False)
        with pytest.raises(config.ConfigError):
            run_preflight()

    def test_happy_path(self, store_env):
        client = MagicMock()
        client.get_shop.return_value = {"name": "Dest", "myshopify_domain": "dst.myshopify.com"}
        client.get_access_scopes.return_value = [
            "write_products", "write_content", "write_metaobjects",
            "write_translations", "write_publications", "write_inventory",
        ]
        with patch("tara_migrate.core.preflight.ShopifyClient", return_value=client):
            result = run_preflight()
        assert result.ok
        assert result.missing_dest_scopes == []

    def test_missing_scope_fails(self, store_env):
        client = MagicMock()
        client.get_shop.return_value = {"name": "Dest"}
        client.get_access_scopes.return_value = ["read_products"]  # read-only token
        with patch("tara_migrate.core.preflight.ShopifyClient", return_value=client):
            with pytest.raises(PreflightError, match="write scopes"):
                run_preflight()

    def test_unreachable_store_fails(self, store_env):
        client = MagicMock()
        client.get_shop.side_effect = Exception("401 Unauthorized")
        with patch("tara_migrate.core.preflight.ShopifyClient", return_value=client):
            with pytest.raises(PreflightError):
                run_preflight()

    def test_magento_required_missing_raises(self, store_env):
        # require_magento=True with no Magento env is a hard error (no silent defaults).
        with pytest.raises(config.ConfigError, match="MAGENTO"):
            run_preflight(require_magento=True)

    def test_unverifiable_scopes_warn_not_fail(self, store_env):
        client = MagicMock()
        client.get_shop.return_value = {"name": "Dest"}
        client.get_access_scopes.return_value = []  # empty -> cannot verify -> warning only
        with patch("tara_migrate.core.preflight.ShopifyClient", return_value=client):
            result = run_preflight()
        assert result.ok
        assert any("scope" in w.lower() for w in result.warnings)
