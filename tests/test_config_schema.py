"""Tests for core.config_schema declarative destination config."""
import pytest

from tara_migrate.core.config_schema import (
    DestinationConfig,
    apply_to_env,
    load_destination_config,
)

VALID_TOML = """
name = "kuwait"

[source]
shop_url = "tara-saudi.myshopify.com"

[dest]
shop_url = "977mp2-qa.myshopify.com"
required_scopes = ["write_products"]

[magento]
site_url = "https://taraformula.com"
store_code = "kw-en"

[market]
currency = "KWD"
locales = ["en", "ar"]
url_strategy = "subfolder"
subfolder_suffix = "kw"
"""


def _write(tmp_path, text, name="kuwait.toml"):
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return str(p)


def test_loads_and_validates(tmp_path):
    cfg = load_destination_config(_write(tmp_path, VALID_TOML))
    assert cfg.name == "kuwait"
    assert cfg.source.shop_url == "tara-saudi.myshopify.com"
    assert cfg.dest.required_scopes == ["write_products"]
    assert cfg.magento.store_code == "kw-en"
    assert cfg.market.currency == "KWD"
    # defaults applied
    assert cfg.source.access_token_env == "SOURCE_ACCESS_TOKEN"


def test_invalid_url_strategy_rejected(tmp_path):
    bad = VALID_TOML.replace('url_strategy = "subfolder"', 'url_strategy = "wormhole"')
    with pytest.raises(Exception):
        load_destination_config(_write(tmp_path, bad))


def test_domain_strategy_requires_domain():
    with pytest.raises(Exception):
        DestinationConfig(
            name="x",
            source={"shop_url": "a"},
            dest={"shop_url": "b"},
            market={"url_strategy": "domain"},  # no domain -> error
        )


def test_apply_to_env_resolves_tokens(tmp_path, monkeypatch):
    monkeypatch.setenv("SOURCE_ACCESS_TOKEN", "shpat_src")
    monkeypatch.setenv("DEST_ACCESS_TOKEN", "shpat_dst")
    cfg = load_destination_config(_write(tmp_path, VALID_TOML))
    env = {}
    # apply into a custom dict so we don't mutate the real environment...
    cfg2 = apply_to_env(cfg, environ=env)
    assert cfg2 is cfg
    assert env["DEST_NAME"] == "kuwait"
    assert env["SOURCE_SHOP_URL"] == "tara-saudi.myshopify.com"
    assert env["DEST_SHOP_URL"] == "977mp2-qa.myshopify.com"
    assert env["MAGENTO_STORE_CODE"] == "kw-en"
    # tokens not present in this custom env -> not aliased
    assert "SOURCE_ACCESS_TOKEN" not in env

    # now with tokens present in the passed env
    env2 = {"SOURCE_ACCESS_TOKEN": "s", "DEST_ACCESS_TOKEN": "d"}
    apply_to_env(cfg, environ=env2)
    assert env2["SOURCE_ACCESS_TOKEN"] == "s"
    assert env2["DEST_ACCESS_TOKEN"] == "d"


def test_custom_token_env_reference(tmp_path, monkeypatch):
    toml = VALID_TOML.replace(
        '[dest]\nshop_url = "977mp2-qa.myshopify.com"',
        '[dest]\nshop_url = "977mp2-qa.myshopify.com"\naccess_token_env = "KUWAIT_TOKEN"',
    )
    cfg = load_destination_config(_write(tmp_path, toml))
    env = {"KUWAIT_TOKEN": "shpat_kw"}
    apply_to_env(cfg, environ=env)
    assert env["DEST_ACCESS_TOKEN"] == "shpat_kw"
