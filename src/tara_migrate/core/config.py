"""Centralized data-directory paths for the Shopify migration pipeline."""

import os


def _env(name, *fallback_names, default=None):
    """Read env var with optional fallback names for backwards compat."""
    val = os.environ.get(name)
    if val:
        return val
    for fb in fallback_names:
        val = os.environ.get(fb)
        if val:
            return val
    if default is not None:
        return default
    raise KeyError(name)


# Store connection env var names (generic)
SOURCE_SHOP_URL_ENV = "SOURCE_SHOP_URL"
SOURCE_ACCESS_TOKEN_ENV = "SOURCE_ACCESS_TOKEN"
DEST_SHOP_URL_ENV = "DEST_SHOP_URL"
DEST_ACCESS_TOKEN_ENV = "DEST_ACCESS_TOKEN"

# Legacy env var names (backwards compat)
_LEGACY_SOURCE = ("SPAIN_SHOP_URL", "SPAIN_ACCESS_TOKEN")
_LEGACY_DEST = ("SAUDI_SHOP_URL", "SAUDI_ACCESS_TOKEN")


def get_source_shop_url():
    return _env(SOURCE_SHOP_URL_ENV, _LEGACY_SOURCE[0])


def get_source_access_token():
    return _env(SOURCE_ACCESS_TOKEN_ENV, _LEGACY_SOURCE[1])


def get_dest_shop_url():
    return _env(DEST_SHOP_URL_ENV, _LEGACY_DEST[0])


def get_dest_access_token():
    return _env(DEST_ACCESS_TOKEN_ENV, _LEGACY_DEST[1])


# Magento settings (for price/product/image imports)
MAGENTO_SITE_URL_ENV = "MAGENTO_SITE_URL"
MAGENTO_STORE_CODE_ENV = "MAGENTO_STORE_CODE"

DEFAULT_MAGENTO_SITE_URL = "https://taraformula.com"
DEFAULT_MAGENTO_STORE_CODE = "us-en"


def get_magento_site_url():
    return _env(MAGENTO_SITE_URL_ENV, default=DEFAULT_MAGENTO_SITE_URL)


def get_magento_store_code():
    return _env(MAGENTO_STORE_CODE_ENV, default=DEFAULT_MAGENTO_STORE_CODE)


SOURCE_DIR = "data/source_export"
EN_DIR = "data/english"
AR_DIR = "data/arabic"
ID_MAP_FILE = "data/id_map.json"
FILE_MAP_FILE = "data/file_map.json"

# Backwards compat aliases
SPAIN_DIR = SOURCE_DIR
