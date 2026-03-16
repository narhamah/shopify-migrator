"""Centralized data-directory paths for the Shopify migration pipeline.

Supports per-destination scoping via the DEST_NAME env var.
When DEST_NAME is set (e.g. "kuwait"), data paths resolve under
``data/kuwait/`` instead of the default ``data/`` flat layout.
This allows running the same pipeline against multiple destination
stores without id_map / progress-file collisions.
"""

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


# ─────────────────────────────────────────────────────────────────────────────
# Destination name (for multi-destination scoping)
# ─────────────────────────────────────────────────────────────────────────────
DEST_NAME_ENV = "DEST_NAME"


def get_dest_name():
    """Return the destination name (e.g. 'kuwait', 'us', 'ae') or None."""
    return os.environ.get(DEST_NAME_ENV)


def _dest_path(default_path):
    """Return *default_path* scoped under ``data/{dest_name}/`` when set."""
    dest = get_dest_name()
    if not dest:
        return default_path
    # e.g. "data/english" → "data/kuwait/english"
    parts = default_path.split("/", 1)
    if len(parts) == 2:
        return f"{parts[0]}/{dest}/{parts[1]}"
    return f"data/{dest}/{default_path}"


# ─────────────────────────────────────────────────────────────────────────────
# Data directories — destination-aware when DEST_NAME is set
# ─────────────────────────────────────────────────────────────────────────────
# Source export is shared across all destinations (never scoped).
SOURCE_DIR = "data/source_export"

def get_en_dir():
    return _dest_path("data/english")


def get_ar_dir():
    return _dest_path("data/arabic")


def get_id_map_file():
    return _dest_path("data/id_map.json")


def get_file_map_file():
    return _dest_path("data/file_map.json")


def get_progress_file(name):
    """Return a destination-scoped progress file path.

    Example: get_progress_file("redirects_progress.json")
    → "data/kuwait/redirects_progress.json" when DEST_NAME=kuwait
    """
    return _dest_path(f"data/{name}")


# Module-level constants for backwards compat (flat layout when DEST_NAME unset)
EN_DIR = "data/english"
AR_DIR = "data/arabic"
ID_MAP_FILE = "data/id_map.json"
FILE_MAP_FILE = "data/file_map.json"

# Backwards compat aliases
SPAIN_DIR = SOURCE_DIR
