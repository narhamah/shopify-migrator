"""Centralized data-directory paths for the Shopify migration pipeline.

Supports per-destination scoping via the DEST_NAME env var.
When DEST_NAME is set (e.g. "kuwait"), data paths resolve under
``data/kuwait/`` instead of the default ``data/`` flat layout.
This allows running the same pipeline against multiple destination
stores without id_map / progress-file collisions.
"""

import os


class ConfigError(KeyError):
    """Raised when required migration configuration is missing or invalid.

    Subclasses KeyError for backwards compatibility: existing code that does
    ``except KeyError`` around an optional config getter (treating "not set" as
    a fallback signal) keeps working. ``__str__`` is overridden so the message
    prints cleanly instead of KeyError's repr-with-quotes.
    """

    def __str__(self):
        return self.args[0] if self.args else ""


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
    hint = f" (legacy alias: {', '.join(fallback_names)})" if fallback_names else ""
    raise ConfigError(
        f"Required environment variable {name} is not set{hint}. "
        "Set it in your .env / destination env file."
    )


def _present(name, *fallback_names):
    """True if *name* or any fallback alias is set to a non-empty value."""
    return bool(os.environ.get(name) or any(os.environ.get(fb) for fb in fallback_names))


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

# ─────────────────────────────────────────────────────────────────────────────
# Configuration validation (used by preflight)
# ─────────────────────────────────────────────────────────────────────────────

def missing_required(require_magento=False):
    """Return a list of human-readable descriptions of missing required vars.

    Empty list means all required connection config is present. This never
    raises so a preflight can report *all* problems at once.
    """
    problems = []
    if not _present(SOURCE_SHOP_URL_ENV, _LEGACY_SOURCE[0]):
        problems.append("SOURCE_SHOP_URL - source store domain (e.g. tara-saudi.myshopify.com)")
    if not _present(SOURCE_ACCESS_TOKEN_ENV, _LEGACY_SOURCE[1]):
        problems.append("SOURCE_ACCESS_TOKEN - source Admin API token (shpat_...)")
    if not _present(DEST_SHOP_URL_ENV, _LEGACY_DEST[0]):
        problems.append("DEST_SHOP_URL - destination store domain (e.g. 977mp2-qa.myshopify.com)")
    if not _present(DEST_ACCESS_TOKEN_ENV, _LEGACY_DEST[1]):
        problems.append("DEST_ACCESS_TOKEN - destination Admin API token (shpat_...)")
    if require_magento:
        if not _present(MAGENTO_SITE_URL_ENV):
            problems.append("MAGENTO_SITE_URL - Magento base URL for prices/images")
        if not _present(MAGENTO_STORE_CODE_ENV):
            problems.append("MAGENTO_STORE_CODE - Magento store code (e.g. kw-en, sa-en)")
    return problems


def magento_is_explicit():
    """True if both Magento vars are explicitly set (i.e. not silent defaults)."""
    return _present(MAGENTO_SITE_URL_ENV) and _present(MAGENTO_STORE_CODE_ENV)


# Backwards compat aliases
SPAIN_DIR = SOURCE_DIR
