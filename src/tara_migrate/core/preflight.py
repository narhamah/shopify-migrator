"""Preflight validation — run before any destructive migration phase.

Refuses to start a run that would waste hours or silently corrupt a store:
  1. All required connection config present (else ConfigError, listing every gap).
  2. Source and destination stores are reachable with the given tokens.
  3. The destination token actually has the write scopes the pipeline needs
     (catches a read-only token before the first create_product call).
  4. Magento is explicitly configured when prices/images will be pulled
     (prevents UAE accidentally pulling USA prices via the silent default).

Returns a PreflightResult; raises PreflightError on any hard failure.
"""

from dataclasses import dataclass, field

from tara_migrate.client import GraphQLAuthError, ShopifyClient
from tara_migrate.core import config
from tara_migrate.core.logging import get_logger

logger = get_logger(__name__)

# Write scopes the core import/translate/publish pipeline requires on the destination.
DEFAULT_DEST_SCOPES = [
    "write_products",
    "write_content",        # pages, blogs, articles, redirects
    "write_metaobjects",
    "write_translations",
    "write_publications",
    "write_inventory",
]


class PreflightError(Exception):
    """Raised when preflight finds a hard blocker (bad token, missing scope, unreachable store)."""


@dataclass
class PreflightResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    source_shop: dict = field(default_factory=dict)
    dest_shop: dict = field(default_factory=dict)
    missing_dest_scopes: list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [f"Preflight: {'OK' if self.ok else 'FAILED'}"]
        if self.source_shop:
            lines.append(f"  source: {self.source_shop.get('myshopify_domain') or self.source_shop.get('name')}")
        if self.dest_shop:
            lines.append(f"  dest:   {self.dest_shop.get('myshopify_domain') or self.dest_shop.get('name')}")
        for w in self.warnings:
            lines.append(f"  WARN: {w}")
        for e in self.errors:
            lines.append(f"  ERROR: {e}")
        return "\n".join(lines)


def _connect_and_describe(shop_url, token, label, errors):
    """Connect and call get_shop(); record an error and return (client, shop|None)."""
    try:
        client = ShopifyClient(shop_url, token)
        shop = client.get_shop()
        if not shop:
            errors.append(f"{label} store returned no shop data ({shop_url}) — bad token or domain?")
            return client, None
        return client, shop
    except GraphQLAuthError as e:
        errors.append(f"{label} store access denied ({shop_url}): {e}")
    except Exception as e:  # connectivity / auth / DNS
        errors.append(f"{label} store unreachable or token invalid ({shop_url}): {e}")
    return None, None


def run_preflight(require_magento=False, required_dest_scopes=None,
                  check_scopes=True, check_connectivity=True):
    """Validate configuration and live store access.

    Raises ConfigError if required vars are missing, PreflightError on any other
    hard blocker. Returns a PreflightResult on success (which may carry warnings).
    """
    required_dest_scopes = required_dest_scopes if required_dest_scopes is not None else DEFAULT_DEST_SCOPES

    missing = config.missing_required(require_magento=require_magento)
    if missing:
        raise config.ConfigError(
            "Missing required configuration:\n  - " + "\n  - ".join(missing)
            + "\nSet these before running the migration."
        )

    result = PreflightResult(ok=True)

    if not check_connectivity:
        return result

    source_client, source_shop = _connect_and_describe(
        config.get_source_shop_url(), config.get_source_access_token(), "Source", result.errors)
    dest_client, dest_shop = _connect_and_describe(
        config.get_dest_shop_url(), config.get_dest_access_token(), "Destination", result.errors)
    result.source_shop = source_shop or {}
    result.dest_shop = dest_shop or {}

    if check_scopes and dest_client is not None and dest_shop is not None:
        try:
            granted = dest_client.get_access_scopes()
            if granted:
                missing_scopes = [s for s in required_dest_scopes if s not in set(granted)]
                result.missing_dest_scopes = missing_scopes
                if missing_scopes:
                    result.errors.append(
                        "Destination token is missing required write scopes: "
                        + ", ".join(missing_scopes)
                        + ". Re-create the custom app with these scopes."
                    )
            else:
                result.warnings.append(
                    "Could not read destination access scopes (empty response); "
                    "skipping scope verification."
                )
        except Exception as e:
            result.warnings.append(f"Could not verify destination scopes: {e}")

    if result.errors:
        result.ok = False
        raise PreflightError(result.summary())

    return result
