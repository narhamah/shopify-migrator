"""Declarative, validated per-destination configuration.

Replaces scattered ``DEST_NAME == "usa"`` checks and N separate ``*.env`` files
with one typed TOML file per destination (``destinations/<name>.toml``). Secrets
are NEVER stored in the file — only the *name* of the env var that holds each
token. Loading validates the shape (pydantic) and then exports the values into
the env vars the rest of the pipeline already reads, so existing code is
unchanged.

    from tara_migrate.core.config_schema import load_destination_config, apply_to_env
    cfg = load_destination_config("destinations/kuwait.toml")
    apply_to_env(cfg)   # SOURCE_SHOP_URL, DEST_SHOP_URL, MAGENTO_*, DEST_NAME, tokens
"""

import os

try:  # Python 3.11+
    import tomllib  # type: ignore
except ModuleNotFoundError:  # 3.10 and earlier
    import tomli as tomllib  # type: ignore

from pydantic import BaseModel, ConfigDict, field_validator


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    shop_url: str
    access_token_env: str = "SOURCE_ACCESS_TOKEN"


class DestConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    shop_url: str
    access_token_env: str = "DEST_ACCESS_TOKEN"
    required_scopes: list[str] = []


class MagentoConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    site_url: str | None = None
    store_code: str | None = None


class MarketConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    countries: list[str] = []          # ISO codes the market covers, e.g. ["KW"]
    currency: str | None = None
    vat_rate: float | None = None
    exchange_rate: float | None = None
    locales: list[str] = ["en"]
    default_locale: str = "en"
    url_strategy: str = "subfolder"   # subfolder | domain
    subfolder_suffix: str | None = None
    domain: str | None = None

    @field_validator("url_strategy")
    @classmethod
    def _strategy(cls, v):
        if v not in ("subfolder", "domain"):
            raise ValueError("url_strategy must be 'subfolder' or 'domain'")
        return v


class DestinationConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    source: SourceConfig
    dest: DestConfig
    magento: MagentoConfig = MagentoConfig()
    market: MarketConfig = MarketConfig()
    apps: dict = {}

    @field_validator("name")
    @classmethod
    def _name(cls, v):
        if not v or not v.strip():
            raise ValueError("destination name must be non-empty")
        return v.strip()

    def model_post_init(self, __context):
        if self.market.url_strategy == "domain" and not self.market.domain:
            raise ValueError("market.domain is required when url_strategy = 'domain'")


def load_destination_config(path) -> DestinationConfig:
    """Load and validate a destination TOML file."""
    with open(path, "rb") as f:
        data = tomllib.load(f)
    return DestinationConfig(**data)


def apply_to_env(cfg: DestinationConfig, environ=None) -> DestinationConfig:
    """Export a validated config into the env vars the pipeline reads.

    Resolves token *references* (e.g. access_token_env="DEST_ACCESS_TOKEN") to
    the canonical SOURCE_ACCESS_TOKEN / DEST_ACCESS_TOKEN names. Does not
    overwrite a token that is already absent from the environment.
    """
    environ = os.environ if environ is None else environ
    environ["DEST_NAME"] = cfg.name
    environ["SOURCE_SHOP_URL"] = cfg.source.shop_url
    environ["DEST_SHOP_URL"] = cfg.dest.shop_url
    if cfg.magento.site_url:
        environ["MAGENTO_SITE_URL"] = cfg.magento.site_url
    if cfg.magento.store_code:
        environ["MAGENTO_STORE_CODE"] = cfg.magento.store_code

    src_tok = environ.get(cfg.source.access_token_env)
    if src_tok:
        environ["SOURCE_ACCESS_TOKEN"] = src_tok
    dst_tok = environ.get(cfg.dest.access_token_env)
    if dst_tok:
        environ["DEST_ACCESS_TOKEN"] = dst_tok
    return cfg
