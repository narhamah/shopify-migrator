"""CSV row classification for Shopify 'Translate and adapt' exports.

Centralizes the logic for determining which CSV rows should be translated,
kept as-is, or skipped entirely. Used by translate_csv, validate_csv,
and clean_translation_csv modules.
"""

import json
import re


# ─────────────────────────────────────────────────────────────────────────────
# CSV type → Shopify GID type name
# ─────────────────────────────────────────────────────────────────────────────

CSV_TYPE_TO_GID = {
    "PRODUCT": "Product",
    "COLLECTION": "Collection",
    "PAGE": "Page",
    "ARTICLE": "Article",
    "BLOG": "Blog",
    "METAOBJECT": "Metaobject",
    "MENU": "Menu",
    "LINK": "Link",
    "MEDIA_IMAGE": "MediaImage",
    "PRODUCT_OPTION": "ProductOption",
    "PRODUCT_OPTION_VALUE": "ProductOptionValue",
    "METAFIELD": "Metafield",
    "FILTER": "Filter",
    "ONLINE_STORE_THEME": "OnlineStoreTheme",
    "COOKIE_BANNER": "CookieBanner",
    "DELIVERY_METHOD_DEFINITION": "DeliveryMethodDefinition",
    "PACKING_SLIP_TEMPLATE": "PackingSlipTemplate",
    "SHOP_POLICY": "ShopPolicy",
}

# Resource types that need parent resolution (can't be translated directly)
NEEDS_PARENT_RESOLUTION = {"METAFIELD", "MEDIA_IMAGE"}

# Resource types that aren't translatable via the Translations API
SKIP_TYPES = {"FILTER", "COOKIE_BANNER"}

ARABIC_LOCALE = "ar"

# ─────────────────────────────────────────────────────────────────────────────
# Row classification
# ─────────────────────────────────────────────────────────────────────────────

# Field patterns whose values should be copied as-is (not translated)
_KEEP_AS_IS_PATTERNS = [
    "facebook_url", "instagram_url", "tiktok_url", "twitter_url",
    "google_maps_api_key", "form_id", "portal_id", "region",
    "anchor_id", "worker_url", "default_lat", "default_lng",
    "custom_max_height", "custom_max_width",
]


def is_non_translatable(row):
    """Return True if this CSV row should never be translated.

    Detects: empty values, handles, URLs, GIDs, pure numbers, hex IDs,
    GID arrays, Liquid template expressions.
    """
    default = row.get("Default content", "").strip()
    field = row.get("Field", "")

    if not default:
        return True
    if field == "handle":
        return True
    if default.startswith(("shopify://", "http://", "https://", "/", "gid://")):
        return True
    if re.match(r"^-?\d+\.?\d*$", default):
        return True
    if re.match(r"^[0-9a-f]{8,}$", default):
        return True

    # JSON arrays of GIDs or numeric IDs
    if default.startswith("[") and default.endswith("]"):
        try:
            parsed = json.loads(default)
            if isinstance(parsed, list) and all(
                isinstance(v, str) and (v.startswith("gid://") or re.match(r"^\d+$", v))
                for v in parsed
            ):
                return True
        except (json.JSONDecodeError, TypeError):
            pass

    # Pure Liquid template expressions (no human-readable text)
    stripped = re.sub(r"<[^>]+>", "", default).strip()
    if stripped and re.match(r"^(\{\{[^}]+\}\}\s*[:;,]?\s*)+$", stripped):
        return True

    # JSON config objects (reviewCount, etc.)
    if default.startswith("{") and '"reviewCount"' in default:
        return True

    return False


def is_keep_as_is(row):
    """Return True if this row's value should be copied unchanged (URLs, images, config).

    These rows need a "Translated content" value in the CSV, but it should
    be identical to "Default content".
    """
    field = row.get("Field", "")

    for pat in _KEEP_AS_IS_PATTERNS:
        if pat in field:
            return True

    if field.endswith(".link") or field.endswith("_url"):
        return True
    if field.endswith(".image") or field.endswith(".image_1") or field.endswith(".image_2"):
        return True
    if ".image_1:" in field or ".image_2:" in field:
        return True
    if ".image_1_mobile:" in field or ".image_2_mobile:" in field:
        return True
    if field in ("general.logo", "general.logo_inverse", "general.favicon"):
        return True
    if ".icon:" in field:
        return True

    return False


def classify_row(row):
    """Classify a CSV row for translation.

    Returns one of:
        "skip"      — row should not appear in translated CSV
        "keep"      — copy Default content as Translated content
        "translate" — needs actual translation
        "done"      — already has valid translated content
    """
    if is_non_translatable(row):
        return "skip"
    if is_keep_as_is(row):
        return "keep"

    translated = row.get("Translated content", "").strip()
    if translated and translated != row.get("Default content", "").strip():
        return "done"

    return "translate"
