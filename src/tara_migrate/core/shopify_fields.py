"""Shopify field classification for translation pipelines.

Centralizes the logic for determining which fields and values should
be translated vs skipped. Used by both audit and fix scripts.
"""

import json
import re

# Field key patterns that should NOT be translated (images, URLs, config, coordinates)
SKIP_FIELD_PATTERNS = [
    r"\.image$", r"\.image_\d", r"\.icon:", r"\.link$", r"_url$",
    r"\.logo", r"\.favicon", r"google_maps", r"form_id", r"portal_id",
    r"anchor_id", r"worker_url", r"default_lat", r"default_lng",
    r"max_height", r"max_width",
    # Theme-specific non-translatable field key patterns
    r"color_scheme",         # color_schemes.scheme_1.settings.*
    r"\.color$", r"\.colors$",  # color fields
    r"gradient",             # button_gradient_1, background_gradient, etc.
    r"\.shadow_", r"shadow_opacity",  # shadow config
    r"\.opacity$",           # opacity values
    r"card_style", r"badge_position",  # style/layout config
    r"crop_position",        # image crop
    r"section_width", r"column_count", r"row_count",  # layout numerics
    r"social_facebook$", r"social_twitter$", r"social_pinterest$",
    r"social_instagram$", r"social_tiktok$", r"social_tumblr$",
    r"social_snapchat$", r"social_youtube$",  # social handles (not URLs, just usernames)
    r"^handle$",  # resource handles are URL slugs, not translatable text
]

# Metafield types that contain translatable text
TEXT_METAFIELD_TYPES = {
    "single_line_text_field",
    "multi_line_text_field",
    "rich_text_field",
}

# All Shopify translatable resource types
TRANSLATABLE_RESOURCE_TYPES = [
    "PRODUCT", "COLLECTION", "METAFIELD", "METAOBJECT",
    "ONLINE_STORE_THEME", "PAGE", "BLOG", "ARTICLE",
]


def is_skippable_field(key):
    """Return True if this field key should not be translated.

    Matches against known non-translatable field patterns (images, URLs,
    coordinates, config IDs).
    """
    for pat in SKIP_FIELD_PATTERNS:
        if re.search(pat, key):
            return True
    return False


def is_skippable_value(value):
    """Return True if this value should not be translated.

    Detects URLs, GIDs, pure numbers, hex IDs, GID arrays, config JSON.
    """
    if not value or not value.strip():
        return True
    v = value.strip()
    # URLs, GIDs, file refs
    if v.startswith(("shopify://", "http://", "https://", "/", "gid://")):
        return True
    # Pure numbers
    if re.match(r"^-?\d+\.?\d*$", v):
        return True
    # Hex strings (hashes, IDs)
    if re.match(r"^[0-9a-f]{8,}$", v):
        return True
    # JSON arrays of IDs
    if v.startswith("[") and v.endswith("]"):
        try:
            parsed = json.loads(v)
            if isinstance(parsed, list) and all(
                isinstance(x, str) and (x.startswith("gid://") or re.match(r"^\d+$", x))
                for x in parsed
            ):
                return True
        except (json.JSONDecodeError, TypeError):
            pass
    # JSON config objects (reviewCount, etc.)
    if v.startswith("{") and '"reviewCount"' in v:
        return True
    # CSS hex colors (#fff, #1a2b3c, #1a2b3cff)
    if re.match(r"^#[0-9a-fA-F]{3,8}$", v):
        return True
    # CSS rgba/hsla values
    if re.match(r"^(?:rgba?|hsla?)\(", v):
        return True
    # Boolean strings
    if v.lower() in ("true", "false"):
        return True
    # CSS/config dimension values (e.g. "16px", "1.5rem", "100%", "50vh")
    if re.match(r"^-?\d+\.?\d*(px|rem|em|vh|vw|%)$", v):
        return True
    return False
