"""Shared utility functions and constants for the Shopify migration pipeline."""

import json
import os
import re


# ---------------------------------------------------------------------------
# Data directory paths
# ---------------------------------------------------------------------------

SPAIN_DIR = "data/spain_export"
EN_DIR = "data/english"
AR_DIR = "data/arabic"
ID_MAP_FILE = "data/id_map.json"
FILE_MAP_FILE = "data/file_map.json"


# ---------------------------------------------------------------------------
# JSON I/O
# ---------------------------------------------------------------------------

def load_json(filepath, default=None):
    """Load JSON from a file. Returns default if missing.

    Default behavior when no explicit default:
      - .json extension → []  (assumed to be a list of resources)
      - other extensions → {} (assumed to be a dict/config)
    """
    if not os.path.exists(filepath):
        if default is not None:
            return default
        if filepath.endswith(".json"):
            return []
        return {}
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data, filepath):
    """Save data as formatted JSON."""
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Rich text JSON sanitization
# ---------------------------------------------------------------------------

def sanitize_rich_text_json(value):
    """Fix rich_text_field JSON corrupted by translation.

    The translator can introduce literal newlines/control chars inside
    JSON string values. This function re-serializes the JSON to fix them.
    Returns the original value if it's not JSON or not fixable.
    """
    if not value or not isinstance(value, str):
        return value
    if not value.strip().startswith("{"):
        return value
    try:
        parsed = json.loads(value)
        return json.dumps(parsed, ensure_ascii=False)
    except json.JSONDecodeError:
        fixed = value
        fixed = fixed.replace('\\\r\n', '\\n').replace('\\\n', '\\n').replace('\\\r', '\\n')
        fixed = fixed.replace('\r\n', '\\n').replace('\n', '\\n').replace('\r', '\\n')
        fixed = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', fixed)
        try:
            parsed = json.loads(fixed)
            return json.dumps(parsed, ensure_ascii=False)
        except json.JSONDecodeError:
            fixed = re.sub(r'[\x00-\x1f]', '', value)
            try:
                parsed = json.loads(fixed)
                return json.dumps(parsed, ensure_ascii=False)
            except json.JSONDecodeError:
                print(f"    WARNING: Could not fix corrupted JSON ({len(value)} chars)")
                return value


# ---------------------------------------------------------------------------
# Image migration constants
# ---------------------------------------------------------------------------

# Setting keys that indicate image fields in theme templates
IMAGE_KEYWORDS = {
    "image", "img", "background", "banner", "hero", "icon",
    "logo", "photo", "picture", "thumbnail", "video_poster",
}

# Map section types to optimization presets
SECTION_PRESETS = {
    "hero": "hero", "hero-section": "hero", "slideshow": "hero",
    "image-banner": "hero", "image-with-text": "hero",
    "collection-list": "collection", "featured-collection": "collection",
    "icon": "icon", "icons-with-text": "icon", "icon-row": "icon",
    "icon-row-with-heading": "icon",
    "logo-list": "logo",
    "multicolumn": "thumbnail",
    "video": "hero",
}

# Metaobject fields that are file references (type → field keys)
METAOBJECT_FILE_FIELDS = {
    "blog_author": ["avatar"],
    "ingredient": ["image", "icon", "science_images"],
}

# Field key → optimization preset for file references
FILE_FIELD_PRESETS = {
    "icon": "icon",
    "avatar": "thumbnail",
    "image": "thumbnail",
    "science_images": "thumbnail",
}

# Article metafields that are file references
ARTICLE_FILE_METAFIELDS = {"custom.listing_image", "custom.hero_image"}


# ---------------------------------------------------------------------------
# Metaobject definition order (dependency-sorted)
# ---------------------------------------------------------------------------

DEFINITION_ORDER = ["benefit", "faq_entry", "blog_author", "ingredient"]


def sort_by_dependency(types):
    """Sort metaobject types so dependencies come first."""
    return sorted(types, key=lambda t: DEFINITION_ORDER.index(t) if t in DEFINITION_ORDER else 999)


# ---------------------------------------------------------------------------
# HTTP request helpers
# ---------------------------------------------------------------------------

MAGENTO_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

REQUEST_DELAY = 3.0
