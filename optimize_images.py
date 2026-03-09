#!/usr/bin/env python3
"""WebP image optimizer for Shopify asset migration.

Converts images to WebP with Shopify-optimized presets for different use cases:
  - hero:       2400x1200 (retina-ready banners)
  - product:    2048x2048 (supports zoom)
  - collection: 1920x1080 (collection banners)
  - icon:       400x400   (small UI icons, lossless for crisp edges)
  - thumbnail:  800x800   (thumbnails, cards)
  - logo:       800x400   (site logos, lossless)
  - default:    2048x2048 (general purpose)

Shopify's CDN auto-converts to WebP/AVIF on delivery, but pre-optimizing
reduces upload size, speeds up the migration, and ensures consistent quality.

Usage:
    from optimize_images import optimize_image, download_and_optimize

    # General purpose
    webp_bytes, new_filename = optimize_image(raw_bytes, "photo.jpg")

    # With Shopify preset
    webp_bytes, new_filename = optimize_image(raw_bytes, "hero.jpg", preset="hero")

    # From URL with preset
    webp_bytes, new_filename, mime = download_and_optimize(url, preset="icon")
"""

import io
import os
import urllib.parse

import requests
from PIL import Image


# Shopify image optimization presets
# Each preset defines: max_width, max_height, quality, lossless
PRESETS = {
    "hero": {
        "max_width": 2400,
        "max_height": 1200,
        "quality": 82,
        "lossless": False,
        "description": "Hero/banner images (retina-ready)",
    },
    "product": {
        "max_width": 2048,
        "max_height": 2048,
        "quality": 85,
        "lossless": False,
        "description": "Product images (supports zoom)",
    },
    "collection": {
        "max_width": 1920,
        "max_height": 1080,
        "quality": 82,
        "description": "Collection banner images",
    },
    "icon": {
        "max_width": 400,
        "max_height": 400,
        "quality": 90,
        "lossless": True,
        "description": "Icons and small UI elements (crisp, lossless)",
    },
    "thumbnail": {
        "max_width": 800,
        "max_height": 800,
        "quality": 80,
        "description": "Thumbnails and card images",
    },
    "logo": {
        "max_width": 800,
        "max_height": 400,
        "quality": 90,
        "lossless": True,
        "description": "Site logos (lossless for crisp text/edges)",
    },
    "default": {
        "max_width": 2048,
        "max_height": 2048,
        "quality": 80,
        "description": "General purpose",
    },
}

# Legacy constants (kept for backward compatibility)
DEFAULT_QUALITY = 80
MAX_DIMENSION = 2048

# File extensions that can be converted to WebP
CONVERTIBLE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".gif"}


def optimize_image(image_bytes, filename, quality=None, max_dimension=None, preset=None):
    """Convert image bytes to optimized WebP format.

    Args:
        image_bytes: Raw image file bytes.
        filename: Original filename (used to determine format and generate new name).
        quality: WebP quality (0-100). Overrides preset quality if set.
        max_dimension: Max width or height. Overrides preset if set.
            Deprecated — use preset instead for independent width/height control.
        preset: Shopify optimization preset name (hero, product, icon, etc.).
            See PRESETS dict for available options.

    Returns:
        Tuple of (optimized_bytes, new_filename). If the image cannot be converted
        (e.g., SVG, already WebP), returns (original_bytes, original_filename).
    """
    name_base = os.path.splitext(filename)[0]
    file_ext = os.path.splitext(filename.lower())[1]

    # Skip non-convertible formats
    if file_ext not in CONVERTIBLE_EXTENSIONS:
        return image_bytes, filename

    try:
        img = Image.open(io.BytesIO(image_bytes))
    except Exception:
        return image_bytes, filename

    # Resolve preset
    p = PRESETS.get(preset or "default", PRESETS["default"])
    q = quality if quality is not None else p["quality"]
    max_w = p["max_width"]
    max_h = p["max_height"]
    lossless = p.get("lossless", False)

    # Legacy max_dimension override
    if max_dimension is not None:
        max_w = max_dimension
        max_h = max_dimension

    # Convert color modes
    if img.mode == "P":
        img = img.convert("RGBA")
    if img.mode == "RGBA":
        pass  # WebP supports transparency
    elif img.mode not in ("RGB", "L"):
        img = img.convert("RGB")

    # Resize to fit within max_w x max_h preserving aspect ratio
    original_size = img.size
    w, h = img.size
    if w > max_w or h > max_h:
        ratio = min(max_w / w, max_h / h)
        new_size = (int(w * ratio), int(h * ratio))
        img = img.resize(new_size, Image.LANCZOS)

    # Convert to WebP
    output = io.BytesIO()
    save_kwargs = {"format": "WEBP", "method": 4}
    if lossless:
        save_kwargs["lossless"] = True
    else:
        save_kwargs["quality"] = q
        save_kwargs["lossless"] = False
    img.save(output, **save_kwargs)
    optimized_bytes = output.getvalue()

    new_filename = name_base + ".webp"

    original_kb = len(image_bytes) / 1024
    optimized_kb = len(optimized_bytes) / 1024
    reduction = (1 - len(optimized_bytes) / len(image_bytes)) * 100 if len(image_bytes) > 0 else 0

    preset_label = f" [{preset}]" if preset else ""
    print(f"    Optimized{preset_label}: {filename} ({original_size[0]}x{original_size[1]}) "
          f"{original_kb:.1f}KB → {new_filename} ({img.size[0]}x{img.size[1]}) "
          f"{optimized_kb:.1f}KB ({reduction:.0f}% reduction)")

    return optimized_bytes, new_filename


def download_and_optimize(url, quality=None, max_dimension=None, preset=None):
    """Download an image from URL and convert to optimized WebP.

    Args:
        url: Public URL of the source image.
        quality: WebP quality (0-100). Overrides preset if set.
        max_dimension: Max width/height. Overrides preset if set. Deprecated.
        preset: Shopify optimization preset (hero, product, icon, etc.).

    Returns:
        Tuple of (optimized_bytes, new_filename, mime_type).
    """
    parsed = urllib.parse.urlparse(url)
    filename = os.path.basename(parsed.path) or "image"
    filename = filename.split("?")[0]

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    raw_bytes = resp.content

    optimized_bytes, new_filename = optimize_image(
        raw_bytes, filename, quality=quality, max_dimension=max_dimension, preset=preset
    )

    mime_type = "image/webp" if new_filename.endswith(".webp") else _guess_mime(new_filename)
    return optimized_bytes, new_filename, mime_type


def _guess_mime(filename):
    """Guess MIME type from filename."""
    import mimetypes
    mime, _ = mimetypes.guess_type(filename)
    return mime or "application/octet-stream"
