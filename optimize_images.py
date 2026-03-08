#!/usr/bin/env python3
"""WebP image optimizer for Shopify asset migration.

Downloads images from source URLs, converts to WebP with quality optimization,
and provides the optimized bytes for upload. Supports PNG, JPEG, GIF, TIFF, BMP.

Usage:
    from optimize_images import optimize_image, download_and_optimize

    # From raw bytes
    webp_bytes, new_filename = optimize_image(raw_bytes, "photo.jpg")

    # From URL
    webp_bytes, new_filename = download_and_optimize("https://cdn.shopify.com/.../photo.jpg")
"""

import io
import os
import urllib.parse

import requests
from PIL import Image


# Default WebP quality (0-100). 80 is a good balance of quality and size.
DEFAULT_QUALITY = 80

# Max dimension (width or height) — resize if larger
MAX_DIMENSION = 2048

# File extensions that can be converted to WebP
CONVERTIBLE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".gif"}


def optimize_image(image_bytes, filename, quality=DEFAULT_QUALITY, max_dimension=MAX_DIMENSION):
    """Convert image bytes to optimized WebP format.

    Args:
        image_bytes: Raw image file bytes.
        filename: Original filename (used to determine format and generate new name).
        quality: WebP quality (0-100). Default 80.
        max_dimension: Max width or height. Images larger are resized proportionally.

    Returns:
        Tuple of (optimized_bytes, new_filename). If the image cannot be converted
        (e.g., SVG, already WebP), returns (original_bytes, original_filename).
    """
    ext = os.path.splitext(filename.lower())[0]
    name_base = os.path.splitext(filename)[0]
    file_ext = os.path.splitext(filename.lower())[1]

    # Skip non-convertible formats
    if file_ext not in CONVERTIBLE_EXTENSIONS:
        return image_bytes, filename

    try:
        img = Image.open(io.BytesIO(image_bytes))
    except Exception:
        # Can't open as image, return original
        return image_bytes, filename

    # Convert palette/RGBA modes appropriately
    if img.mode == "P":
        img = img.convert("RGBA")
    if img.mode == "RGBA":
        # WebP supports transparency
        pass
    elif img.mode not in ("RGB", "L"):
        img = img.convert("RGB")

    # Resize if exceeds max dimension
    original_size = img.size
    if max(img.size) > max_dimension:
        ratio = max_dimension / max(img.size)
        new_size = (int(img.size[0] * ratio), int(img.size[1] * ratio))
        img = img.resize(new_size, Image.LANCZOS)

    # Convert to WebP
    output = io.BytesIO()
    save_kwargs = {"format": "WEBP", "quality": quality, "method": 4}
    if img.mode == "RGBA":
        save_kwargs["lossless"] = False
    img.save(output, **save_kwargs)
    optimized_bytes = output.getvalue()

    new_filename = name_base + ".webp"

    original_kb = len(image_bytes) / 1024
    optimized_kb = len(optimized_bytes) / 1024
    reduction = (1 - len(optimized_bytes) / len(image_bytes)) * 100 if len(image_bytes) > 0 else 0

    print(f"    Optimized: {filename} ({original_size[0]}x{original_size[1]}) "
          f"{original_kb:.1f}KB → {new_filename} ({img.size[0]}x{img.size[1]}) "
          f"{optimized_kb:.1f}KB ({reduction:.0f}% reduction)")

    return optimized_bytes, new_filename


def download_and_optimize(url, quality=DEFAULT_QUALITY, max_dimension=MAX_DIMENSION):
    """Download an image from URL and convert to optimized WebP.

    Args:
        url: Public URL of the source image.
        quality: WebP quality (0-100).
        max_dimension: Max width/height before resizing.

    Returns:
        Tuple of (optimized_bytes, new_filename, mime_type).
    """
    parsed = urllib.parse.urlparse(url)
    filename = os.path.basename(parsed.path) or "image"
    filename = filename.split("?")[0]

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    raw_bytes = resp.content

    optimized_bytes, new_filename = optimize_image(raw_bytes, filename, quality, max_dimension)

    mime_type = "image/webp" if new_filename.endswith(".webp") else _guess_mime(new_filename)
    return optimized_bytes, new_filename, mime_type


def _guess_mime(filename):
    """Guess MIME type from filename."""
    import mimetypes
    mime, _ = mimetypes.guess_type(filename)
    return mime or "application/octet-stream"
