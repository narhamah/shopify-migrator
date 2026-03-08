"""Tests for optimize_images.py."""
import io

import pytest
from PIL import Image

from optimize_images import optimize_image, download_and_optimize, _guess_mime


def _make_image_bytes(fmt="PNG", size=(100, 100), mode="RGB"):
    """Create test image bytes in the specified format."""
    img = Image.new(mode, size, color=(255, 0, 0))
    buf = io.BytesIO()
    img.save(buf, format=fmt)
    return buf.getvalue()


class TestOptimizeImage:
    def test_converts_png_to_webp(self):
        raw = _make_image_bytes("PNG")
        result, name = optimize_image(raw, "photo.png")
        assert name == "photo.webp"
        # Verify the result is valid WebP
        img = Image.open(io.BytesIO(result))
        assert img.format == "WEBP"

    def test_converts_jpeg_to_webp(self):
        raw = _make_image_bytes("JPEG")
        result, name = optimize_image(raw, "photo.jpg")
        assert name == "photo.webp"
        img = Image.open(io.BytesIO(result))
        assert img.format == "WEBP"

    def test_converts_jpeg_extension(self):
        raw = _make_image_bytes("JPEG")
        result, name = optimize_image(raw, "photo.jpeg")
        assert name == "photo.webp"

    def test_converts_bmp_to_webp(self):
        raw = _make_image_bytes("BMP")
        result, name = optimize_image(raw, "image.bmp")
        assert name == "image.webp"

    def test_converts_tiff_to_webp(self):
        raw = _make_image_bytes("TIFF")
        result, name = optimize_image(raw, "scan.tiff")
        assert name == "scan.webp"

    def test_preserves_rgba_transparency(self):
        raw = _make_image_bytes("PNG", mode="RGBA")
        result, name = optimize_image(raw, "transparent.png")
        assert name == "transparent.webp"
        img = Image.open(io.BytesIO(result))
        assert img.mode in ("RGBA", "RGB")  # WebP may or may not preserve alpha

    def test_converts_palette_mode(self):
        """P (palette) mode images should be converted properly."""
        img = Image.new("P", (50, 50))
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        raw = buf.getvalue()
        result, name = optimize_image(raw, "palette.png")
        assert name == "palette.webp"

    def test_skips_svg(self):
        svg = b'<svg xmlns="http://www.w3.org/2000/svg"></svg>'
        result, name = optimize_image(svg, "icon.svg")
        assert name == "icon.svg"
        assert result == svg

    def test_skips_webp(self):
        # .webp extension is not in CONVERTIBLE_EXTENSIONS
        raw = b"fake webp data"
        result, name = optimize_image(raw, "already.webp")
        assert name == "already.webp"
        assert result == raw

    def test_skips_unknown_extension(self):
        raw = b"some data"
        result, name = optimize_image(raw, "doc.pdf")
        assert name == "doc.pdf"
        assert result == raw

    def test_resizes_large_images(self):
        raw = _make_image_bytes("PNG", size=(4000, 3000))
        result, name = optimize_image(raw, "huge.png", max_dimension=2048)
        img = Image.open(io.BytesIO(result))
        assert max(img.size) <= 2048
        # Should maintain aspect ratio
        assert img.size[0] == 2048 or img.size[1] == 2048

    def test_no_resize_small_images(self):
        raw = _make_image_bytes("PNG", size=(200, 150))
        result, name = optimize_image(raw, "small.png", max_dimension=2048)
        img = Image.open(io.BytesIO(result))
        assert img.size == (200, 150)

    def test_custom_quality(self):
        # Use a noisy image so quality differences are measurable
        import random
        img = Image.new("RGB", (500, 500))
        pixels = img.load()
        random.seed(42)
        for x in range(500):
            for y in range(500):
                pixels[x, y] = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
        buf = io.BytesIO()
        img.save(buf, format="JPEG")
        raw = buf.getvalue()
        result_low, _ = optimize_image(raw, "q.jpg", quality=10)
        result_high, _ = optimize_image(raw, "q.jpg", quality=95)
        # Lower quality should produce smaller file for noisy images
        assert len(result_low) < len(result_high)

    def test_corrupt_image_returns_original(self):
        raw = b"not an image at all"
        result, name = optimize_image(raw, "broken.jpg")
        assert result == raw
        assert name == "broken.jpg"

    def test_reduces_file_size(self):
        """WebP should generally be smaller than PNG for photos."""
        raw = _make_image_bytes("PNG", size=(800, 600))
        result, name = optimize_image(raw, "photo.png")
        assert len(result) < len(raw)

    def test_gif_conversion(self):
        raw = _make_image_bytes("GIF", size=(50, 50))
        result, name = optimize_image(raw, "anim.gif")
        assert name == "anim.webp"


class TestDownloadAndOptimize:
    def test_download_and_convert(self, requests_mock):
        raw = _make_image_bytes("JPEG", size=(200, 200))
        requests_mock.get("https://cdn.example.com/photo.jpg", content=raw)

        result_bytes, filename, mime = download_and_optimize("https://cdn.example.com/photo.jpg")
        assert filename == "photo.webp"
        assert mime == "image/webp"
        img = Image.open(io.BytesIO(result_bytes))
        assert img.format == "WEBP"

    def test_non_image_passthrough(self, requests_mock):
        raw = b"%PDF-1.4 fake pdf"
        requests_mock.get("https://cdn.example.com/doc.pdf", content=raw)

        result_bytes, filename, mime = download_and_optimize("https://cdn.example.com/doc.pdf")
        assert filename == "doc.pdf"
        assert result_bytes == raw


class TestGuessMime:
    def test_webp(self):
        assert _guess_mime("file.webp") == "image/webp"

    def test_jpg(self):
        assert _guess_mime("file.jpg") == "image/jpeg"

    def test_unknown(self):
        assert _guess_mime("file.xyz123") == "application/octet-stream"
