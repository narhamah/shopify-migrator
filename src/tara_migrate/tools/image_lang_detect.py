"""OCR-based image language detection.

Classifies text language in product images by analyzing actual pixel content
via Tesseract OCR. Never relies on filenames — only the image bytes matter.

Dependencies:
    pip install pytesseract Pillow
    System: tesseract-ocr tesseract-ocr-ara tesseract-ocr-spa
"""

import re
from io import BytesIO

from PIL import Image

try:
    import pytesseract
except ImportError:
    pytesseract = None

# Arabic Unicode block
AR_RE = re.compile(r"[\u0600-\u06FF]")
# Latin letters (English/Spanish)
LATIN_RE = re.compile(r"[A-Za-z\u00C0-\u00FF]")
# Spanish-specific markers
ES_MARKERS = re.compile(
    r"[ñÑ¿¡]|ción|amiento|cabello|piel|cuero|champú|acondicionador|mascarilla"
)

# Minimum characters to consider meaningful text
MIN_CHARS = 3


def classify_image_language(image_bytes: bytes) -> str | None:
    """Classify text language in an image via OCR on actual pixel content.

    Does NOT use filenames — only the image bytes matter.

    Args:
        image_bytes: Raw image file bytes (JPEG, PNG, WebP, etc.)

    Returns:
        "ar" — Arabic text detected in image
        "en" — English text detected in image (Latin script, no Spanish markers)
        "es" — Spanish text detected in image (Latin script with Spanish markers)
        None  — no meaningful text found (lifestyle/product photo)

    Raises:
        RuntimeError: if pytesseract is not installed
    """
    if pytesseract is None:
        raise RuntimeError(
            "pytesseract is required for image language detection. "
            "Install with: pip install pytesseract"
        )

    img = Image.open(BytesIO(image_bytes))

    # OCR pass 1: Arabic script
    try:
        ar_text = pytesseract.image_to_string(img, lang="ara").strip()
    except Exception:
        ar_text = ""
    ar_chars = len(AR_RE.findall(ar_text))

    # OCR pass 2: Latin script (English + Spanish)
    try:
        lat_text = pytesseract.image_to_string(img, lang="eng+spa").strip()
    except Exception:
        # Fallback to English only if Spanish not installed
        try:
            lat_text = pytesseract.image_to_string(img, lang="eng").strip()
        except Exception:
            lat_text = ""
    lat_chars = len(LATIN_RE.findall(lat_text))

    # Not enough text to classify
    if ar_chars < MIN_CHARS and lat_chars < MIN_CHARS:
        return None

    # More Arabic than Latin → Arabic
    if ar_chars > lat_chars:
        return "ar"

    # Distinguish Spanish from English
    if ES_MARKERS.search(lat_text):
        return "es"
    return "en"
