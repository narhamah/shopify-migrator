"""Language detection utilities for translation quality assessment.

Provides functions to detect Arabic, Latin, and mixed-language text,
used across audit and fix scripts.
"""

import re

# Unicode ranges for Arabic script (covers Arabic, Arabic Supplement, Arabic Extended)
ARABIC_REGEX = re.compile(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]")
LATIN_REGEX = re.compile(r"[a-zA-ZÀ-ÿ]")


def count_chars(text):
    """Count Arabic vs Latin alphabetic characters in text.

    Returns (arabic_count, latin_count).
    """
    if not text:
        return 0, 0
    arabic = len(ARABIC_REGEX.findall(text))
    latin = len(LATIN_REGEX.findall(text))
    return arabic, latin


def has_arabic(text, min_ratio=0.3):
    """Check if text contains sufficient Arabic characters.

    Strips HTML and JSON structure before checking.
    Returns True if no alpha content or if Arabic ratio >= min_ratio.
    """
    if not text:
        return False
    stripped = re.sub(r"<[^>]+>", " ", text)
    stripped = re.sub(r"\{[^}]*\}", " ", stripped).strip()
    if not stripped:
        return True
    arabic, latin = count_chars(stripped)
    total = arabic + latin
    if total == 0:
        return True
    return arabic / total >= min_ratio


def has_significant_english(text, threshold=0.15):
    """Return True if text has significant English (>threshold ratio of Latin chars)."""
    if not text:
        return False
    arabic, latin = count_chars(text.strip())
    total = arabic + latin
    if total == 0:
        return False
    return latin / total > threshold


def is_arabic_visible_text(text, min_ratio=0.4, ok_patterns=None):
    """Check if visible page text is sufficiently Arabic for visual audit.

    More lenient than has_arabic() — used for Playwright-based visual audits.
    Skips short text, numbers, brand names, scientific terms.

    Args:
        ok_patterns: Additional regex patterns to whitelist (compiled or strings).
    """
    if not text or not text.strip():
        return True
    cleaned = text.strip()
    if len(cleaned) < 3:
        return True

    # Default OK patterns (currency, numbers, codes, copyright, phone, measurements)
    default_patterns = [
        r"^\d+",                # starts with number
        r"^[A-Z]{2,5}$",       # short codes
        r"^©", r"^@",          # copyright, social
        r"^\+\d",              # phone numbers
        r"^\d+\s?m[lL]",      # measurements
        r"^INCI", r"^pH\s",   # scientific terms
    ]
    all_patterns = default_patterns + (ok_patterns or [])

    for pat in all_patterns:
        if re.match(pat, cleaned):
            return True

    arabic, latin = count_chars(cleaned)
    total = arabic + latin
    if total == 0 or total < 3:
        return True
    if latin == 0:
        return True
    return arabic / total >= min_ratio


# ─────────────────────────────────────────────────────────────────────────────
# TARA collection/range names: English → Arabic
# These are NOT brand names — they must be translated in Arabic contexts.
# ─────────────────────────────────────────────────────────────────────────────
TARA_RANGE_NAMES_AR = {
    # Collection range names (primary)
    "Date+ Multivitamin":       "التمر + فيتامينات متعددة",
    "Date + Multivitamin":      "التمر + فيتامينات متعددة",
    "Date+Multivitamin":        "التمر + فيتامينات متعددة",
    "Black Garlic+ Ceramides":  "الثوم الأسود + سيراميدات",
    "Black Garlic + Ceramides": "الثوم الأسود + سيراميدات",
    "Onion+ Peptides":          "البصل + ببتيدات",
    "Onion + Peptides":         "البصل + ببتيدات",
    "Strawberry+ NMF":          "الفراولة + عوامل الترطيب الطبيعية",
    "Strawberry + NMF":         "الفراولة + عوامل الترطيب الطبيعية",
    "Rosemary+ Peptides":       "إكليل الجبل + ببتيدات",
    "Rosemary + Peptides":      "إكليل الجبل + ببتيدات",
    "Sage+ Multivitamin":       "الميرمية + فيتامينات متعددة",
    "Sage + Multivitamin":      "الميرمية + فيتامينات متعددة",
    "Detox":                    "ديتوكس",
    "Scalp Serums":             "سيرومات فروة الرأس",
    "Scalp Serum":              "سيروم فروة الرأس",
    "Best Sellers":             "الأكثر مبيعاً",
    # Ingredient extract names used in descriptions
    "Date Extract":             "مستخلص التمر",
    "Black Garlic":             "الثوم الأسود",
    "Strawberry Extract":       "مستخلص الفراولة",
    "Rosemary Extract":         "مستخلص إكليل الجبل",
    "Sage Extract":             "مستخلص الميرمية",
}

# Pre-compiled regex: match longest names first to avoid partial replacements
_RANGE_NAME_RE = re.compile(
    "|".join(re.escape(k) for k in sorted(TARA_RANGE_NAMES_AR.keys(), key=len, reverse=True)),
    re.IGNORECASE,
)


def replace_range_names_ar(text):
    """Replace English TARA collection/range names with Arabic equivalents.

    Case-insensitive matching. Returns the text with all known range names
    replaced. Safe to call on already-Arabic text (no-op if no matches).
    """
    if not text:
        return text

    def _replace(m):
        # Look up the matched text (case-insensitive key)
        key = m.group(0)
        # Try exact match first, then case-folded
        if key in TARA_RANGE_NAMES_AR:
            return TARA_RANGE_NAMES_AR[key]
        for k, v in TARA_RANGE_NAMES_AR.items():
            if k.lower() == key.lower():
                return v
        return key

    return _RANGE_NAME_RE.sub(_replace, text)


def find_untranslated_range_names(text):
    """Find English TARA range/collection names that appear in text.

    Returns list of (matched_text, arabic_equivalent) tuples.
    """
    if not text:
        return []
    matches = []
    for m in _RANGE_NAME_RE.finditer(text):
        key = m.group(0)
        for k, v in TARA_RANGE_NAMES_AR.items():
            if k.lower() == key.lower():
                matches.append((key, v))
                break
    return matches


def detect_mixed_language(text):
    """Detect if text contains mixed Arabic and another language.

    Returns (is_mixed, language_name) where language_name is
    'English', 'Spanish', or None.
    """
    if not text:
        return False, None

    arabic, latin = count_chars(text)
    total = arabic + latin
    if total == 0 or arabic == 0:
        return False, None

    lat_ratio = latin / total
    if lat_ratio <= 0.25 or latin <= 10:
        return False, None

    # Check for multi-word English phrases
    en_phrases = re.findall(r"[A-Z][a-z]+ [A-Z][a-z]+", text)
    # Check for Spanish morphology
    es_indicators = re.findall(
        r"(?:ción|ante|ador|mente|miento|ular|ficante)\b",
        text, re.IGNORECASE,
    )

    if es_indicators:
        return True, "Spanish"
    if en_phrases:
        return True, "English"

    return False, None
