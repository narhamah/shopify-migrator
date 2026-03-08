# TARA Shopify Store Migration: Spain → Saudi Arabia

A Python CLI pipeline for migrating a Shopify store from Spain (Spanish) to Saudi Arabia (Arabic + English).

## Prerequisites

- Python 3.9+
- Shopify Admin API access tokens for both stores
- Anthropic API key (for Claude-powered translations)

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials
```

## Required Shopify Admin API Scopes

**Spain store (source):**
- `read_products`
- `read_content` (pages, blogs, articles)

**Saudi store (destination):**
- `read_products`, `write_products`
- `read_content`, `write_content`

## The 4-Step Pipeline

### Step 1: Export from Spain Store

```bash
python export_spain.py
```

Exports all products (with metafields), collections, pages, blogs, and articles to `data/spain_export/`.

### Step 2: Translate Spanish → English

```bash
python translate_to_english.py
```

Translates all content from Spanish to English using Claude. Saves progress after each item — safe to interrupt and resume. Output: `data/english/`.

### Step 3: Import English Content into Saudi Store

```bash
# Preview what will be created
python import_english.py --dry-run

# Import with currency conversion (EUR → SAR)
python import_english.py --exchange-rate 4.13

# Import without price conversion
python import_english.py
```

Creates products, collections, pages, blogs, and articles in the Saudi store. Saves an ID mapping (`data/id_map.json`) for cross-referencing. Skips items that already exist (matched by handle).

**Flags:**
- `--dry-run` — Show what would be created without making API calls
- `--exchange-rate RATE` — Multiply prices by this factor (default: 1.0). Use ~4.13 for EUR→SAR.

### Step 4: Translate English → Arabic

```bash
python translate_to_arabic.py
```

Translates English content to Modern Standard Arabic (Gulf/Saudi audience). Output: `data/arabic/`.

The Arabic files can be used with Shopify's Translation API or apps like Langify/Transcy to add Arabic as a secondary locale.

## File Structure

```
data/
├── spain_export/    # Raw export from Spain store
│   ├── products.json
│   ├── collections.json
│   ├── pages.json
│   ├── blogs.json
│   └── articles.json
├── english/         # English translations
├── arabic/          # Arabic translations
└── id_map.json      # Source ID → Destination ID mapping
```

## Translation Notes

- Brand name "TARA" is never translated
- Product-specific names (e.g., "Kansa Wand") are preserved
- HTML structure and URLs are maintained
- Arabic uses Modern Standard Arabic for Gulf/Saudi audience
