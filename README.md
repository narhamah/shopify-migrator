# TARA Shopify Store Migration: Spain → Saudi Arabia

A Python CLI pipeline for migrating a Shopify store from Spain (Spanish) to Saudi Arabia (Arabic + English).

Handles: products (with metafields), collections, pages, blogs, articles (with metafields), and **metaobjects** (benefit, faq_entry, blog_author, ingredient).

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

**Spain store (source — read only):**
- `read_products`
- `read_content` (pages, blogs, articles)
- `read_metaobject_definitions`, `read_metaobjects`

**Saudi store (destination — read + write):**
- `read_products`, `write_products`
- `read_content`, `write_content`
- `read_metaobject_definitions`, `write_metaobject_definitions`
- `read_metaobjects`, `write_metaobjects`
- `read_translations`, `write_translations`
- `read_locales`

## The 6-Step Pipeline

```
[0] Setup Schema ──→ [1] Export ES ──→ [2] Translate ES→EN ──→ [3] Import EN ──→ [4] Translate EN→AR ──→ [5] Import AR
```

### Step 0: Set Up Destination Store Schema

```bash
# Preview
python setup_store.py --dry-run

# Run
python setup_store.py
```

Examines the Saudi store and creates any missing definitions:
- 4 metaobject definitions (benefit → faq_entry → blog_author → ingredient) with storefront access
- 19 product metafield definitions (tagline, accordion sections, ingredient/FAQ refs)
- 12 article metafield definitions (featured, author, related articles/products)

Resolves cross-references automatically (e.g., ingredient's `benefits` field → benefit definition GID).
Safe to re-run — skips anything that already exists.

### Step 1: Export from Spain Store

```bash
python export_spain.py
```

Exports all content to `data/spain_export/`:
- Products (with metafields: tagline, accordion content, etc.)
- Collections, pages, blogs, articles (with metafields)
- Metaobject definitions (benefit, faq_entry, blog_author, ingredient)
- Metaobject entries (all instances of each type)

### Step 2: Translate Spanish → English

```bash
python translate_to_english.py
```

Translates all content from Spanish to English using Claude. Includes:
- Product titles, descriptions, tags, variants, options
- Product metafields (tagline, accordion headings/content, etc.)
- Article metafields (blog_summary, hero_caption, etc.)
- Metaobject text fields (benefit titles, FAQ questions/answers, ingredient descriptions, etc.)
- Saves progress after each item — safe to interrupt and resume

Output: `data/english/`

### Step 3: Import English Content into Saudi Store

```bash
# Preview what will be created
python import_english.py --dry-run

# Import with currency conversion (EUR → SAR)
python import_english.py --exchange-rate 4.13
```

1. Examines the destination store for existing metaobject definitions
2. Creates metaobject entries (benefit → faq_entry → blog_author → ingredient)
3. Creates products with text metafields and price conversion
4. Creates collections, pages, blogs, articles
5. **Remaps all reference fields** (ingredient→benefit, product→ingredient/faq, article→author/ingredients/related)
6. Saves ID mapping (`data/id_map.json`); skips existing items (matched by handle)

**Flags:**
- `--dry-run` — Show what would be created without making API calls
- `--exchange-rate RATE` — Multiply prices by this factor (default: 1.0)

### Step 4: Translate English → Arabic

```bash
python translate_to_arabic.py
```

Translates English content to Modern Standard Arabic (Gulf/Saudi audience).
Includes all the same fields as Step 2. Output: `data/arabic/`

### Step 5: Import Arabic Translations into Saudi Store

```bash
# Preview
python import_arabic.py --dry-run

# Import
python import_arabic.py
```

Uses Shopify's Translations API to register Arabic as a secondary locale on all resources created in Step 3. Requires Arabic (ar) to be enabled in the Saudi store (Settings > Languages).

**Flags:**
- `--dry-run` — Show what would be translated without making API calls

## Data Architecture

The TARA theme uses these metaobject types:

| Type | Fields | Purpose |
|------|--------|---------|
| `benefit` | title, description, category, icon_label | Product benefits |
| `faq_entry` | question, answer | Per-product FAQ accordion |
| `blog_author` | name, bio, avatar | Rich author profiles |
| `ingredient` | name, inci_name, benefits, description, source, origin, category, concern, image, icon, is_hero, sort_order, collection | Ingredient library |

Product metafields (19 fields): tagline, short_description, size_ml, 7 accordion heading/content pairs, ingredient refs, FAQ refs.

Article metafields (12 fields): featured, blog_summary, hero_caption, author ref, related articles/products, etc.

## File Structure

```
data/
├── spain_export/              # Raw export from Spain store
│   ├── products.json          # Products with metafields
│   ├── collections.json
│   ├── pages.json
│   ├── blogs.json
│   ├── articles.json          # Articles with metafields
│   ├── metaobject_definitions.json
│   └── metaobjects.json       # All metaobject entries by type
├── english/                   # English translations
├── arabic/                    # Arabic translations
├── id_map.json                # Source ID → Destination ID mapping
└── arabic_import_progress.json # Arabic translation import progress
```

## Translation Notes

- Brand name "TARA" is never translated
- Product-specific names (e.g., "Kansa Wand") are preserved
- INCI ingredient names are preserved
- HTML structure, Shopify Liquid tags, and URLs are maintained
- Rich text fields (JSON) are translated at the text-node level
- Arabic uses Modern Standard Arabic for Gulf/Saudi audience
