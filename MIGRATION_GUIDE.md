# TARA Shopify Migration Guide

## Spain → Saudi Arabia Store Migration

This guide covers migrating the TARA luxury brand from the Spanish Shopify store to a new Saudi Arabian store, with English and Arabic language support.

---

## Prerequisites

1. **Python 3.9+** with dependencies installed:
   ```bash
   pip install -r requirements.txt
   ```

2. **`.env` file** in the project root with:
   ```env
   # Spain (source) store
   SPAIN_SHOP_URL=your-spain-store.myshopify.com
   SPAIN_ACCESS_TOKEN=shpat_xxxxx

   # Saudi (destination) store
   SAUDI_SHOP_URL=your-saudi-store.myshopify.com
   SAUDI_ACCESS_TOKEN=shpat_xxxxx

   # Magento site (for scraping EN/AR content)
   MAGENTO_SITE_URL=https://taraformula.com

   # Google Translate API key (for translation fallback)
   GOOGLE_TRANSLATE_API_KEY=AIzaSy...
   ```

3. **Shopify access tokens** need these scopes:
   - `read_products`, `write_products`
   - `read_content`, `write_content`
   - `read_themes`, `write_themes`
   - `read_locales`, `write_locales`
   - `read_translations`, `write_translations`
   - `read_files`, `write_files`
   - `read_inventory`, `write_inventory`
   - `read_locations`
   - `read_online_store_navigation`, `write_online_store_navigation`
   - `read_publications`, `write_publications`
   - `read_price_rules`, `write_price_rules`
   - `read_discounts`, `write_discounts`

---

## Fresh Start

To wipe all data and start from scratch:

```bash
rm -rf data/
```

All scripts are idempotent — they track progress in `data/*_progress.json` files and skip already-processed items. Deleting `data/` resets everything.

---

## Migration Steps

### Phase 1: Export Data from Spain Store

```bash
python export_spain.py
```

Exports products, collections, pages, blogs, articles, metaobjects, metaobject definitions, collects, redirects, price rules, and policies to `data/spain_export/`.

### Phase 2: Translate to English

```bash
python translate_to_english.py
```

Translates all Spanish content to English. Scrapes the Magento site for existing EN translations, falls back to Google Translate. Output: `data/english/`.

### Phase 3: Translate to Arabic

```bash
python translate_to_arabic.py
```

Translates content to Arabic. Scrapes the Magento site for AR translations, falls back to Google Translate. Output: `data/arabic/`.

### Phase 4: Import English Content

```bash
# Preview what would be created
python import_english.py --dry-run

# Run the import
python import_english.py
```

Creates all resources in the Saudi store:
- Metaobject definitions + entries (benefit, faq_entry, blog_author, ingredient)
- Products with variants, images, and metafields
- Custom collections
- Pages
- Blogs + articles
- Cross-references between metaobjects (Phase 6 remapping)

Saves ID mappings to `data/id_map.json`.

### Phase 5: Import Arabic Translations

```bash
# Preview
python import_arabic.py --dry-run

# Run
python import_arabic.py
```

Registers Arabic translations for all resources via the Shopify Translations API.

### Phase 6: Migrate Images

```bash
# Inspect what needs migration
python migrate_all_images.py --inspect

# Dry run
python migrate_all_images.py --dry-run

# Run all phases
python migrate_all_images.py

# Run specific phases only
python migrate_all_images.py --phase 4,5
```

Six sub-phases:
1. **Product images** — checks for missing images, re-uploads from source
2. **Collection images** — downloads, optimizes to WebP, uploads
3. **Homepage/theme images** — resolves `shopify://shop_images/` refs
4. **Metaobject files** — icon, avatar, image fields
5. **Article files** — listing_image, hero_image
6. **Verification** — generates `data/image_migration_report.json`

### Phase 7: Resolve Metaobject Differences

```bash
# Inspect diffs between Spain and Saudi stores
python resolve_metaobject_diffs.py --inspect

# Dry run
python resolve_metaobject_diffs.py --dry-run

# Fix diffs
python resolve_metaobject_diffs.py

# Fix only a specific type
python resolve_metaobject_diffs.py --type ingredient
```

Compares metaobject schemas and entries, fixes:
- Missing definitions and fields
- Missing entries
- Broken cross-references (ingredient→benefit, product→ingredient, article→author)

### Phase 8: Post-Migration Setup

```bash
# Preview all steps
python post_migration.py --dry-run

# Run all steps
python post_migration.py

# Run specific steps
python post_migration.py --step 2 --step 3
```

Steps:
1. Enable Arabic locale
2. Link products to collections
3. Build navigation menus
4. Set SEO meta tags
5. Create URL redirects (with Spanish→English handle remapping)
6. Set inventory quantities
7. Publish to sales channels
8. Migrate discount codes
9. Activate products (draft → active)
10. Create store policies
11. Update handles (Spanish → English)

---

## Fix Scripts (for incremental corrections)

| Script | Purpose |
|--------|---------|
| `fix_prices.py` | Update product prices from Magento SAR data |
| `fix_images.py` | Re-upload missing product images from Magento |
| `fix_metafields.py` | Fix corrupted rich_text metafields |
| `fix_metaobject_files.py` | Backfill file_reference fields on metaobjects |
| `fix_status.py` | Bulk activate/deactivate products |
| `fix_redirects.py` | Create redirects with handle remapping |

---

## Utility Scripts

| Script | Purpose |
|--------|---------|
| `compare_data.py` | Compare Spain export vs scraped EN/AR data |
| `setup_collections.py` | Create collections from Magento categories |
| `setup_menus.py` | Build navigation from Magento category tree |
| `setup_homepage.py` | Configure homepage sections |

---

## Data Directory Structure

```
data/
├── spain_export/          # Raw export from Spain store
│   ├── products.json
│   ├── collections.json
│   ├── pages.json
│   ├── blogs.json
│   ├── articles.json
│   ├── metaobjects.json
│   ├── metaobject_definitions.json
│   ├── collects.json
│   ├── redirects.json
│   ├── price_rules.json
│   └── policies.json
├── english/               # Translated English content
├── arabic/                # Translated Arabic content
├── id_map.json            # Source ID → Dest ID mappings
├── file_map.json          # Source file GID → Dest file GID
├── *_progress.json        # Progress tracking (resumable)
└── image_migration_report.json
```

---

## Running Tests

```bash
# All tests
python -m pytest

# Specific test file
python -m pytest tests/test_post_migration.py

# With coverage
python -m pytest --cov=. --cov-report=term-missing
```

---

## Manual Steps After Migration

These cannot be automated and must be done in the Shopify admin:

1. **Payment gateways** — Settings → Payments (Tap, Mada, Apple Pay for KSA)
2. **Saudi VAT 15%** — Settings → Taxes and duties
3. **Shipping zones/rates** — Settings → Shipping and delivery
4. **Domain and DNS** — Settings → Domains
5. **Theme installation** — Online Store → Themes
6. **Email notifications** — Settings → Notifications
7. **Third-party apps** — Klaviyo, reviews, etc.
8. **Shopify Flows** — Export `.flow` from Spain, import to Saudi
9. **End-to-end checkout test**
