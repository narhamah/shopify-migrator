# TARA Shopify Migration Guide

## Spain → Saudi Arabia Store Migration

Complete pipeline for migrating the TARA luxury scalp-care brand from the Spanish Shopify store to a new Saudi Arabian store with English (primary) and Arabic (secondary) language support.

---

## Architecture Overview

```
Spain Shopify Store (ES)     Magento Live Sites (EN/AR)
         │                            │
    export_spain.py              scrape_kuwait.py
         │                            │
         ▼                            ▼
  data/spain_export/          data/english/ (scraped)
         │                    data/arabic/  (scraped)
         │                            │
         └────────┬───────────────────┘
                  │
         translate_to_english.py   ← scrape-first: only translates GAPS
                  │
                  ▼
           data/english/ (complete)
                  │
         ┌────────┴────────┐
         │                 │
  import_english.py   translate_to_arabic.py  ← scrape-first: only translates GAPS
         │                 │
         ▼                 ▼
  Saudi Shopify (EN)  data/arabic/ (complete)
         │                 │
         │          import_arabic.py
         │                 │
         ▼                 ▼
  Saudi Shopify (EN + AR translations)
         │
  migrate_all_images.py → resolve_metaobject_diffs.py → post_migration.py
         │
         ▼
  Saudi Shopify (COMPLETE)
```

### Scrape-First Translation Strategy

The translation pipeline uses a **scrape-first** approach to minimize LLM API costs:

1. **Scrape** English and Arabic content from the live Magento sites (taraformula.com / taraformula.ae)
2. **Match** Spain products to scraped products by **SKU** (handles differ across languages)
3. **Identify gaps** — content that exists in the Spain export but not in the scraped data (e.g., Shopify-specific metafields like accordion sections, taglines)
4. **Translate only the gaps** using OpenAI's API with TOON batching (~40x fewer API calls)
5. **Merge** scraped data + translated gaps into complete output files

### TOON Batching Format

Translations use **TOON (Token-Oriented Object Notation)** to batch multiple fields into a single API call:

```
field_id_1|field value one
field_id_2|field value two with HTML <b>tags</b>
field_id_3|another value
```

Escaping: `\\` for backslash, `\p` for pipe, `\n` for newline within values. This reduces ~4,800 individual translation calls down to ~120 batched calls.

---

## Prerequisites

1. **Python 3.9+** with dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. **`.env` file** in the project root:
   ```env
   # Spain (source) store
   SPAIN_SHOP_URL=your-spain-store.myshopify.com
   SPAIN_ACCESS_TOKEN=shpat_xxxxx

   # Saudi (destination) store
   SAUDI_SHOP_URL=your-saudi-store.myshopify.com
   SAUDI_ACCESS_TOKEN=shpat_xxxxx

   # OpenAI API key (for gap translation)
   OPENAI_API_KEY=sk-xxxxx

   # Magento sites (for scraping)
   MAGENTO_SITE_URL=https://taraformula.com
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

   Use `get_token.py` to obtain tokens via OAuth if needed:
   ```bash
   python get_token.py --shop your-store.myshopify.com --client-id XXX --client-secret YYY
   ```

---

## Fresh Start

To wipe all data and start from scratch:

```bash
rm -rf data/
```

All scripts are idempotent — they track progress in `data/*_progress.json` files and skip already-processed items. Deleting `data/` resets everything.

To wipe the Saudi store itself:

```bash
python purge_saudi.py --dry-run   # Preview what would be deleted
python purge_saudi.py --yes       # Delete everything (DESTRUCTIVE)
python purge_saudi.py --only products,collections  # Delete specific types only
```

---

## Complete Pipeline

### Phase 0: Set Up Destination Store Schema

```bash
python setup_store.py --dry-run   # Preview
python setup_store.py             # Create schema
```

Creates on the Saudi store:
- **4 metaobject definitions** in dependency order:
  1. `benefit` — title, description, category, icon_label
  2. `faq_entry` — question, answer (rich_text)
  3. `blog_author` — name, bio, avatar (file_reference)
  4. `ingredient` — 12 fields including benefits (list→benefit reference)
- **19 product metafield definitions** — tagline, short_description, size_ml, 7 accordion heading/content pairs, ingredient/FAQ references
- **12 article metafield definitions** — featured, blog_summary, hero_caption, author reference, related articles/products

Safe to re-run; skips definitions that already exist. Resolves cross-references automatically (e.g., ingredient's `benefits` field points to the benefit definition GID).

### Phase 1: Export from Spain Store

```bash
python export_spain.py
```

Exports all content from the Spain Shopify store to `data/spain_export/`:

| File | Content |
|------|---------|
| `products.json` | Products with variants, options, images, and all metafields |
| `collections.json` | Custom collections with metafields |
| `pages.json` | CMS pages |
| `blogs.json` | Blog containers |
| `articles.json` | Blog posts with metafields |
| `metaobject_definitions.json` | Schema definitions for all metaobject types |
| `metaobjects.json` | All metaobject entries grouped by type |
| `collects.json` | Product↔collection membership links |
| `redirects.json` | URL redirects |
| `price_rules.json` | Discount/price rules |
| `policies.json` | Store policies (refund, privacy, terms, shipping) |

### Phase 1b: Scrape Live Magento Sites

```bash
python scrape_kuwait.py --explore   # Discover available content
python scrape_kuwait.py --scrape    # Scrape everything
python scrape_kuwait.py --scrape --only products     # Products only
python scrape_kuwait.py --scrape --only collections   # Collections only
```

Scrapes English and Arabic content from the live Magento PWA sites:
- **English**: `taraformula.com` (default store view)
- **Arabic**: `taraformula.ae` (Arabic store view)

Outputs pre-populated translation files to `data/english/` and `data/arabic/`:
- Products with titles, descriptions, variants, images
- Collections with titles and descriptions
- Articles and blog content (if available)
- Metaobject entries (ingredients, benefits, etc.)

This data serves as the **primary source** for translation. The subsequent translate steps only translate content that is NOT available from the scraped data.

### Phase 1c: Compare Gaps (Optional)

```bash
python compare_data.py
```

Analyzes the gap between Spain export and scraped data:
- Products matched by SKU
- Fields available vs. missing per product
- Summary of what still needs LLM translation

### Phase 2: Translate Spanish → English

```bash
python translate_to_english.py              # Full translation
python translate_to_english.py --dry        # Show what would be translated (no API calls)
python translate_to_english.py --model o3   # Use a different OpenAI model
```

**Scrape-first workflow:**

1. Loads Spain export (`data/spain_export/`) and scraped English data (`data/english/`)
2. Matches products by **SKU** (not handle — Spanish "champu-densificante" ≠ English "densifying-shampoo")
3. For matched products: uses scraped data as-is, identifies metafield gaps (Shopify-specific fields not in Magento)
4. For unmatched products: translates everything
5. For non-product resources (collections, pages, articles, metaobjects): translates all text fields
6. Sends gap fields to OpenAI in **TOON batches** (120 fields per batch)
7. Merges scraped data + translated gaps → complete `data/english/` output

**CLI options:**
| Flag | Default | Description |
|------|---------|-------------|
| `--dry` | false | Show what would be translated without API calls |
| `--model` | `gpt-5-mini` | OpenAI model for translation |
| `--batch-size` | 120 | Fields per TOON batch |
| `--tpm` | 30000 | Tokens-per-minute rate limit budget |

Resumable: saves progress to `data/en_translation_progress.json` after each batch.

### Phase 3: Import English Content into Saudi Store

```bash
python import_english.py --dry-run              # Preview
python import_english.py                         # Import (default: no price conversion)
python import_english.py --exchange-rate 4.13    # Convert EUR → SAR
```

Creates all resources in the Saudi Shopify store:

1. **Examines** existing metaobject definitions in destination
2. **Creates metaobject entries** in dependency order (benefit → faq_entry → blog_author → ingredient)
3. **Creates products** with text metafields, variants, options, price conversion
4. **Creates collections** with metafields
5. **Creates pages** with metafields
6. **Creates blogs + articles** with metafields
7. **Remaps reference fields** — resolves cross-references using new destination GIDs:
   - ingredient → benefit (list reference)
   - product → ingredients, FAQ entries (list references)
   - article → author, related articles/products

Saves ID mapping to `data/id_map.json`. Skips items already created (matched by handle).

### Phase 4: Translate English → Arabic

```bash
python translate_to_arabic.py              # Full translation
python translate_to_arabic.py --dry        # Dry run
```

Same scrape-first workflow as Phase 2, but:
- Source: `data/english/` (complete English content)
- Scraped reference: `data/arabic/` (from Magento Arabic site)
- Output: `data/arabic/` (complete Arabic content)
- Uses TARA Arabic Tone of Voice (Modern Standard Arabic for Gulf/Saudi audience)

Resumable: saves progress to `data/ar_translation_progress.json`.

### Phase 5: Import Arabic Translations

```bash
python import_arabic.py --dry-run   # Preview
python import_arabic.py             # Import
```

Uses Shopify's Translations API to register Arabic as a secondary locale on all resources created in Phase 3:
- Product titles, descriptions, metafields
- Collection titles, descriptions
- Page titles, content
- Article titles, content, metafields
- Metaobject text fields

**Prerequisite:** Arabic (ar) must be enabled in the Saudi store (Settings → Languages). Phase 8 Step 1 can do this automatically.

Requires `data/id_map.json` from Phase 3.

### Phase 5b: Fix SAR Prices

```bash
python fix_prices.py                    # Fetch SAR prices and update local data files
python fix_prices.py --update-shopify   # Also update already-imported Shopify products
python fix_prices.py --store sa-en --site https://taraformula.com  # Custom store view
```

**When to use:** Run this after Phase 3 (import English) to correct product prices. The import step auto-fetches SAR prices at import time, but if prices change on the Magento site or the initial fetch had issues, run `fix_prices.py` to:

1. Fetch current SAR prices from the Magento Saudi store view (`taraformula.com` with store code `sa-en`)
2. Update local `data/english/products.json` and `data/arabic/products.json` with correct prices
3. Optionally update already-imported Shopify products with `--update-shopify`

Saves fetched prices to `data/sar_prices.json` for reference. Matches products by SKU.

**Run it again** any time Magento prices change and you need to sync them to Shopify.

### Phase 6: Migrate Images & Assets

```bash
python migrate_all_images.py --inspect    # See what needs migration
python migrate_all_images.py --dry-run    # Preview all phases
python migrate_all_images.py              # Run all 6 sub-phases
python migrate_all_images.py --phase 4,5  # Run specific sub-phases only
```

Six sub-phases:

| Phase | What | Details |
|-------|------|---------|
| 1 | Product images | Checks for missing images, re-uploads from Magento or Spain Shopify |
| 2 | Collection images | Downloads source images, optimizes to WebP, uploads to Saudi |
| 3 | Homepage/theme images | Resolves `shopify://shop_images/` refs, uploads to Saudi files API |
| 4 | Metaobject file refs | Avatar, icon, image, science_images fields on metaobject entries |
| 5 | Article file refs | listing_image, hero_image metafields on articles |
| 6 | Verification | Generates `data/image_migration_report.json` with pass/fail summary |

**Image optimization presets:**
| Preset | Max Size | Quality | Use Case |
|--------|----------|---------|----------|
| hero | 2400x1200 | 82 | Banners, slideshows |
| product | 2048x2048 | 85 | Product zoom images |
| collection | 1920x1080 | 82 | Collection headers |
| icon | 400x400 | lossless | Icons, badges |
| thumbnail | 800x800 | 85 | Thumbnails, cards |
| logo | 800x400 | lossless | Logos |

### Phase 7: Resolve Metaobject Differences

```bash
python resolve_metaobject_diffs.py --inspect           # Show diffs
python resolve_metaobject_diffs.py --dry-run            # Preview fixes
python resolve_metaobject_diffs.py                      # Fix everything
python resolve_metaobject_diffs.py --type ingredient    # Fix one type only
```

Compares Spain and Saudi store metaobjects, fixes:
- Missing definitions and fields in schema
- Missing entries (not yet created)
- Broken cross-references (ingredient→benefit, product→ingredient, article→author)

### Phase 8: Post-Migration Setup

```bash
python post_migration.py --dry-run       # Preview all 11 steps
python post_migration.py                 # Run all steps
python post_migration.py --step 2        # Run one step
python post_migration.py --step 2 --step 3  # Run specific steps
```

| Step | What | Details |
|------|------|---------|
| 1 | Enable Arabic locale | Registers `ar` as secondary language |
| 2 | Link products to collections | Creates product↔collection membership from collects data |
| 3 | Build navigation menus | Main menu + footer from Magento category tree |
| 4 | Set SEO meta tags | title_tag + description_tag from product metafields |
| 5 | Create URL redirects | Spanish paths → English paths with handle remapping |
| 6 | Set inventory quantities | Copies inventory levels from Spain export |
| 7 | Publish to sales channels | Makes products visible on Online Store + POS |
| 8 | Migrate discount codes | Recreates price rules and discount codes |
| 9 | Activate products | Switches products from `draft` → `active` |
| 10 | Create store policies | Refund, privacy, terms of service, shipping |
| 11 | Update handles | Renames Spanish handles to English equivalents |

---

## Fix Scripts (Incremental Corrections)

Run these after the main pipeline to fix specific issues:

| Script | CLI Flags | Purpose |
|--------|-----------|---------|
| `fix_prices.py` | `--update-shopify`, `--store sa-en`, `--site URL` | Fetch SAR prices from Magento, update Shopify products |
| `fix_images.py` | `--discover`, `--dry-run`, `--local-only` | Replace Spanish product images with EN/AR images from Magento |
| `fix_metafields.py` | `--dry-run`, `--only-empty` | Backfill missing product metafields on already-imported products |
| `fix_metaobject_files.py` | `--dry-run`, `--source-dir DIR`, `--type TYPE` | Upload missing file_reference fields on metaobject entries |
| `fix_status.py` | `--dry-run`, `--skip-duplicates` | Publish unlisted products, detect and remove duplicates |
| `fix_redirects.py` | `--dry-run` | Remap Spanish handles → English handles in existing redirects |

---

## Utility Scripts

| Script | CLI Flags | Purpose |
|--------|-----------|---------|
| `compare_data.py` | (none) | Analyze gaps between Spain export and scraped EN/AR data |
| `setup_collections.py` | `--dry-run`, `--link-only` | Create collections from Magento category tree, link products by SKU |
| `setup_menus.py` | `--dry-run`, `--config FILE` | Build main/footer navigation from Magento categories |
| `setup_homepage.py` | `--inspect`, `--config FILE`, `--set`, `--image-url URL` | Configure homepage section images from Magento |
| `remap_redirects.py` | (none) | Build remapped redirects file (`data/remapped_redirects.json`) |
| `generate_data_dictionary.py` | (none) | Generate field-level data dictionary from Spain export |
| `get_flow_ids.py` | (none) | Print store GIDs needed for Shopify Flow configuration |
| `migrate_metaobjects.py` | `--list`, `--type TYPE`, `--all`, `--dry-run` | Direct store-to-store metaobject migration (bypasses export/import) |
| `migrate_homepage_images.py` | `--inspect`, `--dry-run`, `--metaobjects-only` | Standalone homepage image migration |
| `migrate_assets.py` | (none) | Standalone file_reference field migration |

---

## Data Architecture

### Metaobject Types

| Type | Fields | Dependencies | Purpose |
|------|--------|-------------|---------|
| `benefit` | title, description, category, icon_label | none | Product benefits (referenced by ingredients) |
| `faq_entry` | question, answer (rich_text) | none | Per-product FAQ accordion entries |
| `blog_author` | name, bio, avatar (file_ref) | none | Rich author profiles for blog articles |
| `ingredient` | name, inci_name, benefits (list→benefit), description (rich_text), source, origin, category, concern, image, icon, science_images, is_hero, sort_order, collection | benefit | Ingredient library |

**Dependency order:** benefit → faq_entry → blog_author → ingredient (ingredients reference benefits)

### Product Metafields (19 fields)

- `custom.tagline` — Product tagline
- `custom.short_description` — Short description
- `custom.size_ml` — Size/volume
- 7 accordion pairs: `custom.{key_benefits,clinical_results,how_to_use,whats_inside,free_of,awards,fragrance}_{heading,content}`
- `custom.ingredients` — List reference → ingredient metaobjects
- `custom.faqs` — List reference → faq_entry metaobjects
- `global.title_tag` — SEO title
- `global.description_tag` — SEO description

### Article Metafields (12 fields)

- `custom.featured` — Boolean
- `custom.blog_summary`, `custom.hero_caption`, `custom.short_title` — Text fields
- `custom.listing_image`, `custom.hero_image` — File references
- `custom.author` — Reference → blog_author metaobject
- `custom.related_articles`, `custom.related_products` — List references
- `custom.ingredients` — List reference → ingredient metaobjects

### Data Directory Structure

```
data/
├── spain_export/                  # Phase 1: Raw export from Spain store
│   ├── products.json
│   ├── collections.json
│   ├── pages.json
│   ├── blogs.json
│   ├── articles.json
│   ├── metaobject_definitions.json
│   ├── metaobjects.json
│   ├── collects.json
│   ├── redirects.json
│   ├── price_rules.json
│   ├── policies.json
│   └── shop.json
├── english/                       # Phase 1b+2: Complete English content
│   ├── products.json              # Scraped from Magento + translated gaps
│   ├── collections.json
│   ├── pages.json
│   ├── blogs.json
│   ├── articles.json
│   └── metaobjects.json
├── arabic/                        # Phase 1b+4: Complete Arabic content
│   ├── products.json
│   ├── collections.json
│   ├── pages.json
│   ├── blogs.json
│   ├── articles.json
│   └── metaobjects.json
├── id_map.json                    # Phase 3: Source ID → Destination ID mappings
├── file_map.json                  # Phase 6: Source file GID → Dest file GID
├── sar_prices.json                # fix_prices.py output
├── remapped_redirects.json        # remap_redirects.py output
├── data_dictionary.json           # generate_data_dictionary.py output
├── en_translation_progress.json   # Phase 2 progress (resumable)
├── ar_translation_progress.json   # Phase 4 progress (resumable)
├── *_import_progress.json         # Import progress tracking
└── image_migration_report.json    # Phase 6 verification report
```

### Key Mapping Files

**`id_map.json`** — Maps Spain Shopify GIDs to Saudi Shopify GIDs:
```json
{
  "products": {"gid://shopify/Product/123": "gid://shopify/Product/456"},
  "collections": {"gid://...": "gid://..."},
  "metaobjects": {"gid://...": "gid://..."},
  "articles": {"gid://...": "gid://..."},
  "pages": {"gid://...": "gid://..."},
  "blogs": {"gid://...": "gid://..."}
}
```

**`file_map.json`** — Maps source file GIDs to destination file GIDs (for image/file migration):
```json
{
  "gid://shopify/MediaImage/123": "gid://shopify/MediaImage/456"
}
```

---

## Translation Details

### What Gets Translated

| Resource | Translated Fields |
|----------|-------------------|
| Products | title, body_html, product_type, vendor, handle, tags, variant titles/options, image alt text, all text metafields |
| Collections | title, body_html, handle, image alt text, text metafields |
| Pages | title, body_html, handle, text metafields |
| Articles | title, body_html, summary_html, handle, author, tags, image alt text, text metafields |
| Blogs | title, handle, tags |
| Metaobjects | handle + all text/rich_text fields per type |

### What Is NOT Translated

- Brand name "TARA"
- Product-specific names (e.g., "Kansa Wand", "Gua Sha")
- INCI ingredient names
- HTML tags and attributes
- Shopify Liquid tags (`{{ }}`, `{% %}`)
- URLs
- Non-text metafield types (references, numbers, booleans, dates, JSON)

### SKU-Based Product Matching

Products are matched between the Spain export and scraped Magento data using **SKU**, not handle. This is critical because handles differ across languages:

| Spain Export (ES) | Magento Scraped (EN) | Match By |
|-------------------|---------------------|----------|
| `champu-densificante` | `densifying-shampoo` | SKU: `TARA-001` |
| `aceite-cuero-cabelludo` | `scalp-oil` | SKU: `TARA-002` |

Fallback: if no SKU match, tries handle match (works for products with the same handle across stores).

### Tone of Voice

All translations follow the TARA brand voice guidelines loaded from:
- `tara_tov_en.txt` — English tone of voice
- `tara_tov_ar.txt` — Arabic tone of voice (Modern Standard Arabic for Gulf audience)

---

## Running Tests

```bash
python -m pytest                                    # All tests
python -m pytest tests/test_post_migration.py       # Specific test file
python -m pytest --cov=. --cov-report=term-missing  # With coverage
python -m pytest -x                                 # Stop on first failure
```

---

## Typical Full Migration Run

### Quick Start (using build_site.py)

```bash
# 0. Install and configure
pip install -r requirements.txt
cp .env.example .env
# Edit .env with credentials

# 1. Set up destination schema
python setup_store.py

# 2. Export from Spain
python export_spain.py

# 3. Scrape live Magento sites
python scrape_kuwait.py --scrape

# 4. Build everything (translate, import, images, configure)
python build_site.py                  # Full build: EN + AR
python build_site.py --lang en        # English only
python build_site.py --lang ar        # Arabic only (after EN is done)
python build_site.py --dry-run        # Preview all phases
```

`build_site.py` orchestrates 8 phases in the correct order:

| Phase | Name | Lang | What |
|-------|------|------|------|
| 1 | Translate ES → EN | en | Scrape-first TOON translation |
| 2 | Fix SAR Prices | en | Magento prices → local data + Shopify |
| 3 | Import English | en | Create resources in Saudi store |
| 4 | Translate EN → AR | ar | Scrape-first TOON translation |
| 5 | Import Arabic | ar | Register translations via API |
| 6 | Migrate All Images | en | Product, collection, homepage, metaobject, article |
| 7 | Resolve MO Diffs | en | Fix schema mismatches and references |
| 8 | Post-Migration Setup | en | Locale, menus, SEO, redirects, publish |

```bash
# Resume from a specific phase
python build_site.py --from 6         # Skip translate+import, just images onwards

# Run specific phases
python build_site.py --phase 2,6      # Just fix prices and migrate images

# Skip specific phases
python build_site.py --skip 2,7       # Skip price fix and metaobject diffs
```

### Step-by-step (manual)

```bash
# 0. Install and configure
pip install -r requirements.txt
cp .env.example .env

# 1. Set up destination schema
python setup_store.py

# 2. Export from Spain
python export_spain.py

# 3. Scrape live Magento sites
python scrape_kuwait.py --scrape

# 4. (Optional) Check what needs translation
python compare_data.py

# 5. Translate gaps to English
python translate_to_english.py

# 6. Fix SAR prices
python fix_prices.py --update-shopify

# 7. Import English into Saudi store
python import_english.py

# 8. Translate gaps to Arabic
python translate_to_arabic.py

# 9. Import Arabic translations
python import_arabic.py

# 10. Migrate all images
python migrate_all_images.py

# 11. Fix any metaobject schema diffs
python resolve_metaobject_diffs.py

# 12. Post-migration setup (all 11 steps)
python post_migration.py
```

---

## Manual Steps After Migration

These cannot be automated and must be done in the Shopify admin:

1. **Payment gateways** — Settings → Payments (Tap, Mada, Apple Pay for KSA)
2. **Saudi VAT 15%** — Settings → Taxes and duties
3. **Shipping zones/rates** — Settings → Shipping and delivery
4. **Domain and DNS** — Settings → Domains
5. **Theme installation** — Online Store → Themes (install and customize)
6. **Email notifications** — Settings → Notifications (translate templates)
7. **Third-party apps** — Klaviyo, reviews, loyalty programs
8. **Shopify Flows** — Export `.flow` files from Spain, import to Saudi (use `get_flow_ids.py` for destination GIDs)
9. **End-to-end checkout test** — Place a test order through complete flow
