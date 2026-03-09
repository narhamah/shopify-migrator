# TARA Shopify Migration Guide

## Spain → Saudi Arabia Store Migration

Complete pipeline for migrating the TARA luxury scalp-care brand from the Spanish Shopify store to a new Saudi Arabian store with English (primary) and Arabic (secondary) language support.

---

## Quick Start: Clean Full Build

```bash
# 1. Wipe Saudi store (data only, keeps definitions)
python purge_saudi.py --yes

# 2. Build the full site from scratch
python build_site.py
```

`build_site.py` runs all 8 phases in order — translate, import, images, configure — and produces a fully functioning Saudi website.

**Prerequisites** (run once before the first build):

```bash
pip install -r requirements.txt
cp .env.example .env              # Edit with your credentials
python setup_store.py             # Create metaobject/metafield definitions
python export_spain.py            # Export from Spain Shopify
python scrape_kuwait.py --scrape  # Scrape EN/AR from Magento
```

---

## Architecture

```
Spain Shopify (ES)          Magento Live Sites (EN/AR)
       │                              │
  export_spain.py              scrape_kuwait.py
       │                              │
       ▼                              ▼
data/spain_export/          data/english/ (scraped)
       │                    data/arabic/ (scraped)
       │                              │
       └──────────┬───────────────────┘
                  │
            build_site.py   ← orchestrates ALL remaining steps
                  │
         ┌────────┴────────────────────────────────┐
         │  Phase 1: translate_gaps ES → EN         │
         │  Phase 2: fix_prices (SAR from Magento)  │
         │  Phase 3: import_english                 │
         │  Phase 4: translate_gaps EN → AR         │
         │  Phase 5: import_arabic                  │
         │  Phase 6: migrate_all_images             │
         │  Phase 7: resolve_metaobject_diffs       │
         │  Phase 8: post_migration (11 sub-steps)  │
         └─────────────────────────────────────────┘
                  │
                  ▼
         Saudi Shopify (COMPLETE)
```

### build_site.py Phases

| Phase | Name | Script Called | What |
|-------|------|-------------|------|
| 1 | Translate ES → EN | `translate_gaps.py` | Scrape-first TOON translation of Spain export → English |
| 2 | Fix SAR Prices | `fix_prices.py` | Fetch Magento SAR prices → update local data + Shopify |
| 3 | Import English | `import_english.py` | Create products, collections, pages, metaobjects in Saudi store |
| 4 | Translate EN → AR | `translate_gaps.py` | Scrape-first TOON translation of English → Arabic |
| 5 | Import Arabic | `import_arabic.py` | Register Arabic translations via Shopify Translations API |
| 6 | Migrate All Images | `migrate_all_images.py` | Product, collection, homepage, metaobject, article images |
| 7 | Resolve MO Diffs | `resolve_metaobject_diffs.py` | Fix schema mismatches and broken cross-references |
| 8 | Post-Migration Setup | `post_migration.py` | Locale, menus, SEO, redirects, inventory, publish, activate |

**Important:** Translation always runs BEFORE import. Phase 1 (translate) must complete before Phase 3 (import English). Phase 4 (translate Arabic) must complete before Phase 5 (import Arabic).

```bash
# Build options
python build_site.py                       # Full build (EN + AR)
python build_site.py --lang en             # English only (phases 1-3, 6-8)
python build_site.py --lang ar             # Arabic only (phases 4-5)
python build_site.py --dry-run             # Preview everything
python build_site.py --from 6              # Resume from phase 6
python build_site.py --phase 2,6           # Run specific phases only
python build_site.py --skip 2,7            # Skip specific phases
```

---

## Full Process: Step by Step

### Step 0: Install & Configure (one-time)

```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials
```

Required `.env` variables:
```env
SPAIN_SHOP_URL=your-spain-store.myshopify.com
SPAIN_ACCESS_TOKEN=shpat_xxxxx
SAUDI_SHOP_URL=your-saudi-store.myshopify.com
SAUDI_ACCESS_TOKEN=shpat_xxxxx
OPENAI_API_KEY=sk-xxxxx
```

Use `get_token.py` to obtain Shopify access tokens via OAuth if needed:
```bash
python get_token.py --shop your-store.myshopify.com --client-id XXX --client-secret YYY
```

Shopify access tokens need these scopes:
- `read_products`, `write_products`, `read_content`, `write_content`
- `read_themes`, `write_themes`, `read_locales`, `write_locales`
- `read_translations`, `write_translations`, `read_files`, `write_files`
- `read_inventory`, `write_inventory`, `read_locations`
- `read_online_store_navigation`, `write_online_store_navigation`
- `read_publications`, `write_publications`
- `read_price_rules`, `write_price_rules`, `read_discounts`, `write_discounts`

### Step 1: Set Up Destination Store Schema (one-time)

```bash
python setup_store.py --dry-run   # Preview
python setup_store.py             # Create schema
```

Creates on the Saudi store:
- **4 metaobject definitions** in dependency order: benefit → faq_entry → blog_author → ingredient
- **19 product metafield definitions** — tagline, short_description, size_ml, 7 accordion heading/content pairs, ingredient/FAQ references
- **12 article metafield definitions** — featured, blog_summary, hero_caption, author reference, related articles/products

Safe to re-run — skips definitions that already exist.

### Step 2: Export from Spain Store

```bash
python export_spain.py
```

Exports everything from the Spain Shopify store → `data/spain_export/`:

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

### Step 3: Scrape Live Magento Sites

```bash
python scrape_kuwait.py --explore   # Discover available content
python scrape_kuwait.py --scrape    # Scrape everything
python scrape_kuwait.py --scrape --only products     # Products only
python scrape_kuwait.py --scrape --only collections   # Collections only
```

Scrapes English and Arabic content from the live Magento PWA sites:
- **English**: `taraformula.com` (default store view)
- **Arabic**: `taraformula.ae` (Arabic store view)

Outputs to `data/english/` and `data/arabic/`. This data serves as the **primary source** for translation — the next step only translates content NOT available from the scraped data.

### Step 4: Build the Saudi Website

```bash
python build_site.py
```

This single command runs all 8 phases in order:

1. **Translate ES → EN** — merges scraped English data with TOON-translated gaps
2. **Fix SAR Prices** — fetches correct prices from Magento Saudi store view
3. **Import English** — creates all resources in the Saudi Shopify store
4. **Translate EN → AR** — merges scraped Arabic data with TOON-translated gaps
5. **Import Arabic** — registers Arabic as secondary locale on all resources
6. **Migrate All Images** — uploads product, collection, homepage, metaobject, article images
7. **Resolve MO Diffs** — fixes any metaobject schema mismatches or broken references
8. **Post-Migration Setup** — locale, menus, SEO, redirects, inventory, publish, activate

If interrupted, resume with:

```bash
python build_site.py --from <phase_number>
```

---

## Starting Over / Re-importing

### Purge Saudi store data (keeps definitions)

```bash
python purge_saudi.py --dry-run   # Preview what would be deleted
python purge_saudi.py --yes       # Delete all data (keeps definitions)
```

Deletes: menus, redirects, price_rules, metaobjects, articles, blogs, pages, collections, products, files, local tracking files.

Then re-run `python build_site.py` — no need to re-run `setup_store.py`.

### Full reset (wipe everything including definitions)

```bash
python purge_saudi.py --definitions --yes   # Delete data + metaobject definitions
python setup_store.py                       # Re-create definitions
python build_site.py                        # Re-import everything
```

### Wipe local tracking data

```bash
python purge_saudi.py --only local_data   # Delete progress/tracking files only
```

### Purge specific resources only

```bash
python purge_saudi.py --only products,collections    # Just products and collections
python purge_saudi.py --only metaobjects             # Just metaobject entries
python purge_saudi.py --only files                   # Just uploaded files
python purge_saudi.py --only menus,redirects          # Just menus and redirects
```

Valid `--only` values: `menus`, `redirects`, `price_rules`, `metaobjects`, `articles`, `blogs`, `pages`, `collections`, `products`, `files`, `local_data`, `metaobject_definitions`

---

## Running Individual Steps

Each phase of `build_site.py` can also be run independently. This is useful for debugging or re-running a specific step.

### Translation

```bash
# Via translate_gaps.py (core engine, called by build_site.py)
python translate_gaps.py --lang en              # Spanish → English
python translate_gaps.py --lang en --dry        # Preview (no API calls)
python translate_gaps.py --lang ar              # English → Arabic
python translate_gaps.py --lang ar --dry        # Preview

# Via CLI wrappers (same functionality, extra flags)
python translate_to_english.py                  # Full ES → EN translation
python translate_to_english.py --dry            # Preview
python translate_to_english.py --model o3       # Use a different OpenAI model
python translate_to_arabic.py                   # Full EN → AR translation
python translate_to_arabic.py --dry             # Preview
```

Translation flags:
| Flag | Default | Description |
|------|---------|-------------|
| `--dry` | false | Show what would be translated without API calls |
| `--model` | `gpt-5-mini` | OpenAI model for translation |
| `--batch-size` | 120 | Fields per TOON batch |
| `--tpm` | 30000 | Tokens-per-minute rate limit budget |

Progress is saved to `data/en_translation_progress.json` / `data/ar_translation_progress.json` after each batch (resumable).

### Import

```bash
# English import
python import_english.py --dry-run              # Preview
python import_english.py                        # Import (default: no price conversion)
python import_english.py --exchange-rate 4.13   # Convert EUR → SAR

# Arabic import
python import_arabic.py --dry-run   # Preview
python import_arabic.py             # Import
```

English import creates resources in dependency order: metaobject entries → products → collections → pages → blogs/articles → remaps cross-references. Saves ID mapping to `data/id_map.json`. Skips items already created (matched by handle).

Arabic import uses Shopify's Translations API to register Arabic on all resources. **Prerequisite:** Arabic (ar) must be enabled in the Saudi store (Settings → Languages). Post-migration Step 1 does this automatically.

### Price Fix

```bash
python fix_prices.py                    # Fetch SAR prices, update local data files
python fix_prices.py --update-shopify   # Also update already-imported Shopify products
python fix_prices.py --store sa-en --site https://taraformula.com  # Custom store view
```

### Image Migration

```bash
python migrate_all_images.py --inspect    # See what needs migration
python migrate_all_images.py --dry-run    # Preview all sub-phases
python migrate_all_images.py              # Run all 6 sub-phases
python migrate_all_images.py --phase 4,5  # Run specific sub-phases only
```

### Metaobject Diffs

```bash
python resolve_metaobject_diffs.py --inspect           # Show diffs
python resolve_metaobject_diffs.py --dry-run            # Preview fixes
python resolve_metaobject_diffs.py                      # Fix everything
python resolve_metaobject_diffs.py --type ingredient    # Fix one type only
```

### Post-Migration

```bash
python post_migration.py --dry-run       # Preview all 11 steps
python post_migration.py                 # Run all steps
python post_migration.py --step 2        # Run one step
python post_migration.py --step 2 --step 3  # Run specific steps
```

---

## Scrape-First Translation Strategy

The translation pipeline minimizes LLM API costs:

1. **Scrape** English/Arabic content from Magento (taraformula.com / taraformula.ae)
2. **Match** Spain products to scraped products by **SKU** (handles differ across languages)
3. **Identify gaps** — content that exists in Spain but not in Magento (e.g., Shopify accordion metafields)
4. **Translate only the gaps** using OpenAI with TOON batching (~40x fewer API calls)
5. **Merge** scraped data + translated gaps → complete output files
6. **Deduplicate** metaobject entries by handle after translation

### TOON Batching

Translations use **TOON (Token-Oriented Object Notation)** to batch ~120 fields per API call:

```
field_id_1|field value one
field_id_2|field value two with HTML <b>tags</b>
field_id_3|another value
```

Escaping: `\\` for backslash, `\p` for pipe, `\n` for newline within values. Reduces ~4,800 individual translation calls → ~120 batched calls.

### SKU-Based Product Matching

Products are matched between the Spain export and scraped Magento data using **SKU**, not handle:

| Spain Export (ES) | Magento Scraped (EN) | Match By |
|-------------------|---------------------|----------|
| `champu-densificante` | `densifying-shampoo` | SKU: `TARA-001` |
| `aceite-cuero-cabelludo` | `scalp-oil` | SKU: `TARA-002` |

Fallback: if no SKU match, tries handle match.

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

### Tone of Voice

All translations follow the TARA brand voice guidelines loaded from:
- `tara_tov_en.txt` — English tone of voice
- `tara_tov_ar.txt` — Arabic tone of voice (Modern Standard Arabic for Gulf audience)

---

## Data Architecture

### Directory Structure

```
data/
├── spain_export/                  # Step 2: Raw export from Spain store
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
├── english/                       # Step 3 + Phase 1: Complete English content
│   ├── products.json              # Scraped from Magento + translated gaps
│   ├── collections.json
│   ├── pages.json
│   ├── blogs.json
│   ├── articles.json
│   └── metaobjects.json
├── arabic/                        # Step 3 + Phase 4: Complete Arabic content
│   └── (same structure as english/)
├── id_map.json                    # Phase 3: Source ID → Destination ID mappings
├── file_map.json                  # Phase 6: Source file GID → Dest file GID
├── sar_prices.json                # Phase 2: SAR prices from Magento
├── remapped_redirects.json        # remap_redirects.py output
├── data_dictionary.json           # generate_data_dictionary.py output
├── en_translation_progress.json   # Phase 1 progress (resumable)
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

### Metaobject Types

| Type | Key Fields | Dependencies | Purpose |
|------|-----------|-------------|---------|
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

---

## Image Migration Details

`migrate_all_images.py` runs 6 sub-phases (also run as build_site.py Phase 6):

| Sub-phase | What |
|-----------|------|
| 1 | Product images — check for missing, re-upload from Magento or Spain Shopify |
| 2 | Collection images — download, optimize to WebP, upload |
| 3 | Homepage/theme images — resolve `shopify://shop_images/` refs, upload to files API |
| 4 | Metaobject file refs — avatar, icon, image, science_images fields |
| 5 | Article file refs — listing_image, hero_image metafields |
| 6 | Verification — generates `data/image_migration_report.json` with pass/fail |

Image optimization presets (all converted to WebP):

| Preset | Max Size | Quality | Use Case |
|--------|----------|---------|----------|
| hero | 2400×1200 | 82 | Banners, slideshows |
| product | 2048×2048 | 85 | Product zoom images |
| collection | 1920×1080 | 82 | Collection headers |
| icon | 400×400 | lossless | Icons, badges |
| thumbnail | 800×800 | 85 | Thumbnails, cards |
| logo | 800×400 | lossless | Logos |

---

## Post-Migration Setup Details

`post_migration.py` runs 11 sub-steps (also run as build_site.py Phase 8):

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

## Complete Script Reference

### Core Pipeline Scripts

| Script | Purpose | When to Run |
|--------|---------|-------------|
| `setup_store.py` | Create metaobject/metafield definitions on Saudi store | Once before first build |
| `export_spain.py` | Export all data from Spain Shopify store → `data/spain_export/` | Once (or when Spain data changes) |
| `scrape_kuwait.py` | Scrape EN/AR content from Magento → `data/english/`, `data/arabic/` | Once (or when Magento data changes) |
| `build_site.py` | **Master orchestrator** — runs all 8 phases in order | Main entry point for building |

### Translation Scripts

| Script | Purpose |
|--------|---------|
| `translate_gaps.py` | Core translation engine — TOON-batched, scrape-first gap translation |
| `translate_to_english.py` | CLI wrapper for ES → EN translation (calls translate_gaps.py) |
| `translate_to_arabic.py` | CLI wrapper for EN → AR translation (calls translate_gaps.py) |
| `translator.py` | LLM translation engine with TARA tone-of-voice system prompts |

### Import Scripts

| Script | Purpose |
|--------|---------|
| `import_english.py` | Create all resources in Saudi store (metaobjects → products → collections → pages → articles) |
| `import_arabic.py` | Register Arabic translations via Shopify Translations API + update metaobject entries |

### Fix Scripts (incremental corrections)

| Script | CLI Flags | Purpose |
|--------|-----------|---------|
| `fix_prices.py` | `--update-shopify`, `--store`, `--site` | Fetch SAR prices from Magento, update local data + Shopify |
| `fix_images.py` | `--discover`, `--dry-run`, `--local-only` | Replace Spanish product images with EN/AR from Magento |
| `fix_metafields.py` | `--dry-run`, `--only-empty` | Backfill missing product metafields on already-imported products |
| `fix_status.py` | `--dry-run`, `--skip-duplicates` | Publish unlisted products, detect and remove duplicates |
| `fix_redirects.py` | `--dry-run` | Remap Spanish handles → English handles in existing redirects |

### Image & Asset Scripts

| Script | Purpose |
|--------|---------|
| `migrate_all_images.py` | Unified 6-phase image migration (products → collections → homepage → metaobjects → articles → verify) |
| `optimize_images.py` | Shared WebP image optimization library with Shopify-specific presets |

### Metaobject & Schema Scripts

| Script | Purpose |
|--------|---------|
| `resolve_metaobject_diffs.py` | Compare Spain/Saudi metaobject schemas and entries; fix mismatches |
| `migrate_metaobjects.py` | Direct store-to-store metaobject migration (bypasses export/import) |

### Post-Migration & Setup Scripts

| Script | Purpose |
|--------|---------|
| `post_migration.py` | 11-step post-migration orchestrator (locale → menus → SEO → publish → activate) |
| `setup_collections.py` | Create collections from Magento category tree, link products by SKU |
| `setup_menus.py` | Build main/footer navigation from Magento categories |
| `setup_homepage.py` | Configure homepage section images from Magento |

### Utility Scripts

| Script | Purpose |
|--------|---------|
| `purge_saudi.py` | Delete Saudi store data (`--yes`) or everything (`--definitions --yes`) |
| `compare_data.py` | Analyze gaps between Spain export and scraped EN/AR data |
| `remap_redirects.py` | Build remapped redirects file (`data/remapped_redirects.json`) |
| `generate_data_dictionary.py` | Generate field-level data dictionary from Spain export |
| `get_flow_ids.py` | Print store GIDs needed for Shopify Flow configuration |
| `get_token.py` | Get Shopify access tokens via OAuth |

### Core Libraries (not run directly)

| Script | Purpose |
|--------|---------|
| `shopify_client.py` | Shopify API client — GraphQL + REST with rate limiting, pagination, batch ops |
| `translator.py` | LLM translation engine with TARA tone-of-voice prompts |
| `utils.py` | Shared utilities — JSON I/O, directory paths, rich-text sanitization, handle conversion |

### Deprecated Scripts (superseded by migrate_all_images.py)

| Script | Replacement |
|--------|-------------|
| `migrate_assets.py` | `migrate_all_images.py --phase 4,5` |
| `migrate_homepage_images.py` | `migrate_all_images.py --phase 3,4` |
| `fix_metaobject_files.py` | `migrate_all_images.py --phase 4` |

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

---

## Running Tests

```bash
python -m pytest                                    # All tests
python -m pytest tests/test_post_migration.py       # Specific test file
python -m pytest --cov=. --cov-report=term-missing  # With coverage
python -m pytest -x                                 # Stop on first failure
```
