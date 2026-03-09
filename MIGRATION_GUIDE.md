# TARA Shopify Migration Guide

## Spain → Saudi Arabia Store Migration

Complete pipeline for migrating the TARA luxury scalp-care brand from the Spanish Shopify store to a new Saudi Arabian store with English (primary) and Arabic (secondary) language support.

---

## Quick Start: Clean Full Build

```powershell
# 1. Wipe Saudi store (data only, keeps definitions)
python purge_saudi.py --yes

# 2. Regenerate clean English data (fixes FAQ duplicates)
python translate_gaps.py --lang en

# 3. Build the full site
python build_site.py
```

That's it. `build_site.py` runs 8 phases in order and produces a fully functioning Saudi website.

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
         translate_gaps.py --lang en   ← scrape-first: only translates GAPS
                  │
                  ▼
           data/english/ (complete)
                  │
                  ▼
            build_site.py              ← runs ALL remaining steps
                  │
                  ▼
         Saudi Shopify (COMPLETE)
```

### What build_site.py Does

| Phase | Name | Lang | What |
|-------|------|------|------|
| 1 | Translate ES → EN | en | Scrape-first TOON translation |
| 2 | Fix SAR Prices | en | Magento prices → local data + Shopify |
| 3 | Import English | en | Create products, collections, pages, metaobjects |
| 4 | Translate EN → AR | ar | Scrape-first TOON translation |
| 5 | Import Arabic | ar | Register translations via Shopify Translations API |
| 6 | Migrate All Images | en | Product, collection, homepage, metaobject, article images |
| 7 | Resolve MO Diffs | en | Fix schema mismatches and broken references |
| 8 | Post-Migration Setup | en | Locale, menus, SEO, redirects, inventory, publish, activate |

```powershell
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

```powershell
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

Shopify access tokens need these scopes:
- `read_products`, `write_products`, `read_content`, `write_content`
- `read_themes`, `write_themes`, `read_locales`, `write_locales`
- `read_translations`, `write_translations`, `read_files`, `write_files`
- `read_inventory`, `write_inventory`, `read_locations`
- `read_online_store_navigation`, `write_online_store_navigation`
- `read_publications`, `write_publications`
- `read_price_rules`, `write_price_rules`, `read_discounts`, `write_discounts`

### Step 1: Set Up Destination Store Schema (one-time)

```powershell
python setup_store.py --dry-run   # Preview
python setup_store.py             # Create schema
```

Creates metaobject definitions (benefit, faq_entry, blog_author, ingredient) and product/article metafield definitions on the Saudi store. Safe to re-run — skips existing definitions.

### Step 2: Export from Spain Store

```powershell
python export_spain.py
```

Exports everything from the Spain Shopify store → `data/spain_export/`.

### Step 3: Scrape Live Magento Sites

```powershell
python scrape_kuwait.py --scrape
```

Scrapes English and Arabic content from Magento live sites → `data/english/` and `data/arabic/`. This is the primary source for translations — the next step only translates what isn't available from scraped data.

### Step 4: Build the Saudi Website

```powershell
python build_site.py
```

This single command runs all 8 phases. If interrupted, resume with:

```powershell
python build_site.py --from <phase_number>
```

---

## Starting Over / Re-importing

### Purge Saudi store data (keeps definitions)

```powershell
python purge_saudi.py --dry-run   # Preview what would be deleted
python purge_saudi.py --yes       # Delete all data (keeps definitions)
```

Then re-run `python build_site.py` — no need to re-run `setup_store.py`.

### Full reset (wipe everything including definitions)

```powershell
python purge_saudi.py --definitions --yes   # Delete data + definitions
python setup_store.py                       # Re-create definitions
python build_site.py                        # Re-import everything
```

### Wipe local data too

```powershell
# Delete all local tracking/progress files (keeps spain_export + english/arabic data)
python purge_saudi.py --only local_data
```

### Purge specific resources only

```powershell
python purge_saudi.py --only products,collections    # Just products and collections
python purge_saudi.py --only metaobjects             # Just metaobject entries
python purge_saudi.py --only files                   # Just uploaded files
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

Reduces ~4,800 individual translation calls → ~120 batched calls.

### Running Translation Independently

```powershell
python translate_gaps.py --lang en              # Spanish → English
python translate_gaps.py --lang en --dry        # Preview (no API calls)
python translate_gaps.py --lang ar              # English → Arabic
python translate_gaps.py --lang ar --dry        # Preview
```

---

## Data Architecture

### Directory Structure

```
data/
├── spain_export/              # Raw export from Spain store
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
│   └── policies.json
├── english/                   # Complete English content (scraped + translated)
│   ├── products.json
│   ├── collections.json
│   ├── pages.json
│   ├── blogs.json
│   ├── articles.json
│   └── metaobjects.json
├── arabic/                    # Complete Arabic content (scraped + translated)
│   └── (same structure)
├── id_map.json                # Source ID → Destination ID mappings
├── file_map.json              # Source file GID → Dest file GID
└── sar_prices.json            # SAR prices from Magento
```

### Metaobject Types

| Type | Key Fields | Dependencies |
|------|-----------|-------------|
| `benefit` | title, description, category, icon_label | none |
| `faq_entry` | question, answer (rich_text) | none |
| `blog_author` | name, bio, avatar (file_ref) | none |
| `ingredient` | name, inci_name, benefits (list→benefit), description, image, icon | benefit |

**Import order:** benefit → faq_entry → blog_author → ingredient

### Product Metafields (19 fields)

- `custom.tagline`, `custom.short_description`, `custom.size_ml`
- 7 accordion pairs: `custom.{key_benefits,clinical_results,how_to_use,whats_inside,free_of,awards,fragrance}_{heading,content}`
- `custom.ingredients` — list reference → ingredient metaobjects
- `custom.faqs` — list reference → faq_entry metaobjects
- `global.title_tag`, `global.description_tag` — SEO

---

## Image Migration Details

`migrate_all_images.py` runs 6 sub-phases (also run as build_site.py phase 6):

| Sub-phase | What |
|-----------|------|
| 1 | Product images — re-upload missing from Magento/Spain |
| 2 | Collection images — download, optimize to WebP, upload |
| 3 | Homepage/theme images — resolve `shopify://shop_images/` refs |
| 4 | Metaobject file refs — avatar, icon, image, science_images |
| 5 | Article file refs — listing_image, hero_image |
| 6 | Verification report → `data/image_migration_report.json` |

All images are optimized to WebP with presets:

| Preset | Max Size | Quality |
|--------|----------|---------|
| hero | 2400×1200 | 82 |
| product | 2048×2048 | 85 |
| collection | 1920×1080 | 82 |
| icon | 400×400 | lossless |
| thumbnail | 800×800 | 85 |

---

## Post-Migration Setup Details

`post_migration.py` runs 11 sub-steps (also run as build_site.py phase 8):

| Step | What |
|------|------|
| 1 | Enable Arabic locale |
| 2 | Link products to collections |
| 3 | Build navigation menus |
| 4 | Set SEO meta tags |
| 5 | Create URL redirects |
| 6 | Set inventory quantities |
| 7 | Publish to sales channels |
| 8 | Migrate discount codes |
| 9 | Activate products (draft → active) |
| 10 | Create store policies |
| 11 | Update handles (Spanish → English) |

---

## Script Reference

### Core Pipeline

| Script | Purpose |
|--------|---------|
| `setup_store.py` | Create metaobject/metafield definitions (one-time) |
| `export_spain.py` | Export all data from Spain store |
| `scrape_kuwait.py` | Scrape EN/AR content from Magento |
| `translate_gaps.py` | Translate gaps (scrape-first, TOON batched) |
| `build_site.py` | **Master orchestrator** — runs everything after export+scrape |

### Individual Steps (used by build_site.py)

| Script | Purpose |
|--------|---------|
| `import_english.py` | Create resources in Saudi store |
| `import_arabic.py` | Register Arabic translations |
| `fix_prices.py` | Fetch SAR prices from Magento |
| `migrate_all_images.py` | Upload all images (6 sub-phases) |
| `resolve_metaobject_diffs.py` | Fix metaobject schema mismatches |
| `post_migration.py` | Configure site (11 sub-steps) |

### Fix Scripts

| Script | Purpose |
|--------|---------|
| `fix_images.py` | Replace Spanish product images with EN/AR from Magento |
| `fix_metafields.py` | Backfill missing product metafields |
| `fix_status.py` | Publish unlisted products, detect duplicates |
| `fix_redirects.py` | Remap Spanish handles in redirects |

### Utilities

| Script | Purpose |
|--------|---------|
| `purge_saudi.py` | Delete Saudi store data (default) or everything (--definitions) |
| `compare_data.py` | Analyze gaps between Spain export and scraped data |
| `optimize_images.py` | Shared image optimization library (WebP conversion) |
| `get_token.py` | Get Shopify access tokens via OAuth |

### Deprecated (superseded by migrate_all_images.py)

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
8. **Shopify Flows** — Export `.flow` files from Spain, import to Saudi
9. **End-to-end checkout test** — Place a test order through complete flow

---

## Running Tests

```powershell
python -m pytest                                    # All tests
python -m pytest tests/test_post_migration.py       # Specific test file
python -m pytest -x                                 # Stop on first failure
```
