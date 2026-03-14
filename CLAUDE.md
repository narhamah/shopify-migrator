# CLAUDE.md — TARA Shopify Store Migration

## Project Overview

Python CLI pipeline migrating the **TARA luxury scalp-care** Shopify store from **Spain (Spanish)** to **Saudi Arabia (English primary + Arabic secondary)**. Handles products, collections, pages, blogs, articles, metaobjects, translations, images, menus, redirects, and post-migration config.

## Architecture

```
src/tara_migrate/          ← Production library (all logic lives here)
  client/shopify_client.py ← Shopify REST + GraphQL API client (1,296 lines)
  core/                    ← Shared utilities: config, utils, rich_text, language, logging, shopify_fields,
                             csv_utils (CSV row classification), graphql_queries (shared GQL templates)
  pipeline/                ← Main migration phases: export, import_english, import_arabic, build_site,
                             post_migration, migrate_all_images
  translation/             ← AI translation: translator, engine, translate_gaps, field_extractors, toon,
                             translate_csv (CSV-based translation), validate_csv (CSV validation/cleaning),
                             verify_fix (unified audit→fix→verify pipeline)
  setup/                   ← Schema creation: setup_store, setup_collections, setup_menus, setup_homepage
  fixers/                  ← Incremental fixes: fix_prices, fix_images, fix_metafields, fix_status,
                             fix_redirects, fix_translations (GraphQL translation fixer)
  tools/                   ← Utilities: scrape_kuwait, purge_saudi, resolve_metaobject_diffs, optimize_images,
                             review_content (English content review), review_arabic (Arabic translation review)
  audit/                   ← Verification: audit_store, compare_stores, compare_stores_offline, compare_data,
                             verify_saudi, audit_translations (GraphQL audit/investigate/upload),
                             audit_site (Playwright visual audit)

*.py (root)                ← Thin wrapper scripts that import from src/tara_migrate/ and call main()
tests/                     ← pytest test suite with mocks and fixtures
data/                      ← Pipeline data (gitignored): spain_export/, english/, arabic/, id_map.json, etc.
```

### Thin Wrapper Pattern

ALL root-level scripts are 3-5 line entry points:
```python
#!/usr/bin/env python3
from tara_migrate.pipeline.export_spain import main
if __name__ == "__main__":
    main()
```

All logic lives in `src/tara_migrate/`. Tests import from there too. No standalone scripts at root.

## 8-Phase Build Pipeline (`build_site.py`)

| Phase | Script | What It Does |
|-------|--------|--------------|
| 1 | `translate_gaps.py --lang en` | Translate ES→EN (merges with Magento scrape, TOON-batched AI for gaps) |
| 2 | `fix_prices.py --update-shopify` | Fetch SAR prices from Magento, push to Shopify |
| 3 | `import_english.py --exchange-rate 4.13` | Create metaobjects, products, collections, pages, articles; remap refs; save id_map.json |
| 4 | `translate_gaps.py --lang ar` | Translate EN→AR (Modern Standard Arabic, Gulf audience) |
| 5 | `import_arabic.py` | Register Arabic translations via Shopify Translations API |
| 6 | `migrate_all_images.py` | 6-stage image migration (products, collections, homepage, metaobjects, articles) |
| 7 | `resolve_metaobject_diffs.py` | Fix schema mismatches and dedup metaobjects |
| 8 | `post_migration.py` | 11 sub-steps: locale, collects, menus, SEO, redirects, inventory, publish, discounts, activate, policies, handles |

## Environment Variables

```
SPAIN_SHOP_URL=xxx.myshopify.com
SPAIN_ACCESS_TOKEN=shpat_xxx
SAUDI_SHOP_URL=xxx.myshopify.com
SAUDI_ACCESS_TOKEN=shpat_xxx
OPENAI_API_KEY=sk-xxx
ANTHROPIC_API_KEY=sk-ant-xxx
```

## Data Pipeline

```
data/
  spain_export/    ← Raw export from Spain store (products, collections, metaobjects, etc.)
  english/         ← Translated English content (39 products, 122 benefits, 122 FAQs, 34 ingredients)
  arabic/          ← Translated Arabic content
  id_map.json      ← Source GID → Destination GID mapping (critical for reference remapping)
  sar_prices.json  ← SKU → {final_price, regular_price, currency}
  file_map.json    ← Media file mappings
  menu_config.json ← Navigation structure
```

## Translation System

- **TOON encoding** (Token-Oriented Object Notation) batches ~120 fields per API call (~40x fewer calls)
- **TARA tone-of-voice** prompts from `tara_tov_en.txt` / `tara_tov_ar.txt`
- **Never-translate rules**: brand name "TARA", product names ("Kansa Wand", "Gua Sha"), INCI names
- **Rich text safety**: translates at text-node level inside JSON, sanitizes corrupted output
- **CRITICAL — rich_text_field metafields**: NEVER pass rich_text JSON through a plain-text or HTML translator. Always use `extract_text_nodes()` + `rebuild()` from `core.rich_text` to translate individual text nodes while preserving the JSON structure. Shopify rejects raw HTML uploads to rich_text_field metafields.
- **Progress tracking**: `_translation_progress_{lang}.json` — safe to interrupt and resume
- **Models**: Uses OpenAI (gpt-5-nano default with minimal reasoning)
- **IMPORTANT — GPT-5 family API constraints**: Do NOT pass `max_tokens` (use `max_completion_tokens` if needed) and do NOT pass `temperature` (only default value 1 is supported). These apply to gpt-5-nano, gpt-5-mini, gpt-4o-mini, and all GPT-5 variants.

### Key Translation Constants (in `src/tara_migrate/translation/translator.py`)

- `TRANSLATABLE_FIELD_TYPES`: single_line_text_field, multi_line_text_field, rich_text_field
- `METAOBJECT_TRANSLATABLE_FIELDS`: per-type field sets (benefit, faq_entry, blog_author, ingredient)
- `PRODUCT_TRANSLATABLE_METAFIELDS`: 19 custom.* and global.* fields
- `ARTICLE_TRANSLATABLE_METAFIELDS`: blog_summary, hero_caption, short_title

## Metaobject Schema

| Type | Key Fields | Purpose |
|------|-----------|---------|
| `benefit` | title, description, category, icon_label | Product benefits (122 entries) |
| `faq_entry` | question, answer | Per-product FAQ (122 entries) |
| `ingredient` | name, inci_name, benefits, description, source, origin, category, concern, image, icon | Ingredient library (34 entries) |
| `blog_author` | name, bio, avatar | Author profiles |

**Product metafields (19)**: tagline, short_description, size_ml, 7 accordion heading/content pairs (key_benefits, clinical_results, how_to_use, whats_inside, free_of, awards, fragrance), ingredient_refs, faq_refs, title_tag, description_tag

**Article metafields (12)**: featured, blog_summary, hero_caption, short_title, author ref, related articles/products, ingredient links

## Testing

```bash
python -m pytest                            # All tests
python -m pytest tests/test_import_english.py  # Specific file
python -m pytest -x                         # Stop on first failure
```

- **Framework**: pytest (`pytest.ini` sets `pythonpath = src`)
- **Fixtures** in `tests/conftest.py`: `make_product()`, `make_collection()`, `make_article()`, `make_metaobject()`, `make_id_map()`, `tmp_data_dir()`
- **All tests use mocks** — no live API calls
- **Test files**: test_shopify_client (44KB), test_translator (20KB), test_import_english (15KB), test_import_arabic (28KB), test_post_migration (19KB), test_setup_store, test_export_spain, test_optimize_images, test_verify_fix

## Dependencies

`requirements.txt`: requests, openai, python-dotenv

## Key Commands

```bash
# Full pipeline
python build_site.py

# Individual steps
python setup_store.py [--dry-run]
python export_spain.py
python translate_gaps.py --lang en
python import_english.py --exchange-rate 4.13 [--dry-run]
python translate_gaps.py --lang ar
python import_arabic.py [--dry-run]
python migrate_all_images.py
python post_migration.py

# Fixers
python fix_prices.py [--update-shopify]
python fix_status.py
python fix_images.py

# Content review (strip HTML bloat, translate remaining Spanish)
python review_content.py --audit                       # Report issues only (Haiku 4.5 detection)
python review_content.py --dry-run                     # Show planned changes
python review_content.py                               # Apply fixes (regex stripping + gpt-4o-mini translation)
python review_content.py --ai-clean                    # Use Sonnet 4.6 to clean HTML (instead of regex)
python review_content.py --ai-clean --dry-run          # Preview AI-cleaned HTML
python review_content.py --scan-bloat                  # AI bloat scan: log patterns to data/html_bloat_debug.jsonl
python review_content.py --type pages --skip-spanish   # Strip HTML bloat from pages only
python review_content.py --skip-html-cleanup           # Only fix Spanish (no HTML stripping)
python review_content.py --audit-model MODEL           # Override audit model (default: claude-haiku-4-5-20251001)
python review_content.py --model MODEL                 # Override translation model (default: gpt-4o-mini)

# Arabic translation review (7-step pipeline: fetch → classify → semantic check → fix → verify)
python review_arabic.py --audit                        # Audit only, no changes
python review_arabic.py --dry-run                      # Show planned changes
python review_arabic.py                                # Full pipeline: audit + fix + verify
python review_arabic.py --type PRODUCT                 # Only audit products
python review_arabic.py --type PRODUCT,METAFIELD       # Multiple types
python review_arabic.py --skip-semantic                # Skip Haiku correspondence check (faster)
python review_arabic.py --model gpt-5-mini             # Override translation model
python review_arabic.py --audit-model MODEL            # Override Haiku audit model
python review_arabic.py --no-verify                    # Skip post-fix re-audit
python review_arabic.py --save-report FILE.json        # Save audit report

# Audit
python compare_stores.py
python verify_saudi.py
python audit_store.py

# CSV translation tools (consolidated into library)
python translate_csv.py --input FILE.csv [--model gpt-5-nano] [--dry-run]
python translate_csv.py --input FILE.csv --mode per-field [--tov FILE.txt]
python translate_csv.py --input FILE.csv --upload       # translate + upload to Shopify
python translate_csv.py --input FILE.csv --upload-only   # upload existing translations

# CSV validation and cleaning
python validate_csv.py --mode validate --input FILE.csv [--skip-ai]
python validate_csv.py --mode verify --input FILE.csv [--no-ai]
python validate_csv.py --mode clean --input FILE.csv [--fix-misaligned]

# Translation audit and investigation
python audit_translations.py --mode audit [--verbose] [--type PRODUCT]
python audit_translations.py --mode investigate --resource-id 12345
python audit_translations.py --mode upload --csv FILE.csv [--dry-run]

# Visual audit (Playwright)
python audit_site.py --base-url https://sa.taraformula.com --locale-prefix /ar
python audit_site.py --url https://sa.taraformula.com/ar/products/some-product

# Translation fixers
python fix_translations.py --audit audit_fix.json --locale ar

# Unified verify-and-fix (audit -> fix -> verify in one pass)
python verify_fix_translations.py                           # full pipeline
python verify_fix_translations.py --audit-only              # audit only, no changes
python verify_fix_translations.py --dry-run                 # show plan, no uploads
python verify_fix_translations.py --type PRODUCT            # single resource type
python verify_fix_translations.py --fix-only MISSING,IDENTICAL  # fix specific problems
python verify_fix_translations.py --no-verify               # skip re-audit after fix
python verify_fix_translations.py --clean-csv FILE.csv      # strip junk rows from CSV before Shopify import
```

## Manual Steps (Cannot Be Automated)

- Payment gateways (Tap, Mada, Apple Pay for KSA)
- Saudi VAT (15%) in Settings > Taxes
- Shipping zones in Settings > Shipping
- Domain/DNS in Settings > Domains
- Theme installation and section customization
- Email notification templates
- Third-party apps (Klaviyo, reviews, loyalty)
- Shopify Flows (export .flow from Spain, import to Saudi — GIDs differ)

## Conventions

- Shopify API version: `2024-10`
- All API interaction goes through `ShopifyClient` — never call requests directly
- Rate limit handling: automatic retry on 429 with exponential backoff
- GraphQL used for: metaobjects, translations, bulk operations
- REST used for: products, collections, pages, blogs, articles
- Progress files prevent duplicate work on re-runs (idempotent by handle matching)
- Rich text fields are JSON — always use `sanitize_rich_text_json()` after translation
