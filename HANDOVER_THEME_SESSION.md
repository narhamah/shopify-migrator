# Handover Prompt — Cross-Codebase Session (Migration Toolkit + Shopify Theme)

Copy this prompt into a Claude Code session that has access to BOTH codebases.

---

## Context

You are working on the **TARA Saudi Arabia Shopify store** (sa.taraformula.com). You have access to two codebases:

1. **Migration toolkit** (`shopify-migrator/`) — Python CLI pipeline that migrates content from the Spain store and manages all Arabic translations. Read its `CLAUDE.md` for full architecture.
2. **Shopify theme** (the theme repo) — Dawn-based Liquid theme with sections, snippets, templates, and locale files (`locales/ar.json`, `locales/en.default.json`).

## Current State

The store migration is complete. All products, collections, metaobjects, pages, articles, images, menus, and redirects are live. The remaining work is **Arabic translation completeness** — ensuring zero visible English on the `/ar` locale.

## The Three Translation Layers

Arabic translations live in three separate places in Shopify. **All three must be complete** for zero visible English:

| # | Layer | Where | Managed By | Limit |
|---|-------|-------|------------|-------|
| 1 | **Theme locale file** | `locales/ar.json` in the theme | `audit_theme_keys.py --populate-locale` | None (file-based) |
| 2 | **Theme editor content** | Shopify Translations API (`section.*` keys) | `audit_theme_keys.py --translate` | ~3,400 keys/locale |
| 3 | **Resource content** | Shopify Translations API (products, etc.) | `review_arabic.py` | No practical limit |

### Layer 1: Theme locale file (`ar.json`)
These are the **theme UI strings** — button labels, form placeholders, accessibility text, section headings baked into Liquid templates via `{{ 'key' | t }}`. They come from `locales/en.default.json` and are translated in `locales/ar.json`.

**What shows up untranslated if missing**: "Add to cart", "Sign Up", "You may also like", "Search", "Continue shopping", filter labels, pagination text, error messages.

**How to fix from migration toolkit**:
```bash
python audit_theme_keys.py --populate-locale --model gpt-5.4 --reasoning xhigh
```

**How to fix from theme repo**: Edit `locales/ar.json` directly. Every key in `en.default.json` should have a corresponding Arabic entry in `ar.json`.

### Layer 2: Theme editor / section content (`section.*` keys)
These are strings entered by merchants in the **Shopify theme customizer** — section headings, badge text, announcement bar text, footer content, etc. They're stored as `section.template-name.blocks.block-id.settings.field-name` in the Translations API.

**What shows up untranslated if missing**: "Sulfate Free", "Cruelty Free", "Dermatologically Tested", "Free Shipping", section headings like "FAQs", "Our Ingredients", announcement bars, footer text.

**How to fix from migration toolkit**:
```bash
python audit_theme_keys.py --translate --model gpt-5.4 --reasoning xhigh
```

**How to fix from theme repo**: These are NOT in the theme code — they're in the Shopify admin theme customizer. The migration toolkit's `--translate` flag handles them via the Translations API.

### Layer 3: Resource content (products, collections, metaobjects, etc.)
Product titles, descriptions, metafield content (taglines, FAQs, benefits, ingredients), collection names, page HTML, article content.

**How to fix**:
```bash
python review_arabic.py --force --model gpt-5.4 --reasoning xhigh
```

## Common Cross-Codebase Tasks

### "I see English text on the Arabic site — where is it coming from?"

1. **Check if it's a theme locale string**: Search `locales/en.default.json` in the theme for the English text. If found, the fix is adding the Arabic version to `locales/ar.json`.

2. **Check if it's theme editor content**: Look in the theme's `templates/*.json` and `sections/*.json` files for the string. If it's in a `settings` block, it's Layer 2 — fix via `audit_theme_keys.py --translate`.

3. **Check if it's product/collection content**: If it's on a product page and relates to the product itself (title, description, metafield), it's Layer 3 — fix via `review_arabic.py`.

4. **Check if it's hardcoded in Liquid**: Search `.liquid` files in the theme for the literal string. If hardcoded (not using `{{ | t }}`), it needs to be converted to use the translation filter and a key added to both locale files.

### "Junk strings are wasting translation key slots"

The Shopify Translations API has a ~3,400 key limit. Junk entries waste slots:
- Empty or whitespace-only translations
- System keys (checkout, customer accounts) that Shopify manages
- Identical AR=EN translations (brand names, URLs, numbers)
- Duplicate values across section keys

**Cleanup sequence**:
```bash
# From migration toolkit:
python audit_theme_keys.py --remove-junk              # Remove system/empty from API
python audit_theme_keys.py --dedup-translations        # Remove duplicate AR values
python audit_theme_keys.py --clean-locale              # Clean ar.json in theme
```

### "Theme has hardcoded English strings"

If Liquid templates contain hardcoded English instead of `{{ 'key' | t }}`:

1. **In the theme repo**: Replace the hardcoded string with `{{ 'section_name.key_name' | t }}`
2. **Add to `en.default.json`**: Add the English value under the appropriate key
3. **Add to `ar.json`**: Add the Arabic translation
4. **Or auto-translate**: After adding to `en.default.json`, run `audit_theme_keys.py --populate-locale` to auto-translate

### "Crawl the site to find what's still in English"

The migration toolkit can crawl the live site with Playwright and identify untranslated visible text:
```bash
python crawl_and_translate.py --dry-run               # See what's English
python crawl_and_translate.py                          # Crawl + match + translate
python crawl_and_translate.py --include-checkout       # Also check checkout pages
```

## Key Files in Each Codebase

### Migration Toolkit (`shopify-migrator/`)
- `src/tara_migrate/tools/audit_theme_keys.py` — Theme key audit, translate, populate-locale, clean-locale
- `src/tara_migrate/tools/review_arabic.py` — Product/collection/metaobject Arabic review + fix
- `src/tara_migrate/tools/crawl_and_translate.py` — Playwright-based visible text translation
- `src/tara_migrate/translation/verify_fix.py` — Unified audit→fix→verify pipeline
- `src/tara_migrate/translation/engine.py` — TranslationEngine (TOON-batched AI translation)
- `src/tara_migrate/core/graphql_queries.py` — Shared GraphQL queries for translations
- `src/tara_migrate/core/shopify_fields.py` — TRANSLATABLE_RESOURCE_TYPES constant

### Shopify Theme (theme repo)
- `locales/en.default.json` — English source strings (theme UI)
- `locales/ar.json` — Arabic translations (theme UI)
- `sections/*.liquid` — Section templates (check for hardcoded strings)
- `snippets/*.liquid` — Reusable components
- `templates/*.json` — Template configurations (section settings = Layer 2 content)
- `config/settings_schema.json` — Global theme settings
- `config/settings_data.json` — Current theme setting values

## Quick Diagnostic Commands

```bash
# How many translation keys are we using vs the limit?
python audit_theme_keys.py

# What's still untranslated?
python audit_theme_keys.py --full-analysis

# Deep analysis of key sources and duplicates
python analyze_theme_keys.py --fetch

# Audit product/collection Arabic translations
python review_arabic.py --audit

# Audit everything, save report
python review_arabic.py --audit --save-report data/arabic_audit.json

# What does a visitor actually see in English?
python crawl_and_translate.py --crawl-only

# Find hardcoded English strings in Liquid files (not using | t filter)
python audit_theme_keys.py --extract-hardcoded /path/to/theme

# Extract translatable text from template JSON settings
python audit_theme_keys.py --extract-templates /path/to/theme

# Generate ar.schema.json (editor labels, section names)
python audit_theme_keys.py --populate-schema --dry-run
```

## Environment

The migration toolkit needs these env vars (in `.env`):
```
DEST_SHOP_URL=xxx.myshopify.com        # Saudi store
DEST_ACCESS_TOKEN=shpat_xxx
OPENAI_API_KEY=sk-xxx                  # For AI translation
ANTHROPIC_API_KEY=sk-ant-xxx           # For semantic audit (Haiku)
```
