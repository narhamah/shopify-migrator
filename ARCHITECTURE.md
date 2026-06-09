# Shopify Migrator — Architecture Review & Roadmap to Robust, Automatic, Seamless Migration

> Produced from a full-codebase audit (10 subsystem deep-readers + 10 manual-step
> automation studies + adversarial verification of every critical/high finding).
> 36 of 40 verified findings **confirmed**, 0 refuted. Every claim below cites `file:line`.

> **Implementation status (branch `robustness-automation`):** Phases 0–3 of §5 are
> implemented and tested (suite: 1147 passed, 1 skipped). Phase 0 = fail-fast
> orchestrator, hardened client (THROTTLED/jitter/typed errors), preflight, dup-step
> fix. Phase 1 = declarative `destinations/*.toml`, resumable `run_manifest.json`,
> per-item failure log, structured logging, acceptance gate. Phase 2 = Markets,
> shipping, theme go-live, notification translation, owned-flow rebuild, guided
> manual-steps engine, Klaviyo auto-config, Bulk Operations. Phase 3 = unified
> `migrate <verb>` CLI, wrapper lint, CI workflow, secret-scan pre-commit.
> **Not yet done:** full PDP-helper extraction; the new Markets/shipping GraphQL
> mutations are unit-tested for call shape but should be verified against a live
> store on first run.

---

## 1. Executive Verdict

The toolkit is **feature-complete but operationally fragile**. It can migrate a store end-to-end on a
clean run, but it is neither robust nor truly automatic. The orchestrator
(`src/tara_migrate/pipeline/build_site.py`) runs five of eight phases as
`subprocess.run(cmd, check=False)` (lines 128, 170, 236, 257) and wraps the rest in
`except Exception: print(...); continue` (lines 108-110, 214-216), so a half-failed phase produces a
confident `"BUILD COMPLETE"` with exit code 0 while leaving Shopify in a corrupted, partially-imported
state. There is no checkpoint, no resume, no run manifest, and no exit-code signal to any caller.

**The single biggest risk is silent partial failure behind a false success signal.** Because `id_map`
is saved per-item inside `try` blocks (`import_english.py:413/422`) while subprocess exit codes are
discarded, an interrupted phase 3 leaves orphaned Shopify products that phase 5 (Arabic) and phase 6
(images) then skip with `None`-lookups — and the operator discovers it only via a manual store audit
days later.

The "no manual steps" goal is achievable for **~70%** of the documented manual gaps (Shopify Markets,
shipping zones, theme publish, resource publish, all Arabic notification/translation work, owned Flow
patterns). The rest is **hard-blocked by Shopify platform design** (VAT self-certification, payment-gateway
KYC, registrar DNS for a new domain, store-password removal, third-party app OAuth). Those cannot be
automated away — but they can be reframed from blind `print()` reminders into **pre-validated, deep-linked,
post-verified one-click guided steps**.

---

## 2. Most Important Weaknesses (ranked, verified)

### #1 — Subprocess phases swallow failures; orchestrator reports false success  · CRITICAL · small fix
**Problem:** `build_site.py` runs phases 3, 5, 7, 8 with `subprocess.run(cmd, check=False)`
(lines 128, 170, 236, 257) and never inspects the return code. The phase loop's `except Exception`
(lines 365-367) never fires because no exception is raised. `"BUILD COMPLETE"` prints unconditionally;
`main()` exits 0.
**Consequence:** A crash in `import_english.py` after creating 50/100 products is invisible. Downstream
phases run against an incomplete `id_map`, failing with cryptic `KeyError`/`None` lookups. CI/automation
sees exit 0 and assumes success.
**Fix:** `result = subprocess.run(cmd, capture_output=True, text=True)`; on `returncode != 0` write the
phase log and raise `PhaseError`. Track per-phase status in a manifest; set process exit code to 1 if any
phase failed. Print the truth.

### #2 — No checkpoint/resume; every re-run replays completed work  · HIGH · medium fix
**Problem:** The loop (`build_site.py:355-370`) calls `func()` unconditionally — zero completion-state
check, no `_build_manifest.json` anywhere.
**Consequence:** Re-running after interruption re-translates already-translated content (wasted
OpenAI/Anthropic spend), re-fetches Magento prices, re-pushes. Operators must hand-manage `--from N`.
Combined with #1: partial Shopify state + blind replay = duplicate-creation risk.
**Fix:** Typed state manifest (§3b). Skip a phase iff `status=="completed"` AND config/source hash
unchanged. Write `completed` only after full success.

### #3 — Resource creation has no transactional boundary; partial failure orphans Shopify resources  · HIGH · large fix
**Problem:** In `import_english.py`, `create_product()` (line 418) is followed by `id_map` update +
`save_json` (421-422) inside the same `try`. An exception between create and save leaves the product
live in Shopify but absent from `id_map`. `import_arabic.py:976-978` then skips it (`if dest_id is None: continue`).
**Consequence:** Orphaned, untranslated, image-less products no later phase can find. Manual cleanup required.
**Fix:** Pre-phase destination snapshot + reconciliation. On startup, match existing dest resources by
handle and backfill `id_map` (idempotent recovery) **before** creating. Persist a per-item failure log
(see #8) so re-runs target only the missing/failed items.

### #4 — API client crashes on cost-based `THROTTLED` (HTTP 200) and has no Bulk Operations  · CRITICAL · medium/large fix
**Problem:** `shopify_client.py:113-114` raises a generic `Exception` for *any* `data["errors"]`, including
`{"extensions":{"code":"THROTTLED"}}` which Shopify returns on **HTTP 200**, not 429 — so the 429 handler
(line 106) never sees it. There is zero `bulkOperation` support despite the project's own bulk mandate.
The sibling TypeScript client (`tara-product-editor/app/lib/shopify-admin.server.ts:62-65`) **already
implements** the correct `THROTTLED` retry — proving the gap.
**Consequence:** Large mutations fail mid-migration on cost throttle. 10k+ product migrations are infeasible
within the ~40-point/s leaky bucket via per-item calls.
**Fix:** In `_graphql`, before raising, scan errors for `extensions.code == "THROTTLED"`; back off on
`extensions.cost.throttleStatus` (`wait = max(1, -currentlyAvailable / restoreRate)`) and retry. Add
`random.uniform(0.9,1.1)` jitter to all backoff (lines 37, 100). Add `bulkOperationRunQuery` /
`bulkOperationRunMutation` with JSONL staging + poll-to-COMPLETED.

### #5 — Duplicate `step_enable_arabic`; the simple "enable Arabic" path is dead code  · CRITICAL · small fix
**Problem:** `post_migration.py` defines `step_enable_arabic` twice — `:55` (enable ar only) and `:83`
(`sync_secondary_locales=True`, the active one). Python keeps the second; the call site passes the sync param.
*(Independently confirmed: grep shows both `def`s.)*
**Consequence:** Step 1 always reaches into the source store and tries to publish *all* non-primary locales
(lines 119-121), which fails when the source has unpublished locales. The intended simple behavior is unreachable.
**Fix:** Delete lines 55-77. If both behaviors are wanted, rename to `step_enable_arabic` vs `step_sync_locales`
and dispatch explicitly. Add a unit test asserting one definition.

### #6 — No preflight validation: missing env vars, unverified token scopes, silent Magento defaults  · HIGH · medium fix
**Problem:** `config.py:24` raises bare `KeyError(name)`. `ShopifyClient.__init__` never verifies write
scopes. `get_magento_site_url/store_code` (lines 62-67) silently fall back to `taraformula.com`/`us-en` when unset.
**Consequence:** Hours of export/translate work complete, then phase 3 fails on the first `create_product`
because the token is read-only — or worse, runs against the wrong Magento store (UAE pulling USA prices)
and corrupts data silently.
**Fix:** A single `preflight()` that (a) validates all required config with a `ConfigError` listing every
missing var + which `.env`, (b) `verify_token_scopes()` via the free `AppInstallation.accessScopes` query
for both stores, (c) probes Magento `availableStores` and asserts the store code exists, (d) calls
`shop.json` for connectivity. Run as **phase 0**; refuse to start otherwise.

### #7 — Print-based output, no structured logging, no run-report artifact  · HIGH · medium fix
**Problem:** 62 files use `print()`; `core/logging.py:12` formats only `%(message)s` (no
timestamp/level/module). No `run_manifest.json` is ever written (`build_site.py:372-375` is stdout-only).
**Consequence:** No machine-readable success/failure signal, no per-phase counts, no audit trail. Debugging
means re-running the failing subprocess by hand.
**Fix:** `get_logger()` → `%(asctime)s %(levelname)s %(name)s %(message)s`. Write a `RunManifest` to
`data/{dest}/run_manifest.json` with per-phase status, counts, errors. This becomes the acceptance-gate
source of truth.

### #8 — No per-item failure log → no targeted retry  · HIGH · large fix
**Problem:** Item errors are `print()`-only (`import_english.py:432`, plus collections/ingredients/
metafields/articles). `id_map` records only successes. No `failed_items.json`.
**Consequence:** One transient failure among 39 products forces a full-phase re-run, wasting quota on 38
successes; the operator must eyeball logs to find the failure.
**Fix:** Write `phase_errors_{phase}.json` (`[{item_id, type, action, error, retry_count}]`). Add
`retry-failed` to re-run only failed/transient items. Phases load this and exclude/retry accordingly.

### #9 — Phase-7 image "verification" only checks existence, not validity  · MEDIUM · medium fix
**Problem:** `migrate_all_images.py:1093-1096` counts a metafield as populated `if f.get("value")` — never
calling the existing `_dest_file_reference_is_valid()` it already uses during phase 5. Phase 4 bypasses
`_get_valid_mapped_dest_file_id` and never persists cleanup (ends line 682).
**Consequence:** A product with a null/deleted/dangling file GID is reported "verified." `file_map.json`
accumulates orphaned GIDs reused on re-runs. False-positive sign-off.
**Fix:** Phase 7 resolves every file GID via `get_file_by_id` + HTTP-HEAD the CDN URL; emit `failed_images.csv`.
Route phase-4 reads through `_get_valid_mapped_dest_file_id` and persist purges.

### #10 — Thin-wrapper violations, PDP duplication, Saudi hardcoding  · MEDIUM · small/medium fix *(down-ranked)*
**Problem:** ~20 root scripts contain real logic (`clean_pdp_images.py` 402 LOC, `fix_pdp_audit.py` 236 LOC,
`fix_pdp_order.py` 270 LOC) with duplicated `classify()`/`fname_from_url()`/GraphQL and a hardcoded
`https://taraformula.ae` (`fix_pdp_audit.py:19`, `fix_pdp_order.py:196`). `purge_saudi.py` and
`purge_destination.py` alias the same function.
**Consequence:** Bug fixes touch 6 files; the "generic toolkit" claim is contradicted by Saudi hardcoding.
Maintainability/correctness, **not a live blocker** — down-ranked.
**Fix:** Extract PDP helpers to `core/pdp_helpers.py` + `core/graphql_queries.py::PDP_QUERIES`; move logic to
`tools/`; replace hardcoded URLs with `config.get_magento_site_url()`; delete `purge_saudi.py`.

> **Down-ranked / overstated by verification (treat as polish, not Phase 0):** "inconsistent progress files
> cause re-run failures" — `step_enable_arabic` and `step_build_navigation` are already idempotent, just
> inefficient. "Bare `except: pass` across 7 fixers" — only `fix_images.py:269-273` is a genuine silent
> swallow. "No metrics anywhere" — `fix_prices.py` / `fix_images.py` / `graphql_queries.py` do count.

---

## 3. Target Architecture

Collapse 64 scripts and scattered env logic into **five pillars**: one declarative per-destination config,
one idempotent orchestrator with a typed state manifest, a hardened cost-aware client with Bulk Operations,
a structured-logging + run-report layer, and a single CLI namespace.

```
┌─────────────────────────────────────────────────────────────────────┐
│  destinations/kuwait.toml   (ONE declarative file per destination)    │
└───────────────┬─────────────────────────────────────────────────────┘
                ▼
        config_schema.py  ──►  preflight()  (scopes, magento, connectivity)
                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  orchestrator  (migrate run / --resume)                              │
│   • loads run_manifest.json   • per-phase checkpoint/resume          │
│   • each phase: idempotent, transactional, writes counts + errors    │
│   • process exit code = manifest.status                              │
└───────────────┬─────────────────────────────────────────────────────┘
                ▼
   ShopifyClient (cost-aware throttle + jitter + Bulk Ops + scope check)
                ▼
   RunManifest + structured logs  ──►  acceptance gate (pytest -m acceptance)
```

### 3a. Declarative per-destination config (one file)
Replace `usa-destination.env` / `uae-destination.env` / scattered `dest_name == "usa"` checks with one
typed, Pydantic-validated file per destination. Secrets stay as **env references**, never inline.

```toml
# destinations/kuwait.toml
name              = "kuwait"

[source]
shop_url          = "tara-saudi.myshopify.com"
access_token_env  = "SOURCE_ACCESS_TOKEN"      # resolved from env, never stored here

[dest]
shop_url          = "977mp2-qa.myshopify.com"
access_token_env  = "DEST_ACCESS_TOKEN"
required_scopes   = ["write_products","write_metaobjects","write_translations",
                     "write_themes","write_markets","write_shipping","write_publications"]

[magento]
site_url          = "https://taraformula.com"
store_code        = "kw-en"

[market]
currency          = "KWD"
vat_rate          = 0.05            # drives the draftOrderCalculate post-verify probe
exchange_rate     = 4.13
locales           = ["en","ar"]
default_locale    = "en"
url_strategy      = "subfolder"     # subfolder = zero DNS; or "domain"
subfolder_suffix  = "kw"

[shipping]
export_from_source = true
zones = [ { countries = ["KW"], method = "Standard", price = "1.500", currency = "KWD" } ]

[apps]
klaviyo            = true
reviews            = "judgeme"
loyalty            = "smile"
```

### 3b. Orchestrator state manifest (typed, resumable)
```json
// data/kuwait/run_manifest.json
{
  "build_id": "2026-06-03T09:12:04Z-kuwait",
  "destination": "kuwait",
  "config_hash": "sha256:ab12…",
  "source_export_hash": "sha256:cd34…",
  "status": "failed",                       // running | completed | failed
  "phases": {
    "0_preflight":     { "status": "completed", "scopes_ok": true, "magento_ok": true },
    "3_import_english":{ "status": "failed",
                         "counts": { "created": 50, "existing": 0, "failed": 1, "skipped": 0 },
                         "error_log": "data/kuwait/phase_errors_import_english.json",
                         "checkpoint": { "last_item": "product:7421", "resumable": true } },
    "4_translate_ar":  { "status": "pending" }
  },
  "summary": { "total_failed": 1,
               "manual_steps": [ { "step": "vat", "state": "PENDING",
                  "deep_link": "https://admin.shopify.com/store/977mp2-qa/settings/taxes/KW" } ] }
}
```
**Resume contract:** a phase is skipped iff `status=="completed"` AND `config_hash`/`source_export_hash`
unchanged. A `failed` phase with `resumable:true` re-enters at `checkpoint.last_item`. `migrate run --resume`
reads this — no more manual `--from N`.

### 3c. Hardened client
`ShopifyClient` gains: `verify_token_scopes()`, `THROTTLED` leaky-bucket retry + jitter in
`_graphql`/`_request_raw`, a typed exception hierarchy (`GraphQLThrottled` / `GraphQLUserError` /
`GraphQLAuthError`), per-request cost extraction from `extensions.cost`, `bulk_query()`/`bulk_mutation()`,
and a `SHOPIFY_API_VERSION` env override with a deprecation check.

### 3d. Acceptance gate
`pytest -m acceptance` runs after a build (and in CI against a sandbox store) asserting: `id_map` fully
populated, every dest product/collection has resolvable images, every locale key registered, manifest
`status=="completed"`, exit 0. The build is "done" only when this gate is green.

### 3e. CLI consolidation
Collapse 64 root scripts into one entry: `migrate <verb>` —
`run | resume | export | import | images | translate | post | verify | manual-steps | retry-failed`.
Root `*.py` shrink to true thin wrappers or are deleted; a `_lint_wrappers.py` CI check fails any root file >10 LOC.

---

## 4. No-Manual-Steps Plan

| Manual step | Automatable? | Exact API mechanism | Residual one-click guided action |
|---|---|---|---|
| **Resource publish** (products/collections → Online Store) | **Yes — already done** | `publishablePublish` (`shopify_client.py:1496`), wired at `post_migration.py:918` | None — verify only |
| **Theme upload + customizer** | **Yes** | `themeFilesUpsert` (migrate off legacy REST `put_asset`) + `stagedUploadsCreate`→`themeCreate`; `sync_theme.py` already clones assets incl. `settings_data.json` | None for asset clone; re-enable theme-app-extension blocks per installed app |
| **Theme go-live** (publish to main) | **Yes — code exists, unwired** | GraphQL `themePublish` or REST `publish_theme()` (`shopify_client.py:1400`, currently dead); CLI `shopify theme publish` fallback | None — runs in a new gated `go_live` step behind `--go-live` |
| **Shopify Markets** (regions, currency, language) | **Yes** | `marketCreate` / `marketRegionsCreate` / `marketCurrencySettingsUpdate` / `marketWebPresenceCreate` (subfolder). Version-branch `regions`(≤2024-10) vs `conditions`(≥2025-04) | None for subfolder strategy (zero DNS) |
| **Shipping zones & flat/weight/price rates** | **Partial (~80%)** | `deliveryProfileUpdate` with `zonesToCreate` / `methodDefinitionsToCreate`; reuse primary-location GID (`post_migration.py:880`). Needs `write_shipping` scope. *Repo does none today.* | Carrier-calculated rates (Aramex/DHL) need a hosted callback + Advanced plan — one guided app-install + `carrierServiceCreate` |
| **Email/SMS notification translation (Arabic)** | **Yes** | Add `EMAIL_TEMPLATE` + `PACKING_SLIP_TEMPLATE` to `TRANSLATABLE_RESOURCE_TYPES` (`core/shopify_fields.py:39`) + `export_translations.py:39`; key off `resourceId`. Reuse existing `register_translations` | None |
| **Email/SMS template *body* edits** | **No (platform gap)** | No public mutation; internal `EmailTemplateUpdate` only | Ship versioned `.liquid` + deep-link Settings→Notifications; optional Playwright paste (best-effort) |
| **Shopify Flows — owned patterns** (ingredient→collection→publish) | **Yes** | Promote `get_flow_ids.py` JSON into executable `collectionCreate(ruleSet)`→`publishablePublish`→`metaobjectUpdate` | None for owned patterns |
| **Shopify Flows — arbitrary/3rd-party** | **No (no workflow CRUD API)** | `.flow` export/import is UI-only, Plus-gated, undocumented format | Playwright-driven export/import + generated GID-remap checklist from `id_map.json` |
| **Customer marketing consent / web pixels** | **Yes** | `customerEmailMarketingConsentUpdate`, `webPixelCreate`, `webhookSubscriptionCreate` | None |
| **Store password removal (go-live)** | **No (read-only API)** | `OnlineStorePasswordProtection.enabled` is read-only; no update mutation | Deep-link `…/admin/online_store/preferences` behind `--password-off` confirm; verify via read query |
| **VAT / ZATCA (15% KSA, 5% GCC)** | **No (legal self-cert)** | `Country.tax` read-only since 2020-10 (resource deprecated 2024-07); no `shopTaxSettingsUpdate`. Per-variant `taxable` already true (`import_english.py:92`) | Deep-link `…/settings/taxes/SA`; post-verify with `draftOrderCalculate` probe (expect ~15% taxLines) |
| **Payment gateways (Tap/Mada/Apple Pay)** | **No (KYC + merchant Activate)** | No provisioning API; only `paymentsAppConfigure` by the provider's own app | Deep-link Payments + provider app; poll `payment_gateways.json` until ≥1 active |
| **Domain / DNS (new custom domain)** | **No (no `domainCreate`)** | `shop.domains` read-only | Subfolder market path avoids it entirely; else emit exact A/CNAME records (+ Cloudflare API if registrar is CF) and poll `sslEnabled` |
| **Third-party app install (Klaviyo/reviews/loyalty)** | **No (merchant OAuth)** | No `appInstallation` mutation; consent required | Deep-link `apps.shopify.com/...`; then **auto-config Klaviyo** (lists/flows/templates/translations) via Klaviyo API once `KLAVIYO_API_KEY` set |

**Honest ceiling:** ~7 of the original "cannot automate" steps become fully hands-off (publish, theme,
markets-subfolder, notification translation, owned flows, consent/pixels, theme-go-live). The irreducible
residue — VAT cert, payment KYC + Activate click, registrar DNS for a *new* domain, password toggle,
3rd-party app OAuth — becomes **one confirmed click each, pre-validated and post-verified**, surfaced in
`run_manifest.summary.manual_steps` with deep links instead of a blind `print()`.

---

## 5. Phased Roadmap

### Phase 0 — Stop the bleeding (correctness/robustness) · ~1 week
*Never report false success; never corrupt the store silently.*

| Item | Size | Files |
|---|---|---|
| `subprocess.run` → return-code check → raise `PhaseError`; capture stdout/stderr per phase | small | `pipeline/build_site.py:128,170,236,257` |
| Orchestrator exits non-zero on any phase failure; truthful BUILD FAILED/COMPLETE | small | `pipeline/build_site.py:355-379` |
| Delete duplicate `step_enable_arabic` (55-77); keep one; add test | small | `pipeline/post_migration.py:55-125`; `tests/test_post_migration.py` |
| `_graphql`: detect `THROTTLED` (HTTP 200) + leaky-bucket backoff; jitter on all backoff | medium | `client/shopify_client.py:37,100,113-114` |
| `preflight()`: `ConfigError` w/ missing-var list; `verify_token_scopes()`; Magento store-code probe; no silent defaults | medium | `core/config.py:13-24,62-67`; new `core/preflight.py` |
| Fix `test_setup_store.py` import (`DEFAULT_` prefix mismatch) so the suite passes | small | `tests/test_setup_store.py:8-11` |
| Real silent-swallow fix | small | `fixers/fix_images.py:269-273` |

### Phase 1 — Automation of config + pipeline + acceptance gate
*One config file, one resumable command, a green gate.*

| Item | Size | Files |
|---|---|---|
| Pydantic `DestinationConfig` + `destinations/*.toml` loader | large | new `core/config_schema.py`, `destinations/kuwait.toml` |
| `RunManifest` class + `data/{dest}/run_manifest.json`; per-phase counts | medium | new `core/run_manifest.py`; `pipeline/build_site.py` |
| Checkpoint/resume by config+source hash; `migrate run --resume` | medium | `pipeline/build_site.py` |
| Per-item failure log + `retry-failed`; transactional id_map recovery (handle-match backfill before create) | large | `pipeline/import_english.py:410-432`; new `tools/retry_failed_items.py` |
| Structured logging (timestamp/level/module) | medium | `core/logging.py:12` (+ mechanical sweep) |
| Real phase-7 image verification (resolve GID + CDN HEAD); route phase-4 through validator | medium | `pipeline/migrate_all_images.py:621,682,1093-1096` |
| `pytest -m acceptance` gate + `test_build_site.py`, `test_migrate_all_images.py` | large | new tests |

### Phase 2 — Manual-step automation
*Collapse the manual checklist to one-click-guided residue.*

| Item | Size | Files |
|---|---|---|
| `setup_markets.py` (regions/currency/subfolder web presence); version-branch | large | new `setup/setup_markets.py` |
| `migrate_shipping.py` (export source profiles → flat/weight/price zones); add `write_shipping` scope | large | new `pipeline/migrate_shipping.py`; `tools/get_token.py` |
| Theme go-live step (`themePublish`/REST/CLI) behind `--go-live`; `put_asset`→`themeFilesUpsert` | medium | `client/shopify_client.py:1400`; `pipeline/post_migration.py` |
| Add `EMAIL_TEMPLATE`/`PACKING_SLIP_TEMPLATE` to translatable types; wire into post-migration | small | `core/shopify_fields.py:39`; `pipeline/export_translations.py:39`; `tools/review_arabic.py` |
| Owned-flow rebuild (ingredient→collection→publish via mutations) | medium | new `pipeline/migrate_flows.py` from `tools/get_flow_ids.py` |
| Guided manual-steps engine: deep links + post-verify probes (VAT `draftOrderCalculate`, payments poll, `sslEnabled` poll, password read-check) | medium | `pipeline/post_migration.py:1395-1409`; `core/run_manifest.py` |
| Klaviyo auto-config post-install (lists/flows/templates/translations) | medium | new `tools/setup_klaviyo.py` |

### Phase 3 — Consolidation + tests + CI
*Maintainable, generic, gated.*

| Item | Size | Files |
|---|---|---|
| Extract PDP helpers + queries; move root logic to `tools/`; delete `purge_saudi.py`; replace hardcoded `taraformula.ae` | medium | `core/pdp_helpers.py`, `core/graphql_queries.py`; `clean_pdp_images.py`, `fix_pdp_*.py:19,196` |
| `migrate` unified CLI; root scripts → thin wrappers; `_lint_wrappers.py` (>10 LOC fails) | medium | new `cli.py`, `core/_lint_wrappers.py`; ~20 root scripts |
| Bulk Operations API for export/large import | large | `client/shopify_client.py` |
| Tests for 9 untested fixers, audit modules, contract fixtures, dry-run no-side-effect assertions | large | `tests/` |
| `.github/workflows/test.yml`: collect + `pytest -v` + acceptance smoke + coverage gate | small | new `.github/workflows/test.yml` |
| Secrets hardening: pre-commit hook rejecting `shpat_`/`sk-`; document rotation | small | `.pre-commit-config.yaml`, `CLAUDE.md` |

---

**Bottom line:** Phase 0 (~1 week) eliminates the catastrophic false-success/silent-corruption risk and is
the highest-leverage work. Phase 1 makes the pipeline genuinely automatic and resumable behind a config file
and acceptance gate. Phase 2 retires the bulk of the manual checklist. Phase 3 pays down the sprawl and locks
it behind CI. "No manual steps" is reachable for everything except five Shopify-platform-gated legal/OAuth/DNS
toggles, which become deterministic one-click guided steps — not silent gaps.
