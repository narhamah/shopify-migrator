# UAE Go-Live Runbook — switch taraformula.ae from Magento to Shopify

**Target Shopify store:** `rvgkkk-g3.myshopify.com` (admin: https://admin.shopify.com/store/rvgkkk-g3/)
**Live domain:** https://taraformula.ae/ (cut over from Magento store code `gl-en`)
**Market:** UAE · currency AED · VAT 5% · locales English + Arabic (`/ar`)

> Post-launch status, 2026-06-09: `taraformula.ae` is live on Shopify store
> `rvgkkk-g3.myshopify.com`. Live acceptance passed 8/8: apex + www serve Shopify,
> storefront password is off, currency is AED, `/ar` is Arabic/RTL, localized Arabic
> PDP images render on the live domain, Magento redirects are firing, and the Product
> Editor auth endpoint is reachable.
>
> The sections below are the historical go-live runbook and remain useful for audits,
> rollback, and future country launches. Treat pre-launch blockers in §1/§2 as resolved
> for UAE unless a fresh audit proves otherwise.

---

## 0. What "make sure it works" means here
Two independent things must both be true before flipping DNS:
1. **Content parity** — the Shopify store faithfully represents taraformula.ae (products,
   prices in AED, images incl. homepage, English + Arabic, redirects from old Magento URLs).
2. **Commerce works** — taxes (5% VAT), payments, shipping, and checkout all function on the
   live domain with no storefront password.

---

## 1. Get a working admin token (blocker for live verification)
The token in `uae-destination.env` returns **401 Unauthorized** against
`rvgkkk-g3.myshopify.com` — it has rotated/expired. Before any live check:

1. In the UAE store admin, create/refresh a custom-app Admin API token with the migration
   scopes (`python get_token.py` prints the scope list; UAE needs the same set incl.
   `write_markets`, `write_shipping`).
2. Put it in `uae-destination.env` as `DEST_ACCESS_TOKEN` (gitignored — never commit).
3. Confirm: `python migrate.py verify` should connect (currently it can't).

---

## 2. Offline completeness audit (from `data/uae/` — done now, no token needed)

| Area | Result | Action |
|---|---|---|
| Products | 39, **0 missing images** | OK |
| EN/AR product parity | 39 EN / 39 AR | OK |
| Collections / pages / blogs / articles | 56 / 13 / 2 / 3 | verify live |
| Metaobjects (benefit/faq/ingredient + shopify std) | complete (122/124/34/…) | OK |
| Metaobject **images** | **34 populated, 68 MISSING** | **FIX — re-run image migration metaobject stage** |
| **Homepage images** | **0 set** | **FIX — homepage hero/sections not migrated** |
| Arabic translations registered | 439 progress entries | verify "zero visible English" live |
| Redirects tracked | 95 — but built from the **Saudi Shopify** source, not taraformula.ae Magento URLs | **REBUILD from taraformula.ae** (see §4) |
| Prior storefront parity report | base was `tara-uae.myshopify.com` (stale URL), 117 http-errors | **re-run against rvgkkk-g3** |

**Fix the two content gaps first (need the token from §1):**
```bash
# load UAE env, then:
python migrate.py images            # re-runs all image stages incl. homepage + metaobjects
# or targeted: python migrate_all_images.py  (phases 3 homepage, 5 metaobject files)
python migrate.py verify            # acceptance gate: products have images, locales, id_map
```

---

## 3. Live verification (read-only; run after §1)
```bash
python migrate.py verify                                  # acceptance gate (manifest/id_map/images/locales)
python audit_store.py                                     # resource counts + integrity on UAE
python compare_stores.py                                  # Saudi source vs UAE parity
python review_arabic.py --audit                           # zero-visible-English check on Arabic content
python audit_site.py --base-url https://rvgkkk-g3.myshopify.com --locale-prefix /ar   # Playwright visual /ar
python test_checkout.py --bogus                           # checkout works (enable test mode first)
```
Pass criteria: acceptance gate green; audit_store counts match §2; review_arabic finds no
English in Arabic resources; audit_site shows no broken images/English on `/ar`; a test order
completes.

---

## 4. Redirects from the OLD taraformula.ae (SEO-critical)
The 95 tracked redirects map the **Saudi Shopify** handles, not the live Magento URL
structure of taraformula.ae. For a Magento→Shopify cutover you must 301 the real old URLs:
```bash
python build_magento_sitemap_redirects.py   # crawl taraformula.ae sitemap -> Shopify handle 301s
python remap_redirects.py                    # remap to dest handles
python post_migration.py --step 5            # create redirects on UAE store
```
Spot-check after go-live: a sample of old `taraformula.ae/...` product + category URLs must 301
to the matching Shopify page (not 404).

---

## 5. Platform-gated go-live steps (guided; see data/uae/manual_steps.json after a post-migration run)
Run `python post_migration.py` (UAE env) to regenerate the deep-linked, verified checklist. The
irreducible UAE steps:
- **VAT 5%** — Settings > Taxes & duties (UAE). Verify with a draft-order tax probe.
- **Payments** — UAE providers (Telr / Checkout.com / Tabby / Tamara / Apple Pay / Tap); KYC is merchant-only.
- **Shipping** — `python migrate.py shipping --dry-run` then live (needs `write_shipping`).
- **Market** — `python migrate.py markets --config destinations/uae.toml --dry-run` then live
  (UAE region, AED, en+ar). Create `destinations/uae.toml` from `destinations/kuwait.toml.example`.
- **Theme go-live** — `python post_migration.py --go-live` (publishes the migrated theme).
- **Notifications** — Arabic email/packing-slip translations export/import; body edits are UI-only.

---

## 6. The cutover sequence (the actual switch)
Do DNS last, and lower the registrar TTL to ~300s 24–48h beforehand.
1. **Freeze** Magento taraformula.ae content edits.
2. **Final delta sync** — prices/inventory: `python fix_prices.py --update-shopify`; inventory via `post_migration --step 6`.
3. **Green gate** — §3 acceptance + audits all pass; §2 gaps fixed; §4 redirects created.
4. **Connect domain** — Shopify admin > Settings > Domains > Connect existing `taraformula.ae`; set as **primary**; enable redirect of `www`.
5. **DNS at registrar** — A record `@` -> `23.227.38.65`, CNAME `www` -> `shops.myshopify.com`. Wait for Shopify to show SSL "active".
6. **Remove storefront password** — Online Store > Preferences (read-only via API; do it in admin).
7. **Live smoke test** on https://taraformula.ae and https://taraformula.ae/ar:
   homepage renders (images!), a PDP, add-to-cart, **complete one real low-value order**,
   search, and 5+ old Magento URLs 301 correctly.
8. **Post-launch** — submit the Shopify sitemap to Google Search Console; watch 404s + redirect hits for a week.

---

## 7. Rollback
If a blocker appears post-cutover, revert the registrar A/CNAME back to Magento (fast because TTL
was lowered in §6). Shopify keeps the data; nothing is destroyed by switching DNS back.

---

## Quick status line
- **Built:** products, prices path, EN+AR product content, collections, metaobjects, 95 (Saudi-derived) redirects.
- **Must fix before launch:** homepage images (0), ~68 metaobject images, redirects rebuilt from taraformula.ae, fresh admin token, live acceptance + checkout verification, VAT/payments/shipping/market/theme-go-live, domain+password cutover.
