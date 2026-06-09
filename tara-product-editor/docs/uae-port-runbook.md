# Runbook — Port the Tara Product Editor to the UAE store

**Goal:** stand up the existing Tara Product Editor embedded app against the new
**UAE** Shopify store, and have its Arabic features (Arabic Content tab, Arabic
PDP galleries, Arabic health indicators) work once the UAE store's Arabic content
exists.

**Target store:** `rvgkkk-g3.myshopify.com` (migrator `DEST_NAME=uae`).
**Arabic status:** ready. UAE has published Arabic, product translations, and
localized `custom.pdp_images_ar` galleries.

> Status, 2026-06-09: the server-side UAE port is complete. The Railway
> `SHOPIFY_APP_CONFIGS` includes UAE, the UAE Shopify app config has been deployed,
> `/auth/login?shop=rvgkkk-g3.myshopify.com` returns the expected OAuth redirect,
> and logs confirmed installation plus `/app/products` loading UAE products. If the
> in-app discovery mapping was not saved in the browser, run `/app/discovery` once
> and save it before relying on the Arabic Content / Locale Images tabs.

---

## 1. How multi-store works here (read first)

You do **not** need a new Railway deployment. The existing production deployment
(`https://tara-product-editor-production.up.railway.app`) already serves multiple
Shopify app IDs through the `SHOPIFY_APP_CONFIGS` env var (one entry per store),
and the Postgres DB stores sessions + the discovery mapping **per shop**. Adding
UAE means:

1. a new Partner/Dev-Dashboard app record (new `client_id` + secret),
2. a new `shopify.app.uae.toml`,
3. one new entry in `SHOPIFY_APP_CONFIGS` on Railway,
4. install on the UAE store,
5. first-run discovery against the UAE schema.

Existing per-store configs for reference: `shopify.app.saudi.toml` (the editor,
`xkgw0m-sm`), `shopify.app.kuwait.toml` (note: that one is the separate
"Migration (receive)" app, **not** the editor — use **saudi** as the template).

---

## 2. Prerequisites

- Shopify Partner / Dev Dashboard access to create an app in the UAE store's context.
- Railway access to the `tara-product-editor` service (to edit env vars + redeploy).
- Shopify CLI logged in (`shopify auth login`) on the machine running the deploy.
- Working dir for all CLI commands: `C:\Users\narha\shopify-migrator\tara-product-editor`.
- Node deps installed (`cmd /c npm install`) if running anything locally.

---

## 3. Create the UAE app record (Dev Dashboard)

1. In the Shopify Dev/Partner Dashboard, create a new app named **"Tara Product Editor — UAE"**
   (or add an app within the UAE store's dashboard context).
2. Set the app URL to `https://tara-product-editor-production.up.railway.app`.
3. Copy the **Client ID** and **Client secret** — you need both below.
4. Set the same scopes as the editor (see §4 `[access_scopes]`).

---

## 4. Add `shopify.app.uae.toml`

Create `tara-product-editor/shopify.app.uae.toml` by copying **saudi** and
swapping only the `client_id` (everything else — Railway URL, editor scopes,
webhooks, redirect URLs — stays identical):

```toml
client_id = "PASTE_UAE_CLIENT_ID_HERE"
name = "Tara Product Editor"
application_url = "https://tara-product-editor-production.up.railway.app"
embedded = true

[webhooks]
api_version = "2026-01"

  [[webhooks.subscriptions]]
  topics = [ "app/uninstalled" ]
  uri = "/webhooks/app/uninstalled"

  [[webhooks.subscriptions]]
  topics = [ "app/scopes_update" ]
  uri = "/webhooks/app/scopes_update"

[access_scopes]
scopes = "read_files,write_files,read_metaobjects,write_metaobjects,read_metaobject_definitions,read_products,write_products,read_translations,write_translations"
optional_scopes = [ ]
use_legacy_install_flow = false

[auth]
redirect_urls = [
  "https://tara-product-editor-production.up.railway.app/auth/callback",
  "https://tara-product-editor-production.up.railway.app/auth/shopify/callback",
  "https://tara-product-editor-production.up.railway.app/api/auth/callback",
]
```

---

## 5. Wire UAE into the running deployment + push config

### 5a. Add UAE to `SHOPIFY_APP_CONFIGS` on Railway

On the Railway `tara-product-editor` service, edit `SHOPIFY_APP_CONFIGS` (a JSON
array) and **append** a UAE entry alongside the existing saudi/kuwait ones:

```json
[
  {"name":"saudi","apiKey":"...","apiSecretKey":"...","allowedShops":["xkgw0m-sm.myshopify.com"]},
  {"name":"kuwait","apiKey":"...","apiSecretKey":"...","allowedShops":["977mp2-qa.myshopify.com"]},
  {"name":"uae","apiKey":"PASTE_UAE_CLIENT_ID","apiSecretKey":"PASTE_UAE_CLIENT_SECRET","allowedShops":["rvgkkk-g3.myshopify.com"]}
]
```

Save — Railway redeploys. The container runs `npm run start:railway` (Prisma
migrate + start), so the DB schema is applied automatically. No new DB needed.

### 5b. Release the UAE app config to Shopify

From `tara-product-editor`:

```bash
cmd /c shopify app config link --client-id PASTE_UAE_CLIENT_ID    # links shopify.app.uae.toml
cmd /c shopify app deploy --config uae --allow-updates --no-build
```

### 5c. Verify the deployment is healthy

- `https://tara-product-editor-production.up.railway.app/healthz` → `ok`
- `…/auth` loads without error.

---

## 6. Verify UAE Arabic readiness

The editor's Arabic features depend on the UAE **store** having Arabic content.
As of 2026-06-09 this gate has passed; re-run these checks after any destructive
remigration or theme reset. On the migrator side (`C:\Users\narha\shopify-migrator`):

```bash
# UAE migration incl. Arabic (uses the new declarative config or uae-destination.env)
python migrate.py run --config destinations/uae.toml        # or: set DEST_NAME=uae and run phases
python migrate.py verify                                     # acceptance gate
```

UAE is "Arabic-ready" for the editor when ALL of these are true on `rvgkkk-g3`:

- `ar` locale is enabled **and published** (post-migration step 1 / `setup_locales`).
- Product **translations** are registered for `ar` (`import_arabic`).
- The product metafield definitions exist, including the Arabic PDP gallery
  `custom.pdp_images_ar` (and the English `custom.pdp_images`); legacy
  `custom.arabic_images` is optional.
- PDP image galleries are populated (`migrate_all_images` / the PDP fixers).

Quick readiness check (run from the migrator repo, UAE creds in env):

```bash
DEST_NAME=uae DEST_SHOP_URL=rvgkkk-g3.myshopify.com python - <<'PY'
from tara_migrate.client import ShopifyClient
from tara_migrate.core import config
c = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())
locales = {l["locale"]: l for l in c.get_locales()}
print("ar locale published:", bool(locales.get("ar", {}).get("published")))
# spot-check a product for the Arabic gallery metafield
prods = c.get_products()[:1]
if prods:
    mfs = c.get_metafields("products", prods[0]["id"])
    keys = {f"{m['namespace']}.{m['key']}" for m in mfs}
    print("has custom.pdp_images_ar:", "custom.pdp_images_ar" in keys)
PY
```

Only proceed to §7 once `ar` is published and the Arabic gallery field is present.

---

## 7. Install + run discovery on UAE

1. **Install:** open the UAE app in the Dev Dashboard → Home → Install on
   `rvgkkk-g3.myshopify.com` → approve the scopes.
2. **Discovery (first run):** open the embedded app → route `/app/discovery`.
   It introspects the UAE store's product metafield definitions + sample products
   and proposes a mapping snapshot (stored per-shop in the Prisma discovery table).
3. **Review the mapping:** confirm it picked up `custom.pdp_images`,
   `custom.pdp_images_ar`, and the Arabic translation surface; save it.

---

## 8. Verify the port worked

- **Product index** (`/app/products`): UAE products load with Arabic
  content/image **health indicators** populated (not all red).
- **Open a product** (`/app/products/$productId`): the **Arabic Content** tab shows
  registered `ar` translations; the **Locale Images** tab shows the Arabic gallery.
- **Bulk tools** (`/app/bulk`): Arabic bulk payload / image copy targets resolve.

If indicators are empty/red, Arabic isn't fully migrated yet — go back to §6.

---

## 9. Notes / rollback

- **Multi-tenant safety:** adding UAE does not affect saudi/kuwait — they are
  separate `SHOPIFY_APP_CONFIGS` entries + separate per-shop discovery rows.
- **Rollback:** uninstall the app from `rvgkkk-g3`, remove the UAE entry from
  `SHOPIFY_APP_CONFIGS`, and delete `shopify.app.uae.toml`. The UAE discovery row
  can be cleared from the DB if you want a clean re-discovery.
- **Secrets:** the UAE client secret lives only in Railway env (and your Dev
  Dashboard) — never commit it. `shopify.app.uae.toml` holds only the public
  `client_id`.

---

## Appendix — exact command sequence

```bash
cd C:\Users\narha\shopify-migrator\tara-product-editor
# (after creating shopify.app.uae.toml with the UAE client_id)
cmd /c shopify app config link --client-id PASTE_UAE_CLIENT_ID
cmd /c shopify app deploy --config uae --allow-updates --no-build
# add UAE to SHOPIFY_APP_CONFIGS on Railway -> redeploy -> check /healthz
# verify UAE Arabic readiness on the migrator side (§6)
# install on rvgkkk-g3 via Dev Dashboard, then open /app/discovery
```
