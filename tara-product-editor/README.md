# Tara Product Editor

Embedded Shopify admin app for internal Tara content operations.

It is built for one operational goal: manage English product source content, Arabic Shopify translations, product metafields, and locale-specific PDP image galleries from one place instead of bouncing between Products, Metafields, and Translate & Adapt.

## Architecture Summary

- Frontend: React Router + React + TypeScript + Polaris
- Backend: Shopify app server with `@shopify/shopify-app-react-router`
- Data layer: Shopify Admin GraphQL for products, metafields, translations, and files
- Persistence: Prisma for Shopify sessions, cached discovery config, favorite metafields, and audit logs
- Discovery model: first-run live schema discovery that inspects product metafield definitions and sample products, then stores a mapping snapshot per shop

Core modules:

- Product index: searchable product table with Arabic content/image health indicators
- Product editor: Core Product, Arabic Content, Metafields, Locale Images, Raw/Developer tabs
- Bulk tools: image copy/clear, shared metafield updates, Arabic bulk payloads, JSON import, JSON/CSV export
- Discovery: live schema introspection and reviewable mapping output

## Live Schema Discovered

The app was seeded against the live Saudi store schema discovered on March 21, 2026.

Key findings:

- Main English PDP gallery: `custom.pdp_images` (`list.file_reference`)
- Explicit Arabic PDP gallery: `custom.pdp_images_ar` (`list.file_reference`)
- Legacy Arabic image field still present: `custom.arabic_images` (`json`)
- Core product translatable keys: `title`, `body_html`, `handle`, `product_type`, `meta_title`, `meta_description`
- PDP content metafields such as `short_description`, `tagline`, `key_benefits_*`, `clinical_results_*`, `how_to_use_*`, `whats_inside_*`, `fragrance_*`, and `free_of_*` are individually translatable through Shopify translations

See [tara-store-mapping.json](./config/tara-store-mapping.json).

## Required Scopes

Minimum scopes currently configured:

- `read_products`
- `write_products`
- `read_files`
- `write_files`
- `read_translations`
- `write_translations`
- `read_metaobjects`

## Step-By-Step Setup

This section is written as a literal checklist.

### Before you start

You need:

- access to the Shopify Partner / Dev Dashboard
- access to the target Shopify store
- Shopify CLI installed locally
- Node.js installed locally

Important:

- Create this as a Shopify Dev Dashboard app
- Do **not** create this as a Shopify Admin "custom app" inside the store admin
- Shopify Admin custom apps are not the same thing, and they are not the embedded app flow this codebase uses

### Step 1. Create the app in Shopify Partner / Dev Dashboard

1. Open the Shopify Partner / Dev Dashboard.
2. Go to `Apps`.
3. Click `Create app`.
4. Create a new app named `Tara Product Editor`.
5. Make sure you are creating the app in the Partner / Dev Dashboard, not in the Shopify store admin.

### Step 2. Understand whether you need a distribution step

As of February 20, 2026, Shopify moved app distribution out of the app creation flow in Partner Dashboard.

There are now two different cases:

#### Case A. You created the app in a Merchant organization for your own store

If you created the app from the store's own Dev Dashboard context, you usually will **not** see a `Distribution` section.

That is expected.

In this case:

- the app is already scoped to your own store
- there is no separate distribution step
- you can skip the old `Distribution` tab instructions

#### Case B. You created the app in a Partner organization and want a custom install link

If you created the app from a Partner organization, the distribution flow is now reached from the Partner Dashboard left sidebar, not from an in-app tab.

Use this path:

1. Open Shopify Partner Dashboard.
2. Go to `App distribution`.
3. Select your app from the list.
4. Click `Choose distribution`.
5. Select `Custom distribution`.
6. Enter the target store domain.
7. Generate the install link.
8. Keep that install link for installation.

### Step 3. Copy the app credentials from Shopify

1. In the app dashboard, open the API credentials / app credentials page.
2. Copy the app client ID.
3. Copy the app client secret.

In this codebase:

- app client ID goes into `SHOPIFY_API_KEY`
- app client secret goes into `SHOPIFY_API_SECRET`

Where to find them:

- `SHOPIFY_API_KEY`
  Shopify Dev Dashboard -> Apps -> Tara Product Editor -> Settings -> Client ID
- `SHOPIFY_API_SECRET`
  Shopify Dev Dashboard -> Apps -> Tara Product Editor -> Settings -> Client secret

Important:

- `SHOPIFY_API_SECRET` is sensitive
- do not put it in frontend code
- do not commit it to git
- rotate it immediately if it is exposed

### Step 4. Create the local environment file

1. In [tara-product-editor](./), copy [`.env.example`](./.env.example) to `.env`.
2. Fill it in like this:

```env
SHOPIFY_API_KEY=your_app_client_id
SHOPIFY_API_SECRET=your_app_client_secret
SHOPIFY_APP_URL=https://your-actual-app-url
SHOPIFY_APP_CONFIGS=
SCOPES=read_products,write_products,read_files,write_files,read_translations,write_translations,read_metaobjects
SHOPIFY_CUSTOM_DOMAIN=
NODE_ENV=development
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/tara_product_editor?schema=public
```

Notes:

- `SHOPIFY_API_KEY`
  This comes from Shopify Dev Dashboard -> your app -> Settings -> Client ID
- `SHOPIFY_API_SECRET`
  This comes from Shopify Dev Dashboard -> your app -> Settings -> Client secret
- `SHOPIFY_APP_URL`
  This is not a Shopify key.
  It is the real base URL where Shopify reaches your app.
  For normal local embedded-app development, this is usually the public tunnel URL created by `shopify app dev`.
  If you explicitly use localhost-based development, use the actual HTTPS localhost URL that Shopify CLI gives you.
  If you deploy the app, use the deployed HTTPS origin.
- `SCOPES`
  This is not a secret.
  It is the list of Shopify permissions the app asks for.
  Use the value already in `.env.example` unless you are intentionally changing app permissions.
- `SHOPIFY_CUSTOM_DOMAIN`
  This is optional.
  Leave it blank unless you know you need a custom shop domain configuration.
- `SHOPIFY_APP_CONFIGS`
  This is optional.
  Use it when you want one deployment to serve multiple Shopify app IDs for multiple stores.
  It must be a JSON array where each object includes `name`, `apiKey`, `apiSecretKey`, and `allowedShops`.
  Example:
  `[{"name":"saudi","apiKey":"...","apiSecretKey":"...","allowedShops":["xkgw0m-sm.myshopify.com"]},{"name":"kuwait","apiKey":"...","apiSecretKey":"...","allowedShops":["977mp2-qa.myshopify.com"]}]`
- `DATABASE_URL`
  This app now expects PostgreSQL.
  For local development, use a local Postgres instance or a dedicated non-production Postgres database.
  For Railway, point this to the Railway PostgreSQL service.

There is no manual long-lived Admin API access token in this setup:

- this app is using the embedded app auth flow
- the server code authenticates with Shopify at runtime
- you do not need to paste a `shpat_...` token into `.env` for this app

### Step 5. Confirm the redirect URLs

Open [shopify.app.toml](./shopify.app.toml).

The redirect URLs in [shopify.app.toml](./shopify.app.toml) must use the same base URL as `SHOPIFY_APP_URL`.

If you are maintaining more than one Shopify app record, create one file per app, for example:

- [shopify.app.saudi.toml](./shopify.app.saudi.toml)
- [shopify.app.kuwait.toml](./shopify.app.kuwait.toml)

Then deploy the exact app you want with `shopify app deploy --config saudi` or `shopify app deploy --config kuwait`.

Examples:

- if `SHOPIFY_APP_URL=https://abc123.trycloudflare.com`, use:
  - `https://abc123.trycloudflare.com/auth/callback`
  - `https://abc123.trycloudflare.com/auth/shopify/callback`
  - `https://abc123.trycloudflare.com/api/auth/callback`
- if `SHOPIFY_APP_URL=https://localhost:3000`, use:
  - `https://localhost:3000/auth/callback`
  - `https://localhost:3000/auth/shopify/callback`
  - `https://localhost:3000/api/auth/callback`

### Step 6. Install the project dependencies

From [tara-product-editor](./), run:

```bash
cmd /c npm install
```

### Step 7. Create the local database

From [tara-product-editor](./), run:

```bash
cmd /c npx prisma migrate dev --name init
```

This creates:

- the Prisma client
- the session tables
- the discovery config table
- the favorite metafields table
- the audit log table

### Step 8. Link the local codebase to the Shopify app

From [tara-product-editor](./), run:

```bash
cmd /c shopify app config link --client-id YOUR_CLIENT_ID
```

What to expect:

1. Shopify CLI will link `shopify.app.toml` to your existing `Tara Product Editor` app.
2. If Shopify CLI asks you to log in, complete the browser login.

### Step 9. Release the localhost app config to Shopify

From [tara-product-editor](./), run:

```bash
cmd /c shopify app deploy --allow-updates --no-build
```

Important:

- this project is configured for `https://localhost:3000`
- for a localhost release, keep the minimal `[webhooks]` block in [shopify.app.toml](./shopify.app.toml) but do not add localhost webhook subscriptions
- Shopify can release the app with a localhost app URL, but live webhook subscriptions cannot target localhost

### Step 10. Generate the localhost certificate

If you do not already have `.shopify/localhost.pem` and `.shopify/localhost-key.pem`, generate them with `mkcert`.

The files must end up here:

- [tara-product-editor/.shopify/localhost.pem](./.shopify/localhost.pem)
- [tara-product-editor/.shopify/localhost-key.pem](./.shopify/localhost-key.pem)

This project’s Vite config is already wired to use those files automatically.

### Step 11. Start the app locally

Do **not** use `cmd /c npm run dev` against this store.

On this store, Shopify CLI dev preview fails with:

- `Shop is not configured for app development`

That is a Shopify platform limitation for non-dev-store previews.

Instead, from [tara-product-editor](./), run:

```bash
cmd /c npx prisma migrate deploy
cmd /c npm exec react-router dev
```

This starts the embedded app directly at:

- `https://localhost:3000`

### Step 12. Install the app on the store

If you created the app in the store’s own Dev Dashboard context:

1. Open the app in Shopify Dev Dashboard.
2. Go to `Home`.
3. Install or open the app from there.
4. Approve the requested app permissions.

If you created the app in a Partner org with custom distribution:

1. Open the custom distribution install link.
2. Choose the target store if Shopify asks.
3. Approve the requested app permissions.

### Step 13. Open the app in Shopify Admin

After install:

1. Open the target Shopify Admin.
2. Go to `Apps`.
3. Open `Tara Product Editor`.

On the first real load, the app will:

- authenticate
- inspect live product metafield definitions
- cache a discovery mapping for the shop
- load the product editor UI around the detected schema

### Step 11. Verify the app is working

Check these pages:

1. `/app`
   You should see the dashboard.
2. `/app/products`
   You should see the product index with Arabic and locale-image indicators.
3. Open one product.
   You should see:
   - `Core Product`
   - `Arabic Content`
   - `Metafields`
   - `Locale Images`
   - `Raw / Developer`
4. `/app/discovery`
   You should see the live mapping snapshot.

### Step 12. Common setup problems

If install fails or OAuth loops:

1. Check that `SHOPIFY_API_KEY` and `SHOPIFY_API_SECRET` match the app you created.
2. Check that `SHOPIFY_APP_URL` matches the actual running app URL.
3. Check that the redirect URLs in [shopify.app.toml](./shopify.app.toml) match the same base URL exactly.
4. Restart the dev server after changing `.env` or `shopify.app.toml`.

If the app opens but data does not load:

1. Confirm the app has the scopes listed in the `Required Scopes` section.
2. Reinstall the app if scopes changed after the first install.

### Step 13. Normal local workflow after the first install

For future local runs, the normal sequence is:

```bash
cmd /c npm install
cmd /c npx prisma migrate dev
cmd /c npm run dev
```

## Verification Completed

The current codebase was verified with:

```bash
cmd /c npm run typecheck
cmd /c npm run build
```

## Production Notes

- This app is now PostgreSQL-first. Railway should use PostgreSQL, not SQLite.
- The app writes Arabic through Shopify translations APIs. It does not touch theme locale JSON files.
- The current Locale Images tab supports:
  - product media
  - searching existing Shopify Files
  - reordering
  - copying English to Arabic
  - saving to both translated and explicit Arabic image storage
- Direct file upload into Shopify Files is not implemented yet. The current workflow assumes images already exist in product media or Shopify Files.

## How To Use

Short operator guide: [docs/how-to-use.md](./docs/how-to-use.md)

## GraphQL Examples

Query and mutation examples used by the app: [docs/graphql-examples.md](./docs/graphql-examples.md)

## Migration Notes

Future cleanup path away from dual metafield image storage: [docs/migration-notes.md](./docs/migration-notes.md)

## Railway Deploy

Step-by-step Railway instructions: [docs/railway-deploy.md](./docs/railway-deploy.md)
