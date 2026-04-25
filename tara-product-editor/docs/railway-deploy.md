# Railway Deployment

This app is prepared for a normal Node deployment on Railway.

It is not a Cloudflare Worker app.

## What Railway Should Run

- Build: handled by the included [Dockerfile](../Dockerfile)
- Start: handled by the included [Dockerfile](../Dockerfile)
- Health check path: `/healthz`

The container starts the app with:

```bash
npm run start:railway
```

That command runs Prisma migrations and then starts the built Node server.

## Railway Steps

### 1. Create the Railway project

1. Open Railway.
2. Create a new project.
3. Add a GitHub repo service pointing at this repository.
4. Set the service root to `tara-product-editor` if Railway asks for a root directory.

### 2. Add PostgreSQL

1. Add a PostgreSQL service to the same Railway project.
2. Keep it in the same environment as the app service.

## 3. Set app variables

On the app service, set:

- `SHOPIFY_API_KEY`
- `SHOPIFY_API_SECRET`
- `SHOPIFY_APP_URL`
- `SCOPES`
- `NODE_ENV`
- `DATABASE_URL`
- `HOST`

Use these values:

- `SHOPIFY_API_KEY`
  Your Shopify app client ID
- `SHOPIFY_API_SECRET`
  Your Shopify app client secret
- `SHOPIFY_APP_URL`
  Your Railway public domain, for example `https://tara-product-editor-production.up.railway.app`
- `SCOPES`
  `read_products,write_products,read_files,write_files,read_translations,write_translations,read_metaobjects`
- `NODE_ENV`
  `production`
- `DATABASE_URL`
  Set this as a variable reference to the Railway PostgreSQL service `DATABASE_URL`
- `HOST`
  `0.0.0.0`

## 4. Deploy once

After the variables are in place, let Railway deploy the app.

Then verify these URLs on the Railway domain:

- `/healthz`
- `/auth`

`/healthz` should return `ok`.

## 5. Update Shopify app config to the Railway domain

Before pushing the production app URL to Shopify:

1. Open [shopify.app.railway.template.toml](../shopify.app.railway.template.toml).
2. Replace `REPLACE_WITH_YOUR_RAILWAY_DOMAIN` with the real Railway HTTPS domain.
3. Copy that content into [shopify.app.toml](../shopify.app.toml).

This restores:

- production `application_url`
- production `redirect_urls`
- real webhook subscriptions for:
  - `app/uninstalled`
  - `app/scopes_update`

## 6. Release the production Shopify app config

From `tara-product-editor`, run:

```bash
cmd /c shopify app deploy --allow-updates --no-build
```

This updates the installed app’s Shopify-side configuration to the Railway domain.

## 7. Open the app in Shopify Admin

The app is already installed on Tara Saudi, so you usually do not need a new install.

After the config release:

1. Open Shopify Admin for Tara Saudi.
2. Go to `Apps`.
3. Open `Tara Product Editor`.

If Shopify asks to re-authenticate, complete that once.

## 8. Verify the app in production

Check:

1. `/app/discovery`
2. `/app/products`
3. Open one product
4. Save one Arabic title change
5. Save one product metafield
6. Save one locale image reorder

## Notes

- This app now expects PostgreSQL, not SQLite.
- The existing localhost config in [shopify.app.toml](../shopify.app.toml) is for local development only.
- Railway should use the Dockerfile in this repo; you do not need a Worker runtime.
