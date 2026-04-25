# Tara USA Destination Setup

This setup is for the migration pipeline custom app/token flow.

It is not the embedded `Tara Product Editor`.

## Goal

Prepare the new USA Shopify store as another destination site in this workspace, using:

- destination-scoped data under `data/usa/`
- English-only build flow
- the same Shopify custom app pattern as Saudi: `Migration (receive)`

## 1. Create the Shopify custom app on Tara USA

In the USA store admin:

1. Go to `Settings -> Apps and sales channels -> Develop apps`.
2. Enable custom app development if Shopify asks.
3. Create a new app.
4. Name it exactly:
   - `Migration (receive)`
5. Configure Admin API scopes.
6. Install the app.
7. Copy the Admin API access token.

This token becomes `DEST_ACCESS_TOKEN` in the migration `.env`.

## 2. Admin API scopes

Replicate the Saudi `Migration (receive)` app scope set exactly.

Use this exact comma-separated scope string:

```text
read_price_rules,write_price_rules,read_discounts,write_discounts,read_discounts_allocator_functions,write_discounts_allocator_functions,read_files,write_files,write_inventory,read_inventory,read_locales,write_locales,read_locations,read_metaobject_definitions,write_metaobject_definitions,read_metaobjects,write_metaobjects,read_online_store_navigation,write_online_store_navigation,read_products,write_products,read_publications,write_publications,read_content,write_content,read_themes,write_themes,read_translations,write_translations
```

Even though USA is English-only right now, keep the same scope footprint as Saudi so the migration app stays operationally consistent across stores.

## 3. Add the USA store to this workspace

Use [usa-destination.env.example](C:/Users/narha/shopify-migrator/usa-destination.env.example) as the profile template.

Key values:

- `DEST_NAME=usa`
- `DEST_SHOP_URL=<confirmed permanent USA myshopify domain>`
- `DEST_ACCESS_TOKEN=<custom app admin API token>`
- `MAGENTO_STORE_CODE=us-en`

`DEST_NAME=usa` is important because it scopes runtime files into `data/usa/` and avoids collisions with Kuwait or other destinations.

## 4. Run the USA build

English-only destination path:

```powershell
python setup_store.py
python prepare_import.py
python build_site.py --lang en
```

The pipeline now skips locale sync during `post_migration.py` when `--lang en` is used, so it will not auto-enable Arabic on the USA store.

## 5. Notes on the shop domain

Two domains were seen during store creation:

- `ixkpkn-za.myshopify.com`
- `tara-usa-2.myshopify.com`

Use the one Shopify shows as the permanent admin `myshopify.com` domain in the USA store settings.

Do not guess this in the live `.env`.
