# How To Use Tara Product Editor

## 1. Product Index

- Open `Products`.
- Search by free text or narrow by status, vendor, product type, and tag.
- Use the Arabic badge to spot products missing translated PDP content.
- Use the locale images badge to spot products missing Arabic galleries.
- Select products and jump into `Bulk Tools`.

## 2. Product Editor

### Core Product

- Edit English source fields.
- Save source content back to Shopify with `productUpdate`.

### Arabic Content

- Edit Arabic title, description, SEO, and relevant PDP metafield translations.
- The app fetches digests first and writes through `translationsRegister`.
- Rich text metafields are shown as Shopify rich text JSON because that is what Shopify stores and translates.

### Metafields

- Filter to populated metafields.
- Pin frequently used metafields.
- Save changed values through `metafieldsSet`.
- File-reference image fields are intentionally managed in `Locale Images`.

### Locale Images

- Manage English and Arabic PDP galleries visually.
- Reorder with drag and drop or with Up/Down buttons.
- Add images from current product media or from Shopify Files search results.
- Duplicate English to Arabic or Arabic back to English.
- Save exact order back to the current storage pattern.

### Raw / Developer

- Review resource IDs, discovered mappings, raw JSON, and translation resource metadata.

## 3. Bulk Tools

- Copy English images to Arabic for selected products.
- Clear Arabic images.
- Set a metafield across selected products.
- Apply a shared Arabic payload to selected products.
- Export JSON or CSV.
- Import structured JSON updates.
