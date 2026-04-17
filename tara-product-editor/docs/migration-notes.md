# Migration Notes

## Current State

The live store currently carries three locale-image patterns:

- `custom.pdp_images` as the English source gallery
- Arabic translation on `custom.pdp_images` via Shopify Translations API
- `custom.pdp_images_ar` as an explicit Arabic gallery metafield

There is also a legacy `custom.arabic_images` JSON field.

## Recommended Next Architecture

Preferred end state:

1. Keep one canonical gallery metafield: `custom.pdp_images`
2. Store Arabic gallery through Shopify translations on that metafield resource
3. Stop writing `custom.pdp_images_ar`
4. Remove `custom.arabic_images`

Benefits:

- one canonical source field
- no duplicate locale image storage
- cleaner theme logic
- fewer content drift cases
- simpler bulk operations

## Safe Transition Plan

1. Audit products where `custom.pdp_images_ar` differs from the Arabic translation on `custom.pdp_images`.
2. Choose a canonical Arabic source for each product.
3. Backfill Arabic translations on `custom.pdp_images`.
4. Update the theme to read only the translated `custom.pdp_images` value for Arabic.
5. Keep `custom.pdp_images_ar` in read-only fallback mode during rollout.
6. Remove writes to `custom.pdp_images_ar` after theme verification.
7. Remove the legacy field after a full content audit.

## Current App Behavior

Tara Product Editor intentionally supports both Arabic storage targets so migration can happen without destructive overwrites.
