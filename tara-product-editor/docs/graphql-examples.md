# GraphQL Examples Used By Tara Product Editor

## Product Source Update

```graphql
mutation ProductUpdate($product: ProductUpdateInput!) {
  productUpdate(product: $product) {
    product { id }
    userErrors { field message }
  }
}
```

## Product Translation Digest Fetch

```graphql
query TranslatableResource($resourceId: ID!, $locale: String!) {
  translatableResource(resourceId: $resourceId) {
    resourceId
    translatableContent {
      key
      value
      digest
    }
    translations(locale: $locale) {
      key
      value
      outdated
    }
  }
}
```

## Batch Translation Lookup By IDs

```graphql
query TranslatableResourcesByIds($resourceIds: [ID!]!, $first: Int!, $locale: String!) {
  translatableResourcesByIds(resourceIds: $resourceIds, first: $first) {
    edges {
      node {
        resourceId
        translatableContent { key value digest }
        translations(locale: $locale) { key value outdated }
      }
    }
  }
}
```

## Register Arabic Translations

```graphql
mutation RegisterTranslations($resourceId: ID!, $translations: [TranslationInput!]!) {
  translationsRegister(resourceId: $resourceId, translations: $translations) {
    translations { key value locale }
    userErrors { field message }
  }
}
```

## Save Metafields

```graphql
mutation SetMetafields($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key }
    userErrors { field message }
  }
}
```

## Search Shopify Files

```graphql
query SearchFiles($first: Int!, $query: String) {
  files(first: $first, query: $query) {
    nodes {
      ... on MediaImage {
        id
        alt
        image { url width height }
      }
    }
  }
}
```
