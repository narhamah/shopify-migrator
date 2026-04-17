/// <reference types="vite/client" />

declare namespace NodeJS {
  interface ProcessEnv {
    DATABASE_URL?: string;
    NODE_ENV?: string;
    SCOPES?: string;
    SHOPIFY_API_KEY?: string;
    SHOPIFY_API_SECRET?: string;
    SHOPIFY_APP_URL?: string;
    SHOPIFY_APP_CONFIGS?: string;
    SHOPIFY_ALLOWED_SHOPS?: string;
    SHOPIFY_CUSTOM_DOMAIN?: string;
  }
}
