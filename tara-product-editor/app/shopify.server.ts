import "@shopify/shopify-app-react-router/server/adapters/node";

import { ApiVersion, shopifyApp } from "@shopify/shopify-app-react-router/server";
import { PrismaSessionStorage } from "@shopify/shopify-app-session-storage-prisma";

import prisma from "./db.server";
import {
  parseShopifyAppConfigs,
  resolveShopifyAppConfig,
  resolveShopifyAppConfigFromLoginRequest,
  resolveShopifyAppConfigFromRequest,
  type ShopifyAppConfigRecord,
} from "./shopify-app-config.server";

const configuredApps = parseShopifyAppConfigs();
const sessionStorage = new PrismaSessionStorage(prisma);
const appInstances = new Map<string, ReturnType<typeof shopifyApp>>();

function getShopifyForConfig(config: ShopifyAppConfigRecord) {
  const existingApp = appInstances.get(config.apiKey);
  if (existingApp) {
    return existingApp;
  }

  const app = shopifyApp({
    apiKey: config.apiKey,
    apiSecretKey: config.apiSecretKey,
    appUrl: config.appUrl,
    authPathPrefix: "/auth",
    scopes: config.scopes,
    apiVersion: ApiVersion.October25,
    isEmbeddedApp: true,
    sessionStorage,
    ...(config.customShopDomains.length > 0
      ? { customShopDomains: config.customShopDomains }
      : {}),
  });

  appInstances.set(config.apiKey, app);
  return app;
}

function getShopifyForRequest(request: Request) {
  return getShopifyForConfig(resolveShopifyAppConfigFromRequest(configuredApps, request));
}

function getShopifyForShop(shop: string) {
  return getShopifyForConfig(resolveShopifyAppConfig(configuredApps, [shop]));
}

async function getShopifyForLoginRequest(request: Request) {
  return getShopifyForConfig(await resolveShopifyAppConfigFromLoginRequest(configuredApps, request));
}

export function getShopifyApiKeyForRequest(request: Request): string {
  return resolveShopifyAppConfigFromRequest(configuredApps, request).apiKey;
}

const shopify = {
  sessionStorage,
  authenticate: {
    admin(request: Request) {
      return getShopifyForRequest(request).authenticate.admin(request);
    },
    flow(request: Request) {
      return getShopifyForRequest(request).authenticate.flow(request);
    },
    fulfillmentService(request: Request) {
      return getShopifyForRequest(request).authenticate.fulfillmentService(request);
    },
    pos(request: Request) {
      return getShopifyForRequest(request).authenticate.pos(request);
    },
    webhook(request: Request) {
      return getShopifyForRequest(request).authenticate.webhook(request);
    },
  },
  unauthenticated: {
    admin(shop: string) {
      return getShopifyForShop(shop).unauthenticated.admin(shop);
    },
    storefront(shop: string) {
      return getShopifyForShop(shop).unauthenticated.storefront(shop);
    },
  },
  async login(request: Request) {
    return (await getShopifyForLoginRequest(request)).login(request);
  },
  registerWebhooks(options: Parameters<ReturnType<typeof shopifyApp>["registerWebhooks"]>[0]) {
    return getShopifyForShop(options.session.shop).registerWebhooks(options);
  },
  addDocumentResponseHeaders(request: Request, headers: Headers) {
    return getShopifyForRequest(request).addDocumentResponseHeaders(request, headers);
  },
};

export default shopify;
export const authenticate = shopify.authenticate;
export const unauthenticated = shopify.unauthenticated;
export const login = shopify.login;
export const registerWebhooks = shopify.registerWebhooks;
export const addDocumentResponseHeaders = shopify.addDocumentResponseHeaders;
