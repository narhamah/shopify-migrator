import { describe, expect, it } from "vitest";

import {
  extractShopFromHost,
  extractShopFromSessionToken,
  parseShopifyAppConfigs,
  resolveShopifyAppConfigFromRequest,
} from "../app/shopify-app-config.server";

function toBase64Url(value: string): string {
  return Buffer.from(value, "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

describe("shopify app config resolver", () => {
  it("parses multi-store app configs", () => {
    const configs = parseShopifyAppConfigs({
      SHOPIFY_APP_URL: "https://tara-product-editor-production.up.railway.app",
      SCOPES: "read_products,write_products",
      SHOPIFY_APP_CONFIGS: JSON.stringify([
        {
          name: "saudi",
          apiKey: "saudi-key",
          apiSecretKey: "saudi-secret",
          allowedShops: ["xkgw0m-sm.myshopify.com"],
        },
        {
          name: "kuwait",
          apiKey: "kuwait-key",
          apiSecretKey: "kuwait-secret",
          allowedShops: ["977mp2-qa.myshopify.com"],
        },
      ]),
    });

    expect(configs.map((config) => config.name)).toEqual(["saudi", "kuwait"]);
    expect(configs[1]?.allowedShops).toEqual(["977mp2-qa.myshopify.com"]);
  });

  it("extracts a shop from the host parameter", () => {
    const host = toBase64Url("admin.shopify.com/store/977mp2-qa");
    expect(extractShopFromHost(host)).toBe("977mp2-qa.myshopify.com");
  });

  it("extracts a shop from the bearer session token", () => {
    const payload = toBase64Url(JSON.stringify({ dest: "https://977mp2-qa.myshopify.com" }));
    expect(extractShopFromSessionToken(`Bearer header.${payload}.signature`)).toBe(
      "977mp2-qa.myshopify.com",
    );
  });

  it("resolves the correct app config for Kuwait requests", () => {
    const configs = parseShopifyAppConfigs({
      SHOPIFY_APP_URL: "https://tara-product-editor-production.up.railway.app",
      SCOPES: "read_products,write_products",
      SHOPIFY_APP_CONFIGS: JSON.stringify([
        {
          name: "saudi",
          apiKey: "saudi-key",
          apiSecretKey: "saudi-secret",
          allowedShops: ["xkgw0m-sm.myshopify.com"],
        },
        {
          name: "kuwait",
          apiKey: "kuwait-key",
          apiSecretKey: "kuwait-secret",
          allowedShops: ["977mp2-qa.myshopify.com"],
        },
      ]),
    });

    const host = toBase64Url("admin.shopify.com/store/977mp2-qa");
    const request = new Request(`https://app.example.com/app?host=${host}`);

    expect(resolveShopifyAppConfigFromRequest(configs, request).name).toBe("kuwait");
  });
});
