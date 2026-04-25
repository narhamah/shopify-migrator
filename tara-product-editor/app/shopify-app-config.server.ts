const MYSHOPIFY_DOMAIN_SUFFIX = ".myshopify.com";

type EnvRecord = Record<string, string | undefined>;

type RawShopifyAppConfig = {
  name?: string;
  apiKey?: string;
  clientId?: string;
  apiSecretKey?: string;
  apiSecret?: string;
  clientSecret?: string;
  appUrl?: string;
  scopes?: string[] | string;
  customShopDomains?: string[] | string;
  allowedShops?: string[] | string;
};

type ConfigDefaults = {
  appUrl?: string;
  scopes: string[];
  customShopDomains: string[];
  allowedShops: string[];
};

export type ShopifyAppConfigRecord = {
  name: string;
  apiKey: string;
  apiSecretKey: string;
  appUrl: string;
  scopes: string[];
  customShopDomains: string[];
  allowedShops: string[];
  allowedStoreHandles: string[];
};

function unique(values: string[]): string[] {
  return [...new Set(values)];
}

function splitCsv(value?: string[] | string): string[] {
  if (Array.isArray(value)) {
    return value.map((entry) => entry.trim()).filter(Boolean);
  }

  if (!value) {
    return [];
  }

  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function normalizeShopDomain(value?: string | null): string | null {
  if (!value) {
    return null;
  }

  const normalized = value
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/\/.*$/, "");

  if (!normalized.endsWith(MYSHOPIFY_DOMAIN_SUFFIX)) {
    return null;
  }

  if (!/^[a-z0-9][a-z0-9-]*\.myshopify\.com$/.test(normalized)) {
    return null;
  }

  return normalized;
}

function normalizeCustomDomain(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/\/.*$/, "");
}

function normalizeCustomShopDomains(value?: string[] | string): string[] {
  return unique(splitCsv(value).map(normalizeCustomDomain).filter(Boolean));
}

function shopToHandle(shop: string): string {
  return shop.slice(0, -MYSHOPIFY_DOMAIN_SUFFIX.length);
}

function decodeBase64Url(value?: string | null): string | null {
  if (!value) {
    return null;
  }

  try {
    const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
    return Buffer.from(padded, "base64").toString("utf8");
  } catch {
    return null;
  }
}

export function extractShopFromHost(hostParam?: string | null): string | null {
  const decoded = decodeBase64Url(hostParam);
  if (!decoded) {
    return null;
  }

  const directDomain = decoded.match(/([a-z0-9][a-z0-9-]*\.myshopify\.com)/i)?.[1] ?? null;
  const normalizedDomain = normalizeShopDomain(directDomain);
  if (normalizedDomain) {
    return normalizedDomain;
  }

  const storeHandle =
    decoded.match(/\/store\/([a-z0-9][a-z0-9-]*)/i)?.[1] ??
    decoded.match(/[?&]store=([a-z0-9][a-z0-9-]*)/i)?.[1] ??
    null;

  return storeHandle ? `${storeHandle.toLowerCase()}${MYSHOPIFY_DOMAIN_SUFFIX}` : null;
}

export function extractShopFromSessionToken(authorizationHeader?: string | null): string | null {
  const match = authorizationHeader?.match(/^Bearer\s+(.+)$/i);
  if (!match) {
    return null;
  }

  const [, token] = match;
  const payloadSegment = token.split(".")[1];
  const payloadJson = decodeBase64Url(payloadSegment);
  if (!payloadJson) {
    return null;
  }

  try {
    const payload = JSON.parse(payloadJson) as { dest?: string; iss?: string };
    return normalizeShopDomain(payload.dest ?? payload.iss ?? null);
  } catch {
    return null;
  }
}

function parseMultiConfig(rawValue: string): RawShopifyAppConfig[] {
  let parsed: unknown;

  try {
    parsed = JSON.parse(rawValue);
  } catch (error) {
    const message = error instanceof Error ? error.message : "unknown parse error";
    throw new Error(`SHOPIFY_APP_CONFIGS is not valid JSON: ${message}`);
  }

  if (!Array.isArray(parsed)) {
    throw new Error("SHOPIFY_APP_CONFIGS must be a JSON array");
  }

  return parsed as RawShopifyAppConfig[];
}

function normalizeAppConfig(
  rawConfig: RawShopifyAppConfig,
  defaults: ConfigDefaults,
  index: number,
): ShopifyAppConfigRecord {
  const apiKey = rawConfig.apiKey?.trim() || rawConfig.clientId?.trim() || "";
  const apiSecretKey =
    rawConfig.apiSecretKey?.trim() ||
    rawConfig.apiSecret?.trim() ||
    rawConfig.clientSecret?.trim() ||
    "";
  const appUrl = rawConfig.appUrl?.trim() || defaults.appUrl || "";
  const scopes = unique(splitCsv(rawConfig.scopes ?? defaults.scopes));
  const customShopDomains = normalizeCustomShopDomains(
    rawConfig.customShopDomains ?? defaults.customShopDomains,
  );
  const allowedShops = unique(
    splitCsv(rawConfig.allowedShops ?? defaults.allowedShops)
      .map((shop) => normalizeShopDomain(shop))
      .filter((shop): shop is string => Boolean(shop)),
  );

  if (!apiKey) {
    throw new Error(`Missing apiKey/clientId for Shopify app config at index ${index}`);
  }

  if (!apiSecretKey) {
    throw new Error(`Missing apiSecretKey/clientSecret for Shopify app config at index ${index}`);
  }

  if (!appUrl) {
    throw new Error(`Missing appUrl for Shopify app config at index ${index}`);
  }

  if (scopes.length === 0) {
    throw new Error(`Missing scopes for Shopify app config at index ${index}`);
  }

  return {
    name: rawConfig.name?.trim() || `app-${index + 1}`,
    apiKey,
    apiSecretKey,
    appUrl,
    scopes,
    customShopDomains,
    allowedShops,
    allowedStoreHandles: allowedShops.map(shopToHandle),
  };
}

export function parseShopifyAppConfigs(env: EnvRecord = process.env): ShopifyAppConfigRecord[] {
  const defaults: ConfigDefaults = {
    appUrl: env.SHOPIFY_APP_URL?.trim(),
    scopes: splitCsv(env.SCOPES),
    customShopDomains: normalizeCustomShopDomains(env.SHOPIFY_CUSTOM_DOMAIN),
    allowedShops: splitCsv(env.SHOPIFY_ALLOWED_SHOPS)
      .map((shop) => normalizeShopDomain(shop))
      .filter((shop): shop is string => Boolean(shop)),
  };

  const rawMultiConfig = env.SHOPIFY_APP_CONFIGS?.trim();
  const configs = rawMultiConfig
    ? parseMultiConfig(rawMultiConfig).map((rawConfig, index) =>
        normalizeAppConfig(rawConfig, defaults, index),
      )
    : [
        normalizeAppConfig(
          {
            name: "default",
            apiKey: env.SHOPIFY_API_KEY,
            apiSecretKey: env.SHOPIFY_API_SECRET,
            appUrl: env.SHOPIFY_APP_URL,
            scopes: env.SCOPES,
            customShopDomains: env.SHOPIFY_CUSTOM_DOMAIN,
            allowedShops: env.SHOPIFY_ALLOWED_SHOPS,
          },
          defaults,
          0,
        ),
      ];

  if (configs.length > 1) {
    for (const config of configs) {
      if (config.allowedShops.length === 0) {
        throw new Error(
          `Shopify app config "${config.name}" is missing allowedShops. ` +
            "Multi-app mode requires an explicit store mapping.",
        );
      }
    }
  }

  const seenShops = new Map<string, string>();
  for (const config of configs) {
    for (const shop of config.allowedShops) {
      const previous = seenShops.get(shop);
      if (previous) {
        throw new Error(`Shop "${shop}" is assigned to both "${previous}" and "${config.name}"`);
      }
      seenShops.set(shop, config.name);
    }
  }

  return configs;
}

export function resolveShopifyAppConfig(
  configs: ShopifyAppConfigRecord[],
  shopCandidates: Array<string | null | undefined>,
): ShopifyAppConfigRecord {
  const normalizedCandidates = unique(
    shopCandidates
      .map((candidate) => normalizeShopDomain(candidate))
      .filter((candidate): candidate is string => Boolean(candidate)),
  );

  for (const shop of normalizedCandidates) {
    const match = configs.find((config) => config.allowedShops.includes(shop));
    if (match) {
      return match;
    }
  }

  if (normalizedCandidates.length > 0 && configs.length > 1) {
    throw new Error(
      `No Shopify app configuration matched shop candidate(s): ${normalizedCandidates.join(", ")}`,
    );
  }

  return configs[0];
}

export function resolveShopifyAppConfigFromRequest(
  configs: ShopifyAppConfigRecord[],
  request: Request,
): ShopifyAppConfigRecord {
  const url = new URL(request.url);
  return resolveShopifyAppConfig(configs, [
    request.headers.get("x-shopify-shop-domain"),
    url.searchParams.get("shop"),
    extractShopFromHost(url.searchParams.get("host")),
    extractShopFromSessionToken(request.headers.get("authorization")),
  ]);
}

export async function resolveShopifyAppConfigFromLoginRequest(
  configs: ShopifyAppConfigRecord[],
  request: Request,
): Promise<ShopifyAppConfigRecord> {
  try {
    return resolveShopifyAppConfigFromRequest(configs, request);
  } catch (error) {
    if (request.method.toUpperCase() !== "POST") {
      throw error;
    }
  }

  const formData = await request.clone().formData().catch(() => null);
  const requestedShop = formData?.get("shop");
  return resolveShopifyAppConfig(configs, [
    typeof requestedShop === "string" ? requestedShop : null,
  ]);
}
