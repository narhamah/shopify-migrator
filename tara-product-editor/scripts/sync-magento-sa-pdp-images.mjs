import { PrismaClient } from "@prisma/client";
import { chromium } from "playwright";
import fs from "node:fs/promises";
import path from "node:path";

const API_VERSION = "2025-10";
const COLLECTION_URL = "https://taraformula.com/sa-en/hair-care";
const REPORT_PATH = path.resolve(process.cwd(), "..", "temp", "sa-magento-sync-report.json");

const HANDLE_MAP = {
  "black-garlic-ceramides-system": "hair-strength-system",
  "black-garlic-ceramides-shampoo": "invigorating-shampoo",
  "black-garlic-ceramides-hair-mask": "repairing-hair-mask",
  "black-garlic-ceramides-serum": "strengthening-scalp-serum",
  "date-multivitamin-hair-wellness-system": "hair-wellness-system",
  "date-multivitamin-nourishing-shampoo": "nourishing-shampoo",
  "date-multivitamin-hydrating-conditioner": "hydrating-conditioner",
  "date-multivitamin-rejuvenating-scalp-serum": "rejuvenating-scalp-serum",
  "strawberry-nmf-nurture-shampoo": "nurture-shampoo",
  "strawberry-nmf-nurture-conditioner": "nurture-conditioner",
  "strawberry-nmf-nurture-leave-in-conditioner": "nurture-leave-in-conditioner",
  "sage-multivitamin-revitalizing-shampoo": "revitalizing-shampoo",
  "sage-multivitamin-replenishing-conditioner": "replenishing-conditioner",
  "sage-multivitamin-age-well-scalp-support-serum": "scalp-support-serum",
  "onion-remedy-strengthening-shampoo": "volumizing-shampoo",
  "onion-remedy-rejuvenating-conditioner": "thickening-conditioner",
  "onion-remedy-follicle-stimulating-concentrate": "follicle-stimulating-scalp-serum",
  "rosemary-peptides-scalp-prep-shampoo": "scalp-prep-shampoo",
  "rosemary-peptides-strand-thicken-conditioner": "strand-thicken-conditioner",
  "rosemary-peptides-follicle-boost-serum": "follicle-boost-serum",
  "detox-charcoal-salicylic-exfoliating-shampoo": "charcoal-salicylic-exfoliating-shampoo",
  "detox-ghassoul-avocado-smoothing-conditioner": "ghassoul-avocado-smoothing-conditioner",
  "detox-cactus-red-seaweed-scalp-serum": "cactus-red-seaweed-scalp-serum",
};

const PRODUCT_QUERY = `#graphql
  query TargetProducts($first: Int!) {
    products(first: $first, sortKey: TITLE) {
      nodes {
        id
        title
        handle
        englishGallery: metafield(namespace: "custom", key: "pdp_images") {
          id
          compareDigest
          value
          type
          references(first: 50) {
            nodes {
              ... on MediaImage {
                id
                image {
                  url
                  width
                  height
                }
              }
            }
          }
        }
        arabicGallery: metafield(namespace: "custom", key: "pdp_images_ar") {
          id
          compareDigest
          value
          type
          references(first: 50) {
            nodes {
              ... on MediaImage {
                id
                image {
                  url
                  width
                  height
                }
              }
            }
          }
        }
      }
    }
  }
`;

const SET_METAFIELDS_MUTATION = `#graphql
  mutation SetMetafields($metafields: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $metafields) {
      metafields {
        id
        namespace
        key
        compareDigest
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const STAGED_UPLOADS_CREATE_MUTATION = `#graphql
  mutation StagedUploadsCreate($input: [StagedUploadInput!]!) {
    stagedUploadsCreate(input: $input) {
      stagedTargets {
        url
        resourceUrl
        parameters {
          name
          value
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const FILE_CREATE_MUTATION = `#graphql
  mutation FileCreate($files: [FileCreateInput!]!) {
    fileCreate(files: $files) {
      files {
        ... on MediaImage {
          id
          fileStatus
          image {
            url
          }
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const FILE_NODE_QUERY = `#graphql
  query FileNode($id: ID!) {
    node(id: $id) {
      ... on MediaImage {
        id
        fileStatus
        image {
          url
          width
          height
        }
      }
    }
  }
`;

const FILE_SEARCH_QUERY = `#graphql
  query FileSearch($first: Int!, $query: String!) {
    files(first: $first, query: $query) {
      nodes {
        ... on MediaImage {
          id
          image {
            url
            width
            height
          }
        }
      }
    }
  }
`;

const PRODUCT_GALLERY_SELECTOR = '[class*="carousel-thumbnailList"] .slick-slide:not(.slick-cloned)';

const dryRun = process.argv.includes("--dry-run") || process.env.DRY_RUN === "1";
const limit = Number(process.env.SYNC_LIMIT || "0") || null;
const syncSlugs = (process.env.SYNC_SLUGS || "")
  .split(",")
  .map((slug) => slug.trim())
  .filter(Boolean);

const prisma = new PrismaClient();

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

async function main() {
  const { shop, accessToken } = await resolveShopifySession();
  const browser = await chromium.launch({ headless: true, channel: "msedge" });

  try {
    const page = await browser.newPage({ viewport: { width: 1600, height: 2000 } });
    const collectionProducts = buildTargetMagentoProducts();
    const targetProducts = limit ? collectionProducts.slice(0, limit) : collectionProducts;

    const magentoProducts = [];
    for (const product of targetProducts) {
      try {
        const payload = await scrapeMagentoProduct(page, product.url);
        magentoProducts.push({
          ...product,
          ...payload,
          shopifyHandle: HANDLE_MAP[product.slug],
        });
        console.log(`Scraped Magento PDP: ${product.slug}`);
      } catch (error) {
        report.skipped.push({
          slug: product.slug,
          reason: `Failed to scrape Magento PDP: ${error.message}`,
        });
        console.warn(`Skipped ${product.slug}: ${error.message}`);
      }
    }

    const shopifyProducts = await fetchShopifyProducts(shop, accessToken);
    const shopifyByHandle = new Map(shopifyProducts.map((product) => [product.handle, product]));
    const knownImages = buildKnownImageIndex(shopifyProducts);

    const report = {
      generatedAt: new Date().toISOString(),
      dryRun,
      updated: [],
      skipped: [],
      uploaded: [],
      missingMagento: [],
    };

    for (const magentoProduct of magentoProducts) {
      const shopifyProduct = shopifyByHandle.get(magentoProduct.shopifyHandle);
      if (!shopifyProduct) {
        report.skipped.push({
          slug: magentoProduct.slug,
          reason: `No Shopify product found for handle ${magentoProduct.shopifyHandle}`,
        });
        continue;
      }

      const englishDesired = await resolveDesiredGallery({
        locale: "english",
        desiredImages: magentoProduct.englishImages,
        shop,
        accessToken,
        knownImages,
        report,
      });
      const arabicDesired = await resolveDesiredGallery({
        locale: "arabic",
        desiredImages: magentoProduct.arabicImages,
        shop,
        accessToken,
        knownImages,
        report,
      });

      const englishCurrent = shopifyProduct.englishGallery.images.map((image) => image.id);
      const arabicCurrent = shopifyProduct.arabicGallery.images.map((image) => image.id);
      const englishNext = englishDesired.map((image) => image.id);
      const arabicNext = arabicDesired.map((image) => image.id);
      const canUpdateEnglish = englishNext.length > 0 || englishCurrent.length === 0;
      const canUpdateArabic = arabicNext.length > 0 || arabicCurrent.length === 0;

      const englishChanged = canUpdateEnglish && !sameArray(englishCurrent, englishNext);
      const arabicChanged = canUpdateArabic && !sameArray(arabicCurrent, arabicNext);

      if (!canUpdateEnglish || !canUpdateArabic) {
        report.skipped.push({
          slug: magentoProduct.slug,
          handle: shopifyProduct.handle,
          reason: [
            !canUpdateEnglish ? "English scrape returned no images, so existing English gallery was preserved." : null,
            !canUpdateArabic ? "Arabic scrape returned no images, so existing Arabic gallery was preserved." : null,
          ]
            .filter(Boolean)
            .join(" "),
        });
      }

      if (!englishChanged && !arabicChanged) {
        if (canUpdateEnglish && canUpdateArabic) {
          report.skipped.push({
            slug: magentoProduct.slug,
            handle: shopifyProduct.handle,
            reason: "Already matched Magento order for English and Arabic.",
          });
        }
        continue;
      }

      const payload = [];
      if (englishChanged) {
        payload.push(
          buildMetafieldPayload({
            ownerId: shopifyProduct.id,
            compareDigest: shopifyProduct.englishGallery.compareDigest,
            key: "pdp_images",
            value: englishNext,
          }),
        );
      }
      if (arabicChanged) {
        payload.push(
          buildMetafieldPayload({
            ownerId: shopifyProduct.id,
            compareDigest: shopifyProduct.arabicGallery.compareDigest,
            key: "pdp_images_ar",
            value: arabicNext,
          }),
        );
      }

      if (!dryRun) {
        await setMetafields(shop, accessToken, payload);
      }

      report.updated.push({
        slug: magentoProduct.slug,
        handle: shopifyProduct.handle,
        englishCount: englishNext.length,
        arabicCount: arabicNext.length,
        englishChanged,
        arabicChanged,
        englishOrder: englishDesired.map((image) => image.filename),
        arabicOrder: arabicDesired.map((image) => image.filename),
      });

      console.log(
        `${dryRun ? "Planned" : "Updated"} ${shopifyProduct.handle}: EN ${englishNext.length}, AR ${arabicNext.length}`,
      );
    }

    await fs.writeFile(REPORT_PATH, JSON.stringify(report, null, 2));
    console.log(`Report written to ${REPORT_PATH}`);
  } finally {
    await browser.close();
  }
}

function buildTargetMagentoProducts() {
  const collectionUrl = new URL(COLLECTION_URL);
  const base = `${collectionUrl.origin}/sa-en`;
  const slugs = syncSlugs.length ? syncSlugs : Object.keys(HANDLE_MAP);
  return slugs.map((slug) => ({
    slug,
    url: `${base}/${slug}`,
  }));
}

async function resolveShopifySession() {
  const explicitShop = process.env.SHOPIFY_SHOP;
  const explicitToken = process.env.SHOPIFY_ACCESS_TOKEN;
  if (explicitShop && explicitToken) {
    return { shop: explicitShop, accessToken: explicitToken };
  }

  const session = await prisma.session.findFirst({
    where: { isOnline: false },
    orderBy: { shop: "asc" },
    select: { shop: true, accessToken: true },
  });

  if (!session) {
    throw new Error("No offline Shopify session found. Set SHOPIFY_SHOP and SHOPIFY_ACCESS_TOKEN explicitly.");
  }

  return session;
}

async function adminGraphql(shop, accessToken, query, variables = {}) {
  const response = await fetch(`https://${shop}/admin/api/${API_VERSION}/graphql.json`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": accessToken,
    },
    body: JSON.stringify({ query, variables }),
  });

  const json = await response.json();
  if (!response.ok || json.errors?.length) {
    throw new Error(`Shopify GraphQL failed: ${JSON.stringify(json.errors || json, null, 2)}`);
  }

  return json.data;
}

async function scrapeCollectionProducts(page) {
  await page.goto(COLLECTION_URL, { waitUntil: "domcontentloaded", timeout: 120000 });
  await page.waitForTimeout(3000);

  let lastCount = 0;
  for (let attempt = 0; attempt < 12; attempt += 1) {
    const nextCount = await page.locator('a[href*="/sa-en/"]').count();
    if (nextCount === lastCount) {
      break;
    }
    lastCount = nextCount;
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(1500);
  }

  const links = await page.locator('a[href*="/sa-en/"]').evaluateAll((elements) => {
    const seen = new Set();
    const rows = [];
    for (const element of elements) {
      const href = element.href;
      const text = (element.textContent || "").trim();
      if (!href || !href.includes("/sa-en/") || !text || seen.has(href)) {
        continue;
      }
      seen.add(href);
      rows.push({ href, text });
    }
    return rows;
  });

  return links
    .map((item) => ({
      url: item.href,
      slug: lastPathSegment(item.href),
      text: item.text,
    }))
    .filter((item) => Boolean(HANDLE_MAP[item.slug]));
}

async function scrapeMagentoProduct(page, englishUrl) {
  const englishImages = await scrapeGalleryImages(page, englishUrl);
  let arabicUrl = await page.locator('head link[hreflang="ar-SA"]').getAttribute("href", { timeout: 5000 }).catch(() => null);
  const englishTitle = await page.title();

  if (!arabicUrl) {
    arabicUrl = deriveArabicUrl(englishUrl);
  }

  const arabicImages = await scrapeGalleryImages(page, arabicUrl);

  return {
    englishUrl,
    englishTitle,
    arabicUrl,
    englishImages,
    arabicImages,
  };
}

function deriveArabicUrl(englishUrl) {
  return englishUrl.replace("/sa-en/", "/ar-sa/");
}

async function scrapeGalleryImages(page, url) {
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 120000 });
  try {
    await page.waitForSelector(PRODUCT_GALLERY_SELECTOR, { timeout: 15000 });
  } catch {
    await page.waitForTimeout(3000);
  }

  const slides = page.locator(`${PRODUCT_GALLERY_SELECTOR}`);
  const slideCount = await slides.count();

  if (slideCount) {
    const images = await slides.evaluateAll((elements) =>
      elements
        .map((element) => {
          const image = element.querySelector('img[src*="/media/catalog/product/"][src*="width=102"], img[src*="/media/catalog/product/"][src*="width=160"]');
          return {
            index: Number(element.getAttribute("data-index") || "0"),
            src: image?.getAttribute("src") || "",
          };
        })
        .filter((entry) => entry.src),
    );

    const imageSources = images.sort((left, right) => left.index - right.index).map((image) => image.src);
    return Promise.all(imageSources.map((src) => toMagentoImage(src, url)));
  }

  const fallback = await page
    .locator('img[src*="/media/catalog/product/"][src*="width=640"], img[src*="/media/catalog/product/"][src*="width=102"]')
    .evaluateAll((elements) =>
      [...new Set(elements.map((element) => element.getAttribute("src")).filter(Boolean))].map((src) => ({ src })),
    );

  const imageSources = fallback.map((image) => image.src);

  return Promise.all(imageSources.map((src) => toMagentoImage(src, url)));
}

async function toMagentoImage(src, pageUrl) {
  const fullUrl = new URL(src, pageUrl).toString();
  const bestUrl = await resolveBestMagentoImageUrl(fullUrl);
  return {
    url: bestUrl,
    filename: basenameFromUrl(bestUrl),
    aliasKeys: buildAliasKeys(basenameFromUrl(bestUrl)),
  };
}

async function fetchShopifyProducts(shop, accessToken) {
  const data = await adminGraphql(shop, accessToken, PRODUCT_QUERY, { first: 100 });
  return data.products.nodes.map((product) => normalizeShopifyProduct(product));
}

function normalizeShopifyProduct(product) {
  return {
    id: product.id,
    title: product.title,
    handle: product.handle,
    englishGallery: normalizeGallery(product.englishGallery),
    arabicGallery: normalizeGallery(product.arabicGallery),
  };
}

function normalizeGallery(metafield) {
  const nodes = metafield?.references?.nodes || [];
  const images = nodes
    .filter((node) => Boolean(node?.id && node?.image?.url))
    .map((node) => ({
      id: node.id,
      url: node.image.url,
      filename: basenameFromUrl(node.image.url),
      aliasKeys: buildAliasKeys(basenameFromUrl(node.image.url)),
      width: node.image.width || 0,
      height: node.image.height || 0,
    }));

  return {
    id: metafield?.id || null,
    compareDigest: metafield?.compareDigest || null,
    images,
  };
}

function buildKnownImageIndex(products) {
  const byId = new Map();
  const byFilename = new Map();
  const byAlias = new Map();

  for (const product of products) {
    for (const image of [...product.englishGallery.images, ...product.arabicGallery.images]) {
      if (!byId.has(image.id)) {
        byId.set(image.id, image);
      }

      const exact = image.filename.toLowerCase();
      if (!byFilename.has(exact)) {
        byFilename.set(exact, image);
      }

      for (const key of image.aliasKeys) {
        if (!byAlias.has(key)) {
          byAlias.set(key, []);
        }
        byAlias.get(key).push(image);
      }
    }
  }

  return { byId, byFilename, byAlias };
}

async function resolveDesiredGallery({ locale, desiredImages, shop, accessToken, knownImages, report }) {
  const resolved = [];

  for (const desired of desiredImages) {
    const existing = findKnownImage(knownImages, desired.filename);
    if (existing) {
      resolved.push(existing);
      continue;
    }

    const reusable = await findReusableFile(shop, accessToken, desired);
    if (reusable) {
      registerKnownImage(knownImages, reusable);
      resolved.push(reusable);
      continue;
    }

    if (dryRun) {
      resolved.push({
        id: `DRY_RUN_UPLOAD:${desired.filename}`,
        url: desired.url,
        filename: desired.filename,
        aliasKeys: desired.aliasKeys,
      });
      report.missingMagento.push({
        locale,
        filename: desired.filename,
        action: "would_upload",
      });
      continue;
    }

    const uploaded = await uploadMagentoImage(shop, accessToken, desired);
    registerKnownImage(knownImages, uploaded);
    resolved.push(uploaded);
    report.uploaded.push({
      locale,
      filename: uploaded.filename,
      id: uploaded.id,
    });
  }

  return resolved;
}

async function findReusableFile(shop, accessToken, desired) {
  const basename = desired.filename.replace(/\.[a-z0-9]+$/i, "");
  const searchTerms = [...new Set([basename, ...desired.aliasKeys.filter((key) => key.length > 8)])];

  for (const term of searchTerms) {
    const data = await adminGraphql(shop, accessToken, FILE_SEARCH_QUERY, {
      first: 20,
      query: `filename:${term}`,
    });
    const candidates = (data.files?.nodes || [])
      .filter((node) => Boolean(node?.id && node?.image?.url))
      .map((node) => ({
        id: node.id,
        url: node.image.url,
        filename: basenameFromUrl(node.image.url),
        aliasKeys: buildAliasKeys(basenameFromUrl(node.image.url)),
        width: node.image.width || 0,
        height: node.image.height || 0,
      }));

    const usableCandidates = candidates.filter((candidate) => isUsableGalleryImage(candidate));

    const exact = usableCandidates.find(
      (candidate) => candidate.filename.toLowerCase() === desired.filename.toLowerCase(),
    );
    if (exact) {
      return exact;
    }

    for (const candidate of usableCandidates) {
      if (candidate.aliasKeys.some((key) => desired.aliasKeys.includes(key))) {
        return candidate;
      }
    }
  }

  return null;
}

function findKnownImage(knownImages, filename) {
  const exact = knownImages.byFilename.get(filename.toLowerCase());
  if (exact && isUsableGalleryImage(exact)) {
    return exact;
  }

  const desiredAliases = buildAliasKeys(filename);
  for (const key of desiredAliases) {
    const candidates = knownImages.byAlias.get(key);
    if (candidates?.length) {
      const usableCandidate = candidates.find((candidate) => isUsableGalleryImage(candidate));
      if (usableCandidate) {
        return usableCandidate;
      }
    }
  }

  return null;
}

function registerKnownImage(knownImages, image) {
  if (knownImages.byId.has(image.id)) {
    return;
  }

  knownImages.byId.set(image.id, image);
  knownImages.byFilename.set(image.filename.toLowerCase(), image);
  for (const key of image.aliasKeys) {
    if (!knownImages.byAlias.has(key)) {
      knownImages.byAlias.set(key, []);
    }
    knownImages.byAlias.get(key).push(image);
  }
}

async function uploadMagentoImage(shop, accessToken, desired) {
  const imageResponse = await fetch(desired.url);
  if (!imageResponse.ok) {
    throw new Error(`Failed to download Magento image ${desired.url}: ${imageResponse.status}`);
  }

  const buffer = Buffer.from(await imageResponse.arrayBuffer());
  const mimeType = imageResponse.headers.get("content-type") || guessMimeType(desired.filename);

  const staged = await adminGraphql(shop, accessToken, STAGED_UPLOADS_CREATE_MUTATION, {
    input: [
      {
        filename: desired.filename,
        mimeType,
        httpMethod: "POST",
        resource: "IMAGE",
      },
    ],
  });

  const target = staged.stagedUploadsCreate.stagedTargets[0];
  if (!target) {
    throw new Error(`No staged upload target returned for ${desired.filename}`);
  }

  const form = new FormData();
  for (const parameter of target.parameters) {
    form.append(parameter.name, parameter.value);
  }
  form.append("file", new Blob([buffer], { type: mimeType }), desired.filename);

  const uploadResponse = await fetch(target.url, {
    method: "POST",
    body: form,
  });

  if (!uploadResponse.ok) {
    throw new Error(`Failed to upload ${desired.filename} to staged target: ${uploadResponse.status}`);
  }

  const created = await adminGraphql(shop, accessToken, FILE_CREATE_MUTATION, {
    files: [
      {
        alt: "",
        contentType: "IMAGE",
        originalSource: target.resourceUrl,
      },
    ],
  });

  const file = created.fileCreate.files[0];
  if (!file?.id) {
    throw new Error(`Shopify did not return a created MediaImage for ${desired.filename}`);
  }

  const readyFile = await waitForReadyFile(shop, accessToken, file.id);
  return {
    id: readyFile.id,
    url: readyFile.image.url,
    filename: basenameFromUrl(readyFile.image.url),
    aliasKeys: buildAliasKeys(basenameFromUrl(readyFile.image.url)),
    width: readyFile.image.width || 0,
    height: readyFile.image.height || 0,
  };
}

async function waitForReadyFile(shop, accessToken, id) {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    const data = await adminGraphql(shop, accessToken, FILE_NODE_QUERY, { id });
    const node = data.node;
    if (node?.fileStatus === "READY" && node?.image?.url) {
      return node;
    }
    await sleep(1000);
  }

  throw new Error(`Timed out waiting for Shopify file ${id} to become READY.`);
}

async function setMetafields(shop, accessToken, metafields) {
  if (!metafields.length) {
    return;
  }

  const data = await adminGraphql(shop, accessToken, SET_METAFIELDS_MUTATION, { metafields });
  const errors = data.metafieldsSet.userErrors || [];
  if (errors.length) {
    throw new Error(`metafieldsSet failed: ${JSON.stringify(errors, null, 2)}`);
  }
}

function basenameFromUrl(url) {
  const parsed = new URL(url);
  return decodeURIComponent(parsed.pathname.split("/").pop() || "");
}

async function resolveBestMagentoImageUrl(url) {
  const candidates = buildMagentoImageCandidates(url);
  let best = null;

  for (const candidate of candidates) {
    const inspected = await inspectRemoteImage(candidate);
    if (!inspected) continue;

    if (!best || inspected.byteLength > best.byteLength) {
      best = inspected;
    }
  }

  return best?.url || candidates[0];
}

function buildMagentoImageCandidates(url) {
  const candidates = new Set();
  const original = new URL(url);
  candidates.add(original.toString());

  const unwrapped = new URL(original.toString());
  unwrapped.search = "";
  unwrapped.pathname = unwrapped.pathname.replace(/\/cache\/[^/]+/i, "");
  candidates.add(unwrapped.toString());

  const basename = basenameFromUrl(unwrapped.toString());
  const withoutUuid = basename.replace(
    /_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?=\.[a-z0-9]+$)/i,
    "",
  );
  if (withoutUuid !== basename) {
    const normalized = new URL(unwrapped.toString());
    normalized.pathname = normalized.pathname.replace(new RegExp(`${escapeRegExp(basename)}$`), withoutUuid);
    candidates.add(normalized.toString());
  }

  return [...candidates];
}

async function inspectRemoteImage(url) {
  try {
    const response = await fetch(url, {
      headers: {
        "user-agent": "Mozilla/5.0",
      },
    });

    if (!response.ok) {
      return null;
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    return {
      url,
      byteLength: buffer.byteLength,
    };
  } catch {
    return null;
  }
}

function lastPathSegment(url) {
  const parsed = new URL(url);
  return parsed.pathname.split("/").filter(Boolean).pop() || "";
}

function buildAliasKeys(filename) {
  const base = filename.toLowerCase().replace(/\.[a-z0-9]+$/i, "");
  const rawTokens = base.split(/[^a-z0-9]+/).filter(Boolean);
  const compact = rawTokens.join("");
  const localeAgnostic = rawTokens.filter((token) => !["eng", "english", "ar", "arabic", "en"].includes(token));
  const keys = new Set([compact, localeAgnostic.join("")]);

  if (localeAgnostic.length > 1 && ["1", "2"].includes(localeAgnostic.at(-1))) {
    keys.add(localeAgnostic.slice(0, -1).join(""));
  }

  if (rawTokens.length > 1 && ["1", "2"].includes(rawTokens.at(-1))) {
    keys.add(rawTokens.slice(0, -1).join(""));
  }

  return [...keys].filter(Boolean);
}

function isUsableGalleryImage(image) {
  const width = Number(image?.width || 0);
  const height = Number(image?.height || 0);
  return Math.max(width, height) >= 1200;
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function sameArray(left, right) {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function buildMetafieldPayload({ ownerId, compareDigest, key, value }) {
  const payload = {
    ownerId,
    namespace: "custom",
    key,
    type: "list.file_reference",
    value: JSON.stringify(value),
  };

  if (compareDigest) {
    payload.compareDigest = compareDigest;
  }

  return payload;
}

function guessMimeType(filename) {
  const lower = filename.toLowerCase();
  if (lower.endsWith(".png")) return "image/png";
  if (lower.endsWith(".webp")) return "image/webp";
  if (lower.endsWith(".gif")) return "image/gif";
  return "image/jpeg";
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
