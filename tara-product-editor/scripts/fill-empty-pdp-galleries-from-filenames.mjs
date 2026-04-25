import fs from "node:fs/promises";
import path from "node:path";

const API_VERSION = "2025-10";
const SHOP = process.env.SHOPIFY_SHOP ?? "xkgw0m-sm.myshopify.com";
const TOKEN = process.env.SHOPIFY_ACCESS_TOKEN ?? process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
const REPORT_PATH = path.resolve(
  process.cwd(),
  "..",
  "temp",
  "fill-empty-pdp-galleries-report.json",
);
const OVERWRITE_ALL = process.env.OVERWRITE_ALL === "1";

if (!TOKEN) {
  throw new Error("Missing SHOPIFY_ACCESS_TOKEN or SHOPIFY_ADMIN_ACCESS_TOKEN");
}

const PRODUCTS_QUERY = `#graphql
  query Products($first: Int!, $after: String) {
    products(first: $first, after: $after) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        handle
        title
        englishGallery: metafield(namespace: "custom", key: "pdp_images") {
          references(first: 50) {
            nodes {
              ... on MediaImage {
                id
                image {
                  url
                }
              }
            }
          }
        }
        arabicGallery: metafield(namespace: "custom", key: "pdp_images_ar") {
          references(first: 50) {
            nodes {
              ... on MediaImage {
                id
                image {
                  url
                }
              }
            }
          }
        }
      }
    }
  }
`;

const FILES_QUERY = `#graphql
  query Files($first: Int!, $after: String) {
    files(first: $first, after: $after, query: "tara_pdp_") {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        ... on MediaImage {
          id
          createdAt
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

const METAFIELDS_SET_MUTATION = `#graphql
  mutation SetMetafields($metafields: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $metafields) {
      metafields {
        id
        namespace
        key
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const TARGETS = [
  explicit("strengthening-scalp-serum", "black_garlic_serum"),
  baseWithArabic("hair-wellness-system", "date_system"),
  explicit("nourishing-shampoo", "date_shampoo"),
  explicit("nurture-conditioner", "strawberry_conditioner"),
  explicit("nurture-shampoo", "strawberry_shampoo"),
  explicit("revitalizing-shampoo", "sage_shampoo"),
  explicit("strengthening-scalp-serum", "black_garlic_serum"),
  baseWithArabic("scalp-prep-shampoo", "rosemary_shampoo"),
  baseWithArabic("follicle-boost-serum", "rosemary_serum"),
  baseWithArabic("ghassoul-avocado-smoothing-conditioner", "detox_conditioner"),
  explicit("hair-strength-system", "black_garlic_system"),
  explicit("invigorating-shampoo", "black_garlic_shampoo"),
  explicit("repairing-hair-mask", "black_garlic_mask"),
  explicit("hydrating-conditioner", "date_conditioner"),
  explicit("rejuvenating-scalp-serum", "date_serum"),
  explicit("nurture-leave-in-conditioner", "strawberry_leave_in_conditioner", {
    allowHyphenatedIn: true,
  }),
  explicit("replenishing-conditioner", "sage_conditioner"),
  explicit("scalp-support-serum", "sage_serum"),
  baseWithArabic("thickening-conditioner", "onion_conditioner"),
  baseWithArabic("volumizing-shampoo", "onion_shampoo"),
  baseWithArabic("charcoal-salicylic-exfoliating-shampoo", "detox_shampoo"),
  baseWithArabic("cactus-red-seaweed-scalp-serum", "detox_serum"),
];

async function main() {
  const products = await fetchAllProducts();
  const files = await fetchAllFiles();
  const fileMap = new Map(files.map((file) => [file.id, file]));
  const updates = [];

  for (const target of TARGETS) {
    const product = products.find((item) => item.handle === target.handle);
    if (!product) {
      continue;
    }

    const englishCurrent = product.englishGallery.map((item) => item.id);
    const arabicCurrent = product.arabicGallery.map((item) => item.id);
    const englishMatches = target.en(files);
    const arabicMatches = target.ar(files);
    const metafields = [];

    const englishNext = dedupeFiles(englishMatches);
    const arabicNext = dedupeFiles(arabicMatches);
    const englishChanged = englishNext.length > 0 && galleriesDiffer(englishCurrent, englishNext);
    const arabicChanged = arabicNext.length > 0 && galleriesDiffer(arabicCurrent, arabicNext);

    if ((OVERWRITE_ALL ? englishChanged : englishCurrent.length === 0 && englishChanged)) {
      metafields.push({
        ownerId: product.id,
        namespace: "custom",
        key: "pdp_images",
        type: "list.file_reference",
        value: JSON.stringify(englishNext.map((file) => file.id)),
      });
    }

    if ((OVERWRITE_ALL ? arabicChanged : arabicCurrent.length === 0 && arabicChanged)) {
      metafields.push({
        ownerId: product.id,
        namespace: "custom",
        key: "pdp_images_ar",
        type: "list.file_reference",
        value: JSON.stringify(arabicNext.map((file) => file.id)),
      });
    }

    if (metafields.length === 0) {
      continue;
    }

    const result = await shopifyGraphql(METAFIELDS_SET_MUTATION, { metafields });
    updates.push({
      handle: product.handle,
      title: product.title,
      overwriteAll: OVERWRITE_ALL,
      englishBefore: englishCurrent.length,
      englishAfter: metafields.some((item) => item.key === "pdp_images")
        ? englishNext.map((file) => serializeFile(file))
        : englishCurrent.map((id) => serializeFile(fileMap.get(id))).filter(Boolean),
      arabicBefore: arabicCurrent.length,
      arabicAfter: metafields.some((item) => item.key === "pdp_images_ar")
        ? arabicNext.map((file) => serializeFile(file))
        : arabicCurrent.map((id) => serializeFile(fileMap.get(id))).filter(Boolean),
      userErrors: result.metafieldsSet.userErrors,
    });
  }

  await fs.writeFile(
    REPORT_PATH,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        updated: updates,
      },
      null,
      2,
    ),
  );

  console.log(JSON.stringify({ updatedProducts: updates.length }, null, 2));
  console.log(`Report written to ${REPORT_PATH}`);
}

function dedupeFiles(files) {
  const seen = new Set();
  return files.filter((file) => {
    if (seen.has(file.id)) {
      return false;
    }
    seen.add(file.id);
    return true;
  });
}

function galleriesDiffer(currentIds, nextFiles) {
  if (currentIds.length !== nextFiles.length) {
    return true;
  }
  return currentIds.some((id, index) => id !== nextFiles[index]?.id);
}

function explicit(handle, family, options = {}) {
  const familyPattern = options.allowHyphenatedIn
    ? family.replace("leave_in", "leave(?:-|_)in")
    : family;
  return {
    handle,
    en: buildMatcher(new RegExp(`^tara_pdp_${familyPattern}_en_(.+)\\.[^.]+$`, "i")),
    ar: buildMatcher(new RegExp(`^tara_pdp_${familyPattern}_ar_(.+)\\.[^.]+$`, "i")),
  };
}

function baseWithArabic(handle, family) {
  return {
    handle,
    en: buildMatcher(new RegExp(`^tara_pdp_${family}_(?!ar_)(.+)\\.[^.]+$`, "i")),
    ar: buildMatcher(new RegExp(`^tara_pdp_${family}_ar_(.+)\\.[^.]+$`, "i")),
  };
}

function buildMatcher(pattern) {
  return (files) => {
    const grouped = new Map();

    for (const file of files) {
      const match = file.filename.toLowerCase().match(pattern);
      if (!match) {
        continue;
      }

      const numbers = extractNumberSequence(match[1]);
      if (numbers.length === 0) {
        continue;
      }

      const key = String(numbers[0]);
      const current = grouped.get(key);
      if (!current || compareFileQuality(file, current) < 0) {
        grouped.set(key, file);
      }
    }

    return [...grouped.entries()]
      .sort((left, right) => compareNumberSequence(left[0], right[0]))
      .map(([, file]) => file);
  };
}

function extractNumberSequence(segment) {
  return [...segment.matchAll(/\d+/g)].map((match) => Number(match[0]));
}

function compareNumberSequence(leftKey, rightKey) {
  const left = leftKey.split(".").map(Number);
  const right = rightKey.split(".").map(Number);
  const length = Math.max(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    const leftValue = left[index] ?? 0;
    const rightValue = right[index] ?? 0;
    if (leftValue !== rightValue) {
      return leftValue - rightValue;
    }
  }
  return 0;
}

function compareFileQuality(left, right) {
  return qualityScore(left) - qualityScore(right) || left.filename.localeCompare(right.filename);
}

function qualityScore(file) {
  let score = 0;
  if (/_asset\./i.test(file.filename)) {
    score += 30;
  }
  if (/_sa\./i.test(file.filename)) {
    score += 20;
  }
  if (/_\d{8}\./i.test(file.filename)) {
    score += 10;
  }
  return score;
}

async function fetchAllProducts() {
  const products = [];
  let after = null;
  while (true) {
    const data = await shopifyGraphql(PRODUCTS_QUERY, { first: 100, after });
    for (const node of data.products.nodes) {
      products.push({
        id: node.id,
        handle: node.handle,
        title: node.title,
        englishGallery: (node.englishGallery?.references?.nodes || [])
          .filter((item) => item.image?.url)
          .map((item) => ({ id: item.id, filename: filenameFromUrl(item.image.url) })),
        arabicGallery: (node.arabicGallery?.references?.nodes || [])
          .filter((item) => item.image?.url)
          .map((item) => ({ id: item.id, filename: filenameFromUrl(item.image.url) })),
      });
    }
    if (!data.products.pageInfo.hasNextPage) {
      break;
    }
    after = data.products.pageInfo.endCursor;
  }
  return products;
}

async function fetchAllFiles() {
  const files = [];
  let after = null;
  while (true) {
    const data = await shopifyGraphql(FILES_QUERY, { first: 250, after });
    for (const node of data.files.nodes) {
      if (!node.image?.url) {
        continue;
      }
      files.push({
        id: node.id,
        createdAt: node.createdAt,
        filename: filenameFromUrl(node.image.url),
        width: node.image.width,
        height: node.image.height,
      });
    }
    if (!data.files.pageInfo.hasNextPage) {
      break;
    }
    after = data.files.pageInfo.endCursor;
  }
  return files;
}

function filenameFromUrl(url) {
  return decodeURIComponent(new URL(url).pathname.split("/").pop() || "");
}

function serializeFile(file) {
  if (!file) {
    return null;
  }
  return {
    id: file.id,
    filename: file.filename,
  };
}

async function shopifyGraphql(query, variables) {
  const response = await fetch(`https://${SHOP}/admin/api/${API_VERSION}/graphql.json`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": TOKEN,
    },
    body: JSON.stringify({ query, variables }),
  });

  const json = await response.json();
  if (!response.ok || json.errors?.length) {
    throw new Error(JSON.stringify(json.errors || json, null, 2));
  }

  return json.data;
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
