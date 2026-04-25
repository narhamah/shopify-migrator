import fs from "node:fs/promises";
import path from "node:path";

const API_VERSION = "2025-10";
const SHOP = process.env.SHOPIFY_SHOP ?? "xkgw0m-sm.myshopify.com";
const TOKEN = process.env.SHOPIFY_ACCESS_TOKEN ?? process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
const EXECUTE = process.env.EXECUTE === "1";
const REPORT_PATH = path.resolve(
  process.cwd(),
  "..",
  "temp",
  "sa-pdp-gallery-optimization-report.json",
);

if (!TOKEN) {
  throw new Error("Missing SHOPIFY_ACCESS_TOKEN or SHOPIFY_ADMIN_ACCESS_TOKEN");
}

const PRODUCTS = [
  {
    handle: "argan-oil",
    english: [
      "tara_pdp_nourish_eng_2_1_1.jpg",
      "tara_pdp_nourish_eng_1_1_1.jpg",
      "tara_pdp_nourish1.jpg",
      "tara_pdp_nourish2.jpg",
      "tara_pdp_nourish3.jpg",
      "tara_pdp_nourish4.jpg",
      "tara_pdp_nourish5.jpg",
      "tara_pdp_nourish6.jpg",
      "tara_pdp_noursih_secondary.jpg",
      "tara_pdp_nourish7.jpg",
      "tara_pdp_nourish_videothumb_2.jpg",
      "pdp_faq_nourish_en.jpg",
    ],
    arabic: [
      "tara_pdp_nourish_eng_2_1_1.jpg",
      "tara_pdp_nourish_eng_1_1_1.jpg",
      "tara_pdp_nourish_ar2.jpg",
      "tara_pdp_nourish_ar3.jpg",
      "tara_pdp_nourish_ar4.jpg",
      "tara_pdp_nourish_ar6.jpg",
      "tara_pdp_nourish_ar5.jpg",
      "tara_pdp_nourish_ar7.jpg",
      "tara_pdp_noursih_secondary.jpg",
      "tara_pdp_nourish7.jpg",
      "tara_pdp_nourish_videothumb_2.jpg",
      "pdp_faq_nourish_en.jpg",
    ],
  },
  {
    handle: "nourishing-dry-oil",
    english: [
      "tara_pdp_glow_eng_1.jpg",
      "tara_pdp_glow_eng_2.jpg",
      "tara_pdp_glow1.jpg",
      "tara_pdp_glow2.jpg",
      "tara_pdp_glow3.jpg",
      "tara_pdp_glow4.jpg",
      "tara_pdp_glow5.jpg",
      "tara_pdp_glow6.jpg",
      "tara_pdp_glow_secondary.jpg",
      "tara_pdp_glow_videothumb_5.jpg",
      "pdp_faq_glow_en.jpg",
    ],
    arabic: [
      "tara_pdp_glow_eng_1.jpg",
      "tara_pdp_glow_eng_2.jpg",
      "tara_pdp_glow_ar2.jpg",
      "tara_pdp_glow_ar3.jpg",
      "tara_pdp_glow_ar4.jpg",
      "tara_pdp_glow_ar5.jpg",
      "tara_pdp_glow_ar6.jpg",
      "tara_pdp_glow_ar7.jpg",
      "tara_pdp_glow_secondary.jpg",
      "tara_pdp_glow_videothumb_5.jpg",
      "pdp_faq_glow_en.jpg",
    ],
  },
  {
    handle: "botanical-water-cream",
    english: [
      "artboard_1.jpg",
      "tara_pdp_dew_swatch-1.jpg",
      "tara_pdp_dew1.jpg",
      "tara_pdp_dew2.jpg",
      "tara_pdp_dew3.jpg",
      "dew_faq.png",
      "tara_pdp_dew6.png",
    ],
    arabic: [
      "artboard_1.jpg",
      "tara_pdp_dew_swatch-1.jpg",
      "tara_pdp_dew2.png",
      "tara_pdp_dew3.png",
      "tara_pdp_dew4.png",
      "dew_faq.png",
      "tara_pdp_dew6.png",
    ],
  },
  {
    handle: "revitalizing-shampoo",
    english: [
      "tara_pdp_sage_shampoo_en_3.jpg",
      "tara_pdp_sage_shampoo_en_4.jpg",
      "tara_pdp_sage_shampoo_en_5.jpg",
      "tara_pdp_sage_shampoo_en_6.jpg",
      "tara_pdp_sage_shampoo_en_7_sa.jpg",
      "tara_pdp_sage_shampoo_en_8_sa.jpg",
      "tara_pdp_sage_shampoo_en_9_sa.jpg",
      "tara_pdp_sage_shampoo_en_10_sa.jpg",
    ],
    arabic: [
      "tara_pdp_sage_shampoo_ar_3_sa.jpg",
      "tara_pdp_sage_shampoo_ar_4_sa.jpg",
      "tara_pdp_sage_shampoo_ar_5_sa.jpg",
      "tara_pdp_sage_shampoo_ar_6_sa.jpg",
      "tara_pdp_sage_shampoo_ar_7_sa.jpg",
      "tara_pdp_sage_shampoo_en_8_sa.jpg",
      "tara_pdp_sage_shampoo_en_9_sa.jpg",
      "tara_pdp_sage_shampoo_en_10_sa.jpg",
    ],
  },
  {
    handle: "scalp-hair-revival-system",
    english: [
      "tara_pdp_detox_system_en_1.jpg",
      "tara_pdp_detox_system_en_3.jpg",
      "tara_pdp_detox_system_en_4.jpg",
      "tara_pdp_detox_system_en_5.jpg",
      "tara_pdp_detox_system_en_8.jpg",
      "tara_pdp_detox_system_en_9.jpg",
      "tara_pdp_detox_system_en_10.jpg",
      "tara_pdp_detox_system_en_11.jpg",
    ],
    arabic: [
      "tara_pdp_detox_system_en_1.jpg",
      "tara_pdp_detox_system_ar_2.jpg",
      "tara_pdp_detox_system_ar_3.jpg",
      "tara_pdp_detox_system_ar_4.jpg",
      "tara_pdp_detox_system_ar_7.jpg",
      "tara_pdp_detox_system_ar_8.jpg",
      "tara_pdp_detox_system_en_10.jpg",
      "tara_pdp_detox_system_ar_9.jpg",
    ],
  },
];

const CLEANUP_FILE_NAMES = [
  "1710768408-64cf01fe5fefa673fd98c2c57ad0a1adad570e5dab817408c86df546f5468c6d-d_295x166_9c2a1977-c039-4259-ab76-af56e322409a.jpg",
];

const PRODUCTS_QUERY = `#graphql
  query Products($first: Int!, $after: String) {
    products(first: $first, after: $after, sortKey: TITLE) {
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
  query Files($first: Int!, $after: String, $query: String!) {
    files(first: $first, after: $after, query: $query) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        __typename
        id
        fileStatus
        ... on MediaImage {
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

const FILE_DELETE_MUTATION = `#graphql
  mutation DeleteFiles($fileIds: [ID!]!) {
    fileDelete(fileIds: $fileIds) {
      deletedFileIds
      userErrors {
        field
        message
        code
      }
    }
  }
`;

const FILE_UPDATE_MUTATION = `#graphql
  mutation UpdateFiles($files: [FileUpdateInput!]!) {
    fileUpdate(files: $files) {
      files {
        id
        alt
      }
      userErrors {
        field
        message
        code
      }
    }
  }
`;

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

async function main() {
  const [products, files] = await Promise.all([fetchAllProducts(), fetchAllFiles("")]);
  const productsByHandle = new Map(products.map((product) => [product.handle, product]));
  const filesByName = new Map(files.map((file) => [file.filename.toLowerCase(), file]));

  const updatePlans = [];
  const missingFiles = [];

  for (const target of PRODUCTS) {
    const product = productsByHandle.get(target.handle);
    if (!product) {
      throw new Error(`Missing product ${target.handle}`);
    }

    const targetMissingFiles = [];
    const englishFiles = resolveFiles(
      target.english,
      filesByName,
      targetMissingFiles,
      target.handle,
      "en",
    );
    const arabicFiles = resolveFiles(
      target.arabic,
      filesByName,
      targetMissingFiles,
      target.handle,
      "ar",
    );

    if (targetMissingFiles.length > 0) {
      missingFiles.push(...targetMissingFiles);
      continue;
    }

    const englishCurrentIds = product.englishGallery.map((item) => item.id);
    const arabicCurrentIds = product.arabicGallery.map((item) => item.id);
    const englishNextIds = englishFiles.map((file) => file.id);
    const arabicNextIds = arabicFiles.map((file) => file.id);

    const englishDuplicates = findDuplicates(product.englishGallery.map((item) => item.filename));
    const arabicDuplicates = findDuplicates(product.arabicGallery.map((item) => item.filename));

    if (
      galleriesDiffer(englishCurrentIds, englishNextIds) ||
      galleriesDiffer(arabicCurrentIds, arabicNextIds)
    ) {
      updatePlans.push({
        handle: product.handle,
        title: product.title,
        ownerId: product.id,
        englishBefore: product.englishGallery.map((item) => item.filename),
        englishAfter: englishFiles.map((file) => file.filename),
        arabicBefore: product.arabicGallery.map((item) => item.filename),
        arabicAfter: arabicFiles.map((file) => file.filename),
        englishDuplicates,
        arabicDuplicates,
        metafields: [
          {
            ownerId: product.id,
            namespace: "custom",
            key: "pdp_images",
            type: "list.file_reference",
            value: JSON.stringify(englishNextIds),
          },
          {
            ownerId: product.id,
            namespace: "custom",
            key: "pdp_images_ar",
            type: "list.file_reference",
            value: JSON.stringify(arabicNextIds),
          },
        ],
      });
    }
  }

  if (missingFiles.length > 0) {
    throw new Error(`Missing referenced files:\n${missingFiles.join("\n")}`);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    execute: EXECUTE,
    summary: {
      auditedProducts: PRODUCTS.length,
      updateCount: updatePlans.length,
      cleanupCandidateCount: CLEANUP_FILE_NAMES.length,
    },
    updates: updatePlans,
    cleanupCandidates: CLEANUP_FILE_NAMES,
  };

  if (EXECUTE) {
    report.execution = {
      galleryResult: await applyGalleryUpdates(updatePlans),
      renameResult: await renameFileIfPresent(
        filesByName,
        "tara_pdp_noursih_secondary.jpg",
        "tara_pdp_nourish_secondary.jpg",
      ),
      deleteResult: await deleteUnusedFiles(filesByName, CLEANUP_FILE_NAMES),
      postAudit: await postAudit(PRODUCTS.map((item) => item.handle)),
    };
  }

  await fs.writeFile(REPORT_PATH, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Report written to ${REPORT_PATH}`);
}

function resolveFiles(names, filesByName, missingFiles, handle, locale) {
  const files = [];
  const seen = new Set();
  for (const name of names) {
    const file = filesByName.get(name.toLowerCase());
    if (!file) {
      missingFiles.push(`${handle}:${locale}:${name}`);
      continue;
    }
    if (seen.has(file.id)) {
      continue;
    }
    seen.add(file.id);
    files.push(file);
  }
  return files;
}

function findDuplicates(items) {
  const seen = new Set();
  const duplicates = [];
  for (const item of items) {
    if (seen.has(item)) {
      duplicates.push(item);
      continue;
    }
    seen.add(item);
  }
  return duplicates;
}

function galleriesDiffer(currentIds, nextIds) {
  if (currentIds.length !== nextIds.length) {
    return true;
  }
  return currentIds.some((value, index) => value !== nextIds[index]);
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
        englishGallery: toGalleryItems(node.englishGallery?.references?.nodes ?? []),
        arabicGallery: toGalleryItems(node.arabicGallery?.references?.nodes ?? []),
      });
    }
    if (!data.products.pageInfo.hasNextPage) {
      break;
    }
    after = data.products.pageInfo.endCursor;
  }
  return products;
}

async function fetchAllFiles(search) {
  const files = [];
  let after = null;
  while (true) {
    const data = await shopifyGraphql(FILES_QUERY, { first: 250, after, query: search });
    for (const node of data.files.nodes) {
      if (node.__typename !== "MediaImage" || !node.image?.url) {
        continue;
      }
      files.push({
        id: node.id,
        fileStatus: node.fileStatus,
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

async function applyGalleryUpdates(updatePlans) {
  if (updatePlans.length === 0) {
    return { applied: 0, userErrors: [] };
  }

  const metafields = updatePlans.flatMap((plan) => plan.metafields);
  const result = await shopifyGraphql(METAFIELDS_SET_MUTATION, { metafields });
  return {
    applied: updatePlans.length,
    userErrors: result.metafieldsSet.userErrors,
  };
}

async function renameFileIfPresent(filesByName, currentName, nextName) {
  const file = filesByName.get(currentName.toLowerCase());
  if (!file) {
    return { renamed: false, reason: "missing" };
  }

  const result = await shopifyGraphql(FILE_UPDATE_MUTATION, {
    files: [{ id: file.id, filename: nextName }],
  });

  return {
    renamed: result.fileUpdate.userErrors.length === 0,
    userErrors: result.fileUpdate.userErrors,
  };
}

async function deleteUnusedFiles(filesByName, names) {
  const unusedLookup = new Set((await fetchAllFiles("used_in:none")).map((file) => file.id));
  const fileIds = names
    .map((name) => filesByName.get(name.toLowerCase()))
    .filter(Boolean)
    .filter((file) => unusedLookup.has(file.id))
    .map((file) => file.id);

  if (fileIds.length === 0) {
    return { deleted: 0, userErrors: [] };
  }

  const result = await shopifyGraphql(FILE_DELETE_MUTATION, { fileIds });
  return {
    deleted: result.fileDelete.deletedFileIds.length,
    userErrors: result.fileDelete.userErrors,
  };
}

async function postAudit(handles) {
  const products = await fetchAllProducts();
  const scoped = products.filter((product) => handles.includes(product.handle));
  return scoped.map((product) => ({
    handle: product.handle,
    englishCount: product.englishGallery.length,
    arabicCount: product.arabicGallery.length,
    english: product.englishGallery.map((item) => item.filename),
    arabic: product.arabicGallery.map((item) => item.filename),
    englishDuplicates: findDuplicates(product.englishGallery.map((item) => item.filename)),
    arabicDuplicates: findDuplicates(product.arabicGallery.map((item) => item.filename)),
  }));
}

function toGalleryItems(nodes) {
  return nodes
    .filter((node) => node.image?.url)
    .map((node) => ({
      id: node.id,
      filename: filenameFromUrl(node.image.url),
    }));
}

function filenameFromUrl(url) {
  const clean = url.split("?")[0];
  return clean.slice(clean.lastIndexOf("/") + 1);
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
  if (!response.ok || json.errors) {
    throw new Error(JSON.stringify({ status: response.status, body: json }, null, 2));
  }
  return json.data;
}
