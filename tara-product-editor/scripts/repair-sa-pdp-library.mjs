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
  "sa-pdp-library-repair-report.json",
);

if (!TOKEN) {
  throw new Error("Missing SHOPIFY_ACCESS_TOKEN or SHOPIFY_ADMIN_ACCESS_TOKEN");
}

const TARGETS = [
  target("hair-strength-system", "black_garlic_system"),
  target("invigorating-shampoo", "black_garlic_shampoo"),
  target("repairing-hair-mask", "black_garlic_mask"),
  target("strengthening-scalp-serum", "black_garlic_serum"),
  target("hair-wellness-system", "date_system"),
  target("nourishing-shampoo", "date_shampoo"),
  target("hydrating-conditioner", "date_conditioner"),
  target("rejuvenating-scalp-serum", "date_serum"),
  target("nurture-system", "strawberry_system"),
  target("nurture-shampoo", "strawberry_shampoo"),
  target("nurture-conditioner", "strawberry_conditioner"),
  target("nurture-leave-in-conditioner", "strawberry_leave_in_conditioner", {
    allowHyphenatedFamily: true,
  }),
  target("age-well-system", "sage_system"),
  target("revitalizing-shampoo", "sage_shampoo"),
  target("replenishing-conditioner", "sage_conditioner"),
  target("scalp-support-serum", "sage_serum"),
  target("hair-density-system", "onion_system"),
  target("volumizing-shampoo", "onion_shampoo"),
  target("thickening-conditioner", "onion_conditioner"),
  target("follicle-stimulating-scalp-serum", "onion_serum"),
  target("hair-stimulation-system", "rosemary_system"),
  target("scalp-prep-shampoo", "rosemary_shampoo"),
  target("strand-thicken-conditioner", "rosemary_conditioner"),
  target("follicle-boost-serum", "rosemary_serum"),
  target("scalp-hair-revival-system", "detox_system"),
  target("charcoal-salicylic-exfoliating-shampoo", "detox_shampoo"),
  target("ghassoul-avocado-smoothing-conditioner", "detox_conditioner"),
  target("cactus-red-seaweed-scalp-serum", "detox_serum"),
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
        createdAt
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
  mutation RenameFiles($files: [FileUpdateInput!]!) {
    fileUpdate(files: $files) {
      files {
        id
        fileStatus
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
  const products = await fetchAllProducts();
  const files = await fetchAllFiles("tara_pdp_");
  const fileById = new Map(files.map((file) => [file.id, file]));
  const productByHandle = new Map(products.map((product) => [product.handle, product]));
  const unusedIdsBefore = new Set((await fetchAllFiles("used_in:none tara_pdp_")).map((file) => file.id));

  const familyPlans = TARGETS.map((entry) => buildFamilyPlan(entry, files)).filter(
    (plan) => plan.canonicalGroups.length > 0,
  );
  const keeperByGroupKey = new Map();
  const duplicateCandidates = [];
  const renameCandidates = [];

  for (const familyPlan of familyPlans) {
    for (const group of familyPlan.canonicalGroups) {
      keeperByGroupKey.set(group.key, group.keeper);
      duplicateCandidates.push(...group.duplicates);
      if (group.renameCandidate) {
        renameCandidates.push(group.renameCandidate);
      }
    }
  }

  const galleryUpdates = [];
  const missingProducts = [];

  for (const entry of TARGETS) {
    const product = productByHandle.get(entry.handle);
    if (!product) {
      missingProducts.push(entry.handle);
      continue;
    }

    const englishNext = buildLocaleGallery(entry.family, "en", keeperByGroupKey);
    const arabicNext = buildMirroredArabicGallery(entry.family, englishNext, keeperByGroupKey);

    const englishCurrent = product.englishGallery.map((item) => item.id);
    const arabicCurrent = product.arabicGallery.map((item) => item.id);
    const englishNextIds = englishNext.map((item) => item.file.id);
    const arabicNextIds = arabicNext.map((item) => item.file.id);

    if (
      galleriesDiffer(englishCurrent, englishNextIds) ||
      galleriesDiffer(arabicCurrent, arabicNextIds)
    ) {
      galleryUpdates.push({
        handle: product.handle,
        title: product.title,
        ownerId: product.id,
        englishBefore: product.englishGallery.map((item) => item.filename),
        englishAfter: englishNext.map((item) => item.file.filename),
        arabicBefore: product.arabicGallery.map((item) => item.filename),
        arabicAfter: arabicNext.map((item) => item.file.filename),
        arabicMirrorsEnglishOrder: true,
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

  const report = {
    generatedAt: new Date().toISOString(),
    execute: EXECUTE,
    summary: {
      targetFamilies: TARGETS.length,
      familyPlans: familyPlans.length,
      missingProducts: missingProducts.length,
      galleryUpdates: galleryUpdates.length,
      duplicateCandidateCount: duplicateCandidates.length,
      renameCandidateCount: renameCandidates.length,
      unusedBeforeCount: unusedIdsBefore.size,
    },
    missingProducts,
    familyPlans: familyPlans.map(serializeFamilyPlan),
    galleryUpdates: galleryUpdates.map(serializeGalleryUpdate),
    duplicateCandidates: duplicateCandidates.map(serializeDuplicateCandidate),
    renameCandidates,
  };

  if (EXECUTE) {
    const galleryResults = await applyGalleryUpdates(galleryUpdates);
    await delay(1500);
    const unusedIdsAfter = new Set((await fetchAllFiles("used_in:none tara_pdp_")).map((file) => file.id));
    const deleteCandidates = duplicateCandidates.filter((file) => unusedIdsAfter.has(file.id));
    const deleteResults = await batchDelete(deleteCandidates.map((file) => file.id));
    await delay(1500);
    const filesAfterDelete = await fetchAllFiles("tara_pdp_");
    const renameResults = await batchRename(renameCandidates, {
      unusedIds: new Set((await fetchAllFiles("used_in:none tara_pdp_")).map((file) => file.id)),
      exactNameMap: buildExactNameMap(filesAfterDelete),
    });
    report.execution = {
      galleryResults,
      deleteCandidates: deleteCandidates.map(serializeDuplicateCandidate),
      deleteResults,
      renameResults,
      unusedAfterCount: unusedIdsAfter.size,
    };
  }

  await fs.writeFile(REPORT_PATH, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Report written to ${REPORT_PATH}`);
}

function target(handle, family, options = {}) {
  return {
    handle,
    family,
    familyPattern: options.allowHyphenatedFamily
      ? family.replace("leave_in", "leave(?:-|_)in")
      : family,
  };
}

function buildFamilyPlan(entry, files) {
  const groups = new Map();
  for (const file of files) {
    const parsed = parseFamilyFile(file, entry.familyPattern);
    if (!parsed) {
      continue;
    }
    const key = `${entry.family}:${parsed.locale}:${parsed.slide}`;
    const existing = groups.get(key) ?? [];
    existing.push(parsed);
    groups.set(key, existing);
  }

  const canonicalGroups = [...groups.entries()]
    .map(([key, records]) => {
      const ranked = [...records].sort(compareParsedFilePriority);
      const keeper = ranked[0];
      const duplicates = ranked.slice(1).map((item) => item.file);
      return {
        key,
        family: entry.family,
        locale: keeper.locale,
        slide: keeper.slide,
        keeper: {
          slide: keeper.slide,
          locale: keeper.locale,
          file: keeper.file,
        },
        duplicates,
        renameCandidate:
          keeper.file.filename.toLowerCase() === keeper.canonicalFilename.toLowerCase()
            ? null
            : {
                id: keeper.file.id,
                from: keeper.file.filename,
                to: keeper.canonicalFilename,
              },
        candidates: ranked.map((item) => ({
          slide: item.slide,
          locale: item.locale,
          canonicalFilename: item.canonicalFilename,
          filename: item.file.filename,
          id: item.file.id,
          width: item.file.width,
          height: item.file.height,
        })),
      };
    })
    .sort(compareCanonicalGroupOrder);

  return {
    handle: entry.handle,
    family: entry.family,
    canonicalGroups,
  };
}

function parseFamilyFile(file, familyPattern) {
  const originalFilename = file.filename;
  const lower = originalFilename.toLowerCase();
  const extension = lower.slice(lower.lastIndexOf("."));
  const stem = lower
    .replace(/\.[^.]+$/, "")
    .replace(/_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i, "")
    .replace(/_(sa|asset)$/i, "")
    .replace(/_\d{8}$/i, "")
    .replace(/__+/g, "_");

  const patterns = [
    { locale: "ar", regex: new RegExp(`^tara_pdp_${familyPattern}_ar_(\\d[\\d_-]*)$`, "i") },
    { locale: "en", regex: new RegExp(`^tara_pdp_${familyPattern}_en_(\\d[\\d_-]*)$`, "i") },
    { locale: "en", regex: new RegExp(`^tara_pdp_${familyPattern}_(\\d[\\d_-]*)$`, "i") },
  ];

  for (const pattern of patterns) {
    const match = stem.match(pattern.regex);
    if (!match) {
      continue;
    }
    const numbers = extractNumberSequence(match[1]);
    if (numbers.length === 0) {
      continue;
    }
    const slide = numbers[0];
    const canonicalFilename = `tara_pdp_${familyPattern.replace("(?:-|_)", "_")}_${pattern.locale}_${slide}${extension}`;
    return {
      file,
      locale: pattern.locale,
      slide,
      canonicalFilename,
      qualityPenalty: buildQualityPenalty(originalFilename, numbers),
    };
  }

  return null;
}

function buildQualityPenalty(filename, numbers) {
  let penalty = 0;
  if (/_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.[^.]+$/i.test(filename)) {
    penalty += 100;
  }
  if (/_asset\.[^.]+$/i.test(filename)) {
    penalty += 40;
  }
  if (/_sa\.[^.]+$/i.test(filename)) {
    penalty += 20;
  }
  if (/_\d{8}\.[^.]+$/i.test(filename)) {
    penalty += 30;
  }
  penalty += Math.max(0, numbers.length - 1) * 3;
  penalty += filename.length / 1000;
  return penalty;
}

function compareParsedFilePriority(left, right) {
  const leftArea = (left.file.width ?? 0) * (left.file.height ?? 0);
  const rightArea = (right.file.width ?? 0) * (right.file.height ?? 0);
  if (leftArea !== rightArea) {
    return rightArea - leftArea;
  }
  if (left.qualityPenalty !== right.qualityPenalty) {
    return left.qualityPenalty - right.qualityPenalty;
  }
  const leftCreated = Date.parse(left.file.createdAt) || 0;
  const rightCreated = Date.parse(right.file.createdAt) || 0;
  if (leftCreated !== rightCreated) {
    return leftCreated - rightCreated;
  }
  return left.file.filename.localeCompare(right.file.filename);
}

function compareCanonicalGroupOrder(left, right) {
  if (left.family !== right.family) {
    return left.family.localeCompare(right.family);
  }
  if (left.locale !== right.locale) {
    return left.locale.localeCompare(right.locale);
  }
  return left.slide - right.slide;
}

function extractNumberSequence(segment) {
  return [...segment.matchAll(/\d+/g)].map((match) => Number(match[0]));
}

function buildLocaleGallery(family, locale, keeperByGroupKey) {
  const items = [];
  for (const [key, keeper] of keeperByGroupKey.entries()) {
    const [groupFamily, groupLocale, slideValue] = key.split(":");
    if (groupFamily !== family || groupLocale !== locale) {
      continue;
    }
    items.push({
      slide: Number(slideValue),
      file: keeper.file,
    });
  }

  return dedupeFilesById(items.sort((left, right) => left.slide - right.slide));
}

function buildMirroredArabicGallery(family, englishItems, keeperByGroupKey) {
  if (englishItems.length === 0) {
    return buildLocaleGallery(family, "ar", keeperByGroupKey);
  }

  const mirrored = englishItems.map((englishItem) => {
    const arabicMatch = keeperByGroupKey.get(`${family}:ar:${englishItem.slide}`);
    return {
      slide: englishItem.slide,
      file: arabicMatch?.file ?? englishItem.file,
    };
  });

  return dedupeFilesById(mirrored);
}

function dedupeFilesById(items) {
  const seen = new Set();
  return items.filter((item) => {
    if (seen.has(item.file.id)) {
      return false;
    }
    seen.add(item.file.id);
    return true;
  });
}

function galleriesDiffer(currentIds, nextIds) {
  if (currentIds.length !== nextIds.length) {
    return true;
  }
  return currentIds.some((id, index) => id !== nextIds[index]);
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

function toGalleryItems(nodes) {
  return nodes
    .filter((node) => node.image?.url)
    .map((node) => ({
      id: node.id,
      filename: filenameFromUrl(node.image.url),
    }));
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
        createdAt: node.createdAt,
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

async function applyGalleryUpdates(updates) {
  const results = [];
  for (const update of updates) {
    const data = await shopifyGraphql(METAFIELDS_SET_MUTATION, { metafields: update.metafields });
    results.push({
      handle: update.handle,
      userErrors: data.metafieldsSet.userErrors,
    });
  }
  return results;
}

async function batchDelete(fileIds) {
  const results = [];
  for (const batch of chunk(fileIds, 50)) {
    const data = await shopifyGraphql(FILE_DELETE_MUTATION, { fileIds: batch });
    results.push({
      requested: batch,
      deletedFileIds: data.fileDelete.deletedFileIds,
      userErrors: data.fileDelete.userErrors,
    });
  }
  return results;
}

async function batchRename(candidates, context = {}) {
  const results = [];
  const unusedIds = context.unusedIds ?? new Set();
  const exactNameMap = context.exactNameMap ?? new Map();
  for (const candidate of candidates) {
    let data = await renameFile(candidate);
    const hasFilenameCollision = (data.fileUpdate.userErrors ?? []).some(
      (error) => error.code === "FILENAME_ALREADY_EXISTS",
    );

    if (hasFilenameCollision) {
      const blockers = (exactNameMap.get(candidate.to.toLowerCase()) ?? []).filter(
        (file) => file.id !== candidate.id && unusedIds.has(file.id),
      );
      if (blockers.length > 0) {
        await batchDelete(blockers.map((file) => file.id));
        exactNameMap.delete(candidate.to.toLowerCase());
        for (const blocker of blockers) {
          unusedIds.delete(blocker.id);
        }
        await delay(1000);
        data = await renameFile(candidate);
      }
    }

    if ((data.fileUpdate.userErrors ?? []).length === 0) {
      exactNameMap.set(candidate.to.toLowerCase(), [{ id: candidate.id, filename: candidate.to }]);
    }

    results.push({
      ...candidate,
      userErrors: data.fileUpdate.userErrors,
    });
  }
  return results;
}

async function renameFile(candidate) {
  return shopifyGraphql(FILE_UPDATE_MUTATION, {
    files: [
      {
        id: candidate.id,
        filename: candidate.to,
      },
    ],
  });
}

function filenameFromUrl(url) {
  return decodeURIComponent(new URL(url).pathname.split("/").pop() || "");
}

function chunk(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function buildExactNameMap(files) {
  const map = new Map();
  for (const file of files) {
    const key = file.filename.toLowerCase();
    const existing = map.get(key) ?? [];
    existing.push(file);
    map.set(key, existing);
  }
  return map;
}

function serializeFamilyPlan(plan) {
  return {
    handle: plan.handle,
    family: plan.family,
    canonicalGroups: plan.canonicalGroups.map((group) => ({
      key: group.key,
      locale: group.locale,
      slide: group.slide,
      keeper: {
        id: group.keeper.file.id,
        filename: group.keeper.file.filename,
      },
      duplicates: group.duplicates.map(serializeDuplicateCandidate),
      renameCandidate: group.renameCandidate,
      candidates: group.candidates,
    })),
  };
}

function serializeGalleryUpdate(update) {
  return {
    handle: update.handle,
    title: update.title,
    englishBefore: update.englishBefore,
    englishAfter: update.englishAfter,
    arabicBefore: update.arabicBefore,
    arabicAfter: update.arabicAfter,
    arabicMirrorsEnglishOrder: update.arabicMirrorsEnglishOrder,
  };
}

function serializeDuplicateCandidate(file) {
  return {
    id: file.id,
    filename: file.filename,
    width: file.width,
    height: file.height,
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

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
