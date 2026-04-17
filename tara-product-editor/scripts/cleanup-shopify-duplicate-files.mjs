import fs from "node:fs/promises";
import path from "node:path";

const API_VERSION = "2025-10";
const SHOP = process.env.SHOPIFY_SHOP ?? "xkgw0m-sm.myshopify.com";
const TOKEN = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
const EXECUTE = process.env.EXECUTE === "1";
const REPORT_PATH = path.resolve(
  process.cwd(),
  "..",
  "temp",
  "saudi-shopify-duplicate-files-report.json",
);

if (!TOKEN) {
  throw new Error("Missing SHOPIFY_ADMIN_ACCESS_TOKEN");
}

const FILES_QUERY = `#graphql
  query LibraryFiles($first: Int!, $after: String, $search: String!) {
    files(first: $first, after: $after, query: $search) {
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
          alt
          image {
            url
            width
            height
          }
        }
        ... on GenericFile {
          alt
          url
          mimeType
        }
      }
    }
  }
`;

const DELETE_MUTATION = `#graphql
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

const UPDATE_MUTATION = `#graphql
  mutation RenameFiles($files: [FileUpdateInput!]!) {
    fileUpdate(files: $files) {
      files {
        id
        fileStatus
        ... on MediaImage {
          image {
            url
          }
        }
        ... on GenericFile {
          url
        }
      }
      userErrors {
        field
        message
        code
      }
    }
  }
`;

async function main() {
  const allFiles = await fetchAllFiles("");
  const unusedFiles = await fetchAllFiles("used_in:none");
  const unusedIds = new Set(unusedFiles.map((file) => file.id));
  const duplicatePlan = buildDuplicatePlan(allFiles, unusedIds);
  const namingPlan = buildNamingPlan(allFiles);

  const report = {
    generatedAt: new Date().toISOString(),
    scope: {
      shop: SHOP,
      fileTypesIncluded: ["MediaImage", "GenericFile"],
      execute: EXECUTE,
    },
    summary: {
      totalFiles: allFiles.length,
      unusedFiles: unusedFiles.length,
      duplicateGroupCount: duplicatePlan.groups.length,
      deleteCandidateCount: duplicatePlan.deleteCandidates.length,
      renameCandidateCount: namingPlan.renameCandidates.length,
      skippedGroups: duplicatePlan.skippedGroups.length + namingPlan.skippedGroups.length,
    },
    duplicateGroups: duplicatePlan.groups.map(toSerializableGroup),
    deleteCandidates: duplicatePlan.deleteCandidates,
    renameCandidates: namingPlan.renameCandidates,
    skippedGroups: [...duplicatePlan.skippedGroups, ...namingPlan.skippedGroups],
  };

  if (EXECUTE) {
    const deleteResults = await batchDelete(duplicatePlan.deleteCandidates.map((file) => file.id));
    await delay(2000);
    const postDeleteFiles = await fetchAllFiles("");
    const postDeleteNamingPlan = buildNamingPlan(postDeleteFiles);
    const renameResults = await batchRename(postDeleteNamingPlan.renameCandidates);
    report.execution = {
      deleteResults,
      renameResults,
    };
    report.postDeleteSummary = {
      totalFiles: postDeleteFiles.length,
      renameCandidateCount: postDeleteNamingPlan.renameCandidates.length,
      skippedGroups: postDeleteNamingPlan.skippedGroups.length,
    };
    report.renameCandidates = postDeleteNamingPlan.renameCandidates;
    report.skippedGroups = [...duplicatePlan.skippedGroups, ...postDeleteNamingPlan.skippedGroups];
  }

  await fs.writeFile(REPORT_PATH, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Report written to ${REPORT_PATH}`);
}

async function fetchAllFiles(search) {
  const files = [];
  let cursor = null;

  while (true) {
    const data = await shopifyGraphQL(FILES_QUERY, {
      first: 250,
      after: cursor,
      search,
    });
    const connection = data.files;
    for (const node of connection.nodes) {
      const file = buildFileRecord(node);
      if (file) {
        files.push(file);
      }
    }
    if (!connection.pageInfo.hasNextPage) {
      break;
    }
    cursor = connection.pageInfo.endCursor;
  }

  return files;
}

function buildFileRecord(node) {
  const url =
    node.__typename === "MediaImage"
      ? node.image?.url
      : node.__typename === "GenericFile"
        ? node.url
        : null;

  if (!url) {
    return null;
  }

  const filename = extractFilename(url);
  if (!filename) {
    return null;
  }

  return {
    id: node.id,
    type: node.__typename,
    createdAt: node.createdAt,
    fileStatus: node.fileStatus,
    url,
    filename,
    normalizedFilename: normalizeFilename(filename),
    hasUuidSuffix: hasUuidSuffix(filename),
    width: node.image?.width ?? null,
    height: node.image?.height ?? null,
  };
}

function extractFilename(url) {
  try {
    const pathname = new URL(url).pathname;
    const filename = decodeURIComponent(pathname.split("/").pop() ?? "");
    return filename || null;
  } catch {
    return null;
  }
}

function normalizeFilename(filename) {
  return filename.replace(
    /_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(\.[^.]+)$/i,
    "$1",
  );
}

function hasUuidSuffix(filename) {
  return /_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.[^.]+$/i.test(
    filename,
  );
}

function buildDuplicatePlan(files, unusedIds) {
  const groupsByNormalizedName = new Map();
  for (const file of files) {
    const key = file.normalizedFilename.toLowerCase();
    const existing = groupsByNormalizedName.get(key) ?? [];
    existing.push({
      ...file,
      isUnused: unusedIds.has(file.id),
    });
    groupsByNormalizedName.set(key, existing);
  }

  const groups = [];
  const deleteCandidates = [];
  const renameCandidates = [];
  const skippedGroups = [];

  for (const groupFiles of groupsByNormalizedName.values()) {
    if (groupFiles.length < 2) {
      continue;
    }

    const usedFiles = groupFiles.filter((file) => !file.isUnused);
    const unusedFiles = groupFiles.filter((file) => file.isUnused);
    const keeper = chooseKeeper(usedFiles.length > 0 ? usedFiles : groupFiles);
    const deleteList =
      usedFiles.length > 0
        ? unusedFiles
        : groupFiles.filter((file) => file.id !== keeper.id);

    groups.push({
      normalizedFilename: keeper.normalizedFilename,
      files: groupFiles.sort(compareByCreatedAt),
      keeperId: keeper.id,
      deleteIds: deleteList.map((file) => file.id),
    });

    deleteCandidates.push(...deleteList);

    if (canRenameKeeper(groupFiles, keeper)) {
      renameCandidates.push({
        id: keeper.id,
        from: keeper.filename,
        to: keeper.normalizedFilename,
        type: keeper.type,
      });
    } else if (keeper.filename !== keeper.normalizedFilename) {
      skippedGroups.push({
        normalizedFilename: keeper.normalizedFilename,
        reason:
          usedFiles.length > 1
            ? "Multiple used files share the same normalized filename; rename skipped to avoid collisions."
            : "Keeper rename was not safe for this duplicate group.",
      });
    }
  }

  return {
    groups: groups.sort((left, right) =>
      left.normalizedFilename.localeCompare(right.normalizedFilename),
    ),
    deleteCandidates,
    renameCandidates,
    skippedGroups,
  };
}

function buildNamingPlan(files) {
  const groupsByNormalizedName = new Map();
  const occupiedNames = new Set(files.map((file) => sanitizeFilename(file.filename).toLowerCase()));
  for (const file of files) {
    const key = file.normalizedFilename.toLowerCase();
    const existing = groupsByNormalizedName.get(key) ?? [];
    existing.push(file);
    groupsByNormalizedName.set(key, existing);
  }

  const renameCandidates = [];
  const skippedGroups = [];

  for (const groupFiles of groupsByNormalizedName.values()) {
    const eligible = groupFiles.filter(
      (file) => file.hasUuidSuffix && /^tara_/i.test(file.filename),
    );
    if (eligible.length === 0) {
      continue;
    }

    const normalizedFilename = groupFiles[0].normalizedFilename;
    const exactNameExists = groupFiles.some(
      (file) => file.filename.toLowerCase() === normalizedFilename.toLowerCase(),
    );

    if (exactNameExists) {
      skippedGroups.push({
        normalizedFilename,
        reason: "A file with the clean normalized filename already exists.",
      });
      continue;
    }

    if (groupFiles.length > 1) {
      skippedGroups.push({
        normalizedFilename,
        reason:
          "Multiple live files still normalize to the same target filename; rename skipped to avoid collisions.",
      });
      continue;
    }

    const keeper = eligible[0];
    const targets = buildRenameTargets(keeper);
    const selectedTarget =
      targets.find((target) => target.toLowerCase() !== sanitizeFilename(keeper.filename).toLowerCase() && !occupiedNames.has(target.toLowerCase())) ??
      targets.find((target) => target.toLowerCase() !== sanitizeFilename(keeper.filename).toLowerCase()) ??
      null;

    if (!selectedTarget) {
      skippedGroups.push({
        normalizedFilename,
        reason: "No safe rename target could be generated for this file.",
      });
      continue;
    }

    occupiedNames.add(selectedTarget.toLowerCase());
    renameCandidates.push({
      id: keeper.id,
      from: keeper.filename,
      to: selectedTarget,
      targets,
      type: keeper.type,
    });
  }

  return {
    renameCandidates: renameCandidates.sort((left, right) => left.to.localeCompare(right.to)),
    skippedGroups: skippedGroups.sort((left, right) =>
      left.normalizedFilename.localeCompare(right.normalizedFilename),
    ),
  };
}

function buildRenameTargets(file) {
  const baseTarget = sanitizeFilename(file.normalizedFilename);
  const { name, extension } = splitFilename(baseTarget);
  const gidSuffix = file.id.split("/").pop()?.slice(-8).toLowerCase() ?? "asset";
  return Array.from(
    new Set([
      baseTarget,
      `${name}_sa${extension}`,
      `${name}_asset${extension}`,
      `${name}_${gidSuffix}${extension}`,
    ]),
  );
}

function sanitizeFilename(filename) {
  const { name, extension } = splitFilename(filename);
  const sanitizedName = name
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/-+/g, "-")
    .replace(/^[_-]+|[_-]+$/g, "");
  return `${sanitizedName}${extension.toLowerCase()}`;
}

function splitFilename(filename) {
  const dotIndex = filename.lastIndexOf(".");
  if (dotIndex === -1) {
    return { name: filename, extension: "" };
  }
  return {
    name: filename.slice(0, dotIndex),
    extension: filename.slice(dotIndex),
  };
}

function chooseKeeper(files) {
  return [...files].sort(compareKeepPriority)[0];
}

function compareKeepPriority(left, right) {
  if (left.hasUuidSuffix !== right.hasUuidSuffix) {
    return left.hasUuidSuffix ? 1 : -1;
  }

  if (left.fileStatus !== right.fileStatus) {
    return left.fileStatus === "READY" ? -1 : 1;
  }

  const leftArea = (left.width ?? 0) * (left.height ?? 0);
  const rightArea = (right.width ?? 0) * (right.height ?? 0);
  if (leftArea !== rightArea) {
    return rightArea - leftArea;
  }

  const leftCreated = Date.parse(left.createdAt) || 0;
  const rightCreated = Date.parse(right.createdAt) || 0;
  if (leftCreated !== rightCreated) {
    return leftCreated - rightCreated;
  }

  return left.filename.localeCompare(right.filename);
}

function compareByCreatedAt(left, right) {
  return (Date.parse(left.createdAt) || 0) - (Date.parse(right.createdAt) || 0);
}

function canRenameKeeper(groupFiles, keeper) {
  if (keeper.filename === keeper.normalizedFilename) {
    return false;
  }

  const usedFiles = groupFiles.filter((file) => !file.isUnused);
  if (usedFiles.length > 1) {
    return false;
  }

  return true;
}

function toSerializableGroup(group) {
  return {
    normalizedFilename: group.normalizedFilename,
    keeperId: group.keeperId,
    deleteIds: group.deleteIds,
    files: group.files.map((file) => ({
      id: file.id,
      type: file.type,
      filename: file.filename,
      normalizedFilename: file.normalizedFilename,
      isUnused: file.isUnused,
      fileStatus: file.fileStatus,
      createdAt: file.createdAt,
      width: file.width,
      height: file.height,
      url: file.url,
    })),
  };
}

async function batchDelete(fileIds) {
  const results = [];
  for (const batch of chunk(fileIds, 50)) {
    const data = await shopifyGraphQL(DELETE_MUTATION, { fileIds: batch });
    results.push({
      requested: batch.length,
      deletedFileIds: data.fileDelete.deletedFileIds,
      userErrors: data.fileDelete.userErrors,
    });
  }
  return results;
}

async function batchRename(renameCandidates) {
  const results = [];
  for (const candidate of renameCandidates) {
    const data = await retryRename(candidate);
    results.push({
      requested: {
        id: candidate.id,
        from: candidate.from,
        to: candidate.to,
        targets: candidate.targets,
      },
      updatedFiles: (data.fileUpdate.files ?? []).map((file) => ({
        id: file.id,
        url: file.image?.url ?? file.url ?? null,
        fileStatus: file.fileStatus,
      })),
      userErrors: data.fileUpdate.userErrors,
    });
  }
  return results;
}

async function retryRename(candidate, attempts = 4) {
  let lastData = null;
  for (const target of candidate.targets ?? [candidate.to]) {
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      const data = await shopifyGraphQL(UPDATE_MUTATION, {
        files: [
          {
            id: candidate.id,
            filename: target,
          },
        ],
      });
      lastData = data;
      const userErrors = data.fileUpdate.userErrors ?? [];
      const hasRetriableCollision = userErrors.some(
        (error) => error.code === "FILENAME_ALREADY_EXISTS",
      );
      const hasInvalidFilename = userErrors.some((error) => error.code === "INVALID_FILENAME");
      if (!hasRetriableCollision && !hasInvalidFilename) {
        return data;
      }
      if (hasInvalidFilename || attempt === attempts) {
        break;
      }
      await delay(attempt * 1000);
    }
  }

  return lastData;
}

function chunk(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

async function shopifyGraphQL(query, variables) {
  const response = await fetch(`https://${SHOP}/admin/api/${API_VERSION}/graphql.json`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": TOKEN,
    },
    body: JSON.stringify({ query, variables }),
  });

  if (!response.ok) {
    throw new Error(`Shopify request failed with status ${response.status}`);
  }

  const payload = await response.json();
  if (payload.errors?.length) {
    throw new Error(`Shopify GraphQL errors: ${JSON.stringify(payload.errors)}`);
  }

  return payload.data;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
