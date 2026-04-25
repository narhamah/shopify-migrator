import type { CursorState, ProductListFilters } from "~/types/editor";

export function assertEnv(name: keyof NodeJS.ProcessEnv) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

export function safeJsonParse<T>(value: string | null | undefined, fallback: T): T {
  if (!value) {
    return fallback;
  }

  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

export function safeJsonStringify(value: unknown) {
  return JSON.stringify(value, null, 2);
}

export function parseReferenceValue(value: string | null | undefined) {
  if (!value) {
    return [] as string[];
  }

  if (value.startsWith("[")) {
    return safeJsonParse<string[]>(value, []);
  }

  return [value];
}

export function normalizeMultilineIds(value: string) {
  return value
    .split(/\r?\n|,/)
    .map((item) => item.trim())
    .filter(Boolean);
}

export function normalizeTags(value: string) {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

export function buildProductSearchQuery(filters: ProductListFilters) {
  const parts: string[] = [];

  if (filters.query) {
    parts.push(filters.query.trim());
  }

  if (filters.status && filters.status !== "all") {
    parts.push(`status:${filters.status}`);
  }

  if (filters.vendor) {
    parts.push(`vendor:${quoteIfNeeded(filters.vendor)}`);
  }

  if (filters.productType) {
    parts.push(`product_type:${quoteIfNeeded(filters.productType)}`);
  }

  if (filters.tag) {
    parts.push(`tag:${quoteIfNeeded(filters.tag)}`);
  }

  return parts.join(" ").trim();
}

export function decodeCursorState(searchParams: URLSearchParams): CursorState {
  const after = searchParams.get("after") || undefined;
  const history = (searchParams.get("history") || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

  return { after, history };
}

export function nextCursorState(current: CursorState, endCursor?: string | null): CursorState {
  if (!endCursor) {
    return current;
  }

  return {
    after: endCursor,
    history: [...current.history, endCursor],
  };
}

export function previousCursorState(current: CursorState): CursorState {
  if (current.history.length <= 1) {
    return { history: [] };
  }

  const previousHistory = current.history.slice(0, -1);
  return {
    after: previousHistory[previousHistory.length - 1],
    history: previousHistory,
  };
}

export function formatTimestamp(value: string) {
  return new Intl.DateTimeFormat("en", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

export function reorderItems<T>(items: T[], fromIndex: number, toIndex: number) {
  const next = [...items];
  const [item] = next.splice(fromIndex, 1);
  next.splice(toIndex, 0, item);
  return next;
}

export function deepEqual(a: unknown, b: unknown) {
  return JSON.stringify(a) === JSON.stringify(b);
}

function quoteIfNeeded(value: string) {
  return /\s/.test(value) ? `"${value}"` : value;
}
