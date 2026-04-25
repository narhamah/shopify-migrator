import { describe, expect, it } from "vitest";

import {
  getProductIssueFocus,
  getProductReadinessScore,
} from "../app/lib/product-work-queue";
import type { ProductIndexItem } from "../app/types/editor";

const baseItem: ProductIndexItem = {
  id: "gid://shopify/Product/1",
  legacyId: "1",
  title: "Volumizing Shampoo",
  handle: "volumizing-shampoo",
  status: "ACTIVE",
  vendor: "Tara",
  productType: "Shampoo",
  tags: [],
  updatedAt: "2026-03-23T00:00:00.000Z",
  thumbnail: null,
  translationStatus: {
    hasArabic: true,
    translatedKeys: 4,
    totalKeys: 6,
    outdatedKeys: 0,
  },
  localeImageStatus: {
    hasArabic: true,
    englishCount: 5,
    arabicCount: 5,
    mismatch: false,
    delta: 0,
    sources: [],
  },
};

describe("product work queue helpers", () => {
  it("routes missing or outdated Arabic products to content and media issues to media", () => {
    expect(getProductIssueFocus(baseItem, "missingArabic")).toBe("content");
    expect(getProductIssueFocus({ ...baseItem, translationStatus: { ...baseItem.translationStatus, outdatedKeys: 2 } }, "outdatedArabic")).toBe("content");
    expect(getProductIssueFocus({ ...baseItem, localeImageStatus: { ...baseItem.localeImageStatus, mismatch: true, delta: 2 } }, "imageMismatch")).toBe("media");
  });

  it("penalizes readiness for missing Arabic, outdated content, and media mismatch", () => {
    const healthy = {
      ...baseItem,
      translationStatus: { ...baseItem.translationStatus, translatedKeys: 6, totalKeys: 6, outdatedKeys: 0 },
    };
    const unhealthy = {
      ...baseItem,
      translationStatus: { ...baseItem.translationStatus, translatedKeys: 2, totalKeys: 6, outdatedKeys: 2 },
      localeImageStatus: { ...baseItem.localeImageStatus, arabicCount: 1, mismatch: true, delta: -4 },
    };

    expect(getProductReadinessScore(healthy)).toBeGreaterThan(getProductReadinessScore(unhealthy));
  });
});
