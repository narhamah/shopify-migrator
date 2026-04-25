import type { ProductIndexItem, ProductListFilters } from "~/types/editor";

export type ProductIssueFocus = "content" | "media";

export function getProductIssueFocus(
  item: ProductIndexItem,
  focus: ProductListFilters["focus"],
): ProductIssueFocus | undefined {
  if (focus === "missingArabic" || focus === "outdatedArabic") {
    return "content";
  }

  if (focus === "imageMismatch" || focus === "missingArabicMedia") {
    return "media";
  }

  if (item.translationStatus.totalKeys === 0 || item.translationStatus.translatedKeys < item.translationStatus.totalKeys) {
    return "content";
  }

  if (item.translationStatus.outdatedKeys > 0) {
    return "content";
  }

  if (item.localeImageStatus.mismatch || (item.localeImageStatus.englishCount > 0 && item.localeImageStatus.arabicCount === 0)) {
    return "media";
  }

  return undefined;
}

export function getProductReadinessScore(item: ProductIndexItem) {
  const translationCompletion =
    item.translationStatus.totalKeys > 0
      ? item.translationStatus.translatedKeys / item.translationStatus.totalKeys
      : 1;
  const outdatedPenalty =
    item.translationStatus.totalKeys > 0
      ? item.translationStatus.outdatedKeys / item.translationStatus.totalKeys
      : 0;
  const mediaCompletion =
    item.localeImageStatus.englishCount === 0
      ? 1
      : Math.max(
          0,
          1 - Math.abs(item.localeImageStatus.englishCount - item.localeImageStatus.arabicCount) / item.localeImageStatus.englishCount,
        );

  const weightedScore = translationCompletion * 0.65 + mediaCompletion * 0.35 - outdatedPenalty * 0.2;
  return Math.max(0, Math.min(100, Math.round(weightedScore * 100)));
}

export function getProductReadinessTone(score: number): "success" | "attention" | "critical" {
  if (score >= 90) {
    return "success";
  }

  if (score >= 65) {
    return "attention";
  }

  return "critical";
}
