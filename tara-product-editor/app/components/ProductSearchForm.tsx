import { useEffect, useState } from "react";

import {
  BlockStack,
  Button,
  Card,
  InlineStack,
  Select,
  Text,
  TextField,
} from "@shopify/polaris";
import { useSearchParams } from "react-router";

import type { ProductListFilters } from "~/types/editor";

type LocalProductFilters = {
  query: string;
  status: string;
  vendor: string;
  productType: string;
  tag: string;
  focus: NonNullable<ProductListFilters["focus"]>;
  sort: NonNullable<ProductListFilters["sort"]>;
  localeView: NonNullable<ProductListFilters["localeView"]>;
};

export function ProductSearchForm({ filters }: { filters: ProductListFilters }) {
  const [, setSearchParams] = useSearchParams();
  const [localFilters, setLocalFilters] = useState<LocalProductFilters>(buildLocalFilters(filters));

  useEffect(() => {
    setLocalFilters(buildLocalFilters(filters));
  }, [filters]);

  useEffect(() => {
    function handleFocusShortcut(event: KeyboardEvent) {
      const target = event.target as HTMLElement | null;
      const isTypingTarget =
        target instanceof HTMLInputElement ||
        target instanceof HTMLTextAreaElement ||
        target?.isContentEditable;

      if (isTypingTarget || event.key !== "/") {
        return;
      }

      event.preventDefault();
      const searchInput = document.getElementById("tara-product-search") as HTMLInputElement | null;
      searchInput?.focus();
      searchInput?.select();
    }

    window.addEventListener("keydown", handleFocusShortcut);
    return () => window.removeEventListener("keydown", handleFocusShortcut);
  }, []);

  function applyFilters(nextFilters: LocalProductFilters = localFilters) {
    const next = new URLSearchParams();

    if (nextFilters.query.trim()) {
      next.set("query", nextFilters.query.trim());
    }
    if (nextFilters.status !== "all") {
      next.set("status", nextFilters.status);
    }
    if (nextFilters.vendor.trim()) {
      next.set("vendor", nextFilters.vendor.trim());
    }
    if (nextFilters.productType.trim()) {
      next.set("productType", nextFilters.productType.trim());
    }
    if (nextFilters.tag.trim()) {
      next.set("tag", nextFilters.tag.trim());
    }
    if (nextFilters.focus !== "all") {
      next.set("focus", nextFilters.focus);
    }
    if (nextFilters.sort !== "updated") {
      next.set("sort", nextFilters.sort);
    }
    if (nextFilters.localeView !== "english") {
      next.set("localeView", nextFilters.localeView);
    }

    setSearchParams(next);
  }

  function setFocusView(focus: LocalProductFilters["focus"]) {
    const nextFilters = { ...localFilters, focus };
    setLocalFilters(nextFilters);
    applyFilters(nextFilters);
  }

  function clearAll() {
    const nextFilters = buildLocalFilters({});
    setLocalFilters(nextFilters);
    applyFilters(nextFilters);
  }

  function setLocaleView(localeView: LocalProductFilters["localeView"]) {
    const nextFilters = { ...localFilters, localeView };
    setLocalFilters(nextFilters);
    applyFilters(nextFilters);
  }

  return (
    <Card>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          applyFilters();
        }}
      >
        <BlockStack gap="300">
          <InlineStack align="space-between" blockAlign="start">
            <BlockStack gap="100">
              <Text as="h2" variant="headingMd">
                Search and filter
              </Text>
              <Text as="p" variant="bodySm" tone="subdued">
                Match Shopify's product workflow by narrowing to the queue you need first, then opening products in sequence.
              </Text>
            </BlockStack>
            <InlineStack gap="200">
              <Button pressed={localFilters.localeView === "english"} onClick={() => setLocaleView("english")}>
                English
              </Button>
              <Button pressed={localFilters.localeView === "arabic"} onClick={() => setLocaleView("arabic")}>
                Arabic
              </Button>
              <Button onClick={clearAll}>Clear all</Button>
            </InlineStack>
          </InlineStack>
          <TextField
            id="tara-product-search"
            label="Search"
            placeholder="Search title, handle, vendor, tags, or paste a product handle"
            autoComplete="off"
            value={localFilters.query}
            onChange={(value) => setLocalFilters((current) => ({ ...current, query: value }))}
            helpText="Press / from anywhere on this page to jump back to search."
          />
          <InlineStack gap="200" wrap>
            <Button pressed={localFilters.focus === "all"} onClick={() => setFocusView("all")}>
              All products
            </Button>
            <Button
              pressed={localFilters.focus === "missingArabic"}
              onClick={() => setFocusView("missingArabic")}
            >
              Missing Arabic
            </Button>
            <Button
              pressed={localFilters.focus === "outdatedArabic"}
              onClick={() => setFocusView("outdatedArabic")}
            >
              Outdated Arabic
            </Button>
            <Button
              pressed={localFilters.focus === "imageMismatch"}
              onClick={() => setFocusView("imageMismatch")}
            >
              Image mismatch
            </Button>
            <Button
              pressed={localFilters.focus === "missingArabicMedia"}
              onClick={() => setFocusView("missingArabicMedia")}
            >
              Missing Arabic media
            </Button>
          </InlineStack>
          <InlineStack align="start" gap="300" blockAlign="end">
            <Select
              label="Status"
              options={[
                { label: "All", value: "all" },
                { label: "Active", value: "active" },
                { label: "Draft", value: "draft" },
                { label: "Archived", value: "archived" },
              ]}
              value={localFilters.status}
              onChange={(value) => setLocalFilters((current) => ({ ...current, status: value }))}
            />
            <TextField
              label="Vendor"
              autoComplete="off"
              value={localFilters.vendor}
              onChange={(value) => setLocalFilters((current) => ({ ...current, vendor: value }))}
            />
            <TextField
              label="Product type"
              autoComplete="off"
              value={localFilters.productType}
              onChange={(value) =>
                setLocalFilters((current) => ({ ...current, productType: value }))
              }
            />
            <TextField
              label="Tag"
              autoComplete="off"
              value={localFilters.tag}
              onChange={(value) => setLocalFilters((current) => ({ ...current, tag: value }))}
            />
            <Select
              label="Sort"
              options={[
                { label: "Recently updated", value: "updated" },
                { label: "Title A-Z", value: "title" },
                { label: "Needs Arabic first", value: "arabicReadiness" },
                { label: "Media issues first", value: "mediaMismatch" },
              ]}
              value={localFilters.sort}
              onChange={(value) =>
                setLocalFilters((current) => ({
                  ...current,
                  sort: value as LocalProductFilters["sort"],
                }))
              }
            />
            <Button submit variant="primary">
              Apply filters
            </Button>
          </InlineStack>
        </BlockStack>
      </form>
    </Card>
  );
}

function buildLocalFilters(filters: ProductListFilters): LocalProductFilters {
  return {
    query: filters.query || "",
    status: filters.status || "all",
    vendor: filters.vendor || "",
    productType: filters.productType || "",
    tag: filters.tag || "",
    focus: filters.focus || "all",
    sort: filters.sort || "updated",
    localeView: filters.localeView || "english",
  };
}
