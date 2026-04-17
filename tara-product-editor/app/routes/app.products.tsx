import {
  Badge,
  BlockStack,
  Button,
  Card,
  IndexTable,
  InlineGrid,
  InlineStack,
  Page,
  Text,
  Thumbnail,
  useIndexResourceState,
} from "@shopify/polaris";
import type { LoaderFunctionArgs } from "react-router";
import { Outlet, useLoaderData, useNavigate, useOutlet, useSearchParams } from "react-router";

import { ProductSearchForm } from "~/components/ProductSearchForm";
import {
  getProductIssueFocus,
  getProductReadinessScore,
  getProductReadinessTone,
} from "~/lib/product-work-queue";
import { decodeCursorState, nextCursorState, previousCursorState } from "~/lib/utils";
import { ensureDiscoveryConfig } from "~/services/discovery.server";
import { listProducts } from "~/services/products.server";
import { authenticate } from "~/shopify.server";
import type { ProductListFilters } from "~/types/editor";

export async function loader({ request }: LoaderFunctionArgs) {
  const { admin, session } = await authenticate.admin(request);
  const url = new URL(request.url);
  const cursorState = decodeCursorState(url.searchParams);
  const filters = {
    query: url.searchParams.get("query") || undefined,
    status: url.searchParams.get("status") || "all",
    vendor: url.searchParams.get("vendor") || undefined,
    productType: url.searchParams.get("productType") || undefined,
    tag: url.searchParams.get("tag") || undefined,
    focus: (url.searchParams.get("focus") || "all") as NonNullable<ProductListFilters["focus"]>,
    sort: (url.searchParams.get("sort") || "updated") as NonNullable<ProductListFilters["sort"]>,
    localeView: (url.searchParams.get("localeView") || "english") as NonNullable<ProductListFilters["localeView"]>,
  };
  const discovery = await ensureDiscoveryConfig(admin, session.shop);
  const result = await listProducts(admin, discovery, filters, cursorState.after);

  return {
    ...result,
    cursorState,
  };
}

export default function ProductIndexPage() {
  const outlet = useOutlet();
  const navigate = useNavigate();
  const { items, pageInfo, filters, cursorState } = useLoaderData<typeof loader>();
  const [searchParams] = useSearchParams();
  const resourceName = {
    singular: "product",
    plural: "products",
  };
  const { selectedResources, allResourcesSelected, handleSelectionChange } =
    useIndexResourceState(items as unknown as Array<{ [key: string]: unknown }>);

  if (outlet) {
    return <Outlet />;
  }

  const returnTo = searchParams.toString() ? `/app/products?${searchParams.toString()}` : "/app/products";
  const queue = items.map((item) => item.legacyId);
  const localeLabel = filters.localeView === "arabic" ? "Arabic" : "English";

  const rows = items.map((item, index) => {
    const editorUrl = buildProductEditorUrl({
      productId: item.legacyId,
      queue,
      currentIndex: index,
      returnTo,
      issueFocus: getProductIssueFocus(item, filters.focus),
      queueFocus: filters.focus === "all" ? undefined : filters.focus,
    });
    const readinessScore = getProductReadinessScore(item);

    return (
      <IndexTable.Row
        id={item.legacyId}
        key={item.id}
        selected={selectedResources.includes(item.legacyId)}
        position={index}
      >
        <IndexTable.Cell>
          {item.thumbnail ? (
            <button
              type="button"
              onClick={() => navigate(editorUrl)}
              aria-label={`Open ${item.title}`}
              style={{
                border: 0,
                padding: 0,
                background: "transparent",
                cursor: "pointer",
              }}
            >
              <Thumbnail source={item.thumbnail.url} alt={item.thumbnail.alt || item.title} size="small" />
            </button>
          ) : (
            <Text as="span" variant="bodySm" tone="subdued">
              No image
            </Text>
          )}
        </IndexTable.Cell>
        <IndexTable.Cell>
          <BlockStack gap="100">
            <Button url={editorUrl} variant="plain">
              {item.title}
            </Button>
            <InlineStack gap="200" wrap>
              <Badge tone={getProductReadinessTone(readinessScore)}>{`${readinessScore}% ready`}</Badge>
              {item.translationStatus.outdatedKeys > 0 ? (
                <Badge tone="attention">{`${item.translationStatus.outdatedKeys} outdated`}</Badge>
              ) : null}
            </InlineStack>
          </BlockStack>
        </IndexTable.Cell>
        <IndexTable.Cell>{item.status}</IndexTable.Cell>
        <IndexTable.Cell>{item.productType || "-"}</IndexTable.Cell>
        <IndexTable.Cell>
          <BlockStack gap="100">
            <InlineStack gap="200">
              <Badge
                tone={
                  item.translationStatus.translatedKeys === item.translationStatus.totalKeys &&
                  item.translationStatus.totalKeys > 0
                    ? "success"
                    : item.translationStatus.translatedKeys > 0
                      ? "attention"
                      : "critical"
                }
              >
                {`${item.translationStatus.translatedKeys}/${item.translationStatus.totalKeys || 0}`}
              </Badge>
              {item.translationStatus.outdatedKeys > 0 ? (
                <Badge tone="attention">{`${item.translationStatus.outdatedKeys} outdated`}</Badge>
              ) : null}
            </InlineStack>
            <Text as="span" variant="bodySm" tone="subdued">
              {item.translationStatus.outdatedKeys > 0
                ? "Needs refresh"
                : item.translationStatus.translatedKeys === item.translationStatus.totalKeys &&
                    item.translationStatus.totalKeys > 0
                  ? "Ready"
                  : "Arabic"}
            </Text>
          </BlockStack>
        </IndexTable.Cell>
        <IndexTable.Cell>
          <BlockStack gap="100">
            <InlineStack gap="200">
              <Badge tone={item.localeImageStatus.hasArabic ? "success" : "critical"}>
                {`EN ${item.localeImageStatus.englishCount} / AR ${item.localeImageStatus.arabicCount}`}
              </Badge>
              {item.localeImageStatus.mismatch ? <Badge tone="critical">Mismatch</Badge> : null}
              {item.localeImageStatus.englishCount > 0 && item.localeImageStatus.arabicCount === 0 ? (
                <Badge tone="critical">No AR media</Badge>
              ) : null}
            </InlineStack>
            {item.localeImageStatus.sources.length ? (
              <Text as="span" variant="bodySm" tone="subdued">
                {item.localeImageStatus.sources.join(", ")}
              </Text>
            ) : null}
          </BlockStack>
        </IndexTable.Cell>
      </IndexTable.Row>
    );
  });

  const nextParams = new URLSearchParams(searchParams);
  const nextState = nextCursorState(cursorState, pageInfo.endCursor);
  if (nextState.after) nextParams.set("after", nextState.after);
  if (nextState.history.length) nextParams.set("history", nextState.history.join(","));

  const previousState = previousCursorState(cursorState);
  const previousParams = new URLSearchParams(searchParams);
  if (previousState.after) {
    previousParams.set("after", previousState.after);
  } else {
    previousParams.delete("after");
  }
  if (previousState.history.length) {
    previousParams.set("history", previousState.history.join(","));
  } else {
    previousParams.delete("history");
  }

  const missingArabicCount = items.filter(
    (item) =>
      item.translationStatus.totalKeys === 0 ||
      item.translationStatus.translatedKeys < item.translationStatus.totalKeys,
  ).length;
  const outdatedArabicCount = items.filter((item) => item.translationStatus.outdatedKeys > 0).length;
  const imageMismatchCount = items.filter((item) => item.localeImageStatus.mismatch).length;
  const missingArabicMediaCount = items.filter(
    (item) => item.localeImageStatus.englishCount > 0 && item.localeImageStatus.arabicCount === 0,
  ).length;

  return (
    <Page
      title="Products"
      subtitle={`Browse every product, triage Arabic gaps, and open the full editor. Previewing ${localeLabel} titles and gallery cover images.`}
    >
      <BlockStack gap="500">
        <ProductSearchForm filters={filters} />

        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingMd">
              Work queues
            </Text>
            <InlineGrid columns={{ xs: 1, md: 4 }} gap="300">
              <QueueCard
                title="Missing Arabic"
                count={missingArabicCount}
                tone={missingArabicCount ? "critical" : "success"}
                active={filters.focus === "missingArabic"}
                onClick={() => navigate(buildQueueUrl(searchParams, "missingArabic"))}
              />
              <QueueCard
                title="Outdated Arabic"
                count={outdatedArabicCount}
                tone={outdatedArabicCount ? "attention" : "success"}
                active={filters.focus === "outdatedArabic"}
                onClick={() => navigate(buildQueueUrl(searchParams, "outdatedArabic"))}
              />
              <QueueCard
                title="Image mismatch"
                count={imageMismatchCount}
                tone={imageMismatchCount ? "critical" : "success"}
                active={filters.focus === "imageMismatch"}
                onClick={() => navigate(buildQueueUrl(searchParams, "imageMismatch"))}
              />
              <QueueCard
                title="Missing Arabic media"
                count={missingArabicMediaCount}
                tone={missingArabicMediaCount ? "critical" : "success"}
                active={filters.focus === "missingArabicMedia"}
                onClick={() => navigate(buildQueueUrl(searchParams, "missingArabicMedia"))}
              />
            </InlineGrid>
          </BlockStack>
        </Card>

        <InlineGrid columns={{ xs: 1, md: 3 }} gap="300">
          <Card>
            <BlockStack gap="100">
              <Text as="span" variant="bodySm" tone="subdued">
                Current results
              </Text>
              <Text as="span" variant="headingLg">
                {String(items.length)}
              </Text>
            </BlockStack>
          </Card>
          <Card>
            <BlockStack gap="100">
              <Text as="span" variant="bodySm" tone="subdued">
                Outdated Arabic
              </Text>
              <Text as="span" variant="headingLg">
                {String(outdatedArabicCount)}
              </Text>
            </BlockStack>
          </Card>
          <Card>
            <BlockStack gap="100">
              <Text as="span" variant="bodySm" tone="subdued">
                Media issues
              </Text>
              <Text as="span" variant="headingLg">
                {String(imageMismatchCount + missingArabicMediaCount)}
              </Text>
            </BlockStack>
          </Card>
        </InlineGrid>

        <Card>
          <BlockStack gap="300">
            <InlineStack align="space-between" blockAlign="center">
              <Text as="h2" variant="headingMd">
                Product index
              </Text>
              <InlineStack gap="200">
                <Button
                  url={
                    selectedResources.length
                      ? `/app/bulk?ids=${selectedResources.join(",")}`
                      : "/app/bulk"
                  }
                  disabled={selectedResources.length === 0}
                >
                  Bulk tools
                </Button>
              </InlineStack>
            </InlineStack>
            <IndexTable
              resourceName={resourceName}
              itemCount={items.length}
              selectedItemsCount={allResourcesSelected ? "All" : selectedResources.length}
              onSelectionChange={handleSelectionChange}
              headings={[
                { title: "Image" },
                { title: `${localeLabel} title` },
                { title: "Status" },
                { title: "Product type" },
                { title: "Arabic content" },
                { title: "Locale media" },
              ]}
            >
              {rows}
            </IndexTable>
            <InlineStack align="space-between">
              <Button disabled={cursorState.history.length === 0} url={`/app/products?${previousParams.toString()}`}>
                Previous
              </Button>
              <Button
                variant="primary"
                disabled={!pageInfo.hasNextPage}
                url={`/app/products?${nextParams.toString()}`}
              >
                Next
              </Button>
            </InlineStack>
          </BlockStack>
        </Card>
      </BlockStack>
    </Page>
  );
}

function buildProductEditorUrl({
  productId,
  queue,
  currentIndex,
  returnTo,
  issueFocus,
  queueFocus,
}: {
  productId: string;
  queue: string[];
  currentIndex: number;
  returnTo: string;
  issueFocus?: "content" | "media";
  queueFocus?: NonNullable<ProductListFilters["focus"]>;
}) {
  const params = new URLSearchParams();
  if (queue.length) {
    params.set("queue", queue.join(","));
    params.set("index", String(currentIndex));
  }
  params.set("returnTo", returnTo);
  if (issueFocus) {
    params.set("issue", issueFocus);
  }
  if (queueFocus) {
    params.set("queueFocus", queueFocus);
  }

  return `/app/products/${productId}?${params.toString()}`;
}

function buildQueueUrl(searchParams: URLSearchParams, focus: NonNullable<ProductListFilters["focus"]>) {
  const next = new URLSearchParams(searchParams);
  if (focus === "all") {
    next.delete("focus");
  } else {
    next.set("focus", focus);
  }
  next.delete("after");
  next.delete("history");
  return `/app/products?${next.toString()}`;
}

function QueueCard({
  title,
  count,
  tone,
  active,
  onClick,
}: {
  title: string;
  count: number;
  tone: "success" | "attention" | "critical";
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        width: "100%",
        border: active ? "2px solid #005bd3" : "1px solid #d2d5d8",
        borderRadius: 12,
        background: "#ffffff",
        padding: 16,
        textAlign: "left",
        cursor: "pointer",
      }}
    >
      <BlockStack gap="100">
        <InlineStack align="space-between" blockAlign="center">
          <Text as="span" variant="bodySm" tone="subdued">
            {title}
          </Text>
          <Badge tone={tone}>{active ? "Active" : count > 0 ? "Needs work" : "Clear"}</Badge>
        </InlineStack>
        <Text as="span" variant="headingLg">
          {String(count)}
        </Text>
      </BlockStack>
    </button>
  );
}
