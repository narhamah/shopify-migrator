import { Fragment, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";

import { SaveBar, TitleBar, useAppBridge } from "@shopify/app-bridge-react";
import {
  Badge,
  Banner,
  BlockStack,
  Box,
  Button,
  Card,
  Checkbox,
  InlineGrid,
  InlineStack,
  Layout,
  Page,
  Select,
  Text,
  TextField,
} from "@shopify/polaris";
import { ArrowDownIcon, ArrowUpIcon } from "@shopify/polaris-icons";
import { useFetcher, useNavigate, useRevalidator, useSearchParams } from "react-router";

import { JsonCodeBlock } from "~/components/JsonCodeBlock";
import { ImageTargetEditor } from "~/components/ImageTargetEditor";
import { MetafieldReferencePicker } from "~/components/MetafieldReferencePicker";
import { NativeRichTextEditor } from "~/components/NativeRichTextEditor";
import { canInlineEditMetafield } from "~/lib/metafields";
import { extractRichTextPlainText, htmlToEditableText } from "~/lib/rich-text";
import { deepEqual, normalizeTags } from "~/lib/utils";
import type {
  DiscoveryConfig,
  EditorImageGroup,
  EditorMetafield,
  EditorTranslatableField,
  MetaobjectDefinitionOption,
  ProductEditorModel,
  ProductImageItem,
  SaveConflict,
  SaveResult,
} from "~/types/editor";

type ContentView = "english" | "arabic";
type MetafieldViewFilter = "all" | "pinned" | "needsArabic" | "changed";
type WorkMode = "all" | "missingArabic" | "outdatedArabic" | "media";
type EditorNavigationState = {
  queue: string[];
  currentIndex: number;
  previousProductId?: string;
  nextProductId?: string;
  returnTo: string;
};

const CONTENT_VIEW_STORAGE_KEY = "tara-product-editor-content-view";
const SECTION_IDS = {
  content: "editor-section-content",
  organization: "editor-section-organization",
  seo: "editor-section-seo",
  metafields: "editor-section-metafields",
} as const;
export function ProductEditorScreen({
  model,
  discovery,
  navigation,
}: {
  model: ProductEditorModel;
  discovery: DiscoveryConfig;
  navigation: EditorNavigationState;
}) {
  const navigate = useNavigate();
  const revalidator = useRevalidator();
  const shopify = useAppBridge();
  const [searchParams] = useSearchParams();
  const issueFocus = searchParams.get("issue");
  const queueFocus = searchParams.get("queueFocus");
  const saveFetcher = useFetcher<SaveResult>();
  const galleryFetcher = useFetcher<{
    append: boolean;
    files: ProductImageItem[];
    pageInfo: { hasNextPage: boolean; endCursor?: string | null };
    query: string;
  }>();
  const uploadFetcher = useFetcher<SaveResult & { file?: ProductImageItem }>();
  const loadedModelSnapshot = useMemo(() => serializeLoadedModelSnapshot(model), [model]);
  const initialState = useMemo(() => buildInitialEditorState(model), [loadedModelSnapshot]);

  const [contentView, setContentView] = useState<ContentView>("english");
  const [core, setCore] = useState(initialState.core);
  const [arabicFields, setArabicFields] = useState(initialState.arabicFields);
  const [metafields, setMetafields] = useState(initialState.metafields);
  const [imageGroups, setImageGroups] = useState(initialState.imageGroups);
  const [metafieldSearch, setMetafieldSearch] = useState("");
  const deferredMetafieldSearch = useDeferredValue(metafieldSearch);
  const [metafieldViewFilter, setMetafieldViewFilter] = useState<MetafieldViewFilter>("all");
  const [workMode, setWorkMode] = useState<WorkMode>(() => deriveWorkMode(queueFocus, issueFocus));
  const [showPopulatedOnly, setShowPopulatedOnly] = useState(true);
  const [libraryQuery, setLibraryQuery] = useState("");
  const [libraryFiles, setLibraryFiles] = useState<ProductImageItem[]>([]);
  const [libraryPageInfo, setLibraryPageInfo] = useState<{ hasNextPage: boolean; endCursor?: string | null }>({
    hasNextPage: false,
    endCursor: null,
  });
  const [lastImageSnapshot, setLastImageSnapshot] = useState<EditorImageGroup[] | null>(null);
  const [activeConflicts, setActiveConflicts] = useState<SaveConflict[]>([]);
  const lastAppliedModelSnapshot = useRef(loadedModelSnapshot);
  const showDeveloperDiagnostics = searchParams.get("developer") === "1";
  const productArabicFields = arabicFields.filter((field) => field.resourceId === model.product.id);
  const missingArabicFields = useMemo(
    () => productArabicFields.filter((field) => !field.arabicValue.trim()),
    [productArabicFields],
  );
  const outdatedArabicFields = useMemo(
    () => productArabicFields.filter((field) => field.outdated),
    [productArabicFields],
  );
  const seoInsights = useMemo(
    () =>
      buildSeoInsights({
        contentView,
        core,
        titleField: findField(productArabicFields, "title"),
        descriptionField: findField(productArabicFields, "body_html"),
        handleField: findField(productArabicFields, "handle"),
        seoTitleField: findField(productArabicFields, "meta_title"),
        seoDescriptionField: findField(productArabicFields, "meta_description"),
      }),
    [contentView, core, productArabicFields],
  );
  const mismatchedImageGroups = useMemo(
    () => imageGroups.filter((group) => group.mismatchWarning),
    [imageGroups],
  );
  const filteredMetafields = useMemo(() => {
    return metafields
      .filter((metafield) => {
        if (showPopulatedOnly && !metafield.isPopulated) return false;
        if (metafieldViewFilter === "pinned" && !metafield.isPinned) return false;
        if (metafieldViewFilter === "needsArabic" && !metafield.translation && !hasArabicMetaobjectGap(metafield)) {
          return false;
        }
        if (metafieldViewFilter === "changed") {
          const initialMetafield = initialState.metafields.find((candidate) => candidate.id === metafield.id);
          if (deepEqual(metafield, initialMetafield)) return false;
        }
        if (!deferredMetafieldSearch.trim()) return true;
        const haystack = `${metafield.namespace}.${metafield.key} ${metafield.name}`.toLowerCase();
        return haystack.includes(deferredMetafieldSearch.toLowerCase());
      })
      .sort((left, right) => {
        if (left.isPinned !== right.isPinned) {
          return left.isPinned ? -1 : 1;
        }
        return left.name.localeCompare(right.name);
      });
  }, [deferredMetafieldSearch, initialState.metafields, metafieldViewFilter, metafields, showPopulatedOnly]);
  const availableImages = useMemo(() => {
    const productImages = model.product.media.map((image) => ({ ...image, source: image.source || "product" as const }));
    const fileImages = libraryFiles.map((image) => ({ ...image, source: image.source || "files" as const }));
    const uploadedImage = uploadFetcher.data?.file?.url
      ? [{ ...uploadFetcher.data.file, source: uploadFetcher.data.file.source || "upload" as const }]
      : [];
    const merged = [...productImages, ...fileImages, ...uploadedImage];
    const byId = new Map(merged.map((image) => [image.id, image]));
    return [...byId.values()];
  }, [libraryFiles, model.product.media, uploadFetcher.data?.file]);
  const dirtySections = useMemo(
    () => ({
      core: !deepEqual(core, initialState.core),
      arabic: !deepEqual(arabicFields, initialState.arabicFields),
      metafields: !deepEqual(metafields, initialState.metafields),
      images: !deepEqual(imageGroups, initialState.imageGroups),
    }),
    [arabicFields, core, imageGroups, initialState, metafields],
  );
  const dirty = Object.values(dirtySections).some(Boolean);
  const isSaving = saveFetcher.state !== "idle";
  const productImageStatus = summarizeLocaleImages(imageGroups);
  const productTranslationStatus = summarizeTranslations(productArabicFields);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const storedView = window.localStorage.getItem(CONTENT_VIEW_STORAGE_KEY);
    if (storedView === "english" || storedView === "arabic") {
      setContentView(storedView);
    }
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    window.localStorage.setItem(CONTENT_VIEW_STORAGE_KEY, contentView);
  }, [contentView]);

  useEffect(() => {
    setWorkMode(deriveWorkMode(queueFocus, issueFocus));
  }, [issueFocus, queueFocus]);

  useEffect(() => {
    if (workMode === "missingArabic" || workMode === "outdatedArabic") {
      setContentView("arabic");
    }
  }, [workMode]);

  useEffect(() => {
    if (lastAppliedModelSnapshot.current === loadedModelSnapshot) {
      return;
    }

    setCore(initialState.core);
    setArabicFields(initialState.arabicFields);
    setMetafields(initialState.metafields);
    setImageGroups(initialState.imageGroups);
    setLastImageSnapshot(null);
    lastAppliedModelSnapshot.current = loadedModelSnapshot;
  }, [initialState, loadedModelSnapshot]);

  useEffect(() => {
    const data = galleryFetcher.data;
    if (!data) return;
    setLibraryPageInfo(data.pageInfo);
    setLibraryFiles((current) => {
      const next = data.append ? [...current, ...data.files] : data.files;
      const byId = new Map(next.map((image) => [image.id, image]));
      return [...byId.values()];
    });
  }, [galleryFetcher.data]);

  useEffect(() => {
    if (!saveFetcher.data?.message) return;
    shopify.toast.show(saveFetcher.data.message, { isError: !saveFetcher.data.ok });
    if (saveFetcher.data.ok) {
      revalidator.revalidate();
    }
  }, [revalidator, saveFetcher.data, shopify]);

  useEffect(() => {
    setActiveConflicts(saveFetcher.data?.conflicts || []);
  }, [saveFetcher.data]);

  useEffect(() => {
    if (!uploadFetcher.data?.message) return;
    shopify.toast.show(uploadFetcher.data.message, { isError: !uploadFetcher.data.ok });
  }, [shopify, uploadFetcher.data]);

  useEffect(() => {
    window.onbeforeunload = dirty ? () => true : null;
    return () => {
      window.onbeforeunload = null;
    };
  }, [dirty]);

  useEffect(() => {
    function handleSaveShortcut(event: KeyboardEvent) {
      if (!(event.metaKey || event.ctrlKey) || event.key.toLowerCase() !== "s") {
        return;
      }

      event.preventDefault();
      if (!dirty || isSaving) {
        return;
      }

      saveAll();
    }

    window.addEventListener("keydown", handleSaveShortcut);
    return () => window.removeEventListener("keydown", handleSaveShortcut);
  }, [dirty, isSaving, core, arabicFields, metafields, imageGroups]);

  async function navigateBack() {
    if (dirty) {
      try {
        await shopify.saveBar.leaveConfirmation();
      } catch {
        return;
      }
    }
    navigate(navigation.returnTo || "/app/products");
  }

  async function navigateAdjacent(direction: "previous" | "next") {
    const targetProductId =
      direction === "previous" ? navigation.previousProductId : navigation.nextProductId;

    if (!targetProductId) {
      return;
    }

    if (dirty) {
      try {
        await shopify.saveBar.leaveConfirmation();
      } catch {
        return;
      }
    }

    const nextIndex =
      direction === "previous" ? navigation.currentIndex - 1 : navigation.currentIndex + 1;

    navigate(
      buildEditorNavigationUrl({
        productId: targetProductId,
        queue: navigation.queue,
        currentIndex: nextIndex,
        returnTo: navigation.returnTo,
      }),
    );
  }

  function restoreFromModel() {
    setCore(initialState.core);
    setArabicFields(initialState.arabicFields);
    setMetafields(initialState.metafields);
    setImageGroups(initialState.imageGroups);
    setLastImageSnapshot(null);
  }

  function saveAll() {
    const changedCore = dirtySections.core ? buildCorePayload(core) : null;
    const changedArabicFields = dirtySections.arabic
      ? arabicFields.filter((field) => {
          const initialField = initialState.arabicFields.find(
            (candidate) =>
              candidate.resourceId === field.resourceId && candidate.translationKey === field.translationKey,
          );

          return !deepEqual(field, initialField);
        })
      : [];
    const changedMetafields = dirtySections.metafields
      ? metafields.filter((metafield) => {
          const initialMetafield = initialState.metafields.find((candidate) => candidate.id === metafield.id);
          return !deepEqual(metafield, initialMetafield);
        })
      : [];
    const changedImageGroups = dirtySections.images
      ? imageGroups.filter((group) => {
          const initialGroup = initialState.imageGroups.find((candidate) => candidate.id === group.id);
          return !deepEqual(group, initialGroup);
        })
      : [];

    submitIntent(saveFetcher, "saveAll", {
      core: changedCore,
      arabicFields: changedArabicFields,
      metafields: changedMetafields.filter((metafield) => canInlineEditMetafield(metafield.type)),
      imageGroups: changedImageGroups,
    });
  }

  function jumpToSection(sectionId: string) {
    if (typeof document === "undefined") {
      return;
    }

    document.getElementById(sectionId)?.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  function keepDraftForConflict(conflict: SaveConflict) {
    if (conflict.kind === "translation") {
      setArabicFields((current) =>
        current.map((field) =>
          field.resourceId === conflict.resourceId && field.translationKey === conflict.fieldKey
            ? {
                ...field,
                digest: conflict.latestDigest || field.digest,
                sourceValue: conflict.latestSourceValue ?? field.sourceValue,
                outdated: false,
              }
            : field,
        ),
      );
    } else if (conflict.kind === "metafield") {
      setMetafields((current) =>
        current.map((metafield) =>
          metafield.id === conflict.resourceId
            ? {
                ...metafield,
                compareDigest: conflict.latestCompareDigest ?? metafield.compareDigest,
              }
            : metafield,
        ),
      );
    } else {
      setImageGroups((current) =>
        current.map((group) => ({
          ...group,
          english: group.english.map((target) =>
            target.resourceId === conflict.resourceId
              ? { ...target, compareDigest: conflict.latestCompareDigest ?? target.compareDigest }
              : target,
          ),
          arabic: group.arabic.map((target) =>
            target.resourceId === conflict.resourceId
              ? { ...target, compareDigest: conflict.latestCompareDigest ?? target.compareDigest }
              : target,
          ),
        })),
      );
    }

    setActiveConflicts((current) => current.filter((item) => item !== conflict));
  }

  function acceptShopifyVersion(conflict: SaveConflict) {
    if (conflict.kind === "translation") {
      setArabicFields((current) =>
        current.map((field) =>
          field.resourceId === conflict.resourceId && field.translationKey === conflict.fieldKey
            ? {
                ...field,
                arabicValue: conflict.latestValue,
                digest: conflict.latestDigest || field.digest,
                sourceValue: conflict.latestSourceValue ?? field.sourceValue,
                outdated: false,
              }
            : field,
        ),
      );
    } else if (conflict.kind === "metafield") {
      setMetafields((current) =>
        current.map((metafield) =>
          metafield.id === conflict.resourceId
            ? {
                ...metafield,
                value: conflict.latestValue,
                compareDigest: conflict.latestCompareDigest ?? metafield.compareDigest,
              }
            : metafield,
        ),
      );
    } else {
      revalidator.revalidate();
    }

    setActiveConflicts((current) => current.filter((item) => item !== conflict));
  }

  useEffect(() => {
    if (issueFocus !== "content" && issueFocus !== "media" && issueFocus !== "metafields") {
      return;
    }

    const sectionId = issueFocus === "metafields" ? SECTION_IDS.metafields : SECTION_IDS.content;
    const timer = window.setTimeout(() => {
      jumpToSection(sectionId);
    }, 60);

    return () => window.clearTimeout(timer);
  }, [issueFocus, loadedModelSnapshot]);

  function loadLibraryFiles(append = false) {
    const params = new URLSearchParams();
    if (libraryQuery.trim()) {
      params.set("query", libraryQuery);
    }
    if (append && libraryPageInfo.endCursor) {
      params.set("after", libraryPageInfo.endCursor);
      params.set("append", "1");
    }
    galleryFetcher.load(`/app/files?${params.toString()}`);
  }

  function applyImageMutation(
    updater: (current: EditorImageGroup[]) => EditorImageGroup[],
    options?: { confirmMessage?: string },
  ) {
    if (options?.confirmMessage && !window.confirm(options.confirmMessage)) {
      return;
    }
    setLastImageSnapshot(imageGroups);
    setImageGroups((current) => updater(current));
  }

  function searchLibraryFiles() {
    loadLibraryFiles(false);
  }

  function loadMoreLibraryFiles() {
    loadLibraryFiles(true);
  }

  function uploadLibraryFile(file: File, alt?: string) {
    const formData = new FormData();
    formData.set("file", file);
    if (alt) {
      formData.set("alt", alt);
    }
    uploadFetcher.submit(formData, {
      method: "post",
      action: "/app/files",
      encType: "multipart/form-data",
    });
  }

  function updateImageGroup(
    groupId: string,
    locale: "english" | "arabic",
    targetId: string,
    images: ProductImageItem[],
  ) {
    setImageGroups((current) =>
      current.map((group) =>
        group.id !== groupId
          ? group
          : {
              ...group,
              [locale]: group[locale].map((target) =>
                target.target.id === targetId ? { ...target, images } : target,
              ),
            },
      ),
    );
  }

  function duplicateImageGroup(groupId: string, sourceLocale: "english" | "arabic") {
    applyImageMutation(
      (current) =>
        current.map((group) => {
          if (group.id !== groupId) return group;
          const source = sourceLocale === "english" ? group.english : group.arabic;
          const destinationLocale = sourceLocale === "english" ? "arabic" : "english";
          return {
            ...group,
            [destinationLocale]: group[destinationLocale].map((target, index) => ({
              ...target,
              images: source[index]?.images || [],
            })),
          };
        }),
      {
        confirmMessage:
          sourceLocale === "english"
            ? "Replace the Arabic gallery with the English gallery?"
            : "Replace the English gallery with the Arabic gallery?",
      },
    );
  }

  function copySelectedImagesToOtherLocale(
    groupId: string,
    sourceLocale: "english" | "arabic",
    targetId: string,
    selectedImageIds: string[],
  ) {
    if (selectedImageIds.length === 0) {
      return;
    }

    applyImageMutation(
      (current) =>
        current.map((group) =>
          group.id !== groupId
            ? group
            : {
                ...group,
                [sourceLocale === "english" ? "arabic" : "english"]: syncSelectedImagesBetweenTargets({
                  sourceTargets: group[sourceLocale],
                  destinationTargets: group[sourceLocale === "english" ? "arabic" : "english"],
                  targetId,
                  selectedImageIds,
                }),
              },
        ),
    );
  }

  return (
    <>
      <TitleBar title={model.product.title}>
        <button variant="breadcrumb" onClick={navigateBack}>
          Products
        </button>
        <button onClick={restoreFromModel} disabled={!dirty || isSaving}>
          Discard
        </button>
        <button variant="primary" onClick={saveAll} disabled={!dirty || isSaving}>
          Save all
        </button>
      </TitleBar>

      <SaveBar id="tara-product-editor-save-bar" open={dirty} discardConfirmation>
        <button onClick={restoreFromModel} disabled={isSaving}>
          Discard
        </button>
        <button variant="primary" onClick={saveAll} disabled={!dirty || isSaving}>
          Save all
        </button>
      </SaveBar>

      <Page fullWidth>
        <Layout>
          <Layout.Section>
            <BlockStack gap="500">
              {activeConflicts.length ? (
                <Banner tone="warning" title="Some fields changed in Shopify while you were editing">
                  <BlockStack gap="300">
                    {activeConflicts.map((conflict) => (
                      <Box key={`${conflict.kind}-${conflict.resourceId}-${conflict.fieldKey}`} padding="200" background="bg-surface-secondary" borderRadius="200">
                        <BlockStack gap="200">
                          <Text as="p" variant="bodySm">
                            <strong>{conflict.label}</strong>: {conflict.message}
                          </Text>
                          {conflict.latestSourceValue ? (
                            <Text as="p" variant="bodySm" tone="subdued">
                              Latest source: {previewConflictValue(conflict.latestSourceValue)}
                            </Text>
                          ) : null}
                          <Text as="p" variant="bodySm" tone="subdued">
                            Shopify version: {previewConflictValue(conflict.latestValue)}
                          </Text>
                          <InlineStack gap="200">
                            <Button size="slim" variant="primary" onClick={() => keepDraftForConflict(conflict)}>
                              Keep my draft
                            </Button>
                            <Button size="slim" onClick={() => acceptShopifyVersion(conflict)}>
                              {conflict.kind === "image" ? "Reload latest gallery" : "Use Shopify version"}
                            </Button>
                          </InlineStack>
                        </BlockStack>
                      </Box>
                    ))}
                  </BlockStack>
                </Banner>
              ) : null}

              {saveFetcher.data?.ok === false ? (
                <Banner tone="critical" title={saveFetcher.data.message}>
                  <BlockStack gap="100">
                    {(saveFetcher.data.sectionResults || []).map((section) => (
                      <Text key={section.key} as="p" variant="bodySm">
                        {formatSectionLabel(section.key)}: {section.ok ? section.message : `${section.errorCount} issue${section.errorCount === 1 ? "" : "s"}`}
                      </Text>
                    ))}
                    {(saveFetcher.data.errors || []).map((error, index) => (
                      <Text key={`${error.field}-${index}`} as="p" variant="bodySm">
                        {error.field ? `${error.field}: ` : ""}
                        {error.message}
                      </Text>
                    ))}
                  </BlockStack>
                </Banner>
              ) : null}

              <EditorHeaderCard
                model={model}
                navigation={navigation}
                dirty={dirty}
                isSaving={isSaving}
                contentView={contentView}
                workMode={workMode}
                translationStatus={productTranslationStatus}
                imageStatus={productImageStatus}
                missingArabicCount={missingArabicFields.length}
                outdatedArabicCount={outdatedArabicFields.length}
                seoInsightCount={seoInsights.warnings.length}
                mediaMismatchCount={mismatchedImageGroups.length}
                dirtySections={dirtySections}
                onChangeView={setContentView}
                onChangeWorkMode={setWorkMode}
                onNavigateAdjacent={navigateAdjacent}
                onReset={restoreFromModel}
                onSaveAll={saveAll}
                onJumpToSection={jumpToSection}
              />
              <ContentTab
                core={core}
                productArabicFields={productArabicFields}
                imageGroups={imageGroups}
                hasUndoImageAction={Boolean(lastImageSnapshot)}
                availableImages={availableImages}
                contentView={contentView}
                workMode={workMode}
                libraryQuery={libraryQuery}
                hasMoreFiles={libraryPageInfo.hasNextPage}
                fileSearchLoading={galleryFetcher.state !== "idle"}
                fileUploadLoading={uploadFetcher.state !== "idle"}
                onCoreChange={setCore}
                onLibraryQueryChange={setLibraryQuery}
                onSearchFiles={searchLibraryFiles}
                onLoadMoreFiles={loadMoreLibraryFiles}
                onUploadFile={uploadLibraryFile}
                onUndoLastImageAction={() => {
                  if (!lastImageSnapshot) return;
                  setImageGroups(lastImageSnapshot);
                  setLastImageSnapshot(null);
                }}
                onUpdateGroup={updateImageGroup}
                onDuplicateGroup={duplicateImageGroup}
                onCopySelectedImages={copySelectedImagesToOtherLocale}
                onArabicChange={(field, value) =>
                  setArabicFields((current) => updateArabicFieldValue(current, field, value))
                }
                seoInsights={seoInsights}
              />
              <MetafieldsTab
                metafields={filteredMetafields}
                allMetafields={metafields}
                initialMetafields={initialState.metafields}
                allArabicFields={arabicFields}
                metaobjectDefinitions={model.metaobjectDefinitions}
                imageGroups={imageGroups}
                contentView={contentView}
                workMode={workMode}
                metafieldSearch={metafieldSearch}
                metafieldViewFilter={metafieldViewFilter}
                showPopulatedOnly={showPopulatedOnly}
                onSearchChange={setMetafieldSearch}
                onViewFilterChange={setMetafieldViewFilter}
                onShowPopulatedOnlyChange={setShowPopulatedOnly}
                onToggleFavorite={(metafield) =>
                  submitIntent(saveFetcher, "toggleFavorite", {
                    namespace: metafield.namespace,
                    key: metafield.key,
                  })
                }
                onMetafieldChange={(nextMetafield) =>
                  setMetafields((current) =>
                    current.map((item) => (item.id === nextMetafield.id ? nextMetafield : item)),
                  )
                }
                onArabicMetafieldChange={(field, value) =>
                  setArabicFields((current) => updateArabicFieldValue(current, field, value))
                }
              />
              {showDeveloperDiagnostics ? (
                <Card>
                  <BlockStack gap="300">
                    <Text as="h2" variant="headingMd">
                      Developer diagnostics
                    </Text>
                    <JsonCodeBlock value={JSON.stringify({ model, discovery }, null, 2)} height={720} />
                  </BlockStack>
                </Card>
              ) : null}
            </BlockStack>
          </Layout.Section>

          <Layout.Section variant="oneThird">
            <SummarySidebar
              model={model}
              metafields={metafields}
              productArabicFields={productArabicFields}
              imageGroups={imageGroups}
              dirtySections={dirtySections}
              seoInsights={seoInsights}
              onJumpToSection={jumpToSection}
            />
          </Layout.Section>
        </Layout>
      </Page>
    </>
  );
}

function EditorHeaderCard({
  model,
  navigation,
  dirty,
  isSaving,
  contentView,
  workMode,
  translationStatus,
  imageStatus,
  missingArabicCount,
  outdatedArabicCount,
  seoInsightCount,
  mediaMismatchCount,
  dirtySections,
  onChangeView,
  onChangeWorkMode,
  onNavigateAdjacent,
  onReset,
  onSaveAll,
  onJumpToSection,
}: {
  model: ProductEditorModel;
  navigation: EditorNavigationState;
  dirty: boolean;
  isSaving: boolean;
  contentView: ContentView;
  workMode: WorkMode;
  translationStatus: { translated: number; total: number };
  imageStatus: { english: number; arabic: number; mismatched: number };
  missingArabicCount: number;
  outdatedArabicCount: number;
  seoInsightCount: number;
  mediaMismatchCount: number;
  dirtySections: Record<string, boolean>;
  onChangeView: (value: ContentView) => void;
  onChangeWorkMode: (value: WorkMode) => void;
  onNavigateAdjacent: (direction: "previous" | "next") => void;
  onReset: () => void;
  onSaveAll: () => void;
  onJumpToSection: (sectionId: string) => void;
}) {
  const changedCount = Object.values(dirtySections).filter(Boolean).length;
  const contentModeLabel = contentView === "english" ? "Editing English" : "Editing Arabic";
  const queuePosition = navigation.currentIndex >= 0 ? `${navigation.currentIndex + 1} of ${navigation.queue.length}` : null;

  return (
    <Card>
      <BlockStack gap="400">
        <InlineStack align="space-between" blockAlign="start">
          <BlockStack gap="200">
            <InlineStack gap="200" blockAlign="center" wrap>
              <Text as="h1" variant="headingLg">
                {model.product.title}
              </Text>
              <InlineStack gap="100" blockAlign="center">
                <Button
                  icon={ArrowUpIcon}
                  accessibilityLabel="Previous product"
                  disabled={!navigation.previousProductId}
                  onClick={() => onNavigateAdjacent("previous")}
                />
                <Button
                  icon={ArrowDownIcon}
                  accessibilityLabel="Next product"
                  disabled={!navigation.nextProductId}
                  onClick={() => onNavigateAdjacent("next")}
                />
                {queuePosition ? (
                  <Text as="span" variant="bodySm" tone="subdued">
                    {queuePosition}
                  </Text>
                ) : null}
              </InlineStack>
            </InlineStack>
            <InlineStack gap="200" blockAlign="center" wrap>
              <Badge tone={toneForStatus(model.product.status)}>{model.product.status}</Badge>
              <Badge tone={contentView === "english" ? "success" : "info"}>{contentModeLabel}</Badge>
              <Badge tone={dirty ? "warning" : "success"}>
                {dirty ? `${changedCount} section${changedCount === 1 ? "" : "s"} changed` : "All changes saved"}
              </Badge>
              {dirty ? <Badge tone="attention">Use Ctrl/Cmd+S to save</Badge> : null}
            </InlineStack>
            <Text as="p" variant="bodyMd" tone="subdued">
              Handle: {model.product.handle} | Vendor: {model.product.vendor || "No vendor"} | Type:{" "}
              {model.product.productType || "No product type"}
            </Text>
          </BlockStack>

          <InlineStack gap="200" blockAlign="end">
            <Box minWidth="220px">
              <BlockStack gap="100">
                <Text as="span" variant="bodySm" tone="subdued">
                  Editing language
                </Text>
                <InlineStack gap="200">
                  <Button pressed={contentView === "english"} onClick={() => onChangeView("english")}>
                    English
                  </Button>
                  <Button pressed={contentView === "arabic"} onClick={() => onChangeView("arabic")}>
                    Arabic
                  </Button>
                </InlineStack>
              </BlockStack>
            </Box>
            <Box minWidth="220px">
              <Select
                label="Work mode"
                options={[
                  { label: "All fields", value: "all" },
                  { label: "Missing Arabic", value: "missingArabic" },
                  { label: "Outdated Arabic", value: "outdatedArabic" },
                  { label: "Media issues", value: "media" },
                ]}
                value={workMode}
                onChange={(value) => onChangeWorkMode(value as WorkMode)}
              />
            </Box>
            <Button onClick={onReset} disabled={!dirty || isSaving}>
              Discard changes
            </Button>
            <Button variant="primary" onClick={onSaveAll} loading={isSaving} disabled={!dirty}>
              Save all
            </Button>
          </InlineStack>
        </InlineStack>

        <InlineGrid columns={{ xs: 2, md: 4 }} gap="300">
          <MetricCard label="Arabic content" value={`${translationStatus.translated}/${translationStatus.total}`} />
          <MetricCard label="Locale images" value={`${imageStatus.english} EN | ${imageStatus.arabic} AR`} />
          <MetricCard label="Metafields" value={String(model.metafields.length)} />
          <MetricCard label="Product media" value={String(model.product.media.length)} />
        </InlineGrid>

        <InlineStack gap="200" wrap>
          <Button
            size="slim"
            onClick={() => onJumpToSection(SECTION_IDS.content)}
            disabled={missingArabicCount === 0 && outdatedArabicCount === 0}
          >
            {missingArabicCount > 0 ? `${missingArabicCount} missing Arabic` : `${outdatedArabicCount} outdated`}
          </Button>
          <Button
            size="slim"
            onClick={() => onJumpToSection(SECTION_IDS.seo)}
            disabled={seoInsightCount === 0}
          >
            {seoInsightCount > 0 ? `${seoInsightCount} SEO checks` : "SEO ready"}
          </Button>
          <Button
            size="slim"
            onClick={() => onJumpToSection(SECTION_IDS.metafields)}
          >
            Review metafields
          </Button>
          <Button
            size="slim"
            onClick={() => onJumpToSection(SECTION_IDS.content)}
            disabled={mediaMismatchCount === 0}
          >
            {mediaMismatchCount > 0 ? `${mediaMismatchCount} media mismatch` : "Media aligned"}
          </Button>
        </InlineStack>
      </BlockStack>
    </Card>
  );
}

function ContentTab({
  core,
  productArabicFields,
  imageGroups,
  hasUndoImageAction,
  availableImages,
  contentView,
  workMode,
  libraryQuery,
  hasMoreFiles,
  fileSearchLoading,
  fileUploadLoading,
  onCoreChange,
  onLibraryQueryChange,
  onSearchFiles,
  onLoadMoreFiles,
  onUploadFile,
  onUndoLastImageAction,
  onUpdateGroup,
  onDuplicateGroup,
  onCopySelectedImages,
  onArabicChange,
  seoInsights,
}: {
  core: ReturnType<typeof buildCoreState>;
  productArabicFields: EditorTranslatableField[];
  imageGroups: EditorImageGroup[];
  hasUndoImageAction: boolean;
  availableImages: ProductImageItem[];
  contentView: ContentView;
  workMode: WorkMode;
  libraryQuery: string;
  hasMoreFiles: boolean;
  fileSearchLoading: boolean;
  fileUploadLoading: boolean;
  onCoreChange: (updater: (current: ReturnType<typeof buildCoreState>) => ReturnType<typeof buildCoreState>) => void;
  onLibraryQueryChange: (value: string) => void;
  onSearchFiles: () => void;
  onLoadMoreFiles: () => void;
  onUploadFile: (file: File, alt?: string) => void;
  onUndoLastImageAction: () => void;
  onUpdateGroup: (groupId: string, locale: "english" | "arabic", targetId: string, images: ProductImageItem[]) => void;
  onDuplicateGroup: (groupId: string, sourceLocale: "english" | "arabic") => void;
  onCopySelectedImages: (
    groupId: string,
    sourceLocale: "english" | "arabic",
    targetId: string,
    selectedImageIds: string[],
  ) => void;
  onArabicChange: (field: EditorTranslatableField, value: string) => void;
  seoInsights: SeoInsights;
}) {
  const titleField = findField(productArabicFields, "title");
  const descriptionField = findField(productArabicFields, "body_html");
  const handleField = findField(productArabicFields, "handle");
  const productTypeField = findField(productArabicFields, "product_type");
  const seoTitleField = findField(productArabicFields, "meta_title");
  const seoDescriptionField = findField(productArabicFields, "meta_description");
  const contentFields = [
    titleField,
    descriptionField,
    handleField,
    productTypeField,
    seoTitleField,
    seoDescriptionField,
  ].filter(Boolean) as EditorTranslatableField[];
  const visibleArabicKeys = new Set(
    contentFields
      .filter((field) => shouldShowArabicField(field, workMode))
      .map((field) => field.translationKey),
  );
  const showOnlyFilteredArabic = contentView === "arabic" && workMode !== "all" && workMode !== "media";

  return (
    <BlockStack gap="400">
      <SectionCard
        title="Title and description"
        description="Primary storefront copy for the selected language. This is the first content customers evaluate on the product page."
      >
        <BlockStack gap="400" id={SECTION_IDS.content}>
          {!showOnlyFilteredArabic || visibleArabicKeys.has("title") ? (
          <BilingualFieldGrid
            contentView={contentView}
            english={
              <TextField
                label="English title"
                autoComplete="off"
                value={core.title}
                onChange={(value) => onCoreChange((current) => ({ ...current, title: value }))}
              />
            }
            arabic={
              titleField ? <ArabicFieldEditor field={titleField} onChange={(value) => onArabicChange(titleField, value)} /> : null
            }
          />
          ) : null}
          {!showOnlyFilteredArabic || visibleArabicKeys.has("body_html") ? (
          <BilingualFieldGrid
            contentView={contentView}
            english={
              <NativeRichTextEditor
                label="English description"
                value={core.descriptionHtml}
                onChange={(value) => onCoreChange((current) => ({ ...current, descriptionHtml: value }))}
                helpText="Use paragraphs and lists directly."
                minHeight={260}
              />
            }
            arabic={
              descriptionField ? (
                <ArabicFieldEditor field={descriptionField} onChange={(value) => onArabicChange(descriptionField, value)} />
              ) : null
            }
          />
          ) : null}
          <BlockStack gap="200">
            <Text as="h3" variant="headingSm">
              Media
            </Text>
            <LocaleImagesTab
              imageGroups={imageGroups}
              hasUndo={hasUndoImageAction}
              availableImages={availableImages}
              contentView={contentView}
              libraryQuery={libraryQuery}
              hasMoreFiles={hasMoreFiles}
              fileSearchLoading={fileSearchLoading}
              fileUploadLoading={fileUploadLoading}
              onLibraryQueryChange={onLibraryQueryChange}
              onSearchFiles={onSearchFiles}
              onLoadMoreFiles={onLoadMoreFiles}
              onUploadFile={onUploadFile}
              onUndoLastAction={onUndoLastImageAction}
              onUpdateGroup={onUpdateGroup}
              onDuplicateGroup={onDuplicateGroup}
              onCopySelectedImages={onCopySelectedImages}
            />
          </BlockStack>
        </BlockStack>
      </SectionCard>

      <SectionCard
        title="Product organization"
        description="Status, handle, vendor, type, and tags. These fields shape product administration and discovery."
      >
        <BlockStack gap="400" id={SECTION_IDS.organization}>
          {!showOnlyFilteredArabic || visibleArabicKeys.has("handle") ? (
          <BilingualFieldGrid
            contentView={contentView}
            english={
              <TextField
                label="English handle"
                autoComplete="off"
                value={core.handle}
                onChange={(value) => onCoreChange((current) => ({ ...current, handle: value }))}
              />
            }
            arabic={
              handleField ? <ArabicFieldEditor field={handleField} onChange={(value) => onArabicChange(handleField, value)} /> : null
            }
          />
          ) : null}
          <InlineGrid columns={{ xs: 1, md: 2 }} gap="300">
            <TextField
              label="Vendor"
              autoComplete="off"
              value={core.vendor}
              onChange={(value) => onCoreChange((current) => ({ ...current, vendor: value }))}
            />
            <Select
              label="Status"
              options={[
                { label: "Active", value: "ACTIVE" },
                { label: "Draft", value: "DRAFT" },
                { label: "Archived", value: "ARCHIVED" },
              ]}
              value={core.status}
              onChange={(value) => onCoreChange((current) => ({ ...current, status: value }))}
            />
          </InlineGrid>
          {!showOnlyFilteredArabic || visibleArabicKeys.has("product_type") ? (
          <BilingualFieldGrid
            contentView={contentView}
            english={
              <TextField
                label="English product type"
                autoComplete="off"
                value={core.productType}
                onChange={(value) => onCoreChange((current) => ({ ...current, productType: value }))}
              />
            }
            arabic={
              productTypeField ? (
                <ArabicFieldEditor field={productTypeField} onChange={(value) => onArabicChange(productTypeField, value)} />
              ) : null
            }
          />
          ) : null}
          <TextField
            label="Tags"
            autoComplete="off"
            helpText="Comma-separated tags."
            value={core.tagsInput}
            onChange={(value) => onCoreChange((current) => ({ ...current, tagsInput: value }))}
          />
        </BlockStack>
      </SectionCard>

      <SectionCard
        title="Search engine listing"
        description="Advanced search metadata. Shopify treats this as a dedicated section because it matters, but not on every edit."
        defaultOpen={false}
      >
        <BlockStack gap="400" id={SECTION_IDS.seo}>
          {!showOnlyFilteredArabic || visibleArabicKeys.has("meta_title") ? (
          <BilingualFieldGrid
            contentView={contentView}
            english={
              <TextField
                label="English SEO title"
                autoComplete="off"
                value={core.seoTitle}
                onChange={(value) => onCoreChange((current) => ({ ...current, seoTitle: value }))}
              />
            }
            arabic={
              seoTitleField ? (
                <ArabicFieldEditor field={seoTitleField} onChange={(value) => onArabicChange(seoTitleField, value)} />
              ) : null
            }
          />
          ) : null}
          {!showOnlyFilteredArabic || visibleArabicKeys.has("meta_description") ? (
          <BilingualFieldGrid
            contentView={contentView}
            english={
              <TextField
                label="English SEO description"
                autoComplete="off"
                multiline={6}
                value={core.seoDescription}
                onChange={(value) => onCoreChange((current) => ({ ...current, seoDescription: value }))}
              />
            }
            arabic={
              seoDescriptionField ? (
                <ArabicFieldEditor field={seoDescriptionField} onChange={(value) => onArabicChange(seoDescriptionField, value)} />
              ) : null
            }
          />
          ) : null}
          <SearchEnginePreviewCard
            locale={contentView === "english" ? "English" : "Arabic"}
            title={
              contentView === "english"
                ? core.seoTitle || core.title
                : seoTitleField?.arabicValue || titleField?.arabicValue || ""
            }
            description={
              contentView === "english"
                ? core.seoDescription || extractRichTextPlainText(core.descriptionHtml)
                : seoDescriptionField?.arabicValue || descriptionField?.arabicValue || ""
            }
            handle={contentView === "english" ? core.handle : handleField?.arabicValue || core.handle}
            insights={seoInsights}
          />
        </BlockStack>
      </SectionCard>
    </BlockStack>
  );
}

function SearchEnginePreviewCard({
  locale,
  title,
  description,
  handle,
  insights,
}: {
  locale: string;
  title: string;
  description: string;
  handle: string;
  insights: SeoInsights;
}) {
  const previewTitle = (title || "Product title").trim().slice(0, 70);
  const previewDescription = (description || "Meta description preview").trim().slice(0, 160);

  return (
    <Box padding="300" background="bg-surface-secondary" borderRadius="300">
      <BlockStack gap="150">
        <InlineStack align="space-between" blockAlign="center">
          <Text as="h3" variant="headingSm">
            Search preview
          </Text>
          <Badge tone="info">{locale}</Badge>
        </InlineStack>
        <Text as="p" variant="bodySm" tone="subdued">
          Preview how this language is likely to appear in search results.
        </Text>
        <BlockStack gap="050">
          <Text as="span" variant="bodyMd" fontWeight="semibold" tone="success">
            {previewTitle || "Product title"}
          </Text>
          <Text as="span" variant="bodySm" tone="subdued">
            {`sa.taraformula.com/products/${handle || "product-handle"}`}
          </Text>
          <Text as="p" variant="bodySm">
            {previewDescription || "Meta description preview"}
          </Text>
        </BlockStack>
        <InlineStack gap="200">
          <Badge tone={previewTitle.length > 60 ? "attention" : "success"}>{`${previewTitle.length}/60 title`}</Badge>
          <Badge tone={previewDescription.length > 155 ? "attention" : "success"}>
            {`${previewDescription.length}/155 description`}
          </Badge>
        </InlineStack>
        {insights.warnings.length ? (
          <BlockStack gap="100">
            {insights.warnings.map((warning) => (
              <Text key={warning} as="p" variant="bodySm" tone="critical">
                {warning}
              </Text>
            ))}
          </BlockStack>
        ) : (
          <Badge tone="success">SEO length looks healthy</Badge>
        )}
      </BlockStack>
    </Box>
  );
}

function MetafieldsTab({
  metafields,
  allMetafields,
  initialMetafields,
  allArabicFields,
  metaobjectDefinitions,
  imageGroups,
  contentView,
  workMode,
  metafieldSearch,
  metafieldViewFilter,
  showPopulatedOnly,
  onSearchChange,
  onViewFilterChange,
  onShowPopulatedOnlyChange,
  onToggleFavorite,
  onMetafieldChange,
  onArabicMetafieldChange,
}: {
  metafields: EditorMetafield[];
  allMetafields: EditorMetafield[];
  initialMetafields: EditorMetafield[];
  allArabicFields: EditorTranslatableField[];
  metaobjectDefinitions: MetaobjectDefinitionOption[];
  imageGroups: EditorImageGroup[];
  contentView: ContentView;
  workMode: WorkMode;
  metafieldSearch: string;
  metafieldViewFilter: MetafieldViewFilter;
  showPopulatedOnly: boolean;
  onSearchChange: (value: string) => void;
  onViewFilterChange: (value: MetafieldViewFilter) => void;
  onShowPopulatedOnlyChange: (value: boolean) => void;
  onToggleFavorite: (metafield: EditorMetafield) => void;
  onMetafieldChange: (metafield: EditorMetafield) => void;
  onArabicMetafieldChange: (field: EditorTranslatableField, value: string) => void;
}) {
  const localeImageKeys = new Set(
    imageGroups.flatMap((group) =>
      [...group.english, ...group.arabic].map((target) => `${target.target.namespace}.${target.target.key}`),
    ),
  );
  const pinnedCount = metafields.filter((metafield) => metafield.isPinned).length;
  const changedCount = allMetafields.filter((metafield) => {
    const initialMetafield = initialMetafields.find((candidate) => candidate.id === metafield.id);
    return !deepEqual(metafield, initialMetafield);
  }).length;
  const needsArabicCount = allMetafields.filter(
    (metafield) => Boolean(metafield.translation) || hasArabicMetaobjectGap(metafield),
  ).length;
  const displayMetafields = metafields.filter((metafield) => {
    if (workMode === "missingArabic") {
      return hasMissingArabicMetafieldWork(metafield);
    }
    if (workMode === "outdatedArabic") {
      return hasOutdatedArabicMetafieldWork(metafield);
    }
    if (workMode === "media") {
      return localeImageKeys.has(`${metafield.namespace}.${metafield.key}`);
    }
    return true;
  });

  return (
    <BlockStack gap="400" id={SECTION_IDS.metafields}>
      <Card>
        <BlockStack gap="300">
          <InlineStack align="space-between" blockAlign="center">
            <Text as="h2" variant="headingMd">
              Metafields
            </Text>
            <Badge tone="info">{`${displayMetafields.length} visible`}</Badge>
          </InlineStack>
          <Text as="p" variant="bodySm" tone="subdued">
            Search and edit product custom fields in the current language.
          </Text>
          <InlineStack align="space-between" blockAlign="center">
            <TextField label="Search metafields" autoComplete="off" value={metafieldSearch} onChange={onSearchChange} />
            <Checkbox label="Only populated" checked={showPopulatedOnly} onChange={onShowPopulatedOnlyChange} />
          </InlineStack>
          <InlineStack gap="200" wrap>
            <Select
              label="View"
              options={[
                { label: "All visible", value: "all" },
                { label: `Pinned (${pinnedCount})`, value: "pinned" },
                { label: `Needs Arabic (${needsArabicCount})`, value: "needsArabic" },
                { label: `Changed (${changedCount})`, value: "changed" },
              ]}
              value={metafieldViewFilter}
              onChange={(value) => onViewFilterChange(value as MetafieldViewFilter)}
            />
          </InlineStack>
          <Text as="p" variant="bodySm" tone="subdued">
            {pinnedCount} pinned metafield{pinnedCount === 1 ? "" : "s"} in this view.
          </Text>
        </BlockStack>
      </Card>

      {displayMetafields.length === 0 ? (
        <Banner tone="info" title="No metafields match this filter">
          <Text as="p" variant="bodySm">
            Try clearing the search or turning off the populated-only filter.
          </Text>
        </Banner>
      ) : null}

      {groupBy(displayMetafields, (metafield) => metafield.namespace).map(([namespace, items]) => (
        <SectionCard
          key={namespace}
          title={namespace}
          description={`${items.length} field${items.length === 1 ? "" : "s"} | ${items.filter((item) => item.isPinned).length} pinned`}
          badgeContent={String(items.length)}
          defaultOpen={Boolean(metafieldSearch.trim()) || items.some((item) => item.isPinned)}
        >
          <BlockStack gap="300">
            {items.map((metafield) => {
              const arabicField = allArabicFields.find((field) => field.resourceId === metafield.id);
              const isLocaleImageField = localeImageKeys.has(`${metafield.namespace}.${metafield.key}`);
              const isReferenceField = isStructuredReferenceType(metafield.type);

              return (
                <Box key={metafield.id} padding="300" background="bg-surface-secondary" borderRadius="300">
                  <BlockStack gap="300">
                    <InlineStack align="space-between" blockAlign="start">
                      <BlockStack gap="100">
                        <InlineStack gap="200" blockAlign="center">
                          <Text as="h3" variant="headingSm">
                            {metafield.name}
                          </Text>
                          <Badge tone="info">{metafield.type}</Badge>
                          {metafield.isPinned ? <Badge tone="attention">Pinned</Badge> : null}
                          {arabicField?.arabicValue.trim() ? <Badge tone="success">Arabic ready</Badge> : null}
                        </InlineStack>
                        <Text as="p" variant="bodySm" tone="subdued">
                          {metafield.namespace}.{metafield.key}
                        </Text>
                        {metafield.description ? (
                          <Text as="p" variant="bodySm" tone="subdued">
                            {metafield.description}
                          </Text>
                        ) : null}
                        {metafield.validations?.length ? (
                          <Text as="p" variant="bodySm" tone="subdued">
                            {formatValidationSummary(metafield.validations)}
                          </Text>
                        ) : null}
                      </BlockStack>
                      <Button onClick={() => onToggleFavorite(metafield)}>
                        {metafield.isPinned ? "Unpin" : "Pin"}
                      </Button>
                    </InlineStack>

                    {isReferenceField ? (
                      <MetafieldReferencePicker
                        key={`${metafield.id}-${contentView}`}
                        metafield={metafield}
                        metaobjectDefinitions={metaobjectDefinitions}
                        contentView={contentView}
                        onChange={onMetafieldChange}
                      />
                    ) : canInlineEditMetafield(metafield.type) ? (
                      <BilingualFieldGrid
                        contentView={contentView}
                        english={renderMetafieldEditor(metafield, onMetafieldChange)}
                        arabic={
                          arabicField ? (
                            <ArabicFieldEditor field={arabicField} onChange={(value) => onArabicMetafieldChange(arabicField, value)} />
                          ) : null
                        }
                      />
                    ) : isLocaleImageField ? (
                      <Banner tone="info" title="Managed in Locale Images">
                        <Text as="p" variant="bodySm">
                          This file reference metafield belongs to the locale-specific PDP galleries.
                        </Text>
                      </Banner>
                    ) : (
                      <Banner tone="warning" title="Specialized file reference">
                        <Text as="p" variant="bodySm">
                          This file reference is visible here for diagnostics. Dedicated inline editing for non-gallery file references is not wired yet.
                        </Text>
                      </Banner>
                    )}
                  </BlockStack>
                </Box>
              );
            })}
          </BlockStack>
        </SectionCard>
      ))}
    </BlockStack>
  );
}

function LocaleImagesTab({
  imageGroups,
  availableImages,
  hasUndo,
  contentView,
  libraryQuery,
  hasMoreFiles,
  fileSearchLoading,
  fileUploadLoading,
  onLibraryQueryChange,
  onSearchFiles,
  onLoadMoreFiles,
  onUploadFile,
  onUndoLastAction,
  onUpdateGroup,
  onDuplicateGroup,
  onCopySelectedImages,
}: {
  imageGroups: EditorImageGroup[];
  availableImages: ProductImageItem[];
  hasUndo: boolean;
  contentView: ContentView;
  libraryQuery: string;
  hasMoreFiles: boolean;
  fileSearchLoading: boolean;
  fileUploadLoading: boolean;
  onLibraryQueryChange: (value: string) => void;
  onSearchFiles: () => void;
  onLoadMoreFiles: () => void;
  onUploadFile: (file: File, alt?: string) => void;
  onUndoLastAction: () => void;
  onUpdateGroup: (groupId: string, locale: "english" | "arabic", targetId: string, images: ProductImageItem[]) => void;
  onDuplicateGroup: (groupId: string, sourceLocale: "english" | "arabic") => void;
  onCopySelectedImages: (
    groupId: string,
    sourceLocale: "english" | "arabic",
    targetId: string,
    selectedImageIds: string[],
  ) => void;
}) {
  const [galleryLocaleView, setGalleryLocaleView] = useState<ContentView>(contentView);
  const activeLocale = galleryLocaleView === "arabic" ? "arabic" : "english";

  useEffect(() => {
    setGalleryLocaleView(contentView);
  }, [contentView]);

  return (
    <BlockStack gap="400">
      {hasUndo ? (
        <InlineStack align="start">
          <Button onClick={onUndoLastAction}>Undo last image action</Button>
        </InlineStack>
      ) : null}

      {imageGroups.map((group) => (
        <Card key={group.id}>
          <BlockStack gap="400">
            <InlineStack align="space-between" blockAlign="start">
              <BlockStack gap="100">
                <InlineStack gap="200" blockAlign="center">
                  <Text as="h2" variant="headingMd">
                    {group.label}
                  </Text>
                  <Badge tone="success">{`${group.english[0]?.images.length || 0} EN`}</Badge>
                  <Badge tone="info">{`${group.arabic[0]?.images.length || 0} AR`}</Badge>
                  {group.mismatchWarning ? <Badge tone="critical">Count mismatch</Badge> : null}
                </InlineStack>
                {group.mismatchWarning ? (
                  <Text as="p" variant="bodySm" tone="critical">
                    {group.mismatchWarning}
                  </Text>
                ) : null}
              </BlockStack>
              <InlineStack gap="200">
                <InlineStack gap="100">
                  <Button pressed={galleryLocaleView === "english"} onClick={() => setGalleryLocaleView("english")}>
                    English
                  </Button>
                  <Button pressed={galleryLocaleView === "arabic"} onClick={() => setGalleryLocaleView("arabic")}>
                    Arabic
                  </Button>
                </InlineStack>
                <Button onClick={() => onDuplicateGroup(group.id, "english")}>Copy EN to AR</Button>
                <Button onClick={() => onDuplicateGroup(group.id, "arabic")}>Copy AR to EN</Button>
              </InlineStack>
            </InlineStack>

            <BlockStack gap="300">
              {(activeLocale === "english" ? group.english : group.arabic).map((target) => (
                  <ImageTargetEditor
                    key={target.target.id}
                    title={target.target.label}
                    tone={activeLocale === "english" ? "success" : "info"}
                    images={target.images}
                    availableImages={availableImages}
                    loading={fileSearchLoading || fileUploadLoading}
                    galleryQuery={libraryQuery}
                    hasMoreFiles={hasMoreFiles}
                    direction={activeLocale === "arabic" ? "rtl" : "ltr"}
                    copySelectedLabel={activeLocale === "english" ? "Copy selected to Arabic" : "Copy selected to English"}
                    onGalleryQueryChange={onLibraryQueryChange}
                    onSearchGallery={onSearchFiles}
                    onLoadMoreFiles={onLoadMoreFiles}
                    onUploadFile={onUploadFile}
                    onCopySelected={(selectedImageIds) =>
                      onCopySelectedImages(group.id, activeLocale, target.target.id, selectedImageIds)
                    }
                    onChange={(images) => onUpdateGroup(group.id, activeLocale, target.target.id, images)}
                  />
              ))}
            </BlockStack>
          </BlockStack>
        </Card>
      ))}
    </BlockStack>
  );
}

function syncSelectedImagesBetweenTargets({
  sourceTargets,
  destinationTargets,
  targetId,
  selectedImageIds,
}: {
  sourceTargets: EditorImageGroup["english"];
  destinationTargets: EditorImageGroup["english"];
  targetId: string;
  selectedImageIds: string[];
}) {
  const sourceIndex = sourceTargets.findIndex((target) => target.target.id === targetId);
  if (sourceIndex === -1) {
    return destinationTargets;
  }

  const destinationIndex = Math.min(sourceIndex, Math.max(destinationTargets.length - 1, 0));
  const sourceTarget = sourceTargets[sourceIndex];
  const destinationTarget = destinationTargets[destinationIndex];

  if (!destinationTarget) {
    return destinationTargets;
  }

  const selectedImages = sourceTarget.images
    .map((image, index) => ({ image, index }))
    .filter((entry) => selectedImageIds.includes(entry.image.id));

  if (selectedImages.length === 0) {
    return destinationTargets;
  }

  const nextImages = [...destinationTarget.images];

  for (const { image, index } of selectedImages) {
    if (index < nextImages.length) {
      nextImages[index] = image;
    } else {
      nextImages.push(image);
    }
  }

  const dedupedImages: ProductImageItem[] = [];
  const seenImageIds = new Set<string>();
  for (const image of nextImages) {
    if (seenImageIds.has(image.id)) {
      continue;
    }
    seenImageIds.add(image.id);
    dedupedImages.push(image);
  }

  return destinationTargets.map((target, index) =>
    index === destinationIndex ? { ...target, images: dedupedImages } : target,
  );
}

function SummarySidebar({
  model,
  metafields,
  productArabicFields,
  imageGroups,
  dirtySections,
  seoInsights,
  onJumpToSection,
}: {
  model: ProductEditorModel;
  metafields: EditorMetafield[];
  productArabicFields: EditorTranslatableField[];
  imageGroups: EditorImageGroup[];
  dirtySections: Record<string, boolean>;
  seoInsights: SeoInsights;
  onJumpToSection: (sectionId: string) => void;
}) {
  const translationStatus = summarizeTranslations(productArabicFields);
  const imageStatus = summarizeLocaleImages(imageGroups);
  const pinnedCount = metafields.filter((metafield) => metafield.isPinned).length;
  const missingArabicFields = productArabicFields.filter((field) => !field.arabicValue.trim());
  const outdatedFields = productArabicFields.filter((field) => field.outdated);
  const mismatchGroups = imageGroups.filter((group) => group.mismatchWarning);

  return (
    <BlockStack gap="300">
      <Card>
        <BlockStack gap="200">
          <Text as="h2" variant="headingMd">
            Save state
          </Text>
          {Object.entries(dirtySections).map(([section, isDirty]) => (
            <InlineStack key={section} align="space-between" blockAlign="center">
              <Text as="span" variant="bodySm">
                {formatSectionLabel(section)}
              </Text>
              <Badge tone={isDirty ? "warning" : "success"}>{isDirty ? "Changed" : "Saved"}</Badge>
            </InlineStack>
          ))}
        </BlockStack>
      </Card>

      <Card>
        <BlockStack gap="200">
          <Text as="h2" variant="headingMd">
            Readiness checklist
          </Text>
          {missingArabicFields.length === 0 && outdatedFields.length === 0 && mismatchGroups.length === 0 ? (
            <Badge tone="success">Ready for review</Badge>
          ) : (
            <BlockStack gap="100">
              {missingArabicFields.slice(0, 4).map((field) => (
                <Text key={`${field.resourceId}-${field.translationKey}`} as="p" variant="bodySm">
                  Missing Arabic: {field.label}
                </Text>
              ))}
              {outdatedFields.slice(0, 2).map((field) => (
                <Text key={`outdated-${field.resourceId}-${field.translationKey}`} as="p" variant="bodySm">
                  Outdated translation: {field.label}
                </Text>
              ))}
              {mismatchGroups.slice(0, 2).map((group) => (
                <Text key={`mismatch-${group.id}`} as="p" variant="bodySm">
                  Media mismatch: {group.label}
                </Text>
              ))}
            </BlockStack>
          )}
          <InlineStack gap="200" wrap>
            <Button size="slim" onClick={() => onJumpToSection(SECTION_IDS.content)} disabled={missingArabicFields.length === 0 && outdatedFields.length === 0}>
              Fix content
            </Button>
            <Button size="slim" onClick={() => onJumpToSection(SECTION_IDS.seo)}>
              Review SEO
            </Button>
            <Button size="slim" onClick={() => onJumpToSection(SECTION_IDS.metafields)}>
              Review metafields
            </Button>
          </InlineStack>
        </BlockStack>
      </Card>

      <Card>
        <BlockStack gap="200">
          <Text as="h2" variant="headingMd">
            Localization health
          </Text>
          <Text as="p" variant="bodySm">
            {translationStatus.translated} of {translationStatus.total} product-level Arabic fields are filled.
          </Text>
          <Text as="p" variant="bodySm">
            {imageStatus.english} English images and {imageStatus.arabic} Arabic images are mapped.
          </Text>
          {imageStatus.mismatched ? (
            <Banner tone="critical" title="Locale media needs review">
              <Text as="p" variant="bodySm">
                Arabic and English counts differ in {imageStatus.mismatched} gallery group
                {imageStatus.mismatched === 1 ? "" : "s"}.
              </Text>
            </Banner>
          ) : (
            <Badge tone="success">Locale gallery counts aligned</Badge>
          )}
        </BlockStack>
      </Card>

      <Card>
        <BlockStack gap="200">
          <Text as="h2" variant="headingMd">
            SEO checks
          </Text>
          {seoInsights.warnings.length ? (
            <BlockStack gap="100">
              {seoInsights.warnings.map((warning) => (
                <Text key={warning} as="p" variant="bodySm">
                  {warning}
                </Text>
              ))}
            </BlockStack>
          ) : (
            <Badge tone="success">No title or description length issues</Badge>
          )}
        </BlockStack>
      </Card>

      <Card>
        <BlockStack gap="200">
          <Text as="h2" variant="headingMd">
            Product facts
          </Text>
          <Text as="p" variant="bodySm">
            {model.product.options.length} option set{model.product.options.length === 1 ? "" : "s"}
          </Text>
          <Text as="p" variant="bodySm">
            {model.product.media.length} product media item{model.product.media.length === 1 ? "" : "s"}
          </Text>
          <Text as="p" variant="bodySm">
            {metafields.length} metafield{metafields.length === 1 ? "" : "s"} | {pinnedCount} pinned
          </Text>
        </BlockStack>
      </Card>
    </BlockStack>
  );
}

function BilingualFieldGrid({
  contentView,
  english,
  arabic,
}: {
  contentView: ContentView;
  english: React.ReactNode;
  arabic?: React.ReactNode | null;
}) {
  if (contentView === "english" || !arabic) {
    return <Fragment key="english">{english}</Fragment>;
  }

  return <Fragment key="arabic">{arabic}</Fragment>;
}

function MetricCard({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <Box padding="300" background="bg-surface-secondary" borderRadius="300">
      <BlockStack gap="050">
        <Text as="span" variant="bodySm" tone="subdued">
          {label}
        </Text>
        <Text as="span" variant="headingMd">
          {value}
        </Text>
      </BlockStack>
    </Box>
  );
}

function SectionCard({
  title,
  description,
  children,
  defaultOpen = true,
  badgeContent,
}: {
  title: string;
  description?: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
  badgeContent?: string;
}) {
  const [open, setOpen] = useState(defaultOpen);

  useEffect(() => {
    setOpen(defaultOpen);
  }, [defaultOpen]);

  return (
    <Card>
      <BlockStack gap="300">
        <InlineStack align="space-between" blockAlign="start">
          <BlockStack gap="100">
            <InlineStack gap="200" blockAlign="center">
              <Text as="h2" variant="headingMd">
                {title}
              </Text>
              {badgeContent ? <Badge tone="info">{badgeContent}</Badge> : null}
            </InlineStack>
            {description ? (
              <Text as="p" variant="bodySm" tone="subdued">
                {description}
              </Text>
            ) : null}
          </BlockStack>
          <Button onClick={() => setOpen((current) => !current)}>{open ? "Hide" : "Show"}</Button>
        </InlineStack>
        {open ? <BlockStack gap="300">{children}</BlockStack> : null}
      </BlockStack>
    </Card>
  );
}

function ArabicFieldEditor({
  field,
  onChange,
}: {
  field: EditorTranslatableField;
  onChange: (value: string) => void;
}) {
  const multiline = isRichTextLikeField(field) || field.fieldType === "multi_line_text_field";
  const useRichEditor = isRichTextLikeField(field) || field.fieldType === "multi_line_text_field";

  return (
    <BlockStack gap="100">
      <InlineStack gap="200" blockAlign="center">
        <Text as="h3" variant="headingSm">
          {field.label}
        </Text>
        {field.outdated ? <Badge tone="warning">Outdated</Badge> : null}
        <Badge tone="info">Arabic</Badge>
      </InlineStack>
      <Text as="p" variant="bodySm" tone="subdued">
        Source: {previewValue(field.sourceValue, field)}
      </Text>
      {useRichEditor ? (
        <NativeRichTextEditor
          label={field.label}
          value={field.arabicValue}
          onChange={onChange}
          helpText={getArabicFieldHelpText(field)}
          minHeight={multiline ? 220 : 160}
          dir="rtl"
          toolbar={isRichTextLikeField(field)}
        />
      ) : (
        <div dir="rtl" style={{ textAlign: "right" }}>
          <TextField
            label={field.label}
            autoComplete="off"
            align="right"
            multiline={5}
            helpText={getArabicFieldHelpText(field)}
            value={field.arabicValue}
            onChange={onChange}
          />
        </div>
      )}
    </BlockStack>
  );
}

function renderMetafieldEditor(
  metafield: EditorMetafield,
  onMetafieldChange: (metafield: EditorMetafield) => void,
) {
  if (metafield.type === "boolean") {
    return (
      <Select
        label={`English ${metafield.name}`}
        options={[
          { label: "True", value: "true" },
          { label: "False", value: "false" },
        ]}
        value={metafield.value || "false"}
        onChange={(value) => onMetafieldChange({ ...metafield, value })}
      />
    );
  }

  if (metafield.type === "rich_text_field") {
    return (
      <NativeRichTextEditor
        label={`English ${metafield.name}`}
        value={metafield.value}
        onChange={(value) => onMetafieldChange({ ...metafield, value })}
        helpText={getMetafieldHelpText(metafield.type)}
        minHeight={220}
      />
    );
  }

  if (metafield.type === "multi_line_text_field") {
    return (
      <NativeRichTextEditor
        label={`English ${metafield.name}`}
        value={metafield.value}
        onChange={(value) => onMetafieldChange({ ...metafield, value })}
        helpText={getMetafieldHelpText(metafield.type)}
        minHeight={180}
        toolbar={false}
      />
    );
  }

  return (
    <TextField
      label={`English ${metafield.name}`}
      autoComplete="off"
      multiline={metafield.type === "json" ? 10 : 1}
      helpText={getMetafieldHelpText(metafield.type)}
      value={metafield.value}
      onChange={(value) => onMetafieldChange({ ...metafield, value })}
    />
  );
}

function toneForStatus(status: string) {
  if (status === "ACTIVE") return "success";
  if (status === "DRAFT") return "attention";
  return "info";
}

function buildEditorNavigationUrl({
  productId,
  queue,
  currentIndex,
  returnTo,
}: {
  productId: string;
  queue: string[];
  currentIndex: number;
  returnTo: string;
}) {
  const params = new URLSearchParams();
  if (queue.length) {
    params.set("queue", queue.join(","));
    params.set("index", String(currentIndex));
  }
  if (returnTo) {
    params.set("returnTo", returnTo);
  }

  return `/app/products/${productId}?${params.toString()}`;
}

function buildCoreState(model: ProductEditorModel) {
  return {
    title: model.product.title,
    handle: model.product.handle,
    descriptionHtml: htmlToEditableText(model.product.descriptionHtml),
    vendor: model.product.vendor,
    productType: model.product.productType,
    tagsInput: model.product.tags.join(", "),
    status: model.product.status,
    seoTitle: model.product.seo.title,
    seoDescription: model.product.seo.description,
  };
}

function buildInitialEditorState(model: ProductEditorModel) {
  return {
    core: buildCoreState(model),
    arabicFields: model.arabicFields,
    metafields: model.metafields,
    imageGroups: model.imageGroups,
  };
}

type SeoInsights = {
  warnings: string[];
};

function buildSeoInsights(input: {
  contentView: ContentView;
  core: ReturnType<typeof buildCoreState>;
  titleField?: EditorTranslatableField;
  descriptionField?: EditorTranslatableField;
  handleField?: EditorTranslatableField;
  seoTitleField?: EditorTranslatableField;
  seoDescriptionField?: EditorTranslatableField;
}) {
  const title =
    input.contentView === "english"
      ? (input.core.seoTitle || input.core.title || "").trim()
      : (input.seoTitleField?.arabicValue || input.titleField?.arabicValue || "").trim();
  const description =
    input.contentView === "english"
      ? (input.core.seoDescription || extractRichTextPlainText(input.core.descriptionHtml) || "").trim()
      : (input.seoDescriptionField?.arabicValue || input.descriptionField?.arabicValue || "").trim();
  const handle =
    input.contentView === "english"
      ? input.core.handle.trim()
      : (input.handleField?.arabicValue || input.core.handle).trim();

  const warnings: string[] = [];

  if (title.length < 30) {
    warnings.push("SEO title is short. Aim for roughly 30 to 60 characters.");
  } else if (title.length > 60) {
    warnings.push("SEO title is long and may truncate in search results.");
  }

  if (description.length < 70) {
    warnings.push("SEO description is short. Add more context for search results.");
  } else if (description.length > 155) {
    warnings.push("SEO description is long and may truncate in search results.");
  }

  if (!handle.length) {
    warnings.push("Handle is missing.");
  }

  return { warnings } satisfies SeoInsights;
}

function deriveWorkMode(queueFocus: string | null, issueFocus: string | null): WorkMode {
  if (queueFocus === "missingArabic" || queueFocus === "outdatedArabic") {
    return queueFocus;
  }

  if (queueFocus === "imageMismatch" || queueFocus === "missingArabicMedia" || issueFocus === "media") {
    return "media";
  }

  return "all";
}

function serializeLoadedModelSnapshot(model: ProductEditorModel) {
  return JSON.stringify({
    product: model.product,
    arabicFields: model.arabicFields,
    metafields: model.metafields,
    imageGroups: model.imageGroups,
  });
}

function buildCorePayload(core: ReturnType<typeof buildCoreState>) {
  return {
    ...core,
    tags: normalizeTags(core.tagsInput),
  };
}

function submitIntent(fetcher: ReturnType<typeof useFetcher<SaveResult>>, intent: string, payload: Record<string, unknown>) {
  const formData = new FormData();
  formData.set("intent", intent);
  formData.set("payload", JSON.stringify(payload));
  fetcher.submit(formData, { method: "post" });
}

function summarizeTranslations(fields: EditorTranslatableField[]) {
  const translated = fields.filter((field) => field.arabicValue.trim().length > 0).length;
  return { translated, total: fields.length };
}

function summarizeLocaleImages(groups: EditorImageGroup[]) {
  return groups.reduce(
    (summary, group) => ({
      english: summary.english + (group.english[0]?.images.length || 0),
      arabic: summary.arabic + (group.arabic[0]?.images.length || 0),
      mismatched: summary.mismatched + (group.mismatchWarning ? 1 : 0),
    }),
    { english: 0, arabic: 0, mismatched: 0 },
  );
}

function updateArabicFieldValue(fields: EditorTranslatableField[], target: EditorTranslatableField, value: string) {
  return fields.map((field) =>
    field.resourceId === target.resourceId && field.translationKey === target.translationKey
      ? { ...field, arabicValue: value }
      : field,
  );
}

function groupBy<T>(items: T[], getKey: (item: T) => string) {
  const map = new Map<string, T[]>();
  for (const item of items) {
    const key = getKey(item);
    if (!map.has(key)) {
      map.set(key, []);
    }
    map.get(key)?.push(item);
  }
  return [...map.entries()];
}

function findField(fields: EditorTranslatableField[], translationKey: string) {
  return fields.find((field) => field.translationKey === translationKey);
}

function isRichTextLikeField(field: EditorTranslatableField) {
  return field.fieldType === "rich_text_field" || field.translationKey === "body_html";
}

function previewValue(value: string, field?: EditorTranslatableField) {
  const preview = field && isRichTextLikeField(field) ? extractRichTextPlainText(value) : value;
  return preview.slice(0, 180) || "No source value";
}

function getArabicFieldHelpText(field: EditorTranslatableField) {
  if (field.translationKey === "body_html") {
    return "Formatted description content. Use bullets and paragraphs directly.";
  }

  if (field.fieldType === "rich_text_field") {
    return "Formatted content. Bullet lines and bold text will be saved as Shopify rich text.";
  }

  return undefined;
}

function getMetafieldHelpText(type: string) {
  if (type === "rich_text_field") {
    return "Formatted content. Bullet lines and bold text are saved as Shopify rich text.";
  }

  if (type === "json") {
    return "Stored as JSON. Save will validate the payload.";
  }

  return undefined;
}

function hasArabicMetaobjectGap(metafield: EditorMetafield) {
  return metafield.references.some((reference) =>
    reference.kind === "METAOBJECT"
      ? reference.metaobject?.fields.some((field) => field.isTranslatable && !field.arabicValue.trim())
      : false,
  );
}

function hasOutdatedArabicMetaobject(reference: EditorMetafield["references"][number]) {
  return reference.kind === "METAOBJECT"
    ? Boolean(reference.metaobject?.fields.some((field) => field.isTranslatable && field.outdated))
    : false;
}

function hasMissingArabicMetafieldWork(metafield: EditorMetafield) {
  return Boolean(metafield.translation && !metafield.translation.arabicValue.trim()) || hasArabicMetaobjectGap(metafield);
}

function hasOutdatedArabicMetafieldWork(metafield: EditorMetafield) {
  return Boolean(metafield.translation?.outdated) || metafield.references.some(hasOutdatedArabicMetaobject);
}

function shouldShowArabicField(field: EditorTranslatableField, workMode: WorkMode) {
  if (workMode === "missingArabic") {
    return !field.arabicValue.trim();
  }

  if (workMode === "outdatedArabic") {
    return Boolean(field.outdated);
  }

  return true;
}

function previewConflictValue(value: string) {
  const compact = value.replace(/\s+/g, " ").trim();
  return compact.length > 160 ? `${compact.slice(0, 157)}...` : compact || "Empty";
}

function formatValidationSummary(validations: Array<{ name: string; value: string }>) {
  return validations
    .slice(0, 3)
    .map((validation) => `${validation.name}: ${validation.value}`)
    .join(" | ");
}

function formatSectionLabel(section: string) {
  if (section === "core") return "Content";
  if (section === "arabic") return "Arabic translations";
  if (section === "metafields") return "Metafields";
  if (section === "images") return "Media";
  return section;
}

function isStructuredReferenceType(type: string) {
  return (
    type === "product_reference" ||
    type === "collection_reference" ||
    type === "metaobject_reference" ||
    type === "list.product_reference" ||
    type === "list.collection_reference" ||
    type === "list.metaobject_reference"
  );
}
