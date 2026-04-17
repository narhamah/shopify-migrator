import type { DragEvent } from "react";
import { useEffect, useMemo, useState } from "react";

import { useAppBridge } from "@shopify/app-bridge-react";
import {
  Badge,
  Banner,
  BlockStack,
  Box,
  Button,
  Card,
  Icon,
  InlineGrid,
  InlineStack,
  Modal,
  Select,
  Text,
  TextField,
  Thumbnail,
} from "@shopify/polaris";
import { ChevronDownIcon, ChevronUpIcon, DeleteIcon, DragHandleIcon } from "@shopify/polaris-icons";
import { useFetcher } from "react-router";

import { NativeRichTextEditor } from "~/components/NativeRichTextEditor";
import { reorderItems } from "~/lib/utils";
import type {
  EditorMetafield,
  MetaobjectDefinitionOption,
  ReferenceSummary,
  SaveResult,
} from "~/types/editor";

type ContentView = "english" | "arabic";

export function MetafieldReferencePicker({
  metafield,
  metaobjectDefinitions,
  contentView,
  onChange,
}: {
  metafield: EditorMetafield;
  metaobjectDefinitions: MetaobjectDefinitionOption[];
  contentView: ContentView;
  onChange: (metafield: EditorMetafield) => void;
}) {
  const shopify = useAppBridge();
  const searchFetcher = useFetcher<{ items: ReferenceSummary[] }>();
  const saveFetcher = useFetcher<SaveResult & { reference?: ReferenceSummary }>();
  const [query, setQuery] = useState("");
  const [selectedMetaobjectType, setSelectedMetaobjectType] = useState("");
  const [pickerOpen, setPickerOpen] = useState(false);
  const [recentSelections, setRecentSelections] = useState<ReferenceSummary[]>([]);
  const [expandedReferenceId, setExpandedReferenceId] = useState<string | null>(null);
  const [drafts, setDrafts] = useState<Record<string, Record<string, string>>>({});
  const [draggedIndex, setDraggedIndex] = useState<number | null>(null);
  const [dropIndex, setDropIndex] = useState<number | null>(null);
  const kind = getReferenceKind(metafield.type);
  const isList = metafield.type.startsWith("list.");
  const direction = contentView === "arabic" ? "rtl" : "ltr";
  const textAlign = contentView === "arabic" ? "right" : "left";

  const metaobjectOptions = useMemo(() => {
    const allowedTypes = metafield.allowedMetaobjectTypes;
    return metaobjectDefinitions
      .filter((definition) => !allowedTypes || allowedTypes.includes(definition.type))
      .map((definition) => ({
        label: `${definition.name} (${definition.type})`,
        value: definition.type,
      }));
  }, [metaobjectDefinitions, metafield.allowedMetaobjectTypes]);

  useEffect(() => {
    if (kind === "metaobject" && !selectedMetaobjectType && metaobjectOptions[0]) {
      setSelectedMetaobjectType(metaobjectOptions[0].value);
    }
  }, [kind, metaobjectOptions, selectedMetaobjectType]);

  useEffect(() => {
    if (!pickerOpen) {
      return;
    }

    setRecentSelections(loadRecentSelections(kind, selectedMetaobjectType));
  }, [kind, pickerOpen, selectedMetaobjectType]);

  useEffect(() => {
    if (!pickerOpen) {
      return;
    }

    const timeout = window.setTimeout(() => {
      submitSearch();
    }, 200);

    return () => window.clearTimeout(timeout);
  }, [contentView, kind, pickerOpen, query, selectedMetaobjectType]);

  useEffect(() => {
    if (!saveFetcher.data?.message) {
      return;
    }

    shopify.toast.show(saveFetcher.data.message, { isError: !saveFetcher.data.ok });

    if (saveFetcher.data.ok && saveFetcher.data.reference) {
      const updatedReference = saveFetcher.data.reference;
      updateSelection(
        metafield.references.map((reference) =>
          reference.id === updatedReference.id ? updatedReference : reference,
        ),
      );
      setDrafts((current) => ({
        ...current,
        [updatedReference.id]: buildFieldDraft(updatedReference, contentView),
      }));
    }
  }, [contentView, metafield.references, saveFetcher.data, shopify]);

  const searchResults = searchFetcher.data?.items || [];
  const visibleResults = searchResults.length ? searchResults : recentSelections;

  function submitSearch() {
    const params = new URLSearchParams({
      kind,
      query,
      locale: contentView === "arabic" ? "ar" : "en",
    });

    if (kind === "metaobject" && selectedMetaobjectType) {
      params.set("metaobjectType", selectedMetaobjectType);
    }

    searchFetcher.load(`/app/references?${params.toString()}`);
  }

  function updateSelection(nextReferences: ReferenceSummary[]) {
    const dedupedReferences = dedupeReferences(nextReferences);
    const nextIds = dedupedReferences.map((reference) => reference.id);
    onChange({
      ...metafield,
      referenceIds: nextIds,
      references: dedupedReferences,
      value: metafield.type.startsWith("list.") ? JSON.stringify(nextIds) : nextIds[0] || "",
    });
    persistRecentSelections(kind, selectedMetaobjectType, dedupedReferences);
    setRecentSelections(loadRecentSelections(kind, selectedMetaobjectType));
  }

  function toggleExpandedReference(reference: ReferenceSummary) {
    setExpandedReferenceId((current) => (current === reference.id ? null : reference.id));
    setDrafts((current) => ({
      ...current,
      [reference.id]: current[reference.id] || buildFieldDraft(reference, contentView),
    }));
  }

  function updateDraft(referenceId: string, fieldKey: string, value: string) {
    setDrafts((current) => ({
      ...current,
      [referenceId]: {
        ...(current[referenceId] || {}),
        [fieldKey]: value,
      },
    }));
  }

  function saveMetaobject(reference: ReferenceSummary) {
    if (!reference.metaobject) {
      return;
    }

    const payload = {
      metaobjectId: reference.id,
      locale: contentView,
      fields: reference.metaobject.fields.map((field) => ({
        key: field.key,
        label: field.label,
        type: field.type,
        value:
          drafts[reference.id]?.[field.key] ??
          (contentView === "arabic" ? field.arabicValue : field.value),
      })),
    };

    const formData = new FormData();
    formData.set("intent", "saveMetaobject");
    formData.set("payload", JSON.stringify(payload));
    saveFetcher.submit(formData, { method: "post", action: "/app/metaobjects" });
  }

  function clearDragState() {
    setDraggedIndex(null);
    setDropIndex(null);
  }

  return (
    <div data-testid="reference-picker-surface" dir={direction} style={{ textAlign }}>
      <Card>
        <BlockStack gap="300">
        <InlineStack align="space-between" blockAlign="center">
          <InlineStack gap="200" blockAlign="center">
            <Text as="h4" variant="headingSm">
              {metafield.name}
            </Text>
            <Badge tone="info">{metafield.type}</Badge>
          </InlineStack>
          <InlineStack gap="200">
            {metafield.referenceIds.length ? (
              <Text as="span" variant="bodySm" tone="subdued">
                {metafield.referenceIds.length} selected
              </Text>
            ) : null}
            <Button onClick={() => setPickerOpen(true)}>
              {isList ? `Select ${kind}s` : `Select ${kind}`}
            </Button>
          </InlineStack>
        </InlineStack>

        {metafield.description ? (
          <Text as="p" variant="bodySm" tone="subdued">
            {metafield.description}
          </Text>
        ) : null}

        {metafield.references.length > 0 ? (
          <BlockStack gap="200">
            {isList ? (
              <Text as="p" variant="bodySm" tone="subdued">
                Drag and drop to reorder these {kind}s.
              </Text>
            ) : null}
            {metafield.references.map((reference, index) => {
              const isExpanded = expandedReferenceId === reference.id;
              const displayTitle = getDisplayTitle(reference, contentView);
              const displaySubtitle = getDisplaySubtitle(reference, contentView);
              const hasMetaobjectEditor =
                reference.kind === "METAOBJECT" && Boolean(reference.metaobject?.fields.length);
              const isDragged = draggedIndex === index;
              const isDropTarget = dropIndex === index && draggedIndex !== null && draggedIndex !== index;
              const leadingVisual = renderReferenceLeadingVisual(reference, displayTitle);

              return (
                <div
                  key={`${reference.id}-${index}`}
                  draggable={isList}
                  onDragStart={(event: DragEvent<HTMLDivElement>) => {
                    if (!isList) {
                      return;
                    }

                    setDraggedIndex(index);
                    setDropIndex(index);
                    if (event.dataTransfer) {
                      event.dataTransfer.effectAllowed = "move";
                    }
                  }}
                  onDragEnter={(event: DragEvent<HTMLDivElement>) => {
                    event.preventDefault();
                    if (isList && draggedIndex !== null) {
                      setDropIndex(index);
                    }
                  }}
                  onDragOver={(event: DragEvent<HTMLDivElement>) => {
                    if (isList) {
                      event.preventDefault();
                    }
                  }}
                  onDrop={() => {
                    if (!isList || draggedIndex === null || draggedIndex === index) {
                      clearDragState();
                      return;
                    }

                    updateSelection(reorderItems(metafield.references, draggedIndex, index));
                    clearDragState();
                  }}
                  onDragEnd={clearDragState}
                  style={{
                    position: "relative",
                    borderRadius: 12,
                    background: "#f6f6f7",
                    border: isDropTarget
                      ? "2px solid #005bd3"
                      : isExpanded
                        ? "1px solid #005bd3"
                        : "1px solid #d2d5d8",
                    boxShadow: isDragged
                      ? "0 18px 36px rgba(0, 0, 0, 0.18)"
                      : isDropTarget
                        ? "0 0 0 4px rgba(0, 91, 211, 0.12)"
                        : "0 1px 2px rgba(0, 0, 0, 0.06)",
                    transform: isDragged
                      ? "scale(0.985) rotate(-0.3deg)"
                      : isDropTarget
                        ? "translateY(-2px)"
                        : "translateY(0)",
                    opacity: isDragged ? 0.7 : 1,
                    transition:
                      "transform 140ms ease, box-shadow 140ms ease, opacity 140ms ease, border-color 140ms ease",
                    overflow: "hidden",
                  }}
                >
                  {isList ? (
                    <div
                      aria-hidden
                      style={{
                        position: "absolute",
                        insetInlineStart: 12,
                        insetBlockStart: 12,
                        width: 28,
                        height: 28,
                        borderRadius: 8,
                        background: "#ffffff",
                        border: "1px solid #d2d5d8",
                        display: "grid",
                        placeItems: "center",
                        color: "#6d7175",
                        boxShadow: isDragged ? "0 6px 14px rgba(0, 0, 0, 0.1)" : "none",
                        zIndex: 2,
                      }}
                    >
                      <Icon source={DragHandleIcon} />
                    </div>
                  ) : null}

                  <div
                    style={{
                      position: "absolute",
                      insetInlineEnd: 12,
                      insetBlockStart: 12,
                      zIndex: 2,
                    }}
                  >
                    <Button
                      icon={DeleteIcon}
                      accessibilityLabel={`Remove ${displayTitle}`}
                      tone="critical"
                      onClick={() =>
                        updateSelection(metafield.references.filter((_, itemIndex) => itemIndex !== index))
                      }
                    />
                  </div>

                  <button
                    type="button"
                    onClick={() => hasMetaobjectEditor && toggleExpandedReference(reference)}
                    style={{
                      width: "100%",
                      border: 0,
                      background: "transparent",
                      paddingBlock: 16,
                      paddingInlineEnd: 56,
                      paddingInlineStart: isList ? 52 : 16,
                      textAlign,
                      cursor: hasMetaobjectEditor ? "pointer" : isList ? (isDragged ? "grabbing" : "grab") : "default",
                      display: "block",
                    }}
                  >
                    <InlineStack align="space-between" blockAlign="start" wrap={false}>
                      <InlineStack gap="200" blockAlign="start" wrap={false}>
                        {leadingVisual ? (
                          <div style={{ paddingBlockStart: 2 }}>{leadingVisual}</div>
                        ) : null}
                        <BlockStack gap="050">
                          <InlineStack gap="100" blockAlign="center" wrap>
                            <Text as="span" variant="bodyMd" fontWeight="medium">
                              {displayTitle}
                            </Text>
                            {contentView === "arabic" && reference.localizedTitle ? (
                              <Badge tone="success">Arabic</Badge>
                            ) : null}
                          </InlineStack>
                          <Text as="span" variant="bodySm" tone="subdued">
                            {displaySubtitle || reference.id}
                          </Text>
                        </BlockStack>
                      </InlineStack>

                      {hasMetaobjectEditor ? (
                        <div
                          aria-hidden
                          style={{
                            width: 28,
                            height: 28,
                            borderRadius: 8,
                            background: "#ffffff",
                            border: "1px solid #d2d5d8",
                            display: "grid",
                            placeItems: "center",
                            color: "#6d7175",
                            flexShrink: 0,
                          }}
                        >
                          <Icon source={isExpanded ? ChevronUpIcon : ChevronDownIcon} />
                        </div>
                      ) : null}
                    </InlineStack>
                  </button>

                  {isDropTarget ? (
                    <div
                      aria-hidden
                      style={{
                        position: "absolute",
                        inset: 0,
                        background: "rgba(0, 91, 211, 0.06)",
                        pointerEvents: "none",
                      }}
                    >
                      <div
                        style={{
                          position: "absolute",
                          insetInline: 12,
                          insetBlockStart: 0,
                          height: 3,
                          borderRadius: 999,
                          background: "#005bd3",
                        }}
                      />
                    </div>
                  ) : null}

                  {isDragged ? (
                    <div
                      aria-hidden
                      style={{
                        position: "absolute",
                        insetInlineStart: isList ? 48 : 16,
                        insetBlockStart: 12,
                        padding: "6px 10px",
                        borderRadius: 999,
                        background: "rgba(17, 24, 39, 0.88)",
                        color: "#ffffff",
                        fontSize: 12,
                        fontWeight: 600,
                        pointerEvents: "none",
                      }}
                    >
                      Dragging
                    </div>
                  ) : null}

                  {isExpanded && reference.metaobject ? (
                    <Box paddingInline="300" paddingBlockEnd="300" paddingBlockStart="0">
                      <InlineMetaobjectEditor
                        reference={reference}
                        contentView={contentView}
                        draft={drafts[reference.id] || buildFieldDraft(reference, contentView)}
                        loading={saveFetcher.state !== "idle"}
                        onFieldChange={(fieldKey, value) => updateDraft(reference.id, fieldKey, value)}
                        onSave={() => saveMetaobject(reference)}
                      />
                    </Box>
                  ) : null}
                </div>
              );
            })}
          </BlockStack>
        ) : (
          <Text as="p" variant="bodySm" tone="subdued">
            No references selected yet.
          </Text>
        )}
        </BlockStack>

        <Modal
          open={pickerOpen}
          onClose={() => setPickerOpen(false)}
          title={`Select ${kind}`}
          size="large"
          primaryAction={
            !isList && metafield.references.length
              ? {
                  content: "Done",
                  onAction: () => setPickerOpen(false),
                }
              : undefined
          }
          secondaryActions={[
            {
              content: "Close",
              onAction: () => setPickerOpen(false),
            },
          ]}
        >
          <Modal.Section>
            <div dir={direction} style={{ textAlign }}>
              <BlockStack gap="300">
            {kind === "metaobject" ? (
              <Select
                label="Metaobject type"
                options={
                  metaobjectOptions.length
                    ? metaobjectOptions
                    : [{ label: "No visible metaobject definitions", value: "" }]
                }
                value={selectedMetaobjectType}
                onChange={setSelectedMetaobjectType}
              />
            ) : null}

            <TextField
              label={`Search ${kind}s`}
              autoComplete="off"
              placeholder={`Search ${kind}s`}
              value={query}
              onChange={setQuery}
            />

            {visibleResults.length > 0 ? (
              <InlineGrid columns={{ xs: 1, md: 2 }} gap="200">
                {visibleResults.map((reference) => {
                  const isSelected = metafield.referenceIds.includes(reference.id);
                  const displayTitle = getDisplayTitle(reference, contentView);
                  const displaySubtitle = getDisplaySubtitle(reference, contentView);

                  return (
                    <Box key={reference.id} padding="200" background="bg-surface-secondary" borderRadius="200">
                      <InlineStack align="space-between" blockAlign="center">
                        <InlineStack gap="200" blockAlign="center">
                          {renderReferenceLeadingVisual(reference, displayTitle)}
                          <BlockStack gap="050">
                            <Text as="span" variant="bodyMd">
                              {displayTitle}
                            </Text>
                            <Text as="span" variant="bodySm" tone="subdued">
                              {displaySubtitle || reference.id}
                            </Text>
                          </BlockStack>
                        </InlineStack>
                        <Button
                          variant={isSelected ? "secondary" : "primary"}
                          onClick={() =>
                            updateSelection(
                              isList
                                ? isSelected
                                  ? metafield.references.filter((item) => item.id !== reference.id)
                                  : [...metafield.references, reference]
                                : [reference],
                            )
                          }
                        >
                          {isList ? (isSelected ? "Remove" : "Add") : isSelected ? "Selected" : "Select"}
                        </Button>
                      </InlineStack>
                    </Box>
                  );
                })}
              </InlineGrid>
            ) : (
              <Box padding="400" background="bg-surface-secondary" borderRadius="200">
                <Text as="p" variant="bodySm" tone="subdued">
                  Search for a {kind} to attach.
                </Text>
              </Box>
            )}
              </BlockStack>
            </div>
          </Modal.Section>
        </Modal>
      </Card>
    </div>
  );
}

function InlineMetaobjectEditor({
  reference,
  contentView,
  draft,
  loading,
  onFieldChange,
  onSave,
}: {
  reference: ReferenceSummary;
  contentView: ContentView;
  draft: Record<string, string>;
  loading: boolean;
  onFieldChange: (fieldKey: string, value: string) => void;
  onSave: () => void;
}) {
  const editableFields = reference.metaobject?.fields.filter(
    (field) => contentView === "english" || field.isTranslatable,
  ) || [];
  const direction = contentView === "arabic" ? "rtl" : "ltr";
  const textAlign = contentView === "arabic" ? "right" : "left";

  return (
    <div dir={direction} style={{ textAlign }}>
      <Card>
        <BlockStack gap="300">
        <InlineStack align="space-between" blockAlign="center">
          <BlockStack gap="050">
            <Text as="h5" variant="headingSm">
              Edit {contentView === "english" ? "English source" : "Arabic translation"}
            </Text>
            <Text as="p" variant="bodySm" tone="subdued">
              {reference.metaobject?.definitionName || reference.subtitle || "Metaobject"} | {editableFields.length} editable field{editableFields.length === 1 ? "" : "s"}
            </Text>
          </BlockStack>
          <Button variant="primary" loading={loading} onClick={onSave} disabled={editableFields.length === 0}>
            {contentView === "english" ? "Save metaobject" : "Save Arabic fields"}
          </Button>
        </InlineStack>

        {!editableFields.length ? (
          <Banner tone="warning" title="No editable fields in this locale">
            <Text as="p" variant="bodySm">
              {contentView === "arabic"
                ? "This metaobject doesn't expose translatable fields through Shopify translations."
                : "This metaobject only contains specialized fields that are not inline-editable in the app yet."}
            </Text>
          </Banner>
        ) : (
          <BlockStack gap="300">
            {editableFields.map((field) => (
              <Box key={field.key} padding="200" background="bg-surface-secondary" borderRadius="200">
                <BlockStack gap="200">
                  <InlineStack gap="200" blockAlign="center" wrap>
                    <Text as="h6" variant="headingSm">
                      {field.label}
                    </Text>
                    <Badge tone="info">{field.type}</Badge>
                    {field.required ? <Badge tone="attention">Required</Badge> : null}
                    {field.outdated ? <Badge tone="warning">Outdated</Badge> : null}
                  </InlineStack>

                  {contentView === "arabic" ? (
                    <Text as="p" variant="bodySm" tone="subdued">
                      English source: {previewFieldValue(field.value)}
                    </Text>
                  ) : null}

                  {renderReferenceFieldEditor({
                    field,
                    contentView,
                    value:
                      draft[field.key] ?? (contentView === "arabic" ? field.arabicValue : field.value),
                    onChange: (value) => onFieldChange(field.key, value),
                  })}
                </BlockStack>
              </Box>
            ))}
          </BlockStack>
        )}
        </BlockStack>
      </Card>
    </div>
  );
}

function renderReferenceFieldEditor({
  field,
  contentView,
  value,
  onChange,
}: {
  field: NonNullable<ReferenceSummary["metaobject"]>["fields"][number];
  contentView: ContentView;
  value: string;
  onChange: (value: string) => void;
}) {
  if (field.type === "rich_text_field") {
    return (
      <NativeRichTextEditor
        label={field.label}
        value={value}
        onChange={onChange}
        minHeight={180}
        dir={contentView === "arabic" ? "rtl" : "ltr"}
      />
    );
  }

  if (field.type === "multi_line_text_field") {
    return (
      <NativeRichTextEditor
        label={field.label}
        value={value}
        onChange={onChange}
        minHeight={140}
        dir={contentView === "arabic" ? "rtl" : "ltr"}
        toolbar={false}
      />
    );
  }

  if (field.type === "boolean") {
    return (
      <Select
        label={field.label}
        options={[
          { label: "True", value: "true" },
          { label: "False", value: "false" },
        ]}
        value={value || "false"}
        onChange={onChange}
      />
    );
  }

  if (contentView === "arabic") {
    return (
      <div dir="rtl" style={{ textAlign: "right" }}>
        <TextField label={field.label} autoComplete="off" align="right" value={value} onChange={onChange} />
      </div>
    );
  }

  return <TextField label={field.label} autoComplete="off" value={value} onChange={onChange} />;
}

function getReferenceKind(type: string) {
  if (type.includes("product_reference")) return "product";
  if (type.includes("collection_reference")) return "collection";
  return "metaobject";
}

function dedupeReferences(references: ReferenceSummary[]) {
  const byId = new Map<string, ReferenceSummary>();
  for (const reference of references) {
    byId.set(reference.id, reference);
  }
  return [...byId.values()];
}

function getDisplayTitle(reference: ReferenceSummary, contentView: ContentView) {
  if (contentView === "arabic" && reference.localizedTitle?.trim()) {
    return reference.localizedTitle;
  }

  return reference.title;
}

function getDisplaySubtitle(reference: ReferenceSummary, contentView: ContentView) {
  if (contentView === "arabic" && reference.localizedSubtitle?.trim()) {
    return reference.localizedSubtitle;
  }

  return reference.subtitle;
}

function buildFieldDraft(reference: ReferenceSummary, contentView: ContentView) {
  return Object.fromEntries(
    (reference.metaobject?.fields || []).map((field) => [
      field.key,
      contentView === "arabic" ? field.arabicValue : field.value,
    ]),
  );
}

function buildRecentKey(kind: string, metaobjectType?: string) {
  return `tara-product-editor-references:${kind}:${metaobjectType || "default"}`;
}

function loadRecentSelections(kind: string, metaobjectType?: string) {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    return JSON.parse(window.localStorage.getItem(buildRecentKey(kind, metaobjectType)) || "[]") as ReferenceSummary[];
  } catch {
    return [];
  }
}

function persistRecentSelections(kind: string, metaobjectType: string | undefined, references: ReferenceSummary[]) {
  if (typeof window === "undefined") {
    return;
  }

  window.localStorage.setItem(
    buildRecentKey(kind, metaobjectType),
    JSON.stringify(dedupeReferences(references).slice(0, 12)),
  );
}

function previewFieldValue(value: string) {
  return value.trim() ? value.slice(0, 140) : "No source value";
}

function renderReferenceLeadingVisual(reference: ReferenceSummary, title: string) {
  if (reference.image) {
    return <Thumbnail source={reference.image.url} alt={reference.image.alt || title} size="small" />;
  }

  if (reference.kind === "METAOBJECT") {
    return null;
  }

  return (
    <Box padding="100" background="bg-fill-tertiary" borderRadius="200">
      <Text as="span" variant="bodySm">
        {reference.kind}
      </Text>
    </Box>
  );
}
