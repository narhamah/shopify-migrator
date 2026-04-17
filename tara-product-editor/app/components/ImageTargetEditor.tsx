import type { DragEvent } from "react";
import { useEffect, useMemo, useRef, useState } from "react";

import {
  Badge,
  BlockStack,
  Box,
  Button,
  Checkbox,
  Icon,
  InlineGrid,
  InlineStack,
  Modal,
  Select,
  Text,
  TextField,
} from "@shopify/polaris";
import { DeleteIcon, UploadIcon } from "@shopify/polaris-icons";

import { reorderItems } from "~/lib/utils";
import type { ProductImageItem } from "~/types/editor";

export function ImageTargetEditor({
  title,
  tone,
  images,
  availableImages,
  loading,
  galleryQuery,
  hasMoreFiles,
  direction = "ltr",
  copySelectedLabel,
  onGalleryQueryChange,
  onSearchGallery,
  onLoadMoreFiles,
  onUploadFile,
  onCopySelected,
  onChange,
}: {
  title: string;
  tone: "success" | "info" | "warning";
  images: ProductImageItem[];
  availableImages: ProductImageItem[];
  loading?: boolean;
  galleryQuery: string;
  hasMoreFiles: boolean;
  direction?: "ltr" | "rtl";
  copySelectedLabel?: string;
  onGalleryQueryChange: (value: string) => void;
  onSearchGallery: () => void;
  onLoadMoreFiles: () => void;
  onUploadFile: (file: File, alt?: string) => void;
  onCopySelected?: (selectedImageIds: string[]) => void;
  onChange: (images: ProductImageItem[]) => void;
}) {
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [galleryOpen, setGalleryOpen] = useState(false);
  const [draggedIndex, setDraggedIndex] = useState<number | null>(null);
  const [dropIndex, setDropIndex] = useState<number | null>(null);
  const [uploadAlt, setUploadAlt] = useState("");
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [isDraggingOverUpload, setIsDraggingOverUpload] = useState(false);
  const [sourceFilter, setSourceFilter] = useState<"all" | "files" | "product" | "upload">("all");
  const [orientationFilter, setOrientationFilter] = useState<"all" | "landscape" | "portrait" | "square">("all");
  const [sortOrder, setSortOrder] = useState<"newest" | "oldest" | "name-asc" | "name-desc">("newest");

  const filteredImages = useMemo(() => {
    const query = galleryQuery.trim().toLowerCase();
    const filtered = availableImages.filter((image) => {
      const name = image.alt || image.url.split("/").pop() || image.id;
      if (query && !name.toLowerCase().includes(query)) {
        return false;
      }
      if (sourceFilter !== "all" && image.source !== sourceFilter) {
        return false;
      }
      if (orientationFilter !== "all" && getOrientation(image) !== orientationFilter) {
        return false;
      }
      return true;
    });

    return filtered.sort((left, right) => compareImages(left, right, sortOrder));
  }, [availableImages, galleryQuery, orientationFilter, sortOrder, sourceFilter]);

  function closeGallery() {
    setGalleryOpen(false);
    setSelectedIds([]);
  }

  function openGallery() {
    onSearchGallery();
    setGalleryOpen(true);
  }

  function toggleSelected(imageId: string) {
    setSelectedIds((current) =>
      current.includes(imageId) ? current.filter((id) => id !== imageId) : [...current, imageId],
    );
  }

  function addSelectedImages() {
    if (selectedIds.length === 0) {
      closeGallery();
      return;
    }

    const currentIds = new Set(images.map((image) => image.id));
    const selectedImages = selectedIds
      .map((id) => availableImages.find((image) => image.id === id))
      .filter((image): image is ProductImageItem => image !== undefined)
      .filter((image) => !currentIds.has(image.id));

    onChange([...images, ...selectedImages]);
    closeGallery();
  }

  function handleUpload(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (!file) return;
    onUploadFile(file, uploadAlt || file.name);
    setUploadAlt("");
    event.currentTarget.value = "";
  }

  function uploadDroppedFile(file?: File) {
    if (!file) return;
    onUploadFile(file, uploadAlt || file.name);
    setUploadAlt("");
    setIsDraggingOverUpload(false);
  }

  function clearDragState() {
    setDraggedIndex(null);
    setDropIndex(null);
  }

  useEffect(() => {
    setSelectedIds((current) => current.filter((id) => images.some((image) => image.id === id)));
  }, [images]);

  const textAlign = direction === "rtl" ? "right" : "left";
  const selectedCount = selectedIds.length;

  return (
    <div data-testid="image-target-editor-surface" dir={direction} style={{ textAlign }}>
      <Box padding="0" borderWidth="025" borderColor="border" borderRadius="300" background="bg-surface">
        <BlockStack gap="300">
          <Box padding="300">
            <InlineStack align="space-between" blockAlign="center">
              <InlineStack gap="200" blockAlign="center">
                <Text as="h3" variant="headingSm">
                  {title}
                </Text>
                <Badge tone={tone}>{`${images.length} images`}</Badge>
                {selectedCount ? <Badge tone="attention">{`${selectedCount} selected`}</Badge> : null}
              </InlineStack>
              <InlineStack gap="200" blockAlign="center">
                {onCopySelected && copySelectedLabel ? (
                  <>
                    <Button
                      size="slim"
                      disabled={selectedCount === 0}
                      onClick={() => {
                        onCopySelected(selectedIds);
                        setSelectedIds([]);
                      }}
                    >
                      {copySelectedLabel}
                    </Button>
                    <Button size="slim" disabled={selectedCount === 0} onClick={() => setSelectedIds([])}>
                      Clear selection
                    </Button>
                  </>
                ) : null}
                <Text as="span" variant="bodySm" tone="subdued">
                  Drag to reorder
                </Text>
              </InlineStack>
            </InlineStack>
          </Box>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(5.75rem, 1fr))",
              gap: 12,
              padding: "0 16px 16px 16px",
            }}
          >
            {images.map((image, index) => {
              const isDragged = draggedIndex === index;
              const isDropTarget = dropIndex === index && draggedIndex !== null && draggedIndex !== index;

              return (
                <div
                  key={image.id}
                  draggable
                  onDragStart={(event) => {
                    setDraggedIndex(index);
                    setDropIndex(index);
                    if (event.dataTransfer) {
                      event.dataTransfer.effectAllowed = "move";
                    }
                  }}
                  onDragEnter={(event: DragEvent<HTMLDivElement>) => {
                    event.preventDefault();
                    if (draggedIndex !== null) {
                      setDropIndex(index);
                    }
                  }}
                  onDragOver={(event: DragEvent<HTMLDivElement>) => event.preventDefault()}
                  onDrop={() => {
                    if (draggedIndex === null || draggedIndex === index) return;
                    onChange(reorderItems(images, draggedIndex, index));
                    clearDragState();
                  }}
                  onDragEnd={clearDragState}
                  style={{
                    gridColumn: index === 0 && images.length > 1 ? "span 2" : undefined,
                    gridRow: index === 0 && images.length > 1 ? "span 2" : undefined,
                    position: "relative",
                    border: isDropTarget ? "2px solid #005bd3" : "1px solid #d2d5d8",
                    borderRadius: 12,
                    padding: 0,
                    background: "#ffffff",
                    cursor: isDragged ? "grabbing" : "grab",
                    overflow: "hidden",
                    aspectRatio: index === 0 && images.length > 1 ? "1 / 1" : "1 / 1",
                    opacity: isDragged ? 0.55 : 1,
                    transform: isDragged
                      ? "scale(0.96) rotate(-1deg)"
                      : isDropTarget
                        ? "scale(1.02)"
                        : "scale(1)",
                    boxShadow: isDragged
                      ? "0 18px 36px rgba(0, 0, 0, 0.22)"
                      : isDropTarget
                        ? "0 0 0 4px rgba(0, 91, 211, 0.12)"
                        : "0 1px 2px rgba(0, 0, 0, 0.06)",
                    transition: "transform 140ms ease, box-shadow 140ms ease, opacity 140ms ease, border-color 140ms ease",
                  }}
                >
                  <div
                    onClick={(event) => event.stopPropagation()}
                    onMouseDown={(event) => event.stopPropagation()}
                    style={{
                      position: "absolute",
                      insetBlockStart: 8,
                      insetInlineStart: 8,
                      zIndex: 2,
                      padding: 4,
                      borderRadius: 999,
                      background: selectedIds.includes(image.id) ? "rgba(0, 91, 211, 0.12)" : "rgba(255, 255, 255, 0.9)",
                      boxShadow: "0 1px 2px rgba(0, 0, 0, 0.12)",
                    }}
                  >
                    <Checkbox
                      label={`Select ${image.alt || `image ${index + 1}`}`}
                      labelHidden
                      checked={selectedIds.includes(image.id)}
                      onChange={(checked) =>
                        setSelectedIds((current) =>
                          checked ? [...new Set([...current, image.id])] : current.filter((id) => id !== image.id),
                        )
                      }
                    />
                  </div>
                  <img
                    src={image.url}
                    alt={image.alt || "Image"}
                    loading={index === 0 ? "eager" : "lazy"}
                    decoding="async"
                    fetchPriority={index === 0 ? "high" : "low"}
                    width={image.width || undefined}
                    height={image.height || undefined}
                    style={{
                      width: "100%",
                      height: "100%",
                      objectFit: "cover",
                      display: "block",
                      filter: isDragged ? "saturate(0.9)" : "none",
                      transition: "filter 140ms ease",
                    }}
                  />
                  {isDropTarget ? (
                    <div
                      style={{
                        position: "absolute",
                        inset: 0,
                        display: "grid",
                        placeItems: "center",
                        background: "rgba(0, 91, 211, 0.08)",
                        pointerEvents: "none",
                      }}
                    >
                      <div
                        style={{
                          padding: "6px 10px",
                          borderRadius: 999,
                          background: "rgba(0, 91, 211, 0.92)",
                          color: "#ffffff",
                          fontSize: 12,
                          fontWeight: 600,
                        }}
                      >
                        Drop here
                      </div>
                    </div>
                  ) : null}
                  {isDragged ? (
                    <div
                      style={{
                        position: "absolute",
                        insetBlockStart: 8,
                        insetInlineStart: 8,
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
                  <div
                    style={{
                      position: "absolute",
                      inset: "auto 8px 8px 8px",
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                      gap: 8,
                    }}
                  >
                    <Badge tone={tone}>{`#${index + 1}`}</Badge>
                    <div style={{ display: "flex", gap: 6 }}>
                      <IconButton
                        icon={DeleteIcon}
                        label="Remove"
                        tone="critical"
                        onClick={() => onChange(images.filter((item) => item.id !== image.id))}
                      />
                    </div>
                  </div>
                </div>
              );
            })}

            <button
              type="button"
              onClick={openGallery}
              style={{
                minHeight: 92,
                borderRadius: 12,
                border: "1px dashed #c9cccf",
                background: "#ffffff",
                cursor: "pointer",
                display: "grid",
                placeItems: "center",
                aspectRatio: "1 / 1",
              }}
            >
              <BlockStack gap="100" inlineAlign="center">
                <PlusIconMark />
                <Text as="span" variant="bodySm">
                  Add media
                </Text>
              </BlockStack>
            </button>
          </div>
        </BlockStack>
      </Box>

      <input ref={fileInputRef} type="file" accept="image/*" hidden onChange={handleUpload} />

      <Modal
        open={galleryOpen}
        onClose={closeGallery}
        title="Select file"
        size="large"
        primaryAction={{
          content: "Done",
          onAction: addSelectedImages,
          disabled: selectedIds.length === 0,
        }}
        secondaryActions={[
          {
            content: "Cancel",
            onAction: closeGallery,
          },
        ]}
      >
        <Modal.Section>
          <div dir={direction} style={{ textAlign }}>
            <BlockStack gap="300">
            <InlineStack align="space-between" gap="200" blockAlign="center">
              <Box minWidth="420px">
                <TextField
                  label="Search files"
                  labelHidden
                  autoComplete="off"
                  placeholder="Search files"
                  value={galleryQuery}
                  onChange={onGalleryQueryChange}
                />
              </Box>
              <InlineStack gap="200">
                <Badge tone="info">{`${selectedIds.length} selected`}</Badge>
                <Button loading={loading} onClick={onSearchGallery}>
                  Search
                </Button>
              </InlineStack>
            </InlineStack>

            <InlineStack gap="200" wrap>
              <Box minWidth="140px">
                <Select
                  label="File source"
                  options={[
                    { label: "All media", value: "all" },
                    { label: "Shopify Files", value: "files" },
                    { label: "Product media", value: "product" },
                    { label: "New uploads", value: "upload" },
                  ]}
                  value={sourceFilter}
                  onChange={(value: string) => setSourceFilter(value as typeof sourceFilter)}
                />
              </Box>
              <Box minWidth="140px">
                <Select
                  label="Shape"
                  options={[
                    { label: "All shapes", value: "all" },
                    { label: "Landscape", value: "landscape" },
                    { label: "Portrait", value: "portrait" },
                    { label: "Square", value: "square" },
                  ]}
                  value={orientationFilter}
                  onChange={(value: string) => setOrientationFilter(value as typeof orientationFilter)}
                />
              </Box>
              <Box minWidth="160px">
                <Select
                  label="Sort"
                  options={[
                    { label: "Newest first", value: "newest" },
                    { label: "Oldest first", value: "oldest" },
                    { label: "Name A-Z", value: "name-asc" },
                    { label: "Name Z-A", value: "name-desc" },
                  ]}
                  value={sortOrder}
                  onChange={(value: string) => setSortOrder(value as typeof sortOrder)}
                />
              </Box>
            </InlineStack>

            <div style={{ position: "relative" }}>
              <div
                style={{
                  border: "1px dashed #d2d5d8",
                  borderRadius: 12,
                  background: isDraggingOverUpload ? "#f2f7fe" : "#ffffff",
                  padding: 24,
                }}
              >
                <BlockStack gap="200" inlineAlign="center">
                  <InlineStack gap="200" blockAlign="center">
                    <Button icon={UploadIcon} variant="primary" onClick={() => fileInputRef.current?.click()}>
                      Add media
                    </Button>
                    <Button disabled>Generate image</Button>
                  </InlineStack>
                  <Text as="p" variant="bodySm" tone="subdued">
                    Drag and drop images, videos, 3D models, and files.
                  </Text>
                </BlockStack>
              </div>
              <div
                onDragOver={(event) => {
                  event.preventDefault();
                  setIsDraggingOverUpload(true);
                }}
                onDragLeave={() => setIsDraggingOverUpload(false)}
                onDrop={(event) => {
                  event.preventDefault();
                  uploadDroppedFile(event.dataTransfer.files?.[0]);
                }}
                style={{
                  position: "absolute",
                  inset: 0,
                }}
              />
            </div>

            <TextField
              label="Alt text for the next upload"
              autoComplete="off"
              value={uploadAlt}
              onChange={setUploadAlt}
              helpText="Optional. Applied only to a new upload created from this selector."
            />

            {filteredImages.length ? (
              <BlockStack gap="300">
                <InlineGrid columns={{ xs: 2, sm: 3, md: 5 }} gap="300">
                  {filteredImages.map((image) => {
                    const isSelected = selectedIds.includes(image.id);
                    const isAlreadyAdded = images.some((existing) => existing.id === image.id);

                    return (
                      <button
                        key={image.id}
                        type="button"
                        onClick={() => !isAlreadyAdded && toggleSelected(image.id)}
                        style={{
                          border: isSelected ? "2px solid #005bd3" : "1px solid #d2d5d8",
                          borderRadius: 12,
                          padding: 10,
                          background: isSelected ? "#ebf4ff" : "#ffffff",
                          cursor: isAlreadyAdded ? "default" : "pointer",
                          opacity: isAlreadyAdded ? 0.7 : 1,
                          textAlign: "left",
                        }}
                      >
                        <BlockStack gap="150">
                          <InlineStack align="space-between" blockAlign="center">
                            <Checkbox
                              label="Select file"
                              labelHidden
                              checked={isSelected || isAlreadyAdded}
                              disabled={isAlreadyAdded}
                              onChange={() => !isAlreadyAdded && toggleSelected(image.id)}
                            />
                            {isAlreadyAdded ? <Badge tone="info">Added</Badge> : sourceBadge(image)}
                          </InlineStack>
                          <div
                            style={{
                              borderRadius: 10,
                              overflow: "hidden",
                              background: "#f6f6f7",
                              aspectRatio: "1 / 1",
                            }}
                          >
                            <img
                              src={image.url}
                              alt={image.alt || "Image"}
                              loading="lazy"
                              decoding="async"
                              width={image.width || undefined}
                              height={image.height || undefined}
                              style={{
                                width: "100%",
                                height: "100%",
                                objectFit: "cover",
                                display: "block",
                              }}
                            />
                          </div>
                          <Text as="span" variant="bodySm" fontWeight="medium" truncate>
                            {image.alt || image.url.split("/").pop() || image.id}
                          </Text>
                          <Text as="span" variant="bodySm" tone="subdued">
                            {formatDimensions(image)}
                          </Text>
                        </BlockStack>
                      </button>
                    );
                  })}
                </InlineGrid>
                {hasMoreFiles ? (
                  <InlineStack align="center">
                    <Button onClick={onLoadMoreFiles} loading={loading}>
                      Load more files
                    </Button>
                  </InlineStack>
                ) : null}
              </BlockStack>
            ) : (
              <Box padding="500" background="bg-surface-secondary" borderRadius="300">
                <BlockStack gap="100" inlineAlign="center">
                  <Text as="p" variant="bodyMd">
                    No files match this search yet.
                  </Text>
                  <Text as="p" variant="bodySm" tone="subdued">
                    Search Shopify Files or upload a new image from this selector.
                  </Text>
                </BlockStack>
              </Box>
            )}
            </BlockStack>
          </div>
        </Modal.Section>
      </Modal>
    </div>
  );
}

function formatDimensions(image: ProductImageItem) {
  if (!image.width || !image.height) {
    return "Unknown size";
  }

  return `${image.width} x ${image.height}`;
}

function getOrientation(image: ProductImageItem) {
  if (!image.width || !image.height) {
    return "unknown";
  }
  if (image.width === image.height) {
    return "square";
  }
  return image.width > image.height ? "landscape" : "portrait";
}

function compareImages(left: ProductImageItem, right: ProductImageItem, sortOrder: "newest" | "oldest" | "name-asc" | "name-desc") {
  const leftName = (left.alt || left.url.split("/").pop() || left.id).toLowerCase();
  const rightName = (right.alt || right.url.split("/").pop() || right.id).toLowerCase();
  const leftTime = left.createdAt ? new Date(left.createdAt).getTime() : 0;
  const rightTime = right.createdAt ? new Date(right.createdAt).getTime() : 0;

  if (sortOrder === "newest") {
    return rightTime - leftTime || leftName.localeCompare(rightName);
  }

  if (sortOrder === "oldest") {
    return leftTime - rightTime || leftName.localeCompare(rightName);
  }

  if (sortOrder === "name-desc") {
    return rightName.localeCompare(leftName);
  }

  return leftName.localeCompare(rightName);
}

function sourceBadge(image: ProductImageItem) {
  if (image.source === "files") {
    return <Badge>Files</Badge>;
  }
  if (image.source === "product") {
    return <Badge tone="success">Product</Badge>;
  }
  if (image.source === "upload") {
    return <Badge tone="attention">Upload</Badge>;
  }
  return <Badge>Media</Badge>;
}

function PlusIconMark() {
  return (
    <div
      aria-hidden
      style={{
        width: 24,
        height: 24,
        borderRadius: 999,
        background: "#f1f2f3",
        color: "#303030",
        display: "grid",
        placeItems: "center",
        fontSize: 18,
        lineHeight: 1,
      }}
    >
      +
    </div>
  );
}

function IconButton({
  icon,
  label,
  onClick,
  disabled,
  tone = "default",
}: {
  icon: any;
  label: string;
  onClick: () => void;
  disabled?: boolean;
  tone?: "default" | "critical";
}) {
  return (
    <button
      type="button"
      aria-label={label}
      disabled={disabled}
      onClick={(event) => {
        event.stopPropagation();
        onClick();
      }}
      style={{
        display: "inline-grid",
        placeItems: "center",
        width: 32,
        height: 32,
        borderRadius: 999,
        border: "1px solid #d2d5d8",
        background: "#ffffff",
        color: tone === "critical" ? "#8e1f0b" : "#303030",
        cursor: disabled ? "not-allowed" : "pointer",
      }}
    >
      <Icon source={icon} tone={tone === "critical" ? "critical" : undefined} />
    </button>
  );
}
