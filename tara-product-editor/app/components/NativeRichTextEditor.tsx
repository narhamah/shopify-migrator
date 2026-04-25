import type { FunctionComponent, SVGProps } from "react";
import { useEffect, useMemo, useRef, useState } from "react";

import { BlockStack, Box, Icon, InlineStack, Select, Text } from "@shopify/polaris";
import {
  CodeIcon,
  ImageIcon,
  LinkIcon,
  ListBulletedIcon,
  ListNumberedIcon,
  TextBoldIcon,
  TextItalicIcon,
  TextUnderlineIcon,
} from "@shopify/polaris-icons";

import { editableTextToHtml, htmlToEditableText } from "~/lib/rich-text";

type BlockStyle = "paragraph" | "bullet" | "numbered";

export function NativeRichTextEditor({
  label,
  value,
  onChange,
  helpText,
  minHeight = 220,
  dir = "ltr",
  toolbar = true,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  helpText?: string;
  minHeight?: number;
  dir?: "ltr" | "rtl";
  toolbar?: boolean;
}) {
  const editorRef = useRef<HTMLDivElement | null>(null);
  const [focused, setFocused] = useState(false);
  const [blockStyle, setBlockStyle] = useState<BlockStyle>(inferBlockStyle(value));
  const editorHtml = useMemo(() => normalizeEditorHtml(editableTextToHtml(value)), [value]);

  useEffect(() => {
    if (!editorRef.current || focused) {
      return;
    }

    if (normalizeEditorHtml(editorRef.current.innerHTML) !== editorHtml) {
      editorRef.current.innerHTML = editorHtml;
    }
  }, [editorHtml, focused]);

  useEffect(() => {
    setBlockStyle(inferBlockStyle(value));
  }, [value]);

  function emitChange() {
    const html = normalizeEditorHtml(editorRef.current?.innerHTML || "");
    onChange(htmlToEditableText(html === EMPTY_EDITOR_HTML ? "" : html));
  }

  function runCommand(command: string, commandValue?: string) {
    if (typeof document === "undefined") {
      return;
    }

    editorRef.current?.focus();
    document.execCommand(command, false, commandValue);
    emitChange();
  }

  function handleFormatChange(nextValue: string) {
    const nextStyle = nextValue as BlockStyle;
    setBlockStyle(nextStyle);

    if (nextStyle === "paragraph") {
      runCommand("formatBlock", "<p>");
      return;
    }

    if (nextStyle === "bullet") {
      runCommand("insertUnorderedList");
      return;
    }

    runCommand("insertOrderedList");
  }

  const isEmpty = value.trim().length === 0;

  return (
    <BlockStack gap="100">
      <Text as="span" variant="bodyMd">
        {label}
      </Text>
      <Box
        borderRadius="300"
        borderWidth={focused ? "050" : "025"}
        borderColor={focused ? "border-emphasis" : "border"}
        overflowX="hidden"
        overflowY="hidden"
        background="bg-surface"
      >
        <BlockStack gap="0">
          {toolbar ? (
            <Box paddingBlock="150" paddingInline="200" background="bg-surface-secondary">
              <InlineStack gap="100" blockAlign="center" wrap={false}>
                <Box minWidth="132px">
                  <Select
                    label="Text style"
                    labelHidden
                    options={[
                      { label: "Paragraph", value: "paragraph" },
                      { label: "Bulleted list", value: "bullet" },
                      { label: "Numbered list", value: "numbered" },
                    ]}
                    value={blockStyle}
                    onChange={handleFormatChange}
                  />
                </Box>
                <ToolbarDivider />
                <ToolbarIconButton icon={TextBoldIcon} label="Bold" onClick={() => runCommand("bold")} />
                <ToolbarIconButton icon={TextItalicIcon} label="Italic" onClick={() => runCommand("italic")} />
                <ToolbarIconButton icon={TextUnderlineIcon} label="Underline" disabled />
                <ToolbarDivider />
                <ToolbarIconButton icon={ListBulletedIcon} label="Bulleted list" onClick={() => handleFormatChange("bullet")} />
                <ToolbarIconButton icon={ListNumberedIcon} label="Numbered list" onClick={() => handleFormatChange("numbered")} />
                <ToolbarDivider />
                <ToolbarIconButton icon={LinkIcon} label="Insert link" disabled />
                <ToolbarIconButton icon={ImageIcon} label="Insert image" disabled />
                <ToolbarDivider />
                <ToolbarIconButton icon={CodeIcon} label="Code" disabled />
              </InlineStack>
            </Box>
          ) : null}
          <div
            style={{
              position: "relative",
              background: focused ? "#ffffff" : "#ffffff",
            }}
          >
            {isEmpty && !focused ? (
              <div
                aria-hidden
                style={{
                  position: "absolute",
                  insetInlineStart: 16,
                  insetBlockStart: 16,
                  color: "#6d7175",
                  pointerEvents: "none",
                  textAlign: dir === "rtl" ? "right" : "left",
                }}
              >
                Start writing
              </div>
            ) : null}
            <div
              ref={editorRef}
              aria-label={label}
              contentEditable
              suppressContentEditableWarning
              dir={dir}
              onFocus={() => setFocused(true)}
              onBlur={() => {
                setFocused(false);
                emitChange();
              }}
              onInput={emitChange}
              style={{
                minHeight,
                padding: 16,
                outline: "none",
                fontFamily: "inherit",
                fontSize: 14,
                lineHeight: "20px",
                textAlign: dir === "rtl" ? "right" : "left",
                whiteSpace: "pre-wrap",
              }}
            />
          </div>
        </BlockStack>
      </Box>
      {helpText ? (
        <Text as="p" variant="bodySm" tone="subdued">
          {helpText}
        </Text>
      ) : null}
    </BlockStack>
  );
}

const EMPTY_EDITOR_HTML = "<p><br></p>";

function normalizeEditorHtml(value: string) {
  const trimmed = value.trim();
  return trimmed || EMPTY_EDITOR_HTML;
}

function inferBlockStyle(value: string): BlockStyle {
  const lines = value.split("\n").map((line) => line.trim()).filter(Boolean);
  if (lines.length === 0) {
    return "paragraph";
  }
  if (lines.every((line) => /^[-*]\s+/.test(line))) {
    return "bullet";
  }
  if (lines.every((line) => /^\d+[.)]\s+/.test(line))) {
    return "numbered";
  }
  return "paragraph";
}

function ToolbarDivider() {
  return (
    <div
      aria-hidden
      style={{
        width: 1,
        height: 24,
        background: "#d2d5d8",
      }}
    />
  );
}

function ToolbarIconButton({
  icon,
  label,
  onClick,
  disabled,
}: {
  icon: FunctionComponent<SVGProps<SVGSVGElement>>;
  label: string;
  onClick?: () => void;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      aria-label={label}
      title={label}
      disabled={disabled}
      onClick={onClick}
      style={{
        width: 32,
        height: 32,
        borderRadius: 8,
        border: "1px solid transparent",
        background: disabled ? "#f6f6f7" : "#ffffff",
        color: disabled ? "#8a8f98" : "#303030",
        cursor: disabled ? "not-allowed" : "pointer",
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <Icon source={icon} />
    </button>
  );
}
