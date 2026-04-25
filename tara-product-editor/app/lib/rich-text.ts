import { safeJsonParse } from "./utils";

type RichTextNode = {
  type: string;
  value?: string;
  bold?: boolean;
  italic?: boolean;
  listType?: "ordered" | "unordered";
  children?: RichTextNode[];
};

export function isRichTextJson(value: string | null | undefined) {
  if (!value || !value.trim().startsWith("{")) {
    return false;
  }

  const parsed = safeJsonParse<Record<string, unknown> | null>(value, null);
  return parsed?.type === "root";
}

export function extractRichTextPlainText(value: string | null | undefined) {
  if (!value || !isRichTextJson(value)) {
    return stripHtml(value || "");
  }

  const parsed = safeJsonParse<RichTextNode>(value, { type: "root", children: [] });
  return flattenPlainText(parsed).replace(/\s+\n/g, "\n").replace(/\n{3,}/g, "\n\n").trim();
}

export function richTextJsonToEditableText(value: string | null | undefined) {
  if (!value) {
    return "";
  }

  if (!isRichTextJson(value)) {
    return value;
  }

  const root = safeJsonParse<RichTextNode>(value, { type: "root", children: [] });
  const blocks = (root.children || [])
    .map(renderBlock)
    .filter(Boolean)
    .join("\n\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

  return blocks;
}

export function htmlToEditableText(value: string | null | undefined) {
  if (!value) {
    return "";
  }

  let output = stripPageBuilderCssNoise(value)
    .replace(/\r\n/g, "\n")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|section|article|h[1-6])>/gi, "\n\n")
    .replace(/<li>/gi, "- ")
    .replace(/<\/li>/gi, "\n")
    .replace(/<(strong|b)>(.*?)<\/\1>/gi, (_, _tag, content) => `**${stripHtml(String(content)).trim()}**`)
    .replace(/<(em|i)>(.*?)<\/\1>/gi, (_, _tag, content) => `_${stripHtml(String(content)).trim()}_`)
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/\n{3,}/g, "\n\n");

  output = output
    .split("\n")
    .map((line) => line.replace(/\s+/g, " ").trimEnd())
    .join("\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

  return output;
}

export function editableTextToHtml(value: string) {
  const root = editableTextToRichText(value);
  return richTextNodeToHtml(root);
}

export function sanitizeRichTextJson(value: string) {
  if (!value.trim()) {
    return value;
  }

  if (isRichTextJson(value)) {
    const parsed = safeJsonParse<Record<string, unknown>>(value, { type: "root", children: [] });
    return JSON.stringify(parsed);
  }

  return JSON.stringify(editableTextToRichText(value));
}

function editableTextToRichText(value: string): RichTextNode {
  const lines = value.replace(/\r\n/g, "\n").split("\n");
  const children: RichTextNode[] = [];
  let index = 0;

  while (index < lines.length) {
    const current = lines[index].trimEnd();

    if (!current.trim()) {
      index += 1;
      continue;
    }

    const unorderedMatch = current.match(/^[-*]\s+(.*)$/);
    if (unorderedMatch) {
      const items: RichTextNode[] = [];
      while (index < lines.length) {
        const line = lines[index].trimEnd();
        const match = line.match(/^[-*]\s+(.*)$/);
        if (!match) break;
        items.push({
          type: "list-item",
          children: parseInlineText(match[1].trim()),
        });
        index += 1;
      }
      children.push({
        type: "list",
        listType: "unordered",
        children: items,
      });
      continue;
    }

    const orderedMatch = current.match(/^\d+[.)]\s+(.*)$/);
    if (orderedMatch) {
      const items: RichTextNode[] = [];
      while (index < lines.length) {
        const line = lines[index].trimEnd();
        const match = line.match(/^\d+[.)]\s+(.*)$/);
        if (!match) break;
        items.push({
          type: "list-item",
          children: parseInlineText(match[1].trim()),
        });
        index += 1;
      }
      children.push({
        type: "list",
        listType: "ordered",
        children: items,
      });
      continue;
    }

    const paragraphLines: string[] = [];
    while (index < lines.length && lines[index].trim()) {
      if (/^[-*]\s+/.test(lines[index]) || /^\d+[.)]\s+/.test(lines[index])) {
        break;
      }
      paragraphLines.push(lines[index].trim());
      index += 1;
    }

    const paragraphText = paragraphLines.join(" ").trim();
    if (paragraphText) {
      children.push({
        type: "paragraph",
        children: parseInlineText(paragraphText),
      });
    }
  }

  return {
    type: "root",
    children,
  };
}

function parseInlineText(value: string) {
  const nodes: RichTextNode[] = [];
  const parts = value.split(/(\*\*[^*]+\*\*|_[^_]+_)/g).filter(Boolean);

  for (const part of parts) {
    const boldMatch = part.match(/^\*\*([^*]+)\*\*$/);
    if (boldMatch) {
      nodes.push({
        type: "text",
        value: boldMatch[1],
        bold: true,
      });
      continue;
    }

    const italicMatch = part.match(/^_([^_]+)_$/);
    if (italicMatch) {
      nodes.push({
        type: "text",
        value: italicMatch[1],
        italic: true,
      });
      continue;
    }

    nodes.push({
      type: "text",
      value: part,
    });
  }

  return nodes.length
    ? nodes
    : [
        {
          type: "text",
          value,
        },
      ];
}

function renderBlock(node: RichTextNode): string {
  if (node.type === "list") {
    return (node.children || [])
      .map((child, index) => {
        const prefix = node.listType === "ordered" ? `${index + 1}. ` : "- ";
        return `${prefix}${renderInlineChildren(child.children || [])}`;
      })
      .join("\n");
  }

  return renderInlineChildren(node.children || []);
}

function renderInlineChildren(children: RichTextNode[]): string {
  return children
    .map((child) => {
      if (child.type === "text") {
        const value = child.value || "";
        if (child.bold) {
          return `**${value}**`;
        }
        if (child.italic) {
          return `_${value}_`;
        }
        return value;
      }

      return renderInlineChildren(child.children || []);
    })
    .join("")
    .trim();
}

function flattenPlainText(node: RichTextNode): string {
  if (node.type === "text") {
    return node.value || "";
  }

  if (node.type === "list") {
    return (node.children || []).map((child) => flattenPlainText(child)).join("\n");
  }

  return (node.children || []).map((child) => flattenPlainText(child)).join(" ");
}

function richTextNodeToHtml(node: RichTextNode): string {
  if (node.type === "root") {
    return (node.children || []).map(richTextNodeToHtml).join("");
  }

  if (node.type === "paragraph") {
    return `<p>${renderInlineHtml(node.children || [])}</p>`;
  }

  if (node.type === "list") {
    const tag = node.listType === "ordered" ? "ol" : "ul";
    return `<${tag}>${(node.children || []).map(richTextNodeToHtml).join("")}</${tag}>`;
  }

  if (node.type === "list-item") {
    return `<li>${renderInlineHtml(node.children || [])}</li>`;
  }

  if (node.type === "text") {
    return escapeHtml(node.value || "");
  }

  return renderInlineHtml(node.children || []);
}

function renderInlineHtml(children: RichTextNode[]) {
  return children
    .map((child) => {
      if (child.type === "text") {
        let value = escapeHtml(child.value || "");
        if (child.bold) {
          value = `<strong>${value}</strong>`;
        }
        if (child.italic) {
          value = `<em>${value}</em>`;
        }
        return value;
      }

      return richTextNodeToHtml(child);
    })
    .join("");
}

function escapeHtml(value: string) {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function stripHtml(value: string) {
  return stripPageBuilderCssNoise(value)
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function stripPageBuilderCssNoise(value: string) {
  return value
    .replace(/#html-body\s+\[data-pb-style=[^\]]+\]\{[^{}]*\}\s*/gi, "")
    .replace(/\[data-pb-style=[^\]]+\]\{[^{}]*\}\s*/gi, "");
}
