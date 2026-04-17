import "@testing-library/jest-dom/vitest";

import React from "react";
import { vi } from "vitest";

vi.mock("@shopify/app-bridge-react", () => ({
  SaveBar: ({ children }: { children?: React.ReactNode }) =>
    React.createElement("div", { "data-testid": "save-bar" }, children),
  TitleBar: ({
    children,
    title,
  }: {
    children?: React.ReactNode;
    title?: string;
  }) => React.createElement("div", { "data-testid": "title-bar", "data-title": title }, children),
  useAppBridge: () => ({
    toast: { show: vi.fn() },
    saveBar: {
      leaveConfirmation: vi.fn().mockResolvedValue(undefined),
    },
  }),
}));

Object.defineProperty(window, "matchMedia", {
  writable: true,
  value: vi.fn().mockImplementation((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
});

class ResizeObserverMock {
  observe() {}
  unobserve() {}
  disconnect() {}
}

class IntersectionObserverMock {
  root = null;
  rootMargin = "";
  thresholds = [];
  observe() {}
  unobserve() {}
  disconnect() {}
  takeRecords() {
    return [];
  }
}

vi.stubGlobal("ResizeObserver", ResizeObserverMock);
vi.stubGlobal("IntersectionObserver", IntersectionObserverMock);
vi.stubGlobal("scrollTo", vi.fn());

Object.defineProperty(HTMLElement.prototype, "scrollIntoView", {
  configurable: true,
  value: vi.fn(),
});
