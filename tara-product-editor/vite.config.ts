import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

import { reactRouter } from "@react-router/dev/vite";
import { defineConfig, type UserConfig } from "vite";
import tsconfigPaths from "vite-tsconfig-paths";

if (
  process.env.HOST &&
  (!process.env.SHOPIFY_APP_URL || process.env.SHOPIFY_APP_URL === process.env.HOST)
) {
  process.env.SHOPIFY_APP_URL = process.env.HOST;
  delete process.env.HOST;
}

const host = new URL(process.env.SHOPIFY_APP_URL || "http://localhost").hostname;
const localhostKeyPath = resolve(".shopify", "localhost-key.pem");
const localhostCertPath = resolve(".shopify", "localhost.pem");
const localhostHttpsConfig =
  host === "localhost" && existsSync(localhostKeyPath) && existsSync(localhostCertPath)
    ? {
        key: readFileSync(localhostKeyPath),
        cert: readFileSync(localhostCertPath),
      }
    : undefined;

const hmrConfig =
  host === "localhost"
    ? {
        protocol: "ws",
        host: "localhost",
        port: 64999,
        clientPort: 64999,
      }
    : {
        protocol: "wss",
        host,
        port: Number.parseInt(process.env.FRONTEND_PORT || "8002", 10),
        clientPort: 443,
      };

export default defineConfig({
  server: {
    allowedHosts: [host],
    cors: {
      preflightContinue: true,
    },
    https: localhostHttpsConfig,
    port: Number(process.env.PORT || 3000),
    hmr: hmrConfig,
    fs: {
      allow: ["app", "node_modules"],
    },
  },
  plugins: [reactRouter(), tsconfigPaths()],
  build: {
    assetsInlineLimit: 0,
  },
  optimizeDeps: {
    include: ["@shopify/app-bridge-react"],
  },
}) satisfies UserConfig;
