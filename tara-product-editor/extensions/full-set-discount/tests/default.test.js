import path from "path";
import fs from "fs";
import { execSync } from "child_process";
import { describe, beforeAll, test, expect } from "vitest";
import { loadSchema, loadInputQuery, loadFixture, validateTestAssets, runFunction } from "@shopify/shopify-function-test-helpers";

process.env.PATH = [
  path.join(process.env.USERPROFILE || "", ".cargo", "bin"),
  path.join(process.env.APPDATA || "", "npm"),
  process.env.PATH || "",
].join(path.delimiter);

const shopifyCliPath = process.platform === "win32"
  ? path.join(process.env.APPDATA || "", "npm", "shopify.cmd")
  : "shopify";

function buildFunctionWithCli(functionDir) {
  const cargoPath = path.join(process.env.USERPROFILE || "", ".cargo", "bin", process.platform === "win32" ? "cargo.exe" : "cargo");
  execSync(`"${cargoPath}" build --target wasm32-unknown-unknown --release`, {
    cwd: functionDir,
    stdio: "pipe",
    env: {
      ...process.env,
      SHOPIFY_INVOKED_BY: "shopify-function-test-helpers",
    },
  });
}

function getFunctionInfoWithCli(functionDir) {
  const appRootDir = path.dirname(functionDir);
  const functionName = path.basename(functionDir);
  const output = execSync(`"${shopifyCliPath}" app function info --json --path ${functionName}`, {
    cwd: appRootDir,
    stdio: "pipe",
    env: {
      ...process.env,
      SHOPIFY_INVOKED_BY: "shopify-function-test-helpers",
    },
    encoding: "utf8",
  });
  return JSON.parse(output.trim());
}

describe("Default Integration Test", () => {
  let schema;
  let functionDir;
  let functionInfo;
  let schemaPath;
  let targeting;
  let functionRunnerPath;
  let wasmPath;

  beforeAll(async () => {
    functionDir = path.dirname(__dirname);
    buildFunctionWithCli(functionDir);
    functionInfo = getFunctionInfoWithCli(functionDir);
    ({ schemaPath, functionRunnerPath, wasmPath, targeting } = functionInfo);
    schema = await loadSchema(schemaPath);
  }, 45000);

  const fixturesDir = path.join(__dirname, "fixtures");
  const fixtureFiles = fs
    .readdirSync(fixturesDir)
    .filter((file) => file.endsWith(".json"))
    .map((file) => path.join(fixturesDir, file));

  fixtureFiles.forEach((fixtureFile) => {
    test(`runs ${path.relative(fixturesDir, fixtureFile)}`, async () => {
      const fixture = await loadFixture(fixtureFile);
      const targetInputQueryPath = targeting[fixture.target].inputQueryPath;
      const inputQueryAST = await loadInputQuery(targetInputQueryPath);

      const validationResult = await validateTestAssets({ schema, fixture, inputQueryAST });
      expect(validationResult.inputQuery.errors).toEqual([]);
      expect(validationResult.inputFixture.errors).toEqual([]);
      expect(validationResult.outputFixture.errors).toEqual([]);

      const runResult = await runFunction(fixture, functionRunnerPath, wasmPath, targetInputQueryPath, schemaPath);
      expect(runResult.error).toBeNull();
      expect(runResult.result.output).toEqual(fixture.expectedOutput);
    }, 10000);
  });
});
