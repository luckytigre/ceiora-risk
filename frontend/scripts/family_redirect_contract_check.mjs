import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { fileURLToPath, pathToFileURL } from "node:url";
import path from "node:path";

const __filename = fileURLToPath(import.meta.url);
const FRONTEND_ROOT = path.resolve(path.dirname(__filename), "..");
const nextConfigPath = path.resolve(FRONTEND_ROOT, "next.config.js");

const nextConfigModule = await import(pathToFileURL(nextConfigPath).href);
const nextConfig = nextConfigModule.default ?? nextConfigModule;
const redirects = await nextConfig.redirects();

const expectedLegacyRedirects = [
  { source: "/exposures", destination: "/cuse/exposures" },
  { source: "/explore", destination: "/cuse/explore" },
  { source: "/health", destination: "/cuse/health" },
];

for (const expected of expectedLegacyRedirects) {
  const match = redirects.find((entry) => entry.source === expected.source);
  assert.ok(match, `Missing redirect for ${expected.source}`);
  assert.equal(match.destination, expected.destination, `Redirect target mismatch for ${expected.source}`);
  assert.equal(match.permanent, false, `Legacy redirect should remain temporary for ${expected.source}`);
}

const duplicateAppPages = [
  path.resolve(FRONTEND_ROOT, "src/app/exposures/page.tsx"),
  path.resolve(FRONTEND_ROOT, "src/app/explore/page.tsx"),
  path.resolve(FRONTEND_ROOT, "src/app/health/page.tsx"),
].filter((filePath) => existsSync(filePath));

assert.deepEqual(
  duplicateAppPages.map((filePath) => path.relative(FRONTEND_ROOT, filePath)),
  [],
  "Legacy cUSE redirects should stay owned in next.config.js only",
);

console.log("family redirect contract ok");
