import fs from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function read(relativePath) {
  return fs.readFileSync(path.join(repoRoot, relativePath), "utf8");
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

const backendHelperPath = "src/app/api/_backend.ts";
const backendHelper = read(backendHelperPath);

assert(
  backendHelper.includes("process.env.BACKEND_CONTROL_ORIGIN") &&
    backendHelper.includes("process.env.BACKEND_API_ORIGIN"),
  `${backendHelperPath} must define BACKEND_CONTROL_ORIGIN fallback to BACKEND_API_ORIGIN`,
);

const controlRoutes = [
  "src/app/api/refresh/route.ts",
  "src/app/api/refresh/status/route.ts",
  "src/app/api/operator/status/route.ts",
  "src/app/api/health/diagnostics/route.ts",
  "src/app/api/data/diagnostics/route.ts",
];

for (const routePath of controlRoutes) {
  const source = read(routePath);
  assert(source.includes("controlBackendOrigin"), `${routePath} must import/use controlBackendOrigin`);
  assert(!source.includes("backendOrigin()"), `${routePath} must not route operator/control traffic through backendOrigin()`);
}

console.log("control plane proxy contract ok");
