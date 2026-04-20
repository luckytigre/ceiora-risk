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
assert(
  !backendHelper.includes("process.env.OPERATOR_API_TOKEN") &&
    !backendHelper.includes("process.env.EDITOR_API_TOKEN") &&
    !backendHelper.includes("process.env.REFRESH_API_TOKEN"),
  `${backendHelperPath} must not source privileged auth headers from frontend runtime env`,
);
assert(
  backendHelper.includes("forwardedAuthHeaders") &&
    backendHelper.includes("x-operator-token") &&
    backendHelper.includes("x-editor-token") &&
    backendHelper.includes("authorization") &&
    !backendHelper.includes("x-refresh-token"),
  `${backendHelperPath} must forward caller auth headers to upstream services`,
);
assert(
  backendHelper.includes("headers: await upstreamHeaders(req, upstream, options?.headers)"),
  `${backendHelperPath} proxyJson must merge caller auth headers into upstream requests and support backend service auth`,
);

const controlRoutes = [
  { routePath: "src/app/api/refresh/route.ts", authMarker: "await upstreamHeaders(req, upstream)" },
  { routePath: "src/app/api/refresh/status/route.ts", authMarker: "await upstreamHeaders(req, upstream)" },
  { routePath: "src/app/api/operator/status/route.ts", authMarker: "proxyJson(" },
  { routePath: "src/app/api/health/diagnostics/route.ts", authMarker: "proxyJson(" },
  { routePath: "src/app/api/data/diagnostics/route.ts", authMarker: "proxyJson(" },
];

for (const { routePath, authMarker } of controlRoutes) {
  const source = read(routePath);
  assert(source.includes("controlBackendOrigin"), `${routePath} must import/use controlBackendOrigin`);
  assert(!source.includes("backendOrigin()"), `${routePath} must not route operator/control traffic through backendOrigin()`);
  assert(!source.includes("operatorHeaders("), `${routePath} must not inject operator headers from frontend runtime env`);
  assert(source.includes(authMarker), `${routePath} must forward caller auth headers to the control backend`);
}

const privilegedWriteRoutes = [
  "src/app/api/portfolio/whatif/route.ts",
  "src/app/api/portfolio/whatif/apply/route.ts",
  "src/app/api/holdings/import/route.ts",
  "src/app/api/holdings/position/route.ts",
  "src/app/api/holdings/position/remove/route.ts",
];

for (const routePath of privilegedWriteRoutes) {
  const source = read(routePath);
  assert(!source.includes("editorHeaders("), `${routePath} must not inject editor headers from frontend runtime env`);
  assert(!source.includes("operatorHeaders("), `${routePath} must not inject operator headers from frontend runtime env`);
  assert(source.includes("proxyJson("), `${routePath} must route privileged writes through proxyJson`);
}

console.log("control plane proxy contract ok");
