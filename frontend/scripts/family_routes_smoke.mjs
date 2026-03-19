import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";

const HOST = "127.0.0.1";
const PORT = 3114;
const BASE_URL = `http://${HOST}:${PORT}`;
const __filename = fileURLToPath(import.meta.url);
const FRONTEND_ROOT = path.resolve(path.dirname(__filename), "..");
const NEXT_BIN = path.resolve(FRONTEND_ROOT, "node_modules", ".bin", process.platform === "win32" ? "next.cmd" : "next");

const serverStdout = [];
const serverStderr = [];
const server = spawn(
  NEXT_BIN,
  ["dev", "-H", HOST, "-p", String(PORT)],
  {
    cwd: FRONTEND_ROOT,
    env: {
      ...process.env,
      NEXT_TELEMETRY_DISABLED: "1",
    },
    stdio: ["ignore", "pipe", "pipe"],
  },
);

server.stdout?.on("data", (chunk) => {
  serverStdout.push(String(chunk));
});
server.stderr?.on("data", (chunk) => {
  serverStderr.push(String(chunk));
});

function tail(lines) {
  return lines.slice(-40).join("");
}

async function waitForServer(url, timeoutMs = 120000) {
  const startedAt = Date.now();
  let lastError = null;
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(url);
      if (response.ok) return;
      lastError = new Error(`Unexpected status ${response.status}`);
    } catch (error) {
      lastError = error;
    }
    await delay(500);
  }
  throw new Error(`Timed out waiting for ${url}: ${lastError instanceof Error ? lastError.message : String(lastError)}`);
}

async function cleanup() {
  if (server.killed) return;
  server.kill("SIGTERM");
  await Promise.race([
    new Promise((resolve) => server.once("exit", resolve)),
    delay(10000).then(() => {
      if (!server.killed) server.kill("SIGKILL");
    }),
  ]);
}

async function assertRedirect(sourcePath, expectedLocation) {
  const response = await fetch(`${BASE_URL}${sourcePath}`, { redirect: "manual" });
  assert.equal(response.status, 307, `${sourcePath} should redirect`);
  assert.equal(response.headers.get("location"), expectedLocation, `${sourcePath} redirect target mismatch`);
}

async function assertOk(pathname) {
  const response = await fetch(`${BASE_URL}${pathname}`);
  assert.equal(response.status, 200, `${pathname} should resolve`);
  return response.text();
}

try {
  await waitForServer(`${BASE_URL}/`);

  const homeHtml = await assertOk("/");
  assert.match(homeHtml, /Choose a model family/i);
  assert.match(homeHtml, /\/cuse\/exposures/i);
  assert.match(homeHtml, /\/cpar\/risk/i);
  assert.match(homeHtml, /\/positions/i);

  await assertRedirect("/exposures", "/cuse/exposures");
  await assertRedirect("/explore", "/cuse/explore");
  await assertRedirect("/health", "/cuse/health");
  await assertRedirect("/cuse", "/cuse/exposures");
  await assertRedirect("/cpar", "/cpar/risk");
  await assertRedirect("/cpar/portfolio", "/cpar/risk");

  await assertOk("/cuse/exposures");
  await assertOk("/cuse/explore");
  await assertOk("/cuse/health");
  await assertOk("/cpar/risk");
  await assertOk("/cpar/explore");
  await assertOk("/cpar/health");
  await assertOk("/cpar/hedge");
  await assertOk("/positions");

  await cleanup();
} catch (error) {
  console.error("STDOUT tail:\n", tail(serverStdout));
  console.error("STDERR tail:\n", tail(serverStderr));
  await cleanup();
  throw error;
}
