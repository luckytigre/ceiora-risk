import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { chromium } from "playwright";

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

async function gotoWithRetry(page, url, attempts = 3) {
  let lastError = null;
  for (let index = 0; index < attempts; index += 1) {
    try {
      const response = await page.goto(url, { waitUntil: "domcontentloaded" });
      if (response && !response.ok()) {
        lastError = new Error(`Unexpected status ${response.status()} for ${url}`);
        if (index === attempts - 1) throw lastError;
        await delay(500);
        continue;
      }
      return;
    } catch (error) {
      lastError = error;
      if (index === attempts - 1) throw error;
      await delay(500);
    }
  }
  throw lastError ?? new Error(`Failed to navigate to ${url}`);
}

try {
  await waitForServer(`${BASE_URL}/`);

  const homeHtml = await assertOk("/");
  assert.match(homeHtml, /cUSE/i);
  assert.match(homeHtml, /cPAR/i);

  await assertRedirect("/exposures", "/cuse/exposures");
  await assertRedirect("/explore", "/cuse/explore");
  await assertRedirect("/health", "/cuse/health");
  await assertRedirect("/cuse", "/cuse/exposures");
  await assertRedirect("/cpar", "/cpar/risk");
  await assertRedirect("/cpar/portfolio", "/cpar/risk");
  await assertRedirect("/cpar/portfolio?account_id=acct_main", "/cpar/risk?account_id=acct_main");

  await assertOk("/cuse/exposures");
  await assertOk("/cuse/explore");
  await assertOk("/cuse/health");
  await assertOk("/cpar/risk");
  await assertOk("/cpar/explore");
  await assertOk("/cpar/health");
  await assertOk("/cpar/hedge");
  await assertOk("/positions");

  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();

    await gotoWithRetry(page, `${BASE_URL}/`);
    await page.getByTestId("family-split-landing").waitFor();
    const chooserLinks = await page.locator('[data-testid="family-split-landing"] a').allTextContents();
    assert.deepEqual(chooserLinks, ["cUSE", "cPAR"]);

    await gotoWithRetry(page, `${BASE_URL}/cuse/exposures`);
    const cuseTabs = await page.locator(".dash-tabs-center .dash-tab-btn").allTextContents();
    assert.deepEqual(cuseTabs, ["Risk", "Explore", "Health"]);

    await gotoWithRetry(page, `${BASE_URL}/cpar/risk`);
    const cparTabs = await page.locator(".dash-tabs-center .dash-tab-btn").allTextContents();
    assert.deepEqual(cparTabs, ["Risk", "Explore", "Hedge", "Health"]);

    await gotoWithRetry(page, `${BASE_URL}/positions`);
    const positionsTabs = await page.locator(".dash-tabs-center .dash-tab-btn").allTextContents();
    assert.deepEqual(positionsTabs, ["Positions"]);
  } finally {
    await browser.close();
  }

  await cleanup();
} catch (error) {
  console.error("STDOUT tail:\n", tail(serverStdout));
  console.error("STDERR tail:\n", tail(serverStderr));
  await cleanup();
  throw error;
}
