import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { chromium } from "playwright";

const HOST = "127.0.0.1";
const PORT = 3105;
const BASE_URL = `http://${HOST}:${PORT}`;
const __filename = fileURLToPath(import.meta.url);
const FRONTEND_ROOT = path.resolve(path.dirname(__filename), "..");
const NEXT_BIN = path.resolve(FRONTEND_ROOT, "node_modules", ".bin", process.platform === "win32" ? "next.cmd" : "next");

async function waitForServer(url, timeoutMs = 120000) {
  const startedAt = Date.now();
  let lastError = null;
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const res = await fetch(url);
      if (res.ok) return;
      lastError = new Error(`Unexpected status ${res.status}`);
    } catch (error) {
      lastError = error;
    }
    await delay(500);
  }
  throw new Error(`Timed out waiting for ${url}: ${lastError instanceof Error ? lastError.message : String(lastError)}`);
}

function tail(lines) {
  return lines.slice(-40).join("");
}

async function gotoWithRetry(page, url, options, attempts = 3) {
  let lastError = null;
  for (let index = 0; index < attempts; index += 1) {
    try {
      await page.goto(url, options);
      return;
    } catch (error) {
      lastError = error;
      const message = error instanceof Error ? error.message : String(error);
      if (!message.includes("ERR_ABORTED") || index === attempts - 1) {
        throw error;
      }
      await delay(500);
    }
  }
  throw lastError ?? new Error(`Failed to navigate to ${url}`);
}

const serverStdout = [];
const serverStderr = [];
let debugPage = null;
let capturedPageError = null;
let scenario = "landing_unavailable";
let detailRequestCount = 0;
let portfolioRequestCount = 0;

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

try {
  await waitForServer(`${BASE_URL}/cpar`);

  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    debugPage = page;
    page.on("pageerror", (error) => {
      capturedPageError = error;
    });

    await page.route("**/api/**", async (route) => {
      const requestUrl = new URL(route.request().url());
      const pathName = requestUrl.pathname;
      const method = route.request().method().toUpperCase();

      const fulfillJson = (payload, status = 200) => route.fulfill({
        status,
        contentType: "application/json",
        body: JSON.stringify(payload),
      });

      if (method === "GET" && pathName === "/api/operator/status") {
        return fulfillJson({
          refresh: { status: "idle", finished_at: "2026-03-18T15:00:00Z" },
          holdings_sync: { pending: false, pending_count: 0, dirty_since: null },
          neon_sync_health: { status: "ok", mirror_status: "ok", parity_status: "ok" },
          lanes: [],
          runtime: { allowed_profiles: ["serve-refresh"] },
        });
      }

      if (method === "GET" && pathName === "/api/cpar/meta") {
        if (scenario === "landing_unavailable") {
          return fulfillJson(
            {
              detail: {
                status: "unavailable",
                error: "cpar_authority_unavailable",
                message: "Neon cPAR read failed.",
              },
            },
            503,
          );
        }
        return fulfillJson({
          package_run_id: "run_curr",
          package_date: "2026-03-14",
          profile: "cpar-weekly",
          method_version: "cPAR1",
          factor_registry_version: "cPAR1_registry_v1",
          data_authority: "neon",
          lookback_weeks: 52,
          half_life_weeks: 26,
          min_observations: 39,
          source_prices_asof: "2026-03-14",
          classification_asof: "2026-03-14",
          universe_count: 100,
          fit_ok_count: 90,
          fit_limited_count: 8,
          fit_insufficient_count: 2,
          factor_count: 1,
          factors: [
            {
              factor_id: "SPY",
              ticker: "SPY",
              label: "Market",
              group: "market",
              display_order: 0,
              method_version: "cPAR1",
              factor_registry_version: "cPAR1_registry_v1",
            },
          ],
        });
      }

      if (method === "GET" && pathName === "/api/cpar/search") {
        return fulfillJson({
          package_run_id: "run_curr",
          package_date: "2026-03-14",
          profile: "cpar-weekly",
          method_version: "cPAR1",
          factor_registry_version: "cPAR1_registry_v1",
          data_authority: "neon",
          lookback_weeks: 52,
          half_life_weeks: 26,
          min_observations: 39,
          source_prices_asof: "2026-03-14",
          classification_asof: "2026-03-14",
          universe_count: 100,
          fit_ok_count: 90,
          fit_limited_count: 8,
          fit_insufficient_count: 2,
          query: requestUrl.searchParams.get("q") || "",
          limit: 12,
          total: 1,
          results: [
            {
              ticker: "AAPL",
              ric: "AAPL.OQ",
              display_name: "Apple Inc.",
              fit_status: "ok",
              warnings: [],
              hq_country_code: "US",
            },
          ],
        });
      }

      if (method === "GET" && pathName === "/api/holdings/accounts") {
        return fulfillJson({
          accounts: [
            {
              account_id: "acct_main",
              account_name: "Main Account",
              is_active: true,
              positions_count: 3,
              gross_quantity: 17,
              last_position_updated_at: "2026-03-18T15:00:00Z",
            },
          ],
        });
      }

      if (method === "GET" && pathName === "/api/cpar/ticker/AAPL") {
        detailRequestCount += 1;
        if (scenario === "hedge_unavailable") {
          return fulfillJson({
            package_run_id: "run_curr",
            package_date: "2026-03-14",
            profile: "cpar-weekly",
            method_version: "cPAR1",
            factor_registry_version: "cPAR1_registry_v1",
            data_authority: "neon",
            lookback_weeks: 52,
            half_life_weeks: 26,
            min_observations: 39,
            source_prices_asof: "2026-03-14",
            classification_asof: "2026-03-14",
            universe_count: 100,
            fit_ok_count: 90,
            fit_limited_count: 8,
            fit_insufficient_count: 2,
            ticker: "AAPL",
            ric: "AAPL.OQ",
            display_name: "Apple Inc.",
            fit_status: "ok",
            warnings: [],
            observed_weeks: 52,
            lookback_weeks: 52,
            longest_gap_weeks: 0,
            price_field_used: "adj_close",
            hq_country_code: "US",
            market_step_alpha: 0.01,
            beta_market_step1: 1.18,
            block_alpha: 0.0,
            beta_spy_trade: 1.12,
            raw_loadings: [{ factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 1.12 }],
            thresholded_loadings: [{ factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 1.12 }],
            pre_hedge_factor_variance_proxy: 0.24,
            pre_hedge_factor_volatility_proxy: 0.49,
          });
        }
        return fulfillJson(
          {
            detail: {
              status: "unavailable",
              error: "cpar_authority_unavailable",
              message: "Neon cPAR read failed.",
            },
          },
          503,
        );
      }

      if (method === "GET" && pathName === "/api/cpar/ticker/AAPL/hedge") {
        if (scenario === "hedge_unavailable") {
          return fulfillJson(
            {
              detail: {
                status: "unavailable",
                error: "cpar_authority_unavailable",
                message: "Neon cPAR read failed.",
              },
            },
            503,
          );
        }
        return fulfillJson({ error: "unexpected hedge request" }, 500);
      }

      if (method === "GET" && pathName === "/api/cpar/portfolio/hedge") {
        portfolioRequestCount += 1;
        if (scenario === "portfolio_unavailable") {
          return fulfillJson(
            {
              detail: {
                status: "unavailable",
                error: "cpar_authority_unavailable",
                message: "Shared holdings/source read failed.",
              },
            },
            503,
          );
        }
        return fulfillJson({ error: "unexpected portfolio request" }, 500);
      }

      return fulfillJson({ error: `Unhandled API route ${pathName}` }, 500);
    });

    await gotoWithRetry(page, `${BASE_URL}/cpar`, { waitUntil: "domcontentloaded" });
    await page.getByText("cPAR Read Surface Unavailable").waitFor();
    await page.getByText("Neon cPAR read failed.").waitFor();

    await gotoWithRetry(page, `${BASE_URL}/cpar/hedge?ticker=AAPL&ric=AAPL.OQ`, { waitUntil: "domcontentloaded" });
    await page.getByTestId("cpar-hedge-not-ready").waitFor();
    await page.getByText("cPAR Hedge Unavailable").waitFor();

    await gotoWithRetry(page, `${BASE_URL}/cpar/portfolio?account_id=acct_main`, { waitUntil: "domcontentloaded" });
    await page.getByTestId("cpar-portfolio-not-ready").waitFor();
    await page.getByText("cPAR Portfolio Unavailable").waitFor();
    assert.equal(detailRequestCount, 0);
    assert.equal(portfolioRequestCount, 0);

    scenario = "explore_unavailable";
    await gotoWithRetry(page, `${BASE_URL}/cpar/explore?ticker=AAPL&ric=AAPL.OQ`, { waitUntil: "domcontentloaded" });
    const detailPanel = page.getByTestId("cpar-detail-panel");
    await detailPanel.getByText("Detail unavailable.").waitFor();
    await detailPanel.getByText("Neon cPAR read failed.").waitFor();
    assert.equal(await page.getByTestId("cpar-hedge-panel").count(), 0);

    scenario = "hedge_unavailable";
    await gotoWithRetry(page, `${BASE_URL}/cpar/hedge?ticker=AAPL&ric=AAPL.OQ`, { waitUntil: "domcontentloaded" });
    await page.getByTestId("cpar-hedge-subject-panel").getByText("Apple Inc.").waitFor();
    await page.getByTestId("cpar-hedge-panel").waitFor();
    await page.getByText("Hedge preview unavailable.").waitFor();
    await page.getByText("Neon cPAR read failed.").waitFor();
    assert.equal(await page.getByTestId("cpar-post-hedge-table").count(), 0);

    scenario = "portfolio_unavailable";
    await gotoWithRetry(page, `${BASE_URL}/cpar/portfolio?account_id=acct_main`, { waitUntil: "domcontentloaded" });
    await page.getByTestId("cpar-portfolio-error").waitFor();
    await page.getByText("Portfolio hedge unavailable.").waitFor();
    await page.getByText("Shared holdings/source read failed.").waitFor();

    if (capturedPageError) {
      throw capturedPageError;
    }
  } finally {
    await browser.close();
  }

  await cleanup();
} catch (error) {
  if (debugPage) {
    try {
      console.error(await debugPage.content());
    } catch {}
  }
  console.error("STDOUT tail:\n", tail(serverStdout));
  console.error("STDERR tail:\n", tail(serverStderr));
  await cleanup();
  throw error;
}
