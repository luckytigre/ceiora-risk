import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { chromium } from "playwright";

const HOST = "127.0.0.1";
const PORT = 3103;
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

const serverStdout = [];
const serverStderr = [];
let debugPage = null;
let capturedPageError = null;
let metaReady = false;
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
        if (!metaReady) {
          return fulfillJson(
            {
              detail: {
                status: "not_ready",
                error: "cpar_not_ready",
                message: "No successful cPAR package is available for read surfaces.",
                build_profile: "cpar-weekly",
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
          factor_count: 2,
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
            {
              factor_id: "XLK",
              ticker: "XLK",
              label: "Technology",
              group: "sector",
              display_order: 15,
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
          total: 2,
          results: [
            {
              ticker: "AAPL",
              ric: "AAPL.OQ",
              display_name: "Apple Inc.",
              fit_status: "ok",
              warnings: [],
              hq_country_code: "US",
            },
            {
              ticker: "AAPL",
              ric: "AAPL.L",
              display_name: "Apple ADR London",
              fit_status: "limited_history",
              warnings: ["ex_us_caution"],
              hq_country_code: "GB",
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

      if (method === "GET" && pathName === "/api/cpar/portfolio/hedge") {
        portfolioRequestCount += 1;
        return fulfillJson(
          {
            detail: {
              status: "not_ready",
              error: "cpar_not_ready",
              message: "No successful cPAR package is available for read surfaces.",
              build_profile: "cpar-weekly",
            },
          },
          503,
        );
      }

      if (method === "GET" && pathName === "/api/cpar/ticker/AAPL") {
        detailRequestCount += 1;
        if (!requestUrl.searchParams.get("ric")) {
          return fulfillJson({ detail: "Ambiguous cPAR instrument fit for ticker AAPL" }, 409);
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

      if (method === "GET" && pathName === "/api/cpar/ticker/AAPL/hedge") {
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
          mode: "factor_neutral",
          hedge_status: "hedge_ok",
          hedge_reason: "Thresholded raw ETF hedge",
          hedge_legs: [{ factor_id: "SPY", label: "Market", group: "market", display_order: 0, weight: -1.12 }],
          post_hedge_exposures: [{ factor_id: "SPY", label: "Market", group: "market", display_order: 0, pre_beta: 1.12, hedge_leg: -1.12, post_beta: 0 }],
          pre_hedge_factor_variance_proxy: 0.24,
          post_hedge_factor_variance_proxy: 0.02,
          gross_hedge_notional: 1.12,
          net_hedge_notional: -1.12,
          non_market_reduction_ratio: 0.86,
          stability: {
            leg_overlap_ratio: 1.0,
            gross_hedge_notional_change: 0.03,
            net_hedge_notional_change: 0.02,
          },
        });
      }

      return fulfillJson({ error: `Unhandled API route ${pathName}` }, 500);
    });

    await gotoWithRetry(page, `${BASE_URL}/cpar`, { waitUntil: "domcontentloaded" });
    await page.getByTestId("cpar-not-ready").waitFor();
    await page.getByText("No request-time fitting exists on this page.").waitFor();
    assert.equal(await page.getByRole("button", { name: "SYNC" }).count(), 0);
    assert.equal(await page.getByRole("button", { name: "RECALC" }).count(), 0);

    await gotoWithRetry(page, `${BASE_URL}/cpar/hedge?ticker=AAPL&ric=AAPL.OQ`, { waitUntil: "domcontentloaded" });
    await page.getByTestId("cpar-hedge-not-ready").waitFor();
    await page.getByText("cPAR Hedge Not Ready").waitFor();
    await page.getByText("Publish a durable cPAR package first, then reload.").waitFor();

    await gotoWithRetry(page, `${BASE_URL}/cpar/portfolio?account_id=acct_main`, { waitUntil: "domcontentloaded" });
    await page.getByTestId("cpar-portfolio-not-ready").waitFor();
    await page.getByText("cPAR Portfolio Not Ready").waitFor();
    await page.getByText("This workflow is package-based and read-only. Publish a durable cPAR package first, then reload.").waitFor();
    assert.equal(detailRequestCount, 0);
    assert.equal(portfolioRequestCount, 0);

    metaReady = true;

    await gotoWithRetry(page, `${BASE_URL}/cpar/explore?ticker=AAPL`, { waitUntil: "domcontentloaded" });
    const detailPanel = page.getByTestId("cpar-detail-panel");
    await detailPanel.getByText("Ticker is ambiguous.").waitFor();
    await detailPanel.getByText("Choose a specific RIC from the search results on the left.").waitFor();
    assert.equal(await page.getByRole("button", { name: "SYNC" }).count(), 0);
    assert.equal(await page.getByRole("button", { name: "RECALC" }).count(), 0);

    await page.getByRole("button", { name: /AAPL\.OQ/i }).click();
    await page.waitForURL(/ric=AAPL\.OQ/);
    await page.getByText("Apple Inc.").waitFor();

    if (capturedPageError) {
      throw capturedPageError;
    }

    assert.equal(page.url().includes("ric=AAPL.OQ"), true);
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
