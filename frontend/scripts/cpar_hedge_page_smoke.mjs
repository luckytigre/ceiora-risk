import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { chromium } from "playwright";

const HOST = "127.0.0.1";
const PORT = 3106;
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

function factorRegistry() {
  return [
    ["SPY", "SPY", "Market", "market", 0],
    ["XLK", "XLK", "Technology", "sector", 15],
    ["QUAL", "QUAL", "Quality", "style", 32],
  ].map(([factor_id, ticker, label, group, display_order]) => ({
    factor_id,
    ticker,
    label,
    group,
    display_order,
    method_version: "cPAR1",
    factor_registry_version: "cPAR1_registry_v1",
  }));
}

const serverStdout = [];
const serverStderr = [];
let debugPage = null;
let capturedPageError = null;
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
  await waitForServer(`${BASE_URL}/cpar/hedge`);

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
        const factors = factorRegistry();
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
          universe_count: 1240,
          fit_ok_count: 1180,
          fit_limited_count: 48,
          fit_insufficient_count: 12,
          factor_count: factors.length,
          factors,
        });
      }

      if (method === "GET" && pathName === "/api/cpar/search") {
        const query = requestUrl.searchParams.get("q") || "";
        const rows = query.toUpperCase().includes("AAP")
          ? [
              {
                ticker: null,
                ric: "AAPL.NA",
                display_name: "Apple Inc. Synthetic Line",
                fit_status: "limited_history",
                warnings: ["continuity_gap"],
                hq_country_code: "US",
              },
              {
                ticker: "AAPL",
                ric: "AAPL.OQ",
                display_name: "Apple Inc.",
                fit_status: "ok",
                warnings: [],
                hq_country_code: "US",
              },
            ]
          : [];
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
          universe_count: 1240,
          fit_ok_count: 1180,
          fit_limited_count: 48,
          fit_insufficient_count: 12,
          query,
          limit: 12,
          total: rows.length,
          results: rows,
        });
      }

      if (method === "GET" && pathName === "/api/cpar/ticker/AAPL") {
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
          universe_count: 1240,
          fit_ok_count: 1180,
          fit_limited_count: 48,
          fit_insufficient_count: 12,
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
          raw_loadings: [
            { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 1.12 },
            { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.34 },
            { factor_id: "QUAL", label: "Quality", group: "style", display_order: 32, beta: 0.11 },
          ],
          thresholded_loadings: [
            { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 1.12 },
            { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.34 },
          ],
          pre_hedge_factor_variance_proxy: 0.24,
          pre_hedge_factor_volatility_proxy: 0.49,
        });
      }

      if (method === "GET" && pathName === "/api/cpar/ticker/AAPL/hedge") {
        const mode = requestUrl.searchParams.get("mode") || "factor_neutral";
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
          universe_count: 1240,
          fit_ok_count: 1180,
          fit_limited_count: 48,
          fit_insufficient_count: 12,
          ticker: "AAPL",
          ric: "AAPL.OQ",
          display_name: "Apple Inc.",
          fit_status: "ok",
          warnings: [],
          mode,
          hedge_status: "hedge_ok",
          hedge_reason: mode === "market_neutral" ? "SPY-only hedge" : "Thresholded raw ETF hedge",
          hedge_legs: mode === "market_neutral"
            ? [{ factor_id: "SPY", label: "Market", group: "market", display_order: 0, weight: -1.12 }]
            : [
                { factor_id: "SPY", label: "Market", group: "market", display_order: 0, weight: -1.12 },
                { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, weight: -0.34 },
              ],
          post_hedge_exposures: [
            { factor_id: "SPY", label: "Market", group: "market", display_order: 0, pre_beta: 1.12, hedge_leg: -1.12, post_beta: 0.0 },
            { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, pre_beta: 0.34, hedge_leg: mode === "market_neutral" ? 0 : -0.34, post_beta: mode === "market_neutral" ? 0.34 : 0.0 },
          ],
          pre_hedge_factor_variance_proxy: 0.24,
          post_hedge_factor_variance_proxy: mode === "market_neutral" ? 0.07 : 0.02,
          gross_hedge_notional: mode === "market_neutral" ? 1.12 : 1.46,
          net_hedge_notional: -1.12,
          non_market_reduction_ratio: mode === "market_neutral" ? 0.0 : 0.86,
          stability: {
            leg_overlap_ratio: 1.0,
            gross_hedge_notional_change: 0.03,
            net_hedge_notional_change: 0.02,
          },
        });
      }

      return fulfillJson({ error: `Unhandled API route ${pathName}` }, 500);
    });

    await gotoWithRetry(page, `${BASE_URL}/cpar/hedge?ticker=AAPL`, { waitUntil: "domcontentloaded" });
    await page.getByTestId("cpar-hedge-subject-panel").getByText("Ticker is ambiguous.").waitFor();
    await page.getByTestId("cpar-hedge-subject-panel").getByText("Choose a specific RIC from the search results on the left.").waitFor();
    assert.equal(await page.getByTestId("cpar-hedge-panel").count(), 0);

    await gotoWithRetry(page, `${BASE_URL}/cpar/hedge?ric=AAPL.NA`, { waitUntil: "domcontentloaded" });
    await page.getByTestId("cpar-hedge-subject-panel").getByText("RIC result cannot open hedge directly.").waitFor();
    assert.equal(await page.getByTestId("cpar-hedge-panel").count(), 0);

    await page.getByTestId("cpar-search-input").fill("AAPL");
    const searchResults = page.getByTestId("cpar-search-results");
    await searchResults.waitFor();
    assert.equal(await searchResults.locator("button").first().isDisabled(), true);
    await searchResults.locator("button").nth(1).click();
    await page.waitForURL(/\/cpar\/hedge\?ticker=AAPL&ric=AAPL\.OQ/);
    await page.getByTestId("cpar-hedge-subject-panel").getByText("Apple Inc.").waitFor();
    await page.getByTestId("cpar-hedge-panel").waitFor();
    await page.getByTestId("cpar-post-hedge-table").waitFor();
    await page.getByRole("link", { name: "Review Loadings In /cpar/explore" }).waitFor();
    assert.equal(await page.getByRole("button", { name: "SYNC" }).count(), 0);
    assert.equal(await page.getByRole("button", { name: "RECALC" }).count(), 0);

    await page.getByRole("button", { name: "Market Neutral" }).click();
    await page.getByText("SPY-only hedge").waitFor();

    if (capturedPageError) {
      throw capturedPageError;
    }

    assert.equal(page.url().includes("/cpar/hedge"), true);
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
