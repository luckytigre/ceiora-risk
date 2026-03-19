import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { chromium } from "playwright";

const HOST = "127.0.0.1";
const PORT = 3108;
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
      const response = await page.goto(url, options);
      if (response && !response.ok()) {
        lastError = new Error(`Unexpected status ${response.status()} for ${url}`);
        if (index === attempts - 1) {
          throw lastError;
        }
        await delay(500);
        continue;
      }
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
  await waitForServer(`${BASE_URL}/cpar/portfolio?account_id=acct_main`);

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
            {
              account_id: "acct_empty",
              account_name: "Empty Account",
              is_active: true,
              positions_count: 0,
              gross_quantity: 0,
              last_position_updated_at: null,
            },
            {
              account_id: "acct_unavailable",
              account_name: "Unavailable Account",
              is_active: true,
              positions_count: 2,
              gross_quantity: 12,
              last_position_updated_at: "2026-03-18T15:00:00Z",
            },
          ],
        });
      }

      if (method === "GET" && pathName === "/api/cpar/portfolio/hedge") {
        const accountId = requestUrl.searchParams.get("account_id") || "";
        const mode = requestUrl.searchParams.get("mode") || "factor_neutral";
        if (accountId === "acct_empty") {
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
            account_id: "acct_empty",
            account_name: "Empty Account",
            mode,
            positions_count: 0,
            covered_positions_count: 0,
            excluded_positions_count: 0,
            gross_market_value: 0,
            net_market_value: 0,
            covered_gross_market_value: 0,
            coverage_ratio: null,
            portfolio_status: "empty",
            portfolio_reason: "No live holdings positions are loaded for this account.",
            aggregate_thresholded_loadings: [],
            hedge_status: null,
            hedge_reason: null,
            hedge_legs: [],
            post_hedge_exposures: [],
            pre_hedge_factor_variance_proxy: null,
            post_hedge_factor_variance_proxy: null,
            gross_hedge_notional: null,
            net_hedge_notional: null,
            non_market_reduction_ratio: null,
            positions: [],
          });
        }
        if (accountId === "acct_unavailable") {
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
            account_id: "acct_unavailable",
            account_name: "Unavailable Account",
            mode,
            positions_count: 2,
            covered_positions_count: 0,
            excluded_positions_count: 2,
            gross_market_value: 2010,
            net_market_value: 2010,
            covered_gross_market_value: 0,
            coverage_ratio: 0,
            portfolio_status: "unavailable",
            portfolio_reason: "No holdings rows in this account have both price coverage and a usable persisted cPAR fit in the active package.",
            aggregate_thresholded_loadings: [],
            hedge_status: null,
            hedge_reason: null,
            hedge_legs: [],
            post_hedge_exposures: [],
            pre_hedge_factor_variance_proxy: null,
            post_hedge_factor_variance_proxy: null,
            gross_hedge_notional: null,
            net_hedge_notional: null,
            non_market_reduction_ratio: null,
            positions: [
              {
                account_id: "acct_unavailable",
                ric: "AAPL.OQ",
                ticker: "AAPL",
                display_name: "Apple Inc.",
                quantity: 10,
                price: 201,
                price_date: "2026-03-14",
                price_field_used: "adj_close",
                market_value: 2010,
                portfolio_weight: null,
                fit_status: "insufficient_history",
                warnings: [],
                beta_spy_trade: 1.12,
                coverage: "insufficient_history",
                coverage_reason: "The persisted cPAR fit status is `insufficient_history`, so this position is excluded from hedge aggregation.",
              },
              {
                account_id: "acct_unavailable",
                ric: "MISS.OQ",
                ticker: "MISS",
                display_name: null,
                quantity: 2,
                price: null,
                price_date: null,
                price_field_used: null,
                market_value: null,
                portfolio_weight: null,
                fit_status: null,
                warnings: [],
                beta_spy_trade: null,
                coverage: "missing_price",
                coverage_reason: "No latest price on or before the active cPAR package date.",
              },
            ],
          });
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
          account_id: "acct_main",
          account_name: "Main Account",
          mode,
          positions_count: 3,
          covered_positions_count: 2,
          excluded_positions_count: 1,
          gross_market_value: 2515.0,
          net_market_value: 2415.0,
          covered_gross_market_value: 2415.0,
          coverage_ratio: 0.96,
          portfolio_status: "partial",
          portfolio_reason: "Some holdings rows were excluded because they lack price coverage or a usable persisted cPAR fit.",
          aggregate_thresholded_loadings: [
            { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 1.04 },
            { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.28 },
          ],
          hedge_status: "hedge_ok",
          hedge_reason: mode === "market_neutral" ? "SPY-only hedge" : "Thresholded raw ETF hedge",
          hedge_legs: mode === "market_neutral"
            ? [{ factor_id: "SPY", label: "Market", group: "market", display_order: 0, weight: -1.04 }]
            : [
                { factor_id: "SPY", label: "Market", group: "market", display_order: 0, weight: -1.04 },
                { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, weight: -0.28 },
              ],
          post_hedge_exposures: [
            { factor_id: "SPY", label: "Market", group: "market", display_order: 0, pre_beta: 1.04, hedge_leg: -1.04, post_beta: 0.0 },
            { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, pre_beta: 0.28, hedge_leg: mode === "market_neutral" ? 0 : -0.28, post_beta: mode === "market_neutral" ? 0.28 : 0.0 },
          ],
          pre_hedge_factor_variance_proxy: 0.19,
          post_hedge_factor_variance_proxy: mode === "market_neutral" ? 0.08 : 0.02,
          gross_hedge_notional: mode === "market_neutral" ? 1.04 : 1.32,
          net_hedge_notional: -1.04,
          non_market_reduction_ratio: mode === "market_neutral" ? 0.0 : 0.81,
          positions: [
            {
              account_id: "acct_main",
              ric: "AAPL.OQ",
              ticker: "AAPL",
              display_name: "Apple Inc.",
              quantity: 10,
              price: 201,
              price_date: "2026-03-14",
              price_field_used: "adj_close",
              market_value: 2010,
              portfolio_weight: 0.8323,
              fit_status: "ok",
              warnings: [],
              beta_spy_trade: 1.12,
              coverage: "covered",
              coverage_reason: null,
            },
            {
              account_id: "acct_main",
              ric: "MSFT.OQ",
              ticker: "MSFT",
              display_name: "Microsoft Corp",
              quantity: 4,
              price: 101.25,
              price_date: "2026-03-14",
              price_field_used: "adj_close",
              market_value: 405,
              portfolio_weight: 0.1677,
              fit_status: "limited_history",
              warnings: ["continuity_gap"],
              beta_spy_trade: 0.92,
              coverage: "covered",
              coverage_reason: null,
            },
            {
              account_id: "acct_main",
              ric: "UNPRICED.OQ",
              ticker: "UNPRICED",
              display_name: null,
              quantity: 2,
              price: null,
              price_date: null,
              price_field_used: null,
              market_value: null,
              portfolio_weight: null,
              fit_status: null,
              warnings: [],
              beta_spy_trade: null,
              coverage: "missing_price",
              coverage_reason: "No latest price on or before the active cPAR package date.",
            },
          ],
        });
      }

      return fulfillJson({ error: `Unhandled API route ${pathName}` }, 500);
    });

    await gotoWithRetry(page, `${BASE_URL}/cpar/portfolio?account_id=acct_main`, { waitUntil: "domcontentloaded" });
    await page.getByTestId("cpar-package-banner").waitFor();
    await page.getByTestId("cpar-portfolio-account-panel").waitFor();
    await page.getByTestId("cpar-portfolio-overview").waitFor();
    await page.getByTestId("cpar-portfolio-hedge-panel").waitFor();
    await page.getByTestId("cpar-portfolio-coverage").waitFor();
    await page.getByText("Partial Coverage").waitFor();
    await page.getByText("Aggregate Thresholded Loadings").waitFor();
    assert.equal(await page.getByRole("button", { name: "SYNC" }).count(), 0);
    assert.equal(await page.getByRole("button", { name: "RECALC" }).count(), 0);

    await page.getByRole("button", { name: "Market Neutral" }).click();
    await page.getByText("SPY-only hedge").waitFor();

    await page.selectOption('[data-testid="cpar-portfolio-account-select"]', "acct_empty");
    await page.getByTestId("cpar-portfolio-overview").getByText("Empty Account").waitFor();
    await page.getByText("No live holdings positions are loaded for this account.").waitFor();
    assert.equal(await page.getByTestId("cpar-portfolio-hedge-panel").count(), 0);

    await page.selectOption('[data-testid="cpar-portfolio-account-select"]', "acct_unavailable");
    await page.getByTestId("cpar-portfolio-overview").getByText("Coverage Unavailable").waitFor();
    await page.getByText("No holdings rows in this account have both price coverage and a usable persisted cPAR fit in the active package.").waitFor();
    assert.equal(await page.getByTestId("cpar-portfolio-hedge-panel").count(), 0);

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
