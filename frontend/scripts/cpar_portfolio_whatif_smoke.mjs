import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { chromium } from "playwright";

const HOST = "127.0.0.1";
const PORT = 3109;
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

function portfolioSnapshot({ mode, grossMarketValue, coveredGrossMarketValue, current = true }) {
  return {
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
    positions_count: current ? 2 : 3,
    covered_positions_count: current ? 2 : 3,
    excluded_positions_count: 0,
    gross_market_value: grossMarketValue,
    net_market_value: grossMarketValue,
    covered_gross_market_value: coveredGrossMarketValue,
    coverage_ratio: 1,
    coverage_breakdown: {
      covered: { positions_count: current ? 2 : 3, gross_market_value: coveredGrossMarketValue },
      missing_price: { positions_count: 0, gross_market_value: 0 },
      missing_cpar_fit: { positions_count: 0, gross_market_value: 0 },
      insufficient_history: { positions_count: 0, gross_market_value: 0 },
    },
    portfolio_status: "ok",
    portfolio_reason: null,
    aggregate_thresholded_loadings: current
      ? [
          { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 1.04 },
          { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.28 },
        ]
      : [
          { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 1.16 },
          { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.36 },
        ],
    factor_variance_contributions: current
      ? [
          {
            factor_id: "SPY",
            label: "Market",
            group: "market",
            display_order: 0,
            beta: 1.04,
            variance_contribution: 0.142,
            variance_share: 0.7474,
          },
          {
            factor_id: "XLK",
            label: "Technology",
            group: "sector",
            display_order: 15,
            beta: 0.28,
            variance_contribution: 0.048,
            variance_share: 0.2526,
          },
        ]
      : [
          {
            factor_id: "SPY",
            label: "Market",
            group: "market",
            display_order: 0,
            beta: 1.16,
            variance_contribution: 0.197,
            variance_share: 0.7296,
          },
          {
            factor_id: "XLK",
            label: "Technology",
            group: "sector",
            display_order: 15,
            beta: 0.36,
            variance_contribution: 0.073,
            variance_share: 0.2704,
          },
        ],
    hedge_status: "hedge_ok",
    hedge_reason: mode === "market_neutral"
      ? (current ? "SPY-only current hedge" : "SPY-only hypothetical hedge")
      : (current ? "Thresholded current hedge" : "Thresholded hypothetical hedge"),
    hedge_legs: mode === "market_neutral"
      ? [{ factor_id: "SPY", label: "Market", group: "market", display_order: 0, weight: current ? -1.04 : -1.16 }]
      : [
          { factor_id: "SPY", label: "Market", group: "market", display_order: 0, weight: current ? -1.04 : -1.16 },
          { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, weight: current ? -0.28 : -0.36 },
        ],
    post_hedge_exposures: [
      { factor_id: "SPY", label: "Market", group: "market", display_order: 0, pre_beta: current ? 1.04 : 1.16, hedge_leg: -(current ? 1.04 : 1.16), post_beta: 0.0 },
      { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, pre_beta: current ? 0.28 : 0.36, hedge_leg: mode === "market_neutral" ? 0 : -(current ? 0.28 : 0.36), post_beta: mode === "market_neutral" ? (current ? 0.28 : 0.36) : 0.0 },
    ],
    pre_hedge_factor_variance_proxy: current ? 0.19 : 0.27,
    post_hedge_factor_variance_proxy: mode === "market_neutral" ? (current ? 0.08 : 0.11) : (current ? 0.02 : 0.03),
    gross_hedge_notional: mode === "market_neutral" ? (current ? 1.04 : 1.16) : (current ? 1.32 : 1.52),
    net_hedge_notional: -(current ? 1.04 : 1.16),
    non_market_reduction_ratio: mode === "market_neutral" ? 0.0 : (current ? 0.81 : 0.84),
    positions: current
      ? [
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
            thresholded_contributions: [
              { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 0.93 },
              { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.24 },
            ],
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
            thresholded_contributions: [
              { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 0.11 },
              { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.04 },
            ],
          },
        ]
      : [
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
            portfolio_weight: 0.6052,
            fit_status: "ok",
            warnings: [],
            beta_spy_trade: 1.12,
            coverage: "covered",
            coverage_reason: null,
            thresholded_contributions: [
              { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 0.7 },
              { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.18 },
            ],
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
            portfolio_weight: 0.1220,
            fit_status: "limited_history",
            warnings: ["continuity_gap"],
            beta_spy_trade: 0.92,
            coverage: "covered",
            coverage_reason: null,
            thresholded_contributions: [
              { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 0.12 },
              { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.04 },
            ],
          },
          {
            account_id: "acct_main",
            ric: "NVDA.OQ",
            ticker: "NVDA",
            display_name: "NVIDIA Corp",
            quantity: 6,
            price: 151,
            price_date: "2026-03-14",
            price_field_used: "adj_close",
            market_value: 906,
            portfolio_weight: 0.2728,
            fit_status: "ok",
            warnings: [],
            beta_spy_trade: 1.24,
            coverage: "covered",
            coverage_reason: null,
            thresholded_contributions: [
              { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 0.34 },
              { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.14 },
            ],
          },
        ],
  };
}

const serverStdout = [];
const serverStderr = [];
let debugPage = null;
let capturedPageError = null;
let whatIfRequestBody = null;
let scenario = "baseline";
const server = spawn(
  NEXT_BIN,
  ["dev", "-H", HOST, "-p", String(PORT)],
  {
    cwd: FRONTEND_ROOT,
    env: { ...process.env, NEXT_TELEMETRY_DISABLED: "1" },
    stdio: ["ignore", "pipe", "pipe"],
  },
);

server.stdout?.on("data", (chunk) => serverStdout.push(String(chunk)));
server.stderr?.on("data", (chunk) => serverStderr.push(String(chunk)));

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
  await waitForServer(`${BASE_URL}/cpar/risk?account_id=acct_main`);

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
              positions_count: 2,
              gross_quantity: 14,
              last_position_updated_at: "2026-03-18T15:00:00Z",
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
          universe_count: 1240,
          fit_ok_count: 1180,
          fit_limited_count: 48,
          fit_insufficient_count: 12,
          query: requestUrl.searchParams.get("q") || "",
          limit: 12,
          total: 3,
          results: [
            { ticker: "NVDA", ric: "NVDA.OQ", display_name: "NVIDIA Corp", fit_status: "ok", warnings: [], hq_country_code: "US" },
            { ticker: "AAPL", ric: "AAPL.OQ", display_name: "Apple Inc.", fit_status: "ok", warnings: [], hq_country_code: "US" },
            { ticker: null, ric: "SHELL.L", display_name: "Shell ADR", fit_status: "limited_history", warnings: [], hq_country_code: "GB" },
          ],
        });
      }

      if (method === "GET" && pathName === "/api/cpar/portfolio/hedge") {
        const mode = requestUrl.searchParams.get("mode") || "factor_neutral";
        return fulfillJson(portfolioSnapshot({ mode, grossMarketValue: 2415, coveredGrossMarketValue: 2415, current: true }));
      }

      if (method === "POST" && pathName === "/api/cpar/portfolio/whatif") {
        whatIfRequestBody = JSON.parse(route.request().postData() || "{}");
        const mode = whatIfRequestBody.mode || "factor_neutral";
        if (scenario === "whatif_not_ready") {
          return fulfillJson({
            status: "not_ready",
            error: "cpar_not_ready",
            message: "Incomplete active package",
            build_profile: "cpar-weekly",
          }, 503);
        }
        return fulfillJson({
          package_run_id: scenario === "package_mismatch" ? "run_old" : "run_curr",
          package_date: scenario === "package_mismatch" ? "2026-03-07" : "2026-03-14",
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
          scenario_row_count: 1,
          changed_positions_count: 1,
          scenario_rows: [
            {
              ric: "NVDA.OQ",
              ticker: "NVDA",
              display_name: "NVIDIA Corp",
              quantity_delta: 6,
              current_quantity: 0,
              hypothetical_quantity: 6,
              price: 151,
              price_date: "2026-03-14",
              price_field_used: "adj_close",
              market_value_delta: 906,
              fit_status: "ok",
              warnings: [],
              coverage: "covered",
              coverage_reason: null,
            },
          ],
          current: {
            ...portfolioSnapshot({ mode, grossMarketValue: 2415, coveredGrossMarketValue: 2415, current: true }),
            package_run_id: scenario === "package_mismatch" ? "run_old" : "run_curr",
            package_date: scenario === "package_mismatch" ? "2026-03-07" : "2026-03-14",
          },
          hypothetical: {
            ...portfolioSnapshot({ mode, grossMarketValue: 3321, coveredGrossMarketValue: 3321, current: false }),
            package_run_id: scenario === "package_mismatch" ? "run_old" : "run_curr",
            package_date: scenario === "package_mismatch" ? "2026-03-07" : "2026-03-14",
          },
          _preview_only: true,
        });
      }

      return fulfillJson({ error: `Unhandled API route ${pathName}` }, 500);
    });

    await gotoWithRetry(page, `${BASE_URL}/cpar/risk?account_id=acct_main`, { waitUntil: "domcontentloaded" });
    await page.getByTestId("cpar-portfolio-whatif-builder").waitFor();
    await page.getByTestId("cpar-risk-factor-summary").waitFor();
    await page.getByTestId("cpar-risk-positions").waitFor();
    const riskSummaryBeforeAccountPanel = await page.evaluate(() => {
      const summary = document.querySelector('[data-testid="cpar-risk-factor-summary"]');
      const accountPanel = document.querySelector('[data-testid="cpar-portfolio-account-panel"]');
      if (!(summary instanceof HTMLElement) || !(accountPanel instanceof HTMLElement)) return false;
      return Boolean(summary.compareDocumentPosition(accountPanel) & Node.DOCUMENT_POSITION_FOLLOWING);
    });
    assert.equal(riskSummaryBeforeAccountPanel, true);
    await page.getByTestId("cpar-search-input").fill("SHELL");
    await page.getByRole("button", { name: /Shell ADR/i }).waitFor();
    assert.equal(await page.getByRole("button", { name: /Shell ADR/i }).isDisabled(), true);
    await page.getByText("Ticker required").waitFor();
    await page.getByTestId("cpar-search-input").fill("NVDA");
    await page.getByRole("button", { name: /NVDA/i }).first().click();
    await page.getByTestId("cpar-whatif-quantity-input").fill("6");
    await page.getByTestId("cpar-whatif-add-btn").click();

    await page.getByTestId("cpar-portfolio-whatif-scenarios").waitFor();
    await page.getByTestId("cpar-portfolio-current-hedge-panel").waitFor();
    await page.getByTestId("cpar-portfolio-hypothetical-hedge-panel").waitFor();
    await page.getByTestId("cpar-risk-factor-summary").locator("tbody").getByText("SPY").waitFor();
    await page.getByRole("heading", { name: "Hypothetical Account Hedge" }).waitFor();
    await page.getByTestId("cpar-portfolio-whatif-scenarios").getByText("NVIDIA Corp").waitFor();
    assert.equal(await page.getByRole("button", { name: "SYNC" }).count(), 0);
    assert.equal(await page.getByRole("button", { name: "RECALC" }).count(), 0);

    assert.deepEqual(whatIfRequestBody, {
      account_id: "acct_main",
      mode: "factor_neutral",
      scenario_rows: [{ ric: "NVDA.OQ", ticker: "NVDA", quantity_delta: 6 }],
    });

    await page.getByLabel("NVDA quantity").fill("0");
    await page.getByTestId("cpar-portfolio-whatif-invalid").waitFor();
    assert.equal(await page.getByTestId("cpar-portfolio-current-hedge-panel").count(), 0);
    assert.equal(await page.getByTestId("cpar-portfolio-hypothetical-hedge-panel").count(), 0);
    await page.getByLabel("NVDA quantity").fill("6");
    await page.getByTestId("cpar-portfolio-current-hedge-panel").waitFor();
    await page.getByTestId("cpar-portfolio-hypothetical-hedge-panel").waitFor();

    await page.getByRole("button", { name: "Market Neutral" }).first().click();
    await page.getByText("SPY-only hypothetical hedge").waitFor();

    scenario = "package_mismatch";
    await gotoWithRetry(page, `${BASE_URL}/cpar/risk?account_id=acct_main`, { waitUntil: "domcontentloaded" });
    await page.getByTestId("cpar-portfolio-whatif-builder").waitFor();
    await page.getByTestId("cpar-search-input").fill("NVDA");
    await page.getByRole("button", { name: /NVDA/i }).first().click();
    await page.getByTestId("cpar-whatif-quantity-input").fill("6");
    await page.getByTestId("cpar-whatif-add-btn").click();
    await page.getByTestId("cpar-portfolio-whatif-package-mismatch").waitFor();
    assert.equal(await page.getByTestId("cpar-portfolio-current-hedge-panel").count(), 0);
    assert.equal(await page.getByTestId("cpar-portfolio-hypothetical-hedge-panel").count(), 0);

    scenario = "whatif_not_ready";
    await gotoWithRetry(page, `${BASE_URL}/cpar/risk?account_id=acct_main`, { waitUntil: "domcontentloaded" });
    await page.getByTestId("cpar-portfolio-whatif-builder").waitFor();
    await page.getByTestId("cpar-search-input").fill("NVDA");
    await page.getByRole("button", { name: /NVDA/i }).first().click();
    await page.getByTestId("cpar-whatif-quantity-input").fill("6");
    await page.getByTestId("cpar-whatif-add-btn").click();
    await page.getByTestId("cpar-portfolio-whatif-error").waitFor();
    await page.getByText("What-if package not ready.").waitFor();

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
