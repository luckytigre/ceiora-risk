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

function baseMeta() {
  const factors = factorRegistry();
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
    factor_count: factors.length,
    factors,
  };
}

function riskPayload() {
  return {
    ...baseMeta(),
    scope: "all_accounts",
    accounts_count: 3,
    portfolio_status: "ok",
    portfolio_reason: null,
    positions_count: 2,
    covered_positions_count: 2,
    excluded_positions_count: 0,
    gross_market_value: 5025,
    net_market_value: 5025,
    covered_gross_market_value: 5025,
    coverage_ratio: 1,
    coverage_breakdown: {
      covered: { positions_count: 2, gross_market_value: 5025 },
      missing_price: { positions_count: 0, gross_market_value: 0 },
      missing_cpar_fit: { positions_count: 0, gross_market_value: 0 },
      insufficient_history: { positions_count: 0, gross_market_value: 0 },
    },
    aggregate_thresholded_loadings: [
      { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 1.08 },
    ],
    factor_variance_contributions: [
      {
        factor_id: "SPY",
        label: "Market",
        group: "market",
        display_order: 0,
        beta: 1.08,
        variance_contribution: 0.162,
        variance_share: 1.0,
      },
    ],
    factor_chart: [
      {
        factor_id: "SPY",
        label: "Market",
        group: "market",
        display_order: 0,
        beta: 1.08,
        aggregate_beta: 1.08,
        factor_volatility: 0.18,
        covariance_adjustment: 0.15,
        sensitivity_beta: 0.1944,
        risk_contribution_pct: 100,
        positive_contribution_beta: 1.08,
        negative_contribution_beta: 0,
        variance_contribution: 0.162,
        variance_share: 1.0,
        drilldown: [
          {
            ric: "AAPL.OQ",
            ticker: "AAPL",
            display_name: "Apple Inc.",
            market_value: 4020,
            portfolio_weight: 0.8,
            fit_status: "ok",
            warnings: [],
            coverage: "covered",
            coverage_reason: null,
            factor_beta: 1.35,
            contribution_beta: 1.08,
            vol_scaled_loading: 0.243,
            vol_scaled_contribution: 0.1944,
            covariance_adjusted_loading: 0.2025,
            risk_contribution_pct: 80.0,
          },
        ],
      },
    ],
    cov_matrix: {
      factors: ["SPY", "XLK", "QUAL"],
      correlation: [
        [1.0, 0.48, 0.17],
        [0.48, 1.0, 0.22],
        [0.17, 0.22, 1.0],
      ],
    },
    positions: [
      {
        account_id: "all_accounts",
        ric: "AAPL.OQ",
        ticker: "AAPL",
        display_name: "Apple Inc.",
        quantity: 20,
        price: 201,
        price_date: "2026-03-14",
        price_field_used: "adj_close",
        market_value: 4020,
        portfolio_weight: 0.8,
        fit_status: "ok",
        warnings: [],
        beta_spy_trade: 1.35,
        coverage: "covered",
        coverage_reason: null,
        thresholded_contributions: [
          { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 1.08 },
        ],
      },
    ],
  };
}

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
        if (index === attempts - 1) throw lastError;
        await delay(500);
        continue;
      }
      return;
    } catch (error) {
      lastError = error;
      const message = error instanceof Error ? error.message : String(error);
      if (!message.includes("ERR_ABORTED") || index === attempts - 1) throw error;
      await delay(500);
    }
  }
  throw lastError ?? new Error(`Failed to navigate to ${url}`);
}

const serverStdout = [];
const serverStderr = [];
let debugPage = null;
let capturedPageError = null;
let scenario = "ok";
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
  await waitForServer(`${BASE_URL}/cpar/risk`);

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
        return fulfillJson(baseMeta());
      }

      if (method === "GET" && pathName === "/api/cpar/risk") {
        return fulfillJson(riskPayload());
      }

      if (method === "GET" && pathName === "/api/cpar/factors/history") {
        if (scenario === "history_not_ready") {
          return fulfillJson({
            detail: {
              status: "not_ready",
              error: "cpar_not_ready",
              message: "Historical cPAR factor returns are not available yet.",
              build_profile: "cpar-weekly",
            },
          }, 503);
        }
        if (scenario === "history_unavailable") {
          return fulfillJson({
            detail: {
              status: "unavailable",
              error: "cpar_authority_unavailable",
              message: "Neon read failed",
            },
          }, 503);
        }
        return fulfillJson({
          factor_id: "SPY",
          factor_name: "Market",
          years: 5,
          points: [
            { date: "2025-03-14", factor_return: 0.01, cum_return: 0.01 },
            { date: "2025-03-21", factor_return: 0.02, cum_return: 0.0302 },
          ],
          _cached: true,
        });
      }

      return fulfillJson({ error: `Unhandled API route ${pathName}` }, 500);
    });

    await gotoWithRetry(page, `${BASE_URL}/cpar/risk`, { waitUntil: "domcontentloaded" });
    await page.getByText("5Y Historical Return — Market").waitFor();
    await page.getByText("+3.0%").waitFor();

    scenario = "history_not_ready";
    await gotoWithRetry(page, `${BASE_URL}/cpar/risk`, { waitUntil: "domcontentloaded" });
    await page.getByText("Historical cPAR factor returns are not ready for Market yet.").waitFor();

    scenario = "history_unavailable";
    await gotoWithRetry(page, `${BASE_URL}/cpar/risk`, { waitUntil: "domcontentloaded" });
    await page.getByText("5Y factor-return history is temporarily unavailable for Market.").waitFor();

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
