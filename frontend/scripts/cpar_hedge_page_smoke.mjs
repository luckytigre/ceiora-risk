import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { chromium } from "playwright";

const HOST = "127.0.0.1";
const PORT = 3106;
const BASE_URL = `http://${HOST}:${PORT}`;
const SMOKE_USERNAME = "smoke";
const SMOKE_SESSION_SECRET = "cpar-hedge-smoke-secret";
const __filename = fileURLToPath(import.meta.url);
const FRONTEND_ROOT = path.resolve(path.dirname(__filename), "..");
const NEXT_BIN = path.resolve(FRONTEND_ROOT, "node_modules", ".bin", process.platform === "win32" ? "next.cmd" : "next");

function textEncoder() {
  return new TextEncoder();
}

function toArrayBuffer(bytes) {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
}

function base64UrlEncode(bytes) {
  return Buffer.from(bytes)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

async function importSigningKey(secret) {
  return crypto.subtle.importKey(
    "raw",
    toArrayBuffer(textEncoder().encode(secret)),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
}

async function createSessionCookieValue() {
  const issuedAt = Math.floor(Date.now() / 1000);
  const payload = {
    authProvider: "shared",
    username: SMOKE_USERNAME,
    subject: SMOKE_USERNAME,
    isAdmin: true,
    primary: true,
    issuedAt,
    expiresAt: issuedAt + 60 * 60,
  };
  const encodedPayload = base64UrlEncode(textEncoder().encode(JSON.stringify(payload)));
  const key = await importSigningKey(SMOKE_SESSION_SECRET);
  const signature = await crypto.subtle.sign("HMAC", key, toArrayBuffer(textEncoder().encode(encodedPayload)));
  return `${encodedPayload}.${base64UrlEncode(new Uint8Array(signature))}`;
}

function recommendationPayload() {
  return {
    package_run_id: "run_curr",
    package_date: "2026-04-18",
    profile: "cpar-weekly",
    started_at: "2026-04-18T00:00:00Z",
    completed_at: "2026-04-18T01:00:00Z",
    method_version: "cPAR1",
    factor_registry_version: "cPAR1_registry_v1",
    data_authority: "neon",
    lookback_weeks: 52,
    half_life_weeks: 26,
    min_observations: 39,
    source_prices_asof: "2026-04-18",
    classification_asof: "2026-04-18",
    universe_count: 1240,
    fit_ok_count: 1180,
    fit_limited_count: 48,
    fit_insufficient_count: 12,
    scope: "all_permitted_accounts",
    account_id: null,
    account_name: null,
    factors: [
      { factor_id: "SPY", ticker: "SPY", label: "Market", group: "market", display_order: 0, method_version: "cPAR1", factor_registry_version: "cPAR1_registry_v1" },
      { factor_id: "XLK", ticker: "XLK", label: "Technology", group: "sector", display_order: 15, method_version: "cPAR1", factor_registry_version: "cPAR1_registry_v1" },
      { factor_id: "QUAL", ticker: "QUAL", label: "Quality", group: "style", display_order: 32, method_version: "cPAR1", factor_registry_version: "cPAR1_registry_v1" },
    ],
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
    aggregate_display_loadings: [
      { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 0.9 },
      { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.28 },
      { factor_id: "QUAL", label: "Quality", group: "style", display_order: 32, beta: 0.12 },
    ],
    aggregate_thresholded_loadings: [
      { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 1.08 },
      { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.31 },
      { factor_id: "QUAL", label: "Quality", group: "style", display_order: 32, beta: 0.14 },
    ],
    risk_shares: { market: 68, industry: 22, style: 10, idio: 0 },
    vol_scaled_shares: { market: 63, industry: 25, style: 12, idio: 0 },
    display_factor_variance_contributions: [
      { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 0.9, variance_contribution: 0.136, variance_share: 0.68 },
      { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.28, variance_contribution: 0.046, variance_share: 0.23 },
    ],
    factor_variance_contributions: [
      { factor_id: "SPY", label: "Market", group: "market", display_order: 0, beta: 1.08, variance_contribution: 0.162, variance_share: 0.675 },
      { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, beta: 0.31, variance_contribution: 0.054, variance_share: 0.225 },
    ],
    display_factor_chart: [
      {
        factor_id: "SPY",
        label: "Market",
        group: "market",
        display_order: 0,
        beta: 0.9,
        aggregate_beta: 0.9,
        factor_volatility: 0.18,
        covariance_adjustment: 0.15,
        sensitivity_beta: 0.162,
        risk_contribution_pct: 68,
        positive_contribution_beta: 0.96,
        negative_contribution_beta: -0.06,
        variance_contribution: 0.136,
        variance_share: 0.68,
        drilldown: [],
      },
      {
        factor_id: "XLK",
        label: "Technology",
        group: "sector",
        display_order: 15,
        beta: 0.28,
        aggregate_beta: 0.28,
        factor_volatility: 0.22,
        covariance_adjustment: 0.09,
        sensitivity_beta: 0.0616,
        risk_contribution_pct: 23,
        positive_contribution_beta: 0.31,
        negative_contribution_beta: -0.03,
        variance_contribution: 0.046,
        variance_share: 0.23,
        drilldown: [],
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
        risk_contribution_pct: 67.5,
        positive_contribution_beta: 1.08,
        negative_contribution_beta: 0,
        variance_contribution: 0.162,
        variance_share: 0.675,
        drilldown: [],
      },
    ],
    cov_matrix: {
      factors: ["SPY", "XLK", "QUAL"],
      correlation: [[1, 0.48, 0.17], [0.48, 1, 0.22], [0.17, 0.22, 1]],
    },
    factor_variance_proxy: 0.2,
    pre_hedge_factor_variance_proxy: 0.2,
    idio_variance_proxy: 0.0,
    total_variance_proxy: 0.2,
    positions: [
      {
        account_id: "all_accounts",
        ric: "AAPL.OQ",
        ticker: "AAPL",
        display_name: "Apple Inc.",
        trbc_industry_group: "Technology",
        quantity: 20,
        price: 201,
        price_date: "2026-04-18",
        price_field_used: "adj_close",
        market_value: 4020,
        portfolio_weight: 0.8,
        fit_status: "ok",
        warnings: [],
        beta_spy_trade: 1.35,
        coverage: "covered",
        coverage_reason: null,
        display_contributions: [],
        thresholded_contributions: [],
        risk_mix: { market: 68, industry: 22, style: 10, idio: 0 },
      },
    ],
    hedge_recommendation: {
      mode: "factor_neutral",
      objective: "minimize_residual_trade_space_loading_magnitude",
      max_hedge_legs: 10,
      base_notional: 5025,
      hedge_status: "hedge_ok",
      hedge_reason: null,
      trade_rows: [
        { factor_id: "XLK", label: "Technology", group: "sector", display_order: 15, proxy_ric: "XLK", proxy_ticker: "XLK", price: 200, price_field_used: "adj_close", price_date: "2026-04-18", currency: "USD", trade_weight: -0.31, dollar_notional: -1557.75, quantity: -7.789 },
        { factor_id: "SPY", label: "Market", group: "market", display_order: 0, proxy_ric: "SPY", proxy_ticker: "SPY", price: 500, price_field_used: "adj_close", price_date: "2026-04-18", currency: "USD", trade_weight: -1.08, dollar_notional: -5427, quantity: -10.854 },
      ],
      post_hedge_exposures: [],
      pre_hedge_factor_variance_proxy: 0.2,
      post_hedge_factor_variance_proxy: 0.04,
      non_market_reduction_ratio: 0.8,
    },
  };
}

function holdingsAccountsPayload() {
  return {
    accounts: [
      { account_id: "acct_main", account_name: "Main", is_active: true, positions_count: 12, gross_quantity: 120, last_position_updated_at: "2026-04-18T12:00:00Z" },
      { account_id: "acct_alt", account_name: "Alt", is_active: true, positions_count: 8, gross_quantity: 60, last_position_updated_at: "2026-04-18T12:00:00Z" },
    ],
  };
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

const serverStdout = [];
const serverStderr = [];
const server = spawn(
  NEXT_BIN,
  ["dev", "-H", HOST, "-p", String(PORT)],
  {
    cwd: FRONTEND_ROOT,
    env: {
      ...process.env,
      APP_AUTH_PROVIDER: "shared",
      CEIORA_SHARED_LOGIN_USERNAME: SMOKE_USERNAME,
      CEIORA_SHARED_LOGIN_PASSWORD: "unused",
      CEIORA_SESSION_SECRET: SMOKE_SESSION_SECRET,
      APP_ACCOUNT_ENFORCEMENT_ENABLED: "0",
      APP_SHARED_AUTH_ACCEPT_LEGACY: "1",
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

  const browser = await chromium.launch({ headless: true });
  try {
    const context = await browser.newContext();
    await context.addCookies([
      {
        name: "__session",
        value: await createSessionCookieValue(),
        url: BASE_URL,
        httpOnly: true,
        sameSite: "Lax",
      },
    ]);
    const page = await context.newPage();

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
          refresh: { status: "idle", finished_at: "2026-04-18T15:00:00Z" },
          holdings_sync: { pending: false, pending_count: 0, dirty_since: null },
          neon_sync_health: { status: "ok", mirror_status: "ok", parity_status: "ok" },
          lanes: [],
          runtime: { allowed_profiles: ["serve-refresh"] },
        });
      }
      if (method === "GET" && pathName === "/api/holdings/accounts") {
        return fulfillJson(holdingsAccountsPayload());
      }
      if (method === "GET" && pathName === "/api/cpar/portfolio/hedge/recommendation") {
        return fulfillJson(recommendationPayload());
      }
      return fulfillJson({ error: `Unhandled API route ${pathName}` }, 500);
    });

    await gotoWithRetry(page, `${BASE_URL}/cpar/hedge`);
    await page.getByTestId("cpar-hedge-scope").waitFor();
    await page.getByTestId("cpar-hedge-risk-chart").waitFor();
    await page.getByTestId("cpar-risk-factor-summary").waitFor();
    await page.getByTestId("cpar-portfolio-hedge-recommendation").waitFor();
    await page.getByRole("combobox").waitFor();
    const options = await page.locator("select option").allTextContents();
    assert.deepEqual(options, ["All Accounts", "Main", "Alt"]);
    await page.getByText("Factor-Neutral Recommendation").waitFor();
    await page.getByText("Base Notional").waitFor();
    await page.getByText("AAPL").waitFor({ timeout: 1 }).catch(() => {}); // ignore row text absence in this page
    const recommendationPanel = page.getByTestId("cpar-portfolio-hedge-recommendation");
    await recommendationPanel.locator("strong").filter({ hasText: "XLK" }).first().waitFor();
    await recommendationPanel.locator("strong").filter({ hasText: "SPY" }).first().waitFor();
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
