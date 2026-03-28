import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { chromium } from "playwright";

const HOST = "127.0.0.1";
const PORT = 3115;
const BASE_URL = `http://${HOST}:${PORT}`;
const __filename = fileURLToPath(import.meta.url);
const FRONTEND_ROOT = path.resolve(path.dirname(__filename), "..");
const NEXT_BIN = path.resolve(FRONTEND_ROOT, "node_modules", ".bin", process.platform === "win32" ? "next.cmd" : "next");

const serverStdout = [];
const serverStderr = [];
let capturedPageError = null;
let debugPage = null;

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

function portfolioPayload() {
  return {
    positions: [
      {
        ticker: "AAPL",
        shares: 100,
        price: 192.5,
        market_value: 19250,
        exposure_origin: "projected_returns",
        model_status: "ok",
      },
      {
        ticker: "MSFT",
        shares: 55,
        price: 410,
        market_value: 22550,
        exposure_origin: "projected_fundamental",
        model_status: "projected_only",
      },
    ],
    total_value: 41800,
    position_count: 2,
    risk_shares: { market: 0.62, sector: 0.21, style: 0.09, specific: 0.08, idio: 0.08 },
    component_shares: { market: 0.62, sector: 0.21, style: 0.09, specific: 0.08 },
    factor_details: [],
    exposure_modes: { raw: [], sensitivity: [], risk_contribution: [] },
    factor_catalog: [],
    snapshot_id: "snap_positions",
    run_id: "run_positions",
    refresh_started_at: "2026-03-28T14:00:00Z",
    source_dates: {
      prices_asof: "2026-03-27",
      fundamentals_asof: "2026-03-26",
      classification_asof: "2026-03-21",
      exposures_served_asof: "2026-03-27",
      exposures_latest_available_asof: "2026-03-27",
    },
  };
}

function riskPayload() {
  return {
    factors: [],
    risk_shares: { market: 0.62, sector: 0.21, style: 0.09, specific: 0.08, idio: 0.08 },
    total_variance_proxy: 0.24,
    snapshot_id: "snap_positions",
    run_id: "run_positions",
    refresh_started_at: "2026-03-28T14:00:00Z",
    source_dates: {
      prices_asof: "2026-03-27",
      fundamentals_asof: "2026-03-26",
      classification_asof: "2026-03-21",
      exposures_served_asof: "2026-03-27",
      exposures_latest_available_asof: "2026-03-27",
    },
    model_sanity: {
      served_loadings_asof: "2026-03-27",
      coverage_date: "2026-03-27",
      latest_loadings_available_asof: "2026-03-27",
      latest_available_date: "2026-03-27",
      update_available: false,
    },
    risk_engine: {
      core_state_through_date: "2026-03-21",
      factor_returns_latest_date: "2026-03-21",
      core_rebuild_date: "2026-03-21",
      last_recompute_date: "2026-03-28",
    },
  };
}

function cparRiskPayload() {
  return {
    package_run_id: "run_curr",
    package_date: "2026-03-21",
    positions: [
      {
        account_id: "acct_main",
        ric: "AAPL.OQ",
        ticker: "AAPL",
        display_name: "Apple Inc.",
        coverage: "covered",
        fit_status: "ok",
        warnings: [],
      },
      {
        account_id: "acct_growth",
        ric: "MSFT.OQ",
        ticker: "MSFT",
        display_name: "Microsoft Corp",
        coverage: "covered",
        fit_status: "limited_history",
        warnings: ["continuity_gap"],
      },
    ],
  };
}

function holdingsModesPayload() {
  return {
    modes: ["replace_account", "upsert_absolute", "increment_delta"],
    default: "upsert_absolute",
  };
}

function holdingsAccountsPayload() {
  return {
    accounts: [
      {
        account_id: "acct_main",
        account_name: "Main",
        is_active: true,
        positions_count: 1,
        gross_quantity: 100,
        last_position_updated_at: "2026-03-28T14:00:00Z",
      },
      {
        account_id: "acct_growth",
        account_name: "Growth",
        is_active: true,
        positions_count: 1,
        gross_quantity: 55,
        last_position_updated_at: "2026-03-28T14:00:00Z",
      },
    ],
  };
}

function holdingsPositionsPayload() {
  return {
    account_id: null,
    count: 2,
    positions: [
      {
        account_id: "acct_main",
        ric: "AAPL.OQ",
        ticker: "AAPL",
        quantity: 100,
        source: "seed",
        updated_at: "2026-03-28T14:00:00Z",
      },
      {
        account_id: "acct_growth",
        ric: "MSFT.OQ",
        ticker: "MSFT",
        quantity: 55,
        source: "seed",
        updated_at: "2026-03-28T14:00:00Z",
      },
    ],
  };
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
  await waitForServer(`${BASE_URL}/positions`);

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
          refresh: { status: "idle", finished_at: "2026-03-28T14:05:00Z" },
          holdings_sync: { pending: false, pending_count: 0, dirty_since: null },
          neon_sync_health: { status: "ok", mirror_status: "ok", parity_status: "ok" },
          lanes: [],
          runtime: { allowed_profiles: ["serve-refresh"] },
        });
      }
      if (method === "GET" && pathName === "/api/portfolio") return fulfillJson(portfolioPayload());
      if (method === "GET" && pathName === "/api/risk") return fulfillJson(riskPayload());
      if (method === "GET" && pathName === "/api/cpar/risk") return fulfillJson(cparRiskPayload());
      if (method === "GET" && pathName === "/api/holdings/modes") return fulfillJson(holdingsModesPayload());
      if (method === "GET" && pathName === "/api/holdings/accounts") return fulfillJson(holdingsAccountsPayload());
      if (method === "GET" && pathName === "/api/holdings/positions") return fulfillJson(holdingsPositionsPayload());
      if (method === "GET" && pathName === "/api/universe/search") {
        return fulfillJson({
          results: [
            { ticker: "AAPL", ric: "AAPL.OQ", display_name: "Apple Inc." },
            { ticker: "MSFT", ric: "MSFT.OQ", display_name: "Microsoft Corp" },
          ],
        });
      }

      return fulfillJson({ error: `Unhandled API route ${pathName}` }, 500);
    });

    await gotoWithRetry(page, `${BASE_URL}/positions`);
    await page.getByRole("heading", { name: "Holdings Manager" }).waitFor();
    await page.getByRole("heading", { name: /Portfolio Holdings \[2\]/ }).waitFor();
    await page.getByRole("heading", { name: "Modeled Snapshot" }).waitFor();
    await page.getByText("Live Neon-backed holdings across all accounts.").waitFor();
    await page.getByRole("button", { name: "SYNC" }).waitFor();

    const positionsTabs = await page.locator(".dash-tabs-center .dash-tab-btn").allTextContents();
    assert.deepEqual(positionsTabs, ["Positions"]);

    await page.getByRole("columnheader", { name: /cUSE Method/ }).waitFor();
    await page.getByRole("columnheader", { name: /cPAR Method/ }).waitFor();
    await page.getByText("Returns Projection").waitFor();
    await page.getByText("Package Fit").waitFor();
    await page.getByText("Package Fit (Limited)").waitFor();
    await page.getByText("Loadings = 2026-03-27").waitFor();

    if (capturedPageError) throw capturedPageError;
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
