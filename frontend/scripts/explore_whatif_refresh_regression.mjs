import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { chromium } from "playwright";

const HOST = "127.0.0.1";
const PORT = 3100;
const BASE_URL = `http://${HOST}:${PORT}`;
const __filename = fileURLToPath(import.meta.url);
const FRONTEND_ROOT = path.resolve(path.dirname(__filename), "..");

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

async function waitForCondition(predicate, timeoutMs, message) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (predicate()) return;
    await delay(100);
  }
  throw new Error(message);
}

function tail(lines) {
  return lines.slice(-40).join("");
}

const serverStdout = [];
const serverStderr = [];
let debugPage = null;
let debugCounters = null;
const server = spawn(
  "npx",
  ["next", "dev", "-H", HOST, "-p", String(PORT)],
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
  await waitForServer(`${BASE_URL}/explore`);

  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    debugPage = page;

    const counters = {
      portfolio: 0,
      refreshStatus: 0,
    };
    debugCounters = counters;

    page.on("pageerror", (error) => {
      throw error;
    });

    await page.route("**/api/**", async (route) => {
      const requestUrl = new URL(route.request().url());
      const pathName = requestUrl.pathname;
      const method = route.request().method().toUpperCase();

      const fulfillJson = (payload) => route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(payload),
      });

      if (method === "GET" && pathName === "/api/universe/factors") {
        return fulfillJson({
          factors: ["market"],
          factor_vols: { market: 0.12 },
          factor_catalog: [
            {
              factor_id: "market",
              factor_name: "Market",
              short_label: "Market",
              family: "market",
              block: "core_structural",
            },
          ],
          _cached: true,
        });
      }

      if (method === "GET" && pathName === "/api/portfolio") {
        counters.portfolio += 1;
        return fulfillJson({
          positions: [
            {
              ticker: "AAPL",
              name: "Apple Inc.",
              long_short: "long",
              trbc_economic_sector_short: "Technology",
              trbc_economic_sector_short_abbr: "Tech",
              shares: 5,
              price: 190,
              market_value: 950,
              weight: 0.1,
              account: "main",
              sleeve: "core",
              source: "seed",
              trbc_industry_group: "Hardware",
              exposures: { market: 1.1 },
              risk_contrib_pct: 4.2,
              model_status: "core_estimated",
            },
          ],
          total_value: 9500,
          position_count: 1,
          _cached: true,
        });
      }

      if (method === "GET" && pathName === "/api/universe/search") {
        return fulfillJson({
          query: requestUrl.searchParams.get("q") || "",
          results: [],
          total: 0,
          _cached: true,
        });
      }

      if (method === "GET" && pathName === "/api/universe/ticker/AAPL") {
        return fulfillJson({
          item: {
            ticker: "AAPL",
            name: "Apple Inc.",
            trbc_economic_sector_short: "Technology",
            trbc_economic_sector_short_abbr: "Tech",
            trbc_industry_group: "Hardware",
            market_cap: 2900000000000,
            price: 190,
            exposures: { market: 1.1 },
            sensitivities: { market: 0.132 },
            risk_loading: 1.1,
            specific_var: 0.02,
            specific_vol: 0.14,
            model_status: "core_estimated",
            as_of_date: "2026-03-13",
          },
          _cached: true,
        });
      }

      if (method === "GET" && pathName === "/api/universe/ticker/AAPL/history") {
        return fulfillJson({
          ticker: "AAPL",
          ric: "AAPL.O",
          years: 5,
          points: [
            { date: "2025-03-14", close: 170 },
            { date: "2026-03-13", close: 190 },
          ],
          _cached: true,
        });
      }

      if (method === "GET" && pathName === "/api/holdings/accounts") {
        return fulfillJson({
          accounts: [
            {
              account_id: "main",
              account_name: "Main Account",
              is_active: true,
              positions_count: 1,
              gross_quantity: 5,
              last_position_updated_at: "2026-03-13T16:00:00Z",
            },
          ],
        });
      }

      if (method === "GET" && pathName === "/api/holdings/positions") {
        return fulfillJson({
          positions: [
            {
              account_id: "main",
              ric: "AAPL.O",
              ticker: "AAPL",
              quantity: 5,
              source: "seed",
            },
          ],
        });
      }

      if (method === "POST" && pathName === "/api/portfolio/whatif/apply") {
        return fulfillJson({
          status: "ok",
          accepted_rows: 1,
          rejected_rows: 0,
          rejection_counts: {},
          warnings: [],
          applied_upserts: 1,
          applied_deletes: 0,
          row_results: [
            {
              account_id: "main",
              ticker: "AAPL",
              ric: "AAPL.O",
              current_quantity: 5,
              applied_quantity: 10,
              delta_quantity: 10,
              action: "upsert",
            },
          ],
          rejected: [],
        });
      }

      if (method === "POST" && pathName === "/api/refresh") {
        return fulfillJson({
          status: "accepted",
          message: "Refresh started in background.",
          refresh: {
            status: "running",
            job_id: "job_123",
            pipeline_run_id: null,
            profile: "serve-refresh",
            requested_profile: "serve-refresh",
            mode: "light",
            as_of_date: null,
            resume_run_id: null,
            from_stage: null,
            to_stage: null,
            force_core: false,
            force_risk_recompute: false,
            requested_at: "2026-03-14T14:00:00Z",
            started_at: "2026-03-14T14:00:00Z",
            finished_at: null,
            current_stage: "serving_refresh",
            current_stage_message: "Publishing serving payloads",
            result: null,
            error: null,
          },
        });
      }

      if (method === "GET" && pathName === "/api/refresh/status") {
        counters.refreshStatus += 1;
        await delay(3500);
        return fulfillJson({
          status: "ok",
          refresh: {
            status: "ok",
            job_id: "job_123",
            pipeline_run_id: "run_123",
            profile: "serve-refresh",
            requested_profile: "serve-refresh",
            mode: "light",
            as_of_date: null,
            resume_run_id: null,
            from_stage: null,
            to_stage: null,
            force_core: false,
            force_risk_recompute: false,
            requested_at: "2026-03-14T14:00:00Z",
            started_at: "2026-03-14T14:00:00Z",
            finished_at: "2026-03-14T14:00:03Z",
            current_stage: null,
            current_stage_message: null,
            result: {
              status: "ok",
              run_id: "run_123",
            },
            error: null,
          },
        });
      }

      if (method === "GET" && pathName === "/api/operator/status") {
        return fulfillJson({
          status: "ok",
          generated_at: "2026-03-14T14:00:00Z",
          lanes: [],
          source_dates: {},
          risk_engine: {},
          core_due: { due: false, reason: "not_due" },
          refresh: {
            status: "ok",
            job_id: "job_123",
            pipeline_run_id: "run_123",
            profile: "serve-refresh",
            requested_profile: "serve-refresh",
            mode: "light",
            as_of_date: null,
            resume_run_id: null,
            from_stage: null,
            to_stage: null,
            force_core: false,
            force_risk_recompute: false,
            requested_at: "2026-03-14T14:00:00Z",
            started_at: "2026-03-14T14:00:00Z",
            finished_at: "2026-03-14T14:00:03Z",
            result: null,
            error: null,
          },
        });
      }

      if (method === "GET" && pathName === "/api/risk") {
        return fulfillJson({
          risk_shares: { market: 20, industry: 30, style: 25, idio: 25 },
          component_shares: { market: 20, industry: 30, style: 25 },
          factor_details: [],
          factor_catalog: [
            {
              factor_id: "market",
              factor_name: "Market",
              short_label: "Market",
              family: "market",
              block: "core_structural",
            },
          ],
          cov_matrix: { factors: ["market"], matrix: [[1]] },
          r_squared: 0.82,
          _cached: true,
        });
      }

      if (method === "GET" && pathName === "/api/exposures") {
        return fulfillJson({
          mode: requestUrl.searchParams.get("mode") || "raw",
          factors: [],
          _cached: true,
        });
      }

      throw new Error(`Unhandled API request: ${method} ${pathName}${requestUrl.search}`);
    });

    await page.goto(`${BASE_URL}/explore`, { waitUntil: "domcontentloaded" });
    await page.locator("#whatif-entry-account").waitFor();
    await waitForCondition(
      () => counters.portfolio === 1,
      10000,
      "Explore did not load portfolio on first render",
    );
    assert.equal(counters.portfolio, 1, "Explore should load portfolio once on first render");

    await page.locator("#whatif-entry-ticker").fill("AAPL");
    await page.locator("#whatif-entry-ticker").press("Enter");
    await page.locator("#whatif-entry-qty").fill("10");
    await page.getByRole("button", { name: "Stage" }).click();
    await page.locator(".whatif-builder-pill").waitFor();

    await page.getByRole("button", { name: "Apply" }).click();
    await page.locator(".whatif-builder-feedback").waitFor();
    await page.waitForFunction(() => {
      const text = document.querySelector(".whatif-builder-feedback")?.textContent || "";
      return text.includes("started RECALC");
    });

    await delay(500);
    assert.equal(
      counters.portfolio,
      1,
      "Portfolio should not revalidate before serve-refresh reaches a terminal state",
    );

    await page.waitForFunction(() => {
      const text = document.querySelector(".whatif-builder-feedback")?.textContent || "";
      return text.includes("RECALC finished");
    }, { timeout: 15000 });

    assert.ok(counters.refreshStatus >= 1, "Refresh status should be requested before analytics revalidate");
    assert.equal(
      counters.portfolio,
      2,
      "Portfolio should revalidate once after serve-refresh finishes",
    );

    console.log("Explore what-if refresh regression passed.");
  } finally {
    await browser.close();
  }
} catch (error) {
  console.error("Explore what-if refresh regression failed.");
  console.error(error);
  if (debugCounters) {
    console.error("counters:", debugCounters);
  }
  if (debugPage) {
    try {
      const feedback = await debugPage.locator(".whatif-builder-feedback").textContent();
      console.error("feedback:", feedback);
    } catch {
      console.error("feedback: <unavailable>");
    }
  }
  if (serverStdout.length > 0) {
    console.error("next dev stdout:");
    console.error(tail(serverStdout));
  }
  if (serverStderr.length > 0) {
    console.error("next dev stderr:");
    console.error(tail(serverStderr));
  }
  process.exitCode = 1;
} finally {
  await cleanup();
}
