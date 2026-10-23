import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { chromium } from "playwright";
import { smokeEnv, authenticateSmokePage, handleSmokeAuth } from "./smoke_auth.mjs";

const port = Number(process.env.CEIORA_SMOKE_PORT ?? 3118);
const origin = `http://127.0.0.1:${port}`;
const server = spawn(process.execPath, ["node_modules/next/dist/bin/next", "start", "-H", "127.0.0.1", "-p", String(port)], {
  env: { ...process.env, ...smokeEnv, NEXT_TELEMETRY_DISABLED: "1" }, stdio: "ignore",
});
const browser = await chromium.launch({ headless: true });
let release;
const waitForB = new Promise(resolve => { release = resolve; });
let user = "smoke";
const unknown = [];
try {
  for (let i = 0; i < 100; i++) {
    try { if ((await fetch(`${origin}/login`)).ok) break; } catch {}
    await delay(100);
  }
  const page = await browser.newPage();
  await authenticateSmokePage(page, origin);
  await page.route("**/api/**", async route => {
    if (await handleSmokeAuth(route, { user })) return;
    const path = new URL(route.request().url()).pathname;
    const owner = user;
    if (owner === "user-b") await waitForB;
    let json;
    if (path === "/api/cuse/risk-page") json = { portfolio: { positions: [] }, risk: { factor_catalog: [] }, exposures: { raw: { factors: [] } } };
    else if (path === "/api/holdings/positions") json = { positions: [{ account_id: "acct_main", ric: "TEST.N", ticker: owner === "smoke" ? "PRIVATE_A" : "PRIVATE_B", quantity: 1, source: "synthetic" }] };
    else { unknown.push(path); return route.fulfill({ status: 500, json: { detail: "Unconfigured fixture" } }); }
    await route.fulfill({ json });
  });
  await page.goto(`${origin}/dash`);
  await page.getByRole("cell", { name: "PRIVATE_A", exact: true }).waitFor();
  let navigations = 0;
  page.on("framenavigated", frame => { if (frame === page.mainFrame()) navigations++; });
  user = "user-b";
  await authenticateSmokePage(page, origin, user);
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await page.waitForEvent("framenavigated", { predicate: frame => frame === page.mainFrame() });
  await page.getByTestId("dash-workspace").waitFor();
  assert(!(await page.locator("body").innerText()).includes("PRIVATE_A"), "Previous login's cached holdings rendered before B's response");
  assert(await page.evaluate(() => performance.getEntriesByType("navigation")[0].type === "reload"), "Principal change did not reset the document/cache");
  release();
  await page.getByRole("cell", { name: "PRIVATE_B", exact: true }).waitFor();
  assert.equal(await page.getByRole("cell", { name: "PRIVATE_A", exact: true }).count(), 0);
  assert(navigations >= 1);
  const settledNavigations = navigations;
  await page.waitForResponse(response => new URL(response.url()).pathname === "/api/auth/session");
  await delay(100);
  assert.equal(navigations, settledNavigations, "Same-principal background refresh caused another navigation");
  assert.deepEqual(unknown, []);
  console.log("Principal cache isolation passed: signed A-to-B cookie switch, document reset, no cached A holdings while B requests pending");
} finally {
  release(); await browser.close(); server.kill("SIGTERM");
}
