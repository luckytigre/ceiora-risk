import assert from "node:assert/strict";
import { chromium } from "playwright";
const origin = process.env.THESIS_TEST_ORIGIN ?? "http://localhost:3102";
const username = process.env.DASH_TEST_USERNAME;
const password = process.env.DASH_TEST_PASSWORD;
if (!username || !password) throw new Error("Supply an isolated browser-test login");
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1500, height: 950 } });
const errors = [];
page.on("pageerror", error => errors.push(error.message));
async function state() {
  const response = await page.request.get(`${origin}/api/portfolio/theses`);
  assert.equal(response.status(), 200);
  return response.json();
}
async function createThesis(name) {
  await page.getByRole("button", { name: "+ New thesis", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Create thesis", exact: true });
  await dialog.getByLabel("Thesis name").fill(name);
  await dialog.getByRole("button", { name: "Create", exact: true }).click();
  await dialog.waitFor({ state: "hidden" });
  await page.getByText(name, { exact: true }).first().waitFor();
}
try {
  await page.goto(`${origin}/login`);
  await page.getByRole("textbox", { name: "Username" }).fill(username);
  await page.getByRole("textbox", { name: "Password" }).fill(password);
  await page.getByRole("button", { name: "Sign in" }).click();
  await page.waitForURL("**/home");
  await page.goto(`${origin}/dash?model=cuse`);
  await createThesis("UI Persistence");
  const row = page.locator("tr.thesis-inline-draggable").filter({ hasText: "MU" }).first();
  await row.getByRole("button", { name: "+ Thesis", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Allocate MU by thesis", exact: true });
  await dialog.locator(".thesis-allocation-row").filter({ hasText: "UI Persistence" }).locator("input").fill("3.125");
  await dialog.getByRole("button", { name: "Save Allocation", exact: true }).click();
  await dialog.waitFor({ state: "hidden" });
  let saved = await state();
  assert.equal(saved.allocations.find(row => row.ticker === "MU").quantity, "3.125000");
  await page.reload();
  await page.getByText("UI Persistence", { exact: true }).first().waitFor();
  assert.equal((await state()).allocations.find(row => row.ticker === "MU").quantity, "3.125000");
  await page.getByRole("button", { name: "cPAR", exact: true }).click();
  await page.getByText("UI Persistence", { exact: true }).first().waitFor();
  assert.equal((await state()).allocations.find(row => row.ticker === "MU").quantity, "3.125000");

  // Start an old focus GET, save a newer mutation, then release the old response.
  const old = await state();
  let releaseOld;
  const gate = new Promise(resolve => { releaseOld = resolve; });
  let intercepted;
  const began = new Promise(resolve => { intercepted = resolve; });
  await page.route("**/api/portfolio/theses", async route => {
    if (route.request().method() !== "GET") return route.continue();
    intercepted(); await gate;
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(old) });
  });
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await began;
  await createThesis("Newer Save Survives");
  releaseOld();
  await page.waitForTimeout(150);
  assert.equal(await page.getByText("Newer Save Survives", { exact: true }).count(), 1);
  await page.unroute("**/api/portfolio/theses");

  // Concrete shared holdings can carry signed short allocations.
  const shortRow = page.locator("tr.thesis-inline-draggable").filter({ hasText: "SPY" }).first();
  await shortRow.getByRole("button", { name: "+ Thesis", exact: true }).click();
  const shortDialog = page.getByRole("dialog", { name: "Allocate SPY by thesis", exact: true });
  await shortDialog.locator(".thesis-allocation-row").filter({ hasText: "UI Persistence" }).locator("input").fill("5");
  await shortDialog.getByRole("button", { name: "Save Allocation", exact: true }).click();
  await shortDialog.waitFor({ state: "hidden" });
  saved = await state();
  assert.equal(saved.allocations.find(row => row.ticker === "SPY").quantity, "-5.000000");

  // A stale second client gets a 409; browser state is durable across sessions.
  const stale = saved.revision;
  await createThesis("Revision Advances");
  const conflict = await page.request.post(`${origin}/api/portfolio/theses`, { data: { name: "Stale Client", expected_revision: stale } });
  assert.equal(conflict.status(), 409);
  const unknown = await page.request.put(`${origin}/api/portfolio/not-a-route`, { data: {} });
  assert.equal(unknown.status(), 404);
  const wrongMethod = await page.request.put(`${origin}/api/portfolio/theses`, { data: {} });
  assert.equal(wrongMethod.status(), 405);
  await page.goto(`${origin}/positions`);
  await page.getByRole("button", { name: "By thesis", exact: true }).click();
  await page.getByText("UI Persistence", { exact: true }).first().waitFor();
  await page.screenshot({ path: "/tmp/ceiora-theses-persistence.png" });
  for (const [family, path] of [["cuse", "/cuse/exposures"], ["cpar", "/cpar/risk"]]) {
    const endpoint = family === "cpar" ? "**/api/cpar/risk" : "**/api/cuse/risk-page";
    await page.route(endpoint, async route => {
      const response = await route.fetch(); const body = await response.json();
      if (family === "cpar") for (const row of body.positions) row.account_id = "all_accounts";
      else for (const row of body.portfolio.positions) { row.account = "MULTI"; delete row.ric; }
      await route.fulfill({ response, json: body });
    });
    await page.goto(`${origin}${path}`);
    await page.getByRole("button", { name: "By thesis", exact: true }).click();
    await page.getByText("UI Persistence", { exact: true }).first().waitFor();
    const identities = await page.locator("tr.thesis-inline-draggable").evaluateAll(rows => rows.map(row => row.dataset.legKey));
    assert(identities.length > 0 && identities.every(key => key.startsWith("acct_") && !key.includes("MULTI")), "Aggregate risk rows entered mutable thesis workspace");
    assert.equal(await page.locator(`#${family}-risk-ledger-thesis-controls`).count(), 1);
    await page.getByRole("button", { name: "By instrument", exact: true }).click();
    assert.equal(await page.locator("tr.thesis-inline-draggable").count(), 0);
    await page.unroute(endpoint);
  }
  const previous = await state();
  await page.route("**/api/portfolio/theses", route => route.request().method() === "GET"
    ? route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ...previous, workspace_id: "different-workspace" }) }) : route.continue());
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await page.getByText("The thesis workspace changed. Reload this page before editing.", { exact: false }).waitFor();
  assert.equal(await page.getByRole("button", { name: "By thesis", exact: true }).count(), 0);
  await page.unroute("**/api/portfolio/theses");
  await page.reload();
  await page.getByRole("button", { name: "By thesis", exact: true }).waitFor();
  await page.request.post(`${origin}/api/auth/logout`);
  await page.goto(`${origin}/login`);
  assert.equal(await page.locator(".thesis-chip").count(), 0);
  assert.deepEqual(errors, []);
  console.log("Real PostgreSQL browser flow passed: create, decimal/short allocation, reload, model switch, stale refresh suppression, stale writer 409, bounded proxy, real account ledgers on aggregate Risk pages, workspace-mismatch rejection and logout cleanup");
} catch (error) { await page.screenshot({ path: "/tmp/ceiora-thesis-failure.png" }); console.log("Thesis feedback:", await page.locator(".thesis-save-feedback").allTextContents()); throw error; } finally { await browser.close(); }
