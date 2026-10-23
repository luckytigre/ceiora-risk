import assert from "node:assert/strict";
import { chromium } from "playwright";

const origin = process.env.DASH_TEST_ORIGIN ?? "http://localhost:3102";
if (!["localhost", "127.0.0.1"].includes(new URL(origin).hostname)) throw new Error("Use the disposable local preview only");
const username = process.env.DASH_TEST_USERNAME;
const password = process.env.DASH_TEST_PASSWORD;
if (!username || !password) throw new Error("Set isolated test credentials");
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
const errors = [];
page.on("pageerror", error => errors.push(error.message));
let createdId;
const name = `Keyboard ${Date.now()}`;
const state = async () => { const r = await page.request.get(`${origin}/api/portfolio/theses`); assert(r.ok()); return r.json(); };
const mutate = async (path, method, data) => {
  const current = await state();
  const response = await page.request.fetch(`${origin}/api/portfolio/${path}`, { method, data: { ...data, expected_revision: current.revision } });
  assert(response.ok(), `${method} ${path}: ${response.status()}`);
  return response.json();
};
const focusedInside = async dialog => assert(await dialog.evaluate(el => el.contains(document.activeElement)), "Focus left the active dialog");
const wrapTabs = async dialog => {
  const controls = dialog.locator("button:not([disabled]), input:not([disabled]), [tabindex='0']");
  await controls.last().focus(); await page.keyboard.press("Tab"); await focusedInside(dialog);
  assert(await controls.first().evaluate(el => el === document.activeElement), "Tab did not wrap forward");
  await page.keyboard.press("Shift+Tab");
  assert(await controls.last().evaluate(el => el === document.activeElement), "Tab did not wrap backward");
};
try {
  await page.goto(`${origin}/login`);
  await page.getByRole("textbox", { name: "Username" }).fill(username);
  await page.getByRole("textbox", { name: "Password" }).fill(password);
  await page.getByRole("button", { name: "Sign in" }).click();
  await page.waitForURL(url => !url.pathname.startsWith("/login"));
  const holdings = await (await page.request.get(`${origin}/api/holdings/positions`)).json();
  const position = holdings.positions.find(p => p.quantity > 1);
  assert(position);
  const result = await mutate("theses", "POST", { name });
  createdId = result.theses.find(t => t.name === name).id;
  const current = await state();
  const existing = current.allocations.filter(a => a.account_id === position.account_id && a.ric === position.ric).map(a => ({ thesis_id: a.thesis_id, quantity: a.quantity }));
  await mutate("thesis-allocations/position", "PUT", { account_id: position.account_id, ric: position.ric, allocations: [...existing, { thesis_id: createdId, quantity: "0.000001" }] });
  await page.goto(`${origin}/dash?model=cuse`);
  const microRow = page.locator(`tr.thesis-inline-draggable[data-thesis-group-name="${name}"]`);
  await microRow.waitFor();
  assert((await microRow.innerText()).includes("0.000001"), "Micro-share leg rendered as zero");
  assert.equal(await page.locator("tr.thesis-inline-draggable[tabindex='0']").count(), 1, "Split holding created duplicate tab stops");

  await microRow.focus();
  await page.getByRole("button", { name: `Collapse ${name}`, exact: true }).click();
  assert.equal(await page.locator("tr.thesis-inline-draggable[tabindex='0']").count(), 1, "Collapsing active leg removed every tab stop");
  await page.getByRole("button", { name: `Expand ${name}`, exact: true }).click();

  const opener = microRow.locator(".thesis-cell-trigger");
  await opener.click();
  const allocation = page.getByRole("dialog", { name: `Allocate ${position.ticker} by thesis` });
  await allocation.waitFor(); await focusedInside(allocation); await wrapTabs(allocation);
  assert((await allocation.locator(".thesis-allocation-meter").innerText()).includes("0.000001") || existing.length > 0);
  const deleteButton = allocation.getByRole("button", { name: `Delete thesis ${name}`, exact: true });
  await deleteButton.click();
  const confirmation = page.getByRole("dialog", { name: `Delete “${name}”?` });
  await confirmation.waitFor(); await focusedInside(confirmation); await wrapTabs(confirmation);
  await page.keyboard.press("Escape"); await confirmation.waitFor({ state: "hidden" });
  await allocation.waitFor();
  assert(await deleteButton.evaluate(el => el === document.activeElement), "Nested confirmation lost opener focus");
  await page.keyboard.press("Escape"); await allocation.waitFor({ state: "hidden" });
  assert(await opener.evaluate(el => el === document.activeElement), "Allocation did not restore row control focus");

  const create = page.getByRole("button", { name: "+ New thesis", exact: true });
  await create.click(); const createDialog = page.getByRole("dialog", { name: "Create thesis", exact: true });
  await focusedInside(createDialog); await wrapTabs(createDialog); await page.keyboard.press("Escape");
  assert(await create.evaluate(el => el === document.activeElement), "Header create lost opener focus");
  const header = page.locator("tr.thesis-inline-group-row").filter({ hasText: name });
  const rename = header.getByRole("button", { name: "Rename", exact: true });
  await rename.click(); const renameDialog = page.getByRole("dialog", { name: "Rename thesis", exact: true });
  await focusedInside(renameDialog); await page.keyboard.press("Escape");
  assert(await rename.evaluate(el => el === document.activeElement), "Rename lost opener focus");

  const ticker = page.getByRole("columnheader", { name: /^Ticker/ });
  for (let i = 0; i < 3; i++) await ticker.click();
  const handle = page.getByRole("button", { name: `Reorder ${name}`, exact: true });
  await handle.focus(); await page.keyboard.press("Alt+ArrowUp");
  await page.waitForFunction(async ({ origin, id }) => { const s = await (await fetch(`${origin}/api/portfolio/theses`)).json(); const k = s.manual_orders.theses ?? []; return k.includes(id) && k.indexOf(id) < k.length - 1; }, { origin, id: createdId });
  await page.reload(); await page.locator(`tr[data-thesis-group-name="${name}"]`).waitFor();
  const newThesisOrder = (await state()).manual_orders.theses;
  assert(newThesisOrder.includes(createdId), "Keyboard thesis order did not persist");

  for (let i = 0; i < 3; i++) await page.getByRole("columnheader", { name: /^Ticker/ }).click();
  const legs = page.locator('tr.thesis-inline-draggable[data-thesis-group-id="unallocated"]');
  const secondKey = await legs.nth(1).getAttribute("data-leg-key");
  await legs.nth(1).focus(); await page.keyboard.press("Alt+ArrowUp");
  await page.waitForFunction(async key => { const s = await (await fetch("/api/portfolio/theses")).json(); return s.manual_orders["unallocated:dash"]?.[0] === key; }, secondKey);
  await page.getByRole("button", { name: `Collapse ${name}`, exact: true }).click();
  const source = page.locator('tr.thesis-inline-draggable[data-thesis-group-id="unallocated"]').first();
  const movedKey = await source.getAttribute("data-leg-key");
  await source.focus(); await page.keyboard.press("Enter");
  await page.getByRole("menuitem", { name: `Move to ${name} thesis`, exact: true }).click();
  await page.getByRole("dialog", { name: /^Move / }).getByRole("button", { name: "Confirm move", exact: true }).click();
  await page.getByRole("button", { name: `Collapse ${name}`, exact: true }).waitFor();
  await page.waitForFunction(({ id, key }) => document.activeElement?.getAttribute("data-focus-key") === `${id}:${key}`, { id: createdId, key: movedKey });
  await page.goto(`${origin}/positions`);
  await page.getByRole("button", { name: "By thesis", exact: true }).waitFor();
  for (let i = 0; i < 3; i++) await page.getByRole("columnheader", { name: /^Ticker/ }).click();
  const flat = page.locator("tr.manual-sort-row");
  await flat.nth(1).focus();
  const flatKey = await flat.nth(1).getAttribute("data-position-key");
  await page.keyboard.press("Alt+ArrowUp");
  await page.waitForFunction(async key => { const s = await (await fetch("/api/portfolio/theses")).json(); return s.manual_orders["positions:holdings"]?.[0] === key; }, flatKey);
  assert.equal(await page.locator("tr.manual-sort-row[tabindex='0']").count(), 1);
  await page.goto(`${origin}/dash?model=cuse`);

  for (const theme of ["dark", "light"]) {
    await page.evaluate(theme => localStorage.setItem("theme-mode", theme), theme);
    await page.emulateMedia({ reducedMotion: "reduce" });
    for (const viewport of [{ width: 1440, height: 900 }, { width: 1280, height: 600 }, { width: 390, height: 844 }]) {
      await page.setViewportSize(viewport); await page.reload(); await page.locator("tr.thesis-inline-draggable").first().waitFor();
      assert.equal(await page.locator("html").getAttribute("data-theme"), theme);
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), "Document overflow");
      await page.screenshot({ path: `/tmp/ceiora-p5-${theme}-${viewport.width}.png` });
      if (viewport.width === 390) {
        await page.locator(".thesis-cell-trigger").first().click();
        const modal = page.locator(".thesis-allocation-modal"); await modal.waitFor();
        const box = await modal.boundingBox();
        assert(box.y >= 0 && box.y + box.height <= viewport.height + 1, "Allocation dialog clipped outside viewport");
        await page.screenshot({ path: `/tmp/ceiora-p5-${theme}-dialog.png` });
        await wrapTabs(modal); await page.keyboard.press("Escape");
      }
    }
  }
  // A different principal must clear global confirmation and page-local state.
  await page.locator(".thesis-cell-trigger").first().click();
  await page.locator(".thesis-allocation-modal").getByRole("button", { name: `Delete thesis ${name}`, exact: true }).click();
  await page.getByRole("dialog", { name: `Delete “${name}”?` }).waitFor();
  const deleted = [];
  const onDelete = request => { if (request.method() === "DELETE") deleted.push(request.url()); };
  page.on("request", onDelete);
  await page.route("**/api/portfolio/theses/capability", route => route.fulfill({ json: { available: false } }));
  await page.route("**/api/auth/session", route => route.fulfill({ json: {
    authenticated: true,
    session: { authProvider: "shared", username: "another-local-user", subject: "another-local-user", isAdmin: false, primary: false },
    context: { auth_provider: "shared", subject: "another-local-user", account_ids: [], is_admin: false, account_enforcement_enabled: true },
  } }));
  await page.evaluate(() => { window.__principalSwitchMarker = "before"; window.dispatchEvent(new Event("focus")); });
  await page.waitForFunction(() => window.__principalSwitchMarker !== "before");
  await page.getByTestId("dash-workspace").waitFor();
  await page.getByRole("dialog", { name: `Delete “${name}”?` }).waitFor({ state: "hidden" });
  await page.waitForFunction(() => document.querySelectorAll(".thesis-cell-trigger").length === 0);
  assert(!(await page.locator("body").innerText()).includes(name), "Previous principal's thesis leaked after session change");
  assert.deepEqual(deleted, [], "Obsolete confirmation submitted a deletion");
  page.off("request", onDelete);
  await page.unroute("**/api/auth/session"); await page.unroute("**/api/portfolio/theses/capability");
  await page.reload(); await page.locator(".thesis-cell-trigger").first().waitFor();
  assert.deepEqual(errors, []);
  console.log("Thesis accessibility passed: micro-shares, unique split-leg tab stop, nested modal focus/Escape, opener restoration, keyboard order persistence, both themes, reduced motion, narrow/short layouts");
} finally {
  if (createdId) await mutate(`theses/${createdId}`, "DELETE", {}).catch(() => {});
  await browser.close();
}
