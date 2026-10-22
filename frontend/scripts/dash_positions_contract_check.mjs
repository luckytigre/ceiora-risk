import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import ts from "typescript";
const source = await readFile(new URL("../src/features/dash/dashPositions.ts", import.meta.url), "utf8");
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext } }).outputText;
const { buildDashPositions } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString("base64")}`);
const holdings = [
  { account_id: "a", ric: "X.O", ticker: "X", quantity: 3 },
  { account_id: "b", ric: "X.O", ticker: "X", quantity: -2 },
  { account_id: "a", ric: "Y.O", ticker: "X", quantity: 4 },
];
const mix = { market: 10, industry: 20, style: 30, idio: 40 };
const analytics = [{ account_id: "all_accounts", ric: "X.O", quantity: 1, price: 10, risk_mix: mix }];
let rows = buildDashPositions(holdings, analytics);
assert.deepEqual(rows.map(row => [row.account, row.shares, row.market_value]), [["a", 3, 30], ["b", -2, -20], ["a", 4, null]]);
assert(rows.every(row => row.risk_mix === null), "Aggregate risk must not be attributed to concrete accounts or joined by ticker");
rows = buildDashPositions(holdings, [{ account: "A", ric: "x.o", shares: 3, price: 0, risk_mix: mix }]);
assert.deepEqual(rows[0].risk_mix, mix);
assert.equal(rows[0].price, 0, "A real zero price differs from unavailable price");
assert.equal(rows[1].risk_mix, null);
rows = buildDashPositions(holdings, [{ account: "a", ric: "X.O", shares: 5, price: 10, risk_mix: mix }]);
assert.equal(rows[0].risk_mix, null, "Stale quantity must not display served risk mix as current");
rows = buildDashPositions(holdings, [{ account_id: "a", ric: "X.O", quantity: 3, price: null, risk_mix: null }]);
assert.equal(rows[0].price, null);
assert.equal(rows[0].market_value, null);
assert.equal(rows[0].risk_mix, null);
const middleware = await readFile(new URL("../src/middleware.ts", import.meta.url), "utf8");
assert(middleware.includes('"/dash/:path*"'), "DASH must enter server auth middleware");
console.log("DASH holdings identity, signed values, nullable analytics, and stale snapshot contracts passed");
