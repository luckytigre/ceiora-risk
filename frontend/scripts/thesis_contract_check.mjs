import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import ts from "typescript";
async function load(file) {
  const source = await readFile(new URL(`../src/lib/${file}.ts`, import.meta.url), "utf8");
  const code = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext } }).outputText;
  return import(`data:text/javascript;base64,${Buffer.from(code).toString("base64")}`);
}
const { thesisQuantityWire } = await load("thesisQuantity");
const { isConcreteThesisPosition } = await load("thesisIdentity");
assert.equal(thesisQuantityWire(.1 + .2), "0.300000");
assert.equal(thesisQuantityWire(-.000001), "-0.000001");
for (const quantity of [NaN, Infinity, -.0000001, 1_000_000_001]) assert.throws(() => thesisQuantityWire(quantity));
assert(isConcreteThesisPosition("acct_main", "X.O"));
for (const [account, ric] of [["MULTI", "X.O"], ["all_accounts", "X.O"], ["acct_main", ""], ["", "X.O"]]) assert(!isConcreteThesisPosition(account, ric));
console.log("Thesis quantity precision and concrete holding identity checks passed");
