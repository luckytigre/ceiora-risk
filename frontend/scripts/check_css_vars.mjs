#!/usr/bin/env node
// CI-01 — fail the build on CSS custom properties that are referenced without a
// fallback and never defined.
//
// Rationale: an undefined var() inside color-mix() makes the whole function invalid
// at computed-value time, so the declaration falls back to `inherit` rather than to a
// near-miss value. That renders wrong and is invisible in review.
//
// Two references are legitimately undefined in CSS and are NOT failures:
//   - `var(--x, fallback)` — the fallback makes it safe by construction.
//   - properties set from JS at runtime, listed in RUNTIME_SET below.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
// tokens.css owns the :root layer; globals.css may still define component-SCOPED
// custom properties on their own selectors (e.g. --family-badge-color per variant),
// which is correct and stays with the component. Both are read so a reference in
// one file resolves against a definition in the other.
const files = ["src/app/tokens.css", "src/app/globals.css"];
const css = files.map((f) => readFileSync(join(root, f), "utf8")).join("\n");

// Custom properties assigned from JS via inline style objects.
const RUNTIME_SET = new Set([
  "--thesis-color",         // ThesisAllocationCell.tsx
  "--thesis-active-color",  // InlineThesisRows.tsx
  "--kpi-row-index",        // KpiCard.tsx
]);

const defined = new Set(
  [...css.matchAll(/^\s*(--[A-Za-z0-9-]+)\s*:/gm)].map((m) => m[1]),
);

// Only flag references with no fallback: `var(--x)` / `var( --x )`, not `var(--x, y)`.
const missing = new Map();
for (const m of css.matchAll(/var\(\s*(--[A-Za-z0-9-]+)\s*\)/g)) {
  const name = m[1];
  if (defined.has(name) || RUNTIME_SET.has(name)) continue;
  const upto = css.slice(0, m.index).split("\n").length;
  const tokenLines = readFileSync(join(root, files[0]), "utf8").split("\n").length;
  const line = upto <= tokenLines ? `tokens.css:${upto}` : `globals.css:${upto - tokenLines}`;
  if (!missing.has(name)) missing.set(name, []);
  missing.get(name).push(line);
}

if (missing.size === 0) {
  console.log(`check_css_vars: OK — ${defined.size} properties defined, no undefined references.`);
  process.exit(0);
}

console.error("check_css_vars: FAIL — referenced without a fallback and never defined:\n");
for (const [name, lines] of [...missing].sort()) {
  console.error(`  ${name}`);
  console.error(`    globals.css:${lines.join(", ")}`);
}
console.error(
  "\nDefine the property in the token block, add a fallback (var(--x, value)),\n" +
  "or add it to RUNTIME_SET if it is assigned from JS.",
);
process.exit(1);
