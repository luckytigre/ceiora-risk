#!/usr/bin/env node
// CI-02 — reject raw values for axes that have been tokenised.
//
// Enabled incrementally, one axis per phase, so the gate never fails on an axis
// that has not been converted yet:
//   z-index    — enabled (Phase 3, Z-01)
//   box-shadow — enabled (Phase 3, ELEV-01), drop shadows only
//   font-size  — enabled (Phase 5, TOKEN-02), px values only
//   duration   — enabled (Phase 5, TOKEN-01), `s` notation only

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const css = ["src/app/tokens.css", "src/app/globals.css"]
  .map((f) => readFileSync(join(root, f), "utf8")).join("\n");
const lines = css.split("\n");

const failures = [];

// A raw z-index is allowed only when the line documents that it is local to an
// element which is already its own stacking context.
lines.forEach((line, i) => {
  const m = line.match(/(?:^|[;{\s])z-index:\s*(-?\d+)\s*;/);
  if (!m) return;
  if (/local to /.test(line)) return;              // documented local value
  failures.push({
    line: i + 1,
    text: line.trim(),
    why: `raw z-index: ${m[1]} — use a --z-* tier, or document it as local to its stacking context`,
  });
});

// Raw drop shadows. Focus rings (0 0 0 Npx) and inset decoration are a separate
// concern (A11Y-04) and are not flagged here.
lines.forEach((line, i) => {
  for (const m of line.matchAll(/box-shadow:\s*([^;]+);/g)) {
    const v = m[1].trim();
    if (v.startsWith("var(") || v === "none" || v.includes("inset")) continue;
    if (/^0 0 0 /.test(v)) continue;               // focus ring
    failures.push({
      line: i + 1,
      text: line.trim(),
      why: `raw drop shadow — use var(--shadow-1|2|3)`,
    });
  }
});

// Raw pixel font-sizes. rem/em and the clamp() display sizes on the public pages
// are deliberately not tokenised and are not flagged.
lines.forEach((line, i) => {
  const m = line.match(/font-size:\s*([0-9.]+px)\s*;/);
  if (!m) return;
  failures.push({
    line: i + 1,
    text: line.trim(),
    why: `raw font-size: ${m[1]} — use a --fs-* step`,
  });
});

// `s` notation for motion. Everything readable is in ms; mixing the two produced
// 0.15s / 0.16s / 160ms as three ways of saying the same thing.
lines.forEach((line, i) => {
  if (!/\b(transition|animation)[^;]*;/.test(line)) return;
  for (const m of line.matchAll(/(?<![\w.])(\d*\.?\d+)s(?![\w.])/g)) {
    if (Number(m[1]) >= 1) continue;          // long ambient loops stay readable in s
    failures.push({
      line: i + 1,
      text: line.trim(),
      why: `duration written in s (${m[0]}) — use ms, or a --dur-* token`,
    });
  }
});

if (!failures.length) {
  console.log("check_css_raw_values: OK — z-index, shadows, font sizes and durations are tokenised.");
  process.exit(0);
}

console.error("check_css_raw_values: FAIL\n");
for (const f of failures) {
  console.error(`  globals.css:${f.line}`);
  console.error(`    ${f.text.slice(0, 110)}`);
  console.error(`    ${f.why}\n`);
}
process.exit(1);
