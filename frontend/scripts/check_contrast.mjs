#!/usr/bin/env node
// CI-03 — assert the token pairs that matter clear their contrast floor in BOTH themes,
// and that the ink tiers stay ordered.
//
// Without this, a palette tweak can silently drop text below WCAG AA, or invert the
// visual hierarchy so a "muted" tier reads stronger than the "secondary" above it.
// Both had happened in this file before Phase 1.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const css = readFileSync(join(root, "src/app/tokens.css"), "utf8");

const AA = 4.5;        // WCAG AA, normal-size text

function token(name) {
  const re = new RegExp(`--${name}\\s*:\\s*(rgba?\\([^)]*\\)|#[0-9a-fA-F]{3,8})`);
  const m = css.match(re);
  if (!m) throw new Error(`token --${name} not found in globals.css`);
  return parse(m[1]);
}

function parse(v) {
  if (v.startsWith("#")) {
    let h = v.slice(1);
    if (h.length === 3) h = [...h].map((c) => c + c).join("");
    return [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16)).concat(1);
  }
  const n = v.match(/[\d.]+/g).map(Number);
  return [n[0], n[1], n[2], n[3] ?? 1];
}

const srgb = (c) => (c / 255 <= 0.03928 ? c / 255 / 12.92 : (((c / 255) + 0.055) / 1.055) ** 2.4);
const lum = ([r, g, b]) => 0.2126 * srgb(r) + 0.7152 * srgb(g) + 0.0722 * srgb(b);
const over = ([r, g, b, a], bg) => [r, g, b].map((c, i) => Math.round(c * a + bg[i] * (1 - a)));

function ratio(fg, bg) {
  const [hi, lo] = [lum(over(fg, bg)), lum(bg)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}

const CANVAS = { dark: token("canvas-dark-mid"), light: token("canvas-light-mid") };
const failures = [];
const report = [];

// --- text tiers must clear AA on their own canvas, and stay ordered ---
for (const theme of ["dark", "light"]) {
  const bg = CANVAS[theme];
  let prev = null;
  for (const tier of ["primary", "secondary", "muted", "folio"]) {
    const r = ratio(token(`ink-${tier}-${theme}`), bg);
    report.push(`  ${theme.padEnd(5)} ink-${tier.padEnd(10)} ${r.toFixed(2)}`);
    if (r < AA) failures.push(`--ink-${tier}-${theme} is ${r.toFixed(2)}:1, below AA ${AA}:1`);
    if (prev !== null && r > prev.r) {
      failures.push(
        `ink tier order inverted in ${theme}: ${tier} (${r.toFixed(2)}) reads stronger than ${prev.t} (${prev.r.toFixed(2)})`,
      );
    }
    prev = { t: tier, r };
  }
}

// --- signal colours must be legible as TEXT on their own ground ---
//
// NOTE: this deliberately does NOT assert a contrast ratio BETWEEN positive and
// negative. That target is unachievable and was wrong in the original plan. On the
// dark ground, a negative that clears AA needs L >= 0.238; a 3:1 separation would
// then force positive to L >= 0.814 — effectively white, with no hue left. The two
// requirements are in direct conflict.
//
// WCAG 1.4.1 is satisfied instead by a NON-COLOUR channel: every .positive /
// .negative cell renders an explicit + or - sign. That is asserted by
// scripts/check_signed_cells.mjs, not here, because it is a property of the
// rendered content rather than of the palette.
for (const theme of ["dark", "light"]) {
  const bg = CANVAS[theme];
  for (const sig of ["positive", "negative"]) {
    const r = ratio(token(`signal-${sig}-${theme}`), bg);
    report.push(`  ${theme.padEnd(5)} signal-${sig.padEnd(9)} ${r.toFixed(2)}`);
    if (r < AA) failures.push(`--signal-${sig}-${theme} is ${r.toFixed(2)}:1, below AA ${AA}:1`);
  }
}

console.log("check_contrast:\n" + report.join("\n"));

if (failures.length) {
  console.error("\ncheck_contrast: FAIL\n");
  for (const f of failures) console.error(`  - ${f}`);
  process.exit(1);
}
console.log("\ncheck_contrast: OK");
