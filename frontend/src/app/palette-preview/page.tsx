import { IBM_Plex_Sans, IBM_Plex_Mono } from "next/font/google";

const body = IBM_Plex_Sans({
  subsets: ["latin"],
  weight: ["300", "400", "500", "600"],
  style: ["normal", "italic"],
  display: "swap",
  variable: "--font-body",
});

const mono = IBM_Plex_Mono({
  subsets: ["latin"],
  weight: ["300", "400", "500"],
  style: ["normal", "italic"],
  display: "swap",
  variable: "--font-mono",
});

const displayStack = '"Portrait Text", "Ivar Text", "Canela", "Tiempos Headline", "Bodoni Moda", "Libre Caslon Display", Georgia, serif';

type FamilyKey = "cuse" | "cpar" | "cmac";
type SignalKey = "positive" | "negative" | "warning" | "neutral";

const PALETTE = {
  name: "Ceiora — Edition II",
  shell: {
    top: "#242429",
    base: "#1f1f24",
    deep: "#18181d",
  },
  ink: {
    display: "rgba(248, 248, 247, 0.99)",
    heading: "rgba(246, 246, 245, 0.98)",
    body: "rgba(222, 227, 233, 0.84)",
    muted: "rgba(173, 183, 199, 0.70)",
    folio: "rgba(140, 148, 162, 0.62)",
    accent: "rgba(200, 214, 228, 0.94)",
  },
  paper: {
    card: "#f6f5f2",
    ink: "#1a1a1e",
  },
  family: {
    cuse: "#c063a4",
    cpar: "#5a9ecb",
    cmac: "#e5a15f",
  } satisfies Record<FamilyKey, string>,
  signals: {
    positive: "#79bc9c",
    negative: "#d27186",
    warning: "#d8a85b",
    neutral: "#b6bcc8",
  } satisfies Record<SignalKey, string>,
  chart: ["#d477b5", "#69b4df", "#e3a860", "#86c8a8", "#cf7f92", "#cad377"],
};

const LIGHT_PALETTE = {
  shell: {
    top: "#f2f2ef",
    base: "#edede9",
    deep: "#e5e4e0",
  },
  ink: {
    display: "rgba(27, 28, 31, 0.96)",
    heading: "rgba(32, 34, 38, 0.92)",
    body: "rgba(58, 61, 67, 0.84)",
    muted: "rgba(108, 113, 122, 0.72)",
    folio: "rgba(129, 133, 141, 0.62)",
  },
  surface: {
    lift: "rgba(255, 255, 255, 0.52)",
    hair: "rgba(31, 34, 39, 0.10)",
    hairStrong: "rgba(31, 34, 39, 0.16)",
  },
  signals: {
    positive: "#45936f",
    negative: "#ae4660",
  },
  method: {
    core: "rgba(82, 88, 97, 0.82)",
    projection: "rgba(177, 145, 76, 0.92)",
    fundamental: "rgba(118, 126, 138, 0.34)",
    returns: "rgba(118, 126, 138, 0.20)",
  },
};

const CSS = `
.palette-study {
  --font-display: ${displayStack};
  --applied-factor-label-col: 220px;
  --applied-factor-value-col: 92px;
  --shell-top: ${PALETTE.shell.top};
  --shell-base: ${PALETTE.shell.base};
  --shell-deep: ${PALETTE.shell.deep};
  --surface-lift-1: rgba(255, 255, 255, 0.018);
  --surface-lift-2: rgba(255, 255, 255, 0.028);
  --hair: rgba(255, 255, 255, 0.06);
  --hair-strong: rgba(255, 255, 255, 0.11);
  --ink-display: ${PALETTE.ink.display};
  --ink-wordmark: rgba(250, 250, 249, 0.995);
  --ink-heading: ${PALETTE.ink.heading};
  --ink-body: ${PALETTE.ink.body};
  --ink-muted: ${PALETTE.ink.muted};
  --ink-folio: ${PALETTE.ink.folio};
  --ink-accent: ${PALETTE.ink.accent};
  --paper-card: ${PALETTE.paper.card};
  --paper-ink: ${PALETTE.paper.ink};
  --paper-ink-muted: rgba(26, 26, 30, 0.64);
  --paper-hair: rgba(26, 26, 30, 0.12);
  --fam-cuse: ${PALETTE.family.cuse};
  --fam-cpar: ${PALETTE.family.cpar};
  --fam-cmac: ${PALETTE.family.cmac};
  --ana-cuse: ${PALETTE.family.cuse};
  --ana-cpar: ${PALETTE.family.cpar};
  --ana-cmac: ${PALETTE.family.cmac};
  --sig-pos: ${PALETTE.signals.positive};
  --sig-neg: ${PALETTE.signals.negative};
  --sig-wrn: ${PALETTE.signals.warning};
  --sig-ntr: ${PALETTE.signals.neutral};
  --method-core: rgba(214, 219, 226, 0.82);
  --method-projection: rgba(223, 197, 132, 0.88);
  --projection-fundamental: rgba(196, 204, 215, 0.74);
  --projection-returns: rgba(160, 169, 183, 0.54);
  color: var(--ink-body);
  font-family: var(--font-body), system-ui, sans-serif;
  font-weight: 400;
  font-feature-settings: "ss01", "cv11";
  min-height: 100vh;
  position: relative;
  overflow-x: hidden;
}
.palette-study * { box-sizing: border-box; }

.palette-study .root {
  position: relative;
  max-width: 1340px;
  margin: 0 auto;
  padding: 128px 48px 112px;
  display: grid;
  gap: 136px;
}
@media (max-width: 760px) {
  .palette-study .root { padding: 64px 20px 56px; gap: 80px; }
}

@keyframes rise {
  from { opacity: 0; transform: translateY(14px); }
  to { opacity: 1; transform: translateY(0); }
}
.palette-study .rise { animation: rise 900ms cubic-bezier(0.2, 0.7, 0.2, 1) both; }
.palette-study .d0 { animation-delay: 20ms; }
.palette-study .d1 { animation-delay: 120ms; }
.palette-study .d2 { animation-delay: 220ms; }
.palette-study .d3 { animation-delay: 320ms; }
.palette-study .d4 { animation-delay: 440ms; }
.palette-study .d5 { animation-delay: 580ms; }
.palette-study .d6 { animation-delay: 720ms; }

/* ─── folio ─── */
.palette-study .folio {
  display: flex; align-items: center; gap: 20px; flex-wrap: wrap;
  font-family: var(--font-mono); font-size: 10px; font-weight: 400;
  letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--ink-folio);
}
.palette-study .folio .rule {
  flex: 0 0 48px; height: 1px; background: var(--hair-strong);
}

/* ─── COVER ─── */
.palette-study .cover { display: grid; gap: 44px; }
.palette-study .cover-title {
  font-family: var(--font-display);
  font-weight: 300;
  font-size: clamp(46px, 8vw, 112px);
  line-height: 1.01;
  letter-spacing: -0.045em;
  color: var(--ink-display);
  margin: 0;
  max-width: 14ch;
}
.palette-study .cover-title em {
  font-style: italic; font-weight: 400;
  color: var(--ink-accent);
  font-feature-settings: "ss01";
}
.palette-study .cover-sub {
  font-family: var(--font-body);
  font-weight: 300;
  font-size: clamp(17px, 1.6vw, 20px);
  line-height: 1.6;
  letter-spacing: -0.002em;
  color: var(--ink-body);
  max-width: 56ch;
  margin: 0;
}
.palette-study .cover-meta {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 32px;
  padding-top: 32px;
  border-top: 1px solid var(--hair);
  max-width: 780px;
}
@media (max-width: 640px) {
  .palette-study .cover-meta { grid-template-columns: 1fr 1fr; gap: 20px; }
}
.palette-study .cover-meta > div { display: grid; gap: 8px; }
.palette-study .meta-label {
  font-family: var(--font-body); font-size: 11px; font-weight: 500;
  letter-spacing: 0.12em; text-transform: uppercase;
  color: var(--ink-folio);
}
.palette-study .meta-value {
  font-family: var(--font-body); font-weight: 400;
  font-size: 14px; color: var(--ink-body);
}

/* ─── section head ─── */
.palette-study .sec { display: grid; gap: 48px; }
.palette-study .sec-head { display: grid; gap: 16px; max-width: 720px; }
.palette-study .sec-num {
  font-family: var(--font-body); font-size: 11px; font-weight: 500;
  letter-spacing: 0.12em; text-transform: uppercase;
  color: var(--ink-folio);
}
.palette-study .sec-title {
  font-family: var(--font-display);
  font-weight: 350;
  font-size: clamp(30px, 4.2vw, 52px);
  line-height: 1.04;
  letter-spacing: -0.033em;
  color: var(--ink-display);
  margin: 0;
}
.palette-study .sec-title em {
  font-style: italic; font-weight: 400;
  color: var(--ink-accent);
}
.palette-study .sec-lede {
  font-family: var(--font-body); font-size: 15px; line-height: 1.65;
  color: var(--ink-body); max-width: 60ch; margin: 0;
}

/* ─── COLOR FIELD ─── */
.palette-study .field {
  position: relative;
  height: 520px;
  overflow: hidden;
  margin: 0 -48px;
}
@media (max-width: 760px) {
  .palette-study .field { margin: 0 -20px; height: 420px; }
}
.palette-study .field-gradient {
  position: absolute; inset: 0;
  background: linear-gradient(180deg,
    var(--shell-top) 0%,
    var(--shell-base) 52%,
    var(--shell-deep) 100%);
}
.palette-study .field-stops {
  position: absolute;
  right: 48px; top: 48px; bottom: 48px;
  display: grid; grid-template-rows: 1fr 1fr 1fr; gap: 12px;
  text-align: right;
}
@media (max-width: 760px) { .palette-study .field-stops { right: 20px; top: 24px; bottom: 24px; } }
.palette-study .field-stop { display: grid; gap: 4px; align-self: center; }
.palette-study .field-stop-hex {
  font-family: var(--font-mono); font-size: 12px;
  color: var(--ink-heading); letter-spacing: 0.06em;
}
.palette-study .field-stop-role {
  font-family: var(--font-mono); font-size: 9px;
  letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--ink-muted);
}
.palette-study .field-attrib {
  position: absolute; left: 48px; bottom: 24px;
  font-family: var(--font-mono); font-size: 10px;
  letter-spacing: 0.2em; text-transform: uppercase;
  color: var(--ink-folio);
}
@media (max-width: 760px) { .palette-study .field-attrib { left: 20px; } }

/* ─── INSTRUMENT (app chrome) ─── */
.palette-study .instrument {
  display: grid;
  grid-template-columns: 1.32fr 0.88fr;
  gap: 24px;
}
@media (max-width: 900px) { .palette-study .instrument { grid-template-columns: 1fr; } }

.palette-study .chrome {
  background: transparent;
  display: grid;
}
.palette-study .chrome-top {
  display: grid;
  grid-template-columns: auto 1fr auto;
  gap: 32px;
  align-items: center;
  padding: 0 0 18px;
  border-bottom: 1px solid var(--hair);
}
.palette-study .wordmark {
  font-family: var(--font-body);
  font-weight: 500;
  font-size: 18px;
  letter-spacing: -0.005em;
  color: var(--ink-wordmark);
}
.palette-study .nav { display: flex; gap: 22px; flex-wrap: wrap; }
.palette-study .nav-item {
  font-family: var(--font-body); font-size: 11px; font-weight: 400;
  letter-spacing: 0.01em; text-transform: none;
  color: rgba(158, 162, 170, 0.88);
  padding: 10px 0;
  border-bottom: 1px solid transparent;
  transition: color 200ms ease, border-color 200ms ease;
}
.palette-study .nav-item.active {
  color: #d7d9dd;
  border-bottom-color: var(--fam-cmac);
}
.palette-study .cta {
  font-family: var(--font-body); font-size: 12px; font-weight: 400;
  letter-spacing: 0.01em; text-transform: none;
  color: var(--ink-heading);
  background: transparent;
  border: 1px solid var(--hair-strong);
  padding: 9px 14px;
}

.palette-study .chrome-body { padding: 22px 0 0; display: grid; gap: 24px; }
.palette-study .chrome-meta {
  display: flex; justify-content: space-between; align-items: baseline; gap: 16px;
  padding-bottom: 16px; border-bottom: 1px solid var(--hair);
}
.palette-study .chrome-meta-left { display: grid; gap: 4px; }
.palette-study .chrome-meta-eyebrow {
  font-family: var(--font-body); font-size: 12px; font-weight: 400;
  letter-spacing: 0.01em; text-transform: none;
  color: var(--ink-folio);
}
.palette-study .chrome-meta-title {
  font-family: var(--font-body); font-weight: 400;
  font-size: 14px; color: var(--ink-body);
}
.palette-study .chrome-meta-right { display: inline-flex; align-items: center; gap: 10px; }
.palette-study .chrome-meta-dot {
  width: 6px; height: 6px; border-radius: 50%;
  background: var(--sig-pos);
}
.palette-study .chrome-meta-label {
  font-family: var(--font-mono); font-size: 10px;
  letter-spacing: 0.2em; text-transform: uppercase;
  color: var(--ink-body);
}

.palette-study .headline {
  font-family: var(--font-body);
  font-weight: 400;
  font-size: 22px;
  line-height: 1.35;
  letter-spacing: -0.01em;
  color: var(--ink-heading);
  max-width: 30ch;
}

.palette-study .stats {
  display: grid; grid-template-columns: repeat(4, 1fr); gap: 32px;
  padding: 2px 0 16px;
}
@media (max-width: 560px) { .palette-study .stats { grid-template-columns: repeat(2, 1fr); gap: 20px; } }
.palette-study .stat { display: grid; gap: 6px; }
.palette-study .stat-label {
  font-family: var(--font-body); font-size: 12px; font-weight: 400;
  letter-spacing: 0.01em; color: var(--ink-folio);
}
.palette-study .stat-val {
  font-family: var(--font-body); font-weight: 400;
  font-size: 22px; letter-spacing: -0.012em;
  color: var(--ink-heading);
  font-feature-settings: "tnum", "lnum";
}
.palette-study .stat-val.family-cuse { color: var(--ana-cuse); }
.palette-study .stat-val.family-cpar { color: var(--ana-cpar); }
.palette-study .stat-val.family-cmac { color: var(--ana-cmac); }
.palette-study .stat-delta {
  font-family: var(--font-body); font-size: 12px;
  letter-spacing: 0.01em; color: var(--ink-muted);
}

.palette-study .exposures { display: grid; gap: 16px; }
.palette-study .exposure { display: grid; gap: 8px; }
.palette-study .exp-head {
  display: flex; justify-content: space-between; align-items: baseline;
}
.palette-study .exp-label {
  font-family: var(--font-body); font-size: 12px;
  letter-spacing: 0.01em; color: var(--ink-muted);
}
.palette-study .exp-val {
  font-family: var(--font-body); font-size: 12px;
  color: var(--ink-heading); font-feature-settings: "tnum";
}
.palette-study .exp-track {
  position: relative;
  height: 5px;
  background: rgba(255, 255, 255, 0.035);
}
.palette-study .exp-axis {
  position: absolute; left: 50%; top: -2px; bottom: -2px;
  width: 1px; background: var(--hair-strong);
}
.palette-study .exp-fill { position: absolute; top: 0; bottom: 0; }

.palette-study .table { }
.palette-study .tr {
  display: grid;
  grid-template-columns: 0.3fr 1.4fr 0.7fr 0.7fr;
  gap: 14px;
  padding: 14px 0;
  border-bottom: 1px solid var(--hair);
  align-items: baseline;
}
.palette-study .tr:last-child { border-bottom: none; }
.palette-study .tr-head { padding: 10px 0; border-bottom: 1px solid var(--hair); }
.palette-study .th {
  font-family: var(--font-mono); font-size: 9px; font-weight: 400;
  letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--ink-folio);
}
.palette-study .th.right, .palette-study .td-num { text-align: right; }
.palette-study .td-idx {
  font-family: var(--font-mono); font-size: 10px;
  color: var(--ink-folio); letter-spacing: 0.1em;
}
.palette-study .td {
  font-family: var(--font-body); font-size: 14px;
  color: var(--ink-body);
}
.palette-study .td-fam {
  font-family: var(--font-mono); font-size: 10px;
  letter-spacing: 0.18em; text-transform: uppercase;
}
.palette-study .td-num {
  font-family: var(--font-mono); font-size: 13px;
  color: var(--ink-heading); font-feature-settings: "tnum";
}
.palette-study .td-num.pos { color: var(--sig-pos); }
.palette-study .td-num.neg { color: var(--sig-neg); }
.palette-study .td-num.wrn { color: var(--sig-wrn); }

/* ─── ASIDE (paper card) ─── */
.palette-study .aside {
  background: var(--paper-card);
  color: var(--paper-ink);
  padding: 36px 32px;
  display: grid; gap: 24px;
  align-content: start;
}
.palette-study .aside-eyebrow {
  font-family: var(--font-mono); font-size: 10px;
  letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--paper-ink-muted);
}
.palette-study .aside-title {
  font-family: var(--font-body);
  font-weight: 400;
  font-size: 22px;
  line-height: 1.3;
  letter-spacing: -0.012em;
  color: var(--paper-ink);
}
.palette-study .aside-body {
  font-family: var(--font-body); font-size: 14px; line-height: 1.65;
  color: var(--paper-ink-muted); max-width: 34ch;
}
.palette-study .aside-foot {
  font-family: var(--font-mono); font-size: 10px;
  letter-spacing: 0.2em; text-transform: uppercase;
  color: var(--paper-ink-muted);
  padding-top: 16px; border-top: 1px solid var(--paper-hair);
}

/* ─── TOKENS ─── */
.palette-study .chips {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 32px 24px;
}
@media (max-width: 900px) { .palette-study .chips { grid-template-columns: repeat(2, 1fr); } }
.palette-study .chip { display: grid; gap: 12px; }
.palette-study .chip-swatch {
  height: 52px;
  position: relative;
  box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.04);
}
.palette-study .chip-swatch.is-light {
  box-shadow: inset 0 0 0 1px rgba(26, 26, 30, 0.06);
}
.palette-study .chip-meta { display: grid; gap: 6px; }
.palette-study .chip-hex {
  font-family: var(--font-mono); font-size: 11px;
  color: var(--ink-heading); letter-spacing: 0.04em;
}
.palette-study .chip-name {
  font-family: var(--font-body); font-weight: 500;
  font-size: 13px; color: var(--ink-body);
}
.palette-study .chip-role {
  font-family: var(--font-mono); font-size: 9px;
  letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--ink-folio);
}

/* ─── FAMILIES ─── */
.palette-study .families {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 20px;
}
@media (max-width: 900px) { .palette-study .families { grid-template-columns: 1fr; } }
.palette-study .fam { display: grid; gap: 16px; }
.palette-study .fam-plate {
  height: 88px;
  padding: 18px;
  display: flex; align-items: flex-start; justify-content: space-between;
  color: rgba(12, 12, 16, 0.58);
  position: relative;
}
.palette-study .fam-code {
  font-family: var(--font-mono); font-size: 10px;
  letter-spacing: 0.24em; text-transform: uppercase;
}
.palette-study .fam-hex {
  font-family: var(--font-mono); font-size: 10px;
  letter-spacing: 0.06em;
}
.palette-study .fam-sub { display: grid; gap: 10px; }
.palette-study .fam-logo {
  display: grid;
  gap: 8px;
  padding-top: 2px;
}
.palette-study .fam-logo-word {
  font-family: var(--font-family);
  font-size: clamp(34px, 4vw, 52px);
  font-weight: 500;
  letter-spacing: -0.04em;
  line-height: 0.92;
  position: relative;
  text-shadow:
    0 1px 0 rgba(255, 255, 255, 0.05),
    0 1px 3px rgba(0, 0, 0, 0.18);
  -webkit-text-stroke: 0.45px rgba(255, 255, 255, 0.055);
}
.palette-study .fam-logo-word .lead {
  font-weight: 300;
  font-size: 0.92em;
  letter-spacing: -0.07em;
  display: inline-block;
  transform: translateY(-0.015em);
}
.palette-study .fam-logo-word.cuse {
  color: color-mix(in srgb, var(--fam-cuse) 84%, white 16%);
}
.palette-study .fam-logo-word.cpar {
  color: color-mix(in srgb, var(--fam-cpar) 84%, white 16%);
}
.palette-study .fam-logo-word.cmac {
  color: color-mix(in srgb, var(--fam-cmac) 86%, white 14%);
}
.palette-study .fam-title {
  font-family: var(--font-body); font-weight: 500;
  font-size: 14px; color: var(--ink-heading);
  letter-spacing: -0.005em;
}
.palette-study .fam-desc {
  font-family: var(--font-body); font-size: 13px; line-height: 1.55;
  color: var(--ink-body); max-width: 34ch;
}

/* ─── SIGNALS ─── */
.palette-study .signals-table { }
.palette-study .sig-row {
  display: grid;
  grid-template-columns: 36px 0.9fr 0.7fr 1.6fr 0.6fr;
  gap: 24px;
  padding: 20px 0;
  border-bottom: 1px solid var(--hair);
  align-items: center;
}
@media (max-width: 720px) {
  .palette-study .sig-row { grid-template-columns: 28px 1fr; gap: 12px; row-gap: 6px; padding: 14px 0; }
  .palette-study .sig-row > *:nth-child(n+3) { grid-column: 2; }
}
.palette-study .sig-chip { width: 24px; height: 24px; }
.palette-study .sig-name {
  font-family: var(--font-body); font-weight: 500;
  font-size: 15px; color: var(--ink-heading);
  letter-spacing: -0.005em;
}
.palette-study .sig-hex {
  font-family: var(--font-mono); font-size: 11px;
  color: var(--ink-muted); letter-spacing: 0.06em;
}
.palette-study .sig-use {
  font-family: var(--font-body); font-size: 13px; line-height: 1.5;
  color: var(--ink-body);
}
.palette-study .sig-example {
  font-family: var(--font-mono); font-size: 14px;
  text-align: right; font-feature-settings: "tnum";
}

/* ─── CHART ─── */
.palette-study .chart-wrap {
  border-top: 1px solid var(--hair);
  border-bottom: 1px solid var(--hair);
  padding: 24px 0;
  display: grid;
  gap: 28px;
  background: transparent;
}
.palette-study .chart-title {
  font-family: var(--font-body); font-weight: 500;
  font-size: 14px; letter-spacing: -0.005em;
  color: var(--ink-heading);
}
.palette-study .chart-stack { display: grid; gap: 12px; }
.palette-study .chart-row {
  display: grid; grid-template-columns: 110px 1fr 60px; gap: 18px;
  align-items: center;
}
@media (max-width: 560px) { .palette-study .chart-row { grid-template-columns: 80px 1fr 52px; gap: 10px; } }
.palette-study .chart-row-label {
  font-family: var(--font-mono); font-size: 10px;
  letter-spacing: 0.2em; text-transform: uppercase;
  color: var(--ink-muted);
}
.palette-study .chart-bar { display: flex; height: 20px; }
.palette-study .chart-seg { position: relative; }
.palette-study .chart-val {
  font-family: var(--font-mono); font-size: 11px;
  color: var(--ink-heading); text-align: right; font-feature-settings: "tnum";
}
.palette-study .chart-legend {
  display: grid; grid-template-columns: repeat(6, 1fr); gap: 16px;
  padding-top: 20px; border-top: 1px solid var(--hair);
}
@media (max-width: 760px) { .palette-study .chart-legend { grid-template-columns: repeat(3, 1fr); } }
.palette-study .legend { display: grid; gap: 8px; }
.palette-study .legend-bar { height: 2px; }
.palette-study .legend-label {
  font-family: var(--font-mono); font-size: 10px;
  letter-spacing: 0.14em; text-transform: uppercase;
  color: var(--ink-body);
}
.palette-study .legend-hex {
  font-family: var(--font-mono); font-size: 10px;
  color: var(--ink-folio);
}

/* ─── EDITORIAL ─── */
.palette-study .editorial {
  margin: 0 -48px;
  padding: 104px 72px;
  background: var(--paper-card);
  color: var(--paper-ink);
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 80px;
}
@media (max-width: 900px) {
  .palette-study .editorial { margin: 0 -20px; padding: 56px 28px; grid-template-columns: 1fr; gap: 36px; }
}
.palette-study .editorial-left { display: grid; gap: 20px; align-content: start; }
.palette-study .editorial-folio {
  font-family: var(--font-mono); font-size: 10px; font-weight: 400;
  letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--paper-ink-muted);
}
.palette-study .editorial-title {
  font-family: var(--font-display);
  font-weight: 300;
  font-size: clamp(44px, 6.2vw, 80px);
  line-height: 0.96;
  letter-spacing: -0.04em;
  color: var(--paper-ink);
  margin: 0;
}
.palette-study .editorial-title em {
  font-style: italic; font-weight: 400;
  color: rgba(26, 26, 30, 0.60);
}
.palette-study .editorial-right { display: grid; gap: 24px; align-content: start; }
.palette-study .editorial-eyebrow {
  font-family: var(--font-mono); font-size: 10px;
  letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--paper-ink-muted);
}
.palette-study .editorial-body {
  font-family: var(--font-body); font-size: 15px; line-height: 1.7;
  color: rgba(26, 26, 30, 0.78);
  max-width: 46ch;
}
.palette-study .editorial-body p { margin: 0 0 16px; }
.palette-study .editorial-body p:last-child { margin-bottom: 0; }
.palette-study .editorial-body strong {
  font-weight: 500;
  color: var(--paper-ink);
}
.palette-study .editorial-pairs {
  display: grid; grid-template-columns: 1fr 1fr; gap: 18px;
  padding-top: 24px;
  border-top: 1px solid rgba(26, 26, 30, 0.18);
}
.palette-study .editorial-pair { display: grid; gap: 6px; }
.palette-study .editorial-pair-label {
  font-family: var(--font-mono); font-size: 9px;
  letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--paper-ink-muted);
}
.palette-study .editorial-pair-value {
  font-family: var(--font-body); font-weight: 500;
  font-size: 13px; color: var(--paper-ink);
  letter-spacing: -0.005em;
}

/* ─── COLOPHON ─── */
.palette-study .colophon {
  display: grid; gap: 48px;
}
.palette-study .specimens {
  display: grid; grid-template-columns: repeat(3, 1fr); gap: 24px;
}
@media (max-width: 900px) { .palette-study .specimens { grid-template-columns: 1fr; } }
.palette-study .specimen {
  display: grid; gap: 18px;
  padding: 20px 0;
  border-top: 1px solid var(--hair);
}
.palette-study .specimen-label {
  font-family: var(--font-mono); font-size: 10px;
  letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--ink-folio);
  display: flex; justify-content: space-between;
}
.palette-study .specimen-display {
  font-family: var(--font-display); font-weight: 300;
  font-size: 44px; line-height: 1.0;
  letter-spacing: -0.035em; color: var(--ink-display);
}
.palette-study .specimen-display em { font-style: italic; font-weight: 400; color: var(--ink-accent); }
.palette-study .specimen-sans {
  font-family: var(--font-body); font-size: 15px; line-height: 1.6;
  color: var(--ink-body);
}
.palette-study .specimen-mono {
  font-family: var(--font-mono); font-size: 12px; line-height: 1.7;
  color: var(--ink-heading); letter-spacing: 0.02em;
  white-space: pre-line;
}
.palette-study .specimen-foot {
  font-family: var(--font-mono); font-size: 9px;
  letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--ink-folio);
}

.palette-study .credits {
  display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 32px;
  padding-top: 32px;
  border-top: 1px solid var(--hair);
}
@media (max-width: 760px) { .palette-study .credits { grid-template-columns: 1fr; gap: 16px; } }
.palette-study .credit-label {
  font-family: var(--font-mono); font-size: 9px;
  letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--ink-folio); margin-bottom: 10px;
}
.palette-study .credit-body {
  font-family: var(--font-body); font-size: 13px; line-height: 1.65;
  color: var(--ink-body);
}
.palette-study .credit-body strong {
  font-weight: 500; color: var(--ink-heading);
}
.palette-study .credit-body strong.wordmark-credit {
  color: var(--ink-wordmark);
}

/* ─── INTERACTION ─── */
.palette-study .motion {
  display: grid;
  gap: 42px;
}
.palette-study .motion-grid {
  display: grid;
  grid-template-columns: 1.15fr 0.85fr;
  gap: 48px;
  align-items: start;
}
@media (max-width: 980px) {
  .palette-study .motion-grid { grid-template-columns: 1fr; gap: 36px; }
}
.palette-study .motion-main,
.palette-study .motion-notes {
  display: grid;
  gap: 28px;
}
.palette-study .motion-block {
  display: grid;
  gap: 16px;
  padding-top: 18px;
  border-top: 1px solid var(--hair);
}
.palette-study .motion-label {
  font-family: var(--font-body);
  font-size: 12px;
  font-weight: 500;
  letter-spacing: 0.01em;
  color: var(--ink-heading);
}
.palette-study .motion-copy {
  font-family: var(--font-body);
  font-size: 13px;
  line-height: 1.6;
  color: var(--ink-body);
  max-width: 48ch;
}
.palette-study .motion-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 14px 18px;
  align-items: center;
}
.palette-study .motion-btn-text,
.palette-study .motion-btn-rect,
.palette-study .motion-tab,
.palette-study .motion-row {
  transition:
    color 160ms ease,
    border-color 160ms ease,
    background-color 160ms ease,
    transform 180ms cubic-bezier(0.2, 0.7, 0.2, 1),
    opacity 160ms ease;
}
.palette-study .motion-btn-text {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  font-family: var(--font-body);
  font-size: 13px;
  font-weight: 400;
  letter-spacing: 0.01em;
  color: rgba(214, 219, 226, 0.82);
  background: transparent;
  border: none;
  padding: 0;
  cursor: default;
}
.palette-study .motion-btn-text:hover {
  color: var(--ink-heading);
  transform: translateY(-1px);
}
.palette-study .motion-btn-text:focus-visible {
  outline: 1px solid rgba(214, 219, 226, 0.34);
  outline-offset: 6px;
}
.palette-study .motion-btn-rect {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  font-family: var(--font-body);
  font-size: 13px;
  font-weight: 400;
  letter-spacing: 0.01em;
  color: var(--ink-heading);
  background: transparent;
  border: 1px solid var(--hair-strong);
  padding: 10px 16px;
  cursor: default;
}
.palette-study .motion-btn-rect:hover {
  border-color: rgba(255, 255, 255, 0.22);
  background: rgba(255, 255, 255, 0.02);
  transform: translateY(-1px);
}
.palette-study .motion-btn-rect:active {
  transform: translateY(0);
  background: rgba(255, 255, 255, 0.014);
}
.palette-study .motion-btn-rect:focus-visible {
  outline: 1px solid rgba(214, 219, 226, 0.36);
  outline-offset: 4px;
}
.palette-study .motion-btn-arrow {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 11px;
  height: 11px;
  line-height: 1;
  transform: translateY(0.5px);
}
.palette-study .motion-btn-arrow svg {
  width: 100%;
  height: 100%;
  display: block;
  overflow: visible;
}
.palette-study .motion-tabs {
  display: flex;
  gap: 22px;
  flex-wrap: wrap;
  align-items: center;
}
.palette-study .motion-toolbar {
  display: flex;
  justify-content: flex-start;
  align-items: flex-start;
  gap: 24px;
}
.palette-study .motion-toolbar-tools {
  position: relative;
  display: grid;
  gap: 8px;
  justify-items: start;
}
.palette-study .motion-menu-wrap {
  display: inline-flex;
  align-items: center;
  gap: 12px;
}
.palette-study .motion-menu-text {
  font-family: var(--font-body);
  font-size: 12px;
  color: var(--ink-folio);
  letter-spacing: 0.01em;
}
.palette-study .motion-menu-btn {
  color: rgba(214, 219, 226, 0.82);
  background: transparent;
  border: none;
  padding: 4px;
  width: 26px;
  height: 22px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  transition:
    color 160ms ease,
    transform 180ms cubic-bezier(0.2, 0.7, 0.2, 1);
}
.palette-study .motion-menu-btn:hover {
  color: var(--ink-heading);
  transform: translateY(-1px);
}
.palette-study .motion-menu-btn:hover .motion-menu-line-1 {
  transform: translateX(1px);
}
.palette-study .motion-menu-btn:hover .motion-menu-line-2 {
  transform: scaleX(0.78);
}
.palette-study .motion-menu-btn:hover .motion-menu-line-3 {
  transform: translateX(-1px);
}
.palette-study .motion-menu-icon {
  width: 18px;
  height: 18px;
  display: block;
}
.palette-study .motion-menu-icon line {
  stroke: currentColor;
  stroke-width: 1.4;
  stroke-linecap: round;
  transform-box: fill-box;
  transform-origin: center;
  transition: transform 180ms cubic-bezier(0.2, 0.7, 0.2, 1), opacity 160ms ease;
}
.palette-study .motion-dropdown {
  width: 220px;
  display: grid;
  gap: 1px;
  padding: 8px 12px 3px;
  border-top: 1px solid var(--hair-strong);
  border-bottom: 1px solid var(--hair);
  background: rgba(13, 14, 18, 0.28);
}
.palette-study .motion-dropdown-label {
  font-family: var(--font-body);
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.01em;
  color: var(--ink-folio);
  padding: 0 0 5px;
}
.palette-study .motion-dropdown-item {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 14px;
  align-items: center;
  padding: 7px 0;
  border-top: 1px solid transparent;
  border-bottom: 1px solid transparent;
  color: rgba(214, 219, 226, 0.76);
  transition:
    color 160ms ease,
    border-color 160ms ease,
    transform 180ms cubic-bezier(0.2, 0.7, 0.2, 1);
}
.palette-study .motion-dropdown-item:hover {
  color: var(--ink-heading);
  border-bottom-color: rgba(255, 255, 255, 0.08);
  transform: translateX(3px);
}
.palette-study .motion-dropdown-item.active {
  color: var(--ink-heading);
}
.palette-study .motion-dropdown-key {
  font-family: var(--font-body);
  font-size: 13px;
}
.palette-study .motion-dropdown-meta {
  font-family: var(--font-mono);
  font-size: 10px;
  letter-spacing: 0.08em;
  color: var(--ink-folio);
}
.palette-study .motion-tab {
  font-family: var(--font-body);
  font-size: 13px;
  color: rgba(165, 171, 181, 0.84);
  padding: 0 0 10px;
  border-bottom: 1px solid transparent;
}
.palette-study .motion-tab.active {
  color: var(--ink-heading);
  border-bottom-color: var(--fam-cmac);
}
.palette-study .motion-tab:hover:not(.active) {
  color: rgba(218, 223, 230, 0.92);
  border-bottom-color: rgba(255, 255, 255, 0.12);
}
.palette-study .motion-rows {
  display: grid;
  border-top: 1px solid var(--hair);
}
.palette-study .motion-row {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 20px;
  padding: 14px 0;
  border-bottom: 1px solid var(--hair);
  color: var(--ink-body);
}
.palette-study .motion-row:hover {
  color: var(--ink-heading);
  transform: translateX(3px);
}
.palette-study .motion-row-title {
  font-family: var(--font-body);
  font-size: 14px;
  letter-spacing: -0.003em;
}
.palette-study .motion-row-meta {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--ink-folio);
  letter-spacing: 0.08em;
}
.palette-study .motion-specs {
  display: grid;
  gap: 14px;
  padding-top: 6px;
}
.palette-study .motion-spec {
  display: grid;
  grid-template-columns: 120px 1fr;
  gap: 18px;
  align-items: baseline;
  padding-top: 12px;
  border-top: 1px solid var(--hair);
}
@media (max-width: 560px) {
  .palette-study .motion-spec { grid-template-columns: 1fr; gap: 8px; }
}
.palette-study .motion-spec-key {
  font-family: var(--font-body);
  font-size: 12px;
  font-weight: 500;
  color: var(--ink-heading);
}
.palette-study .motion-spec-val {
  font-family: var(--font-body);
  font-size: 13px;
  line-height: 1.6;
  color: var(--ink-body);
}

/* ─── APPLIED FRAGMENTS ─── */
.palette-study .applied {
  display: grid;
  gap: 34px;
}
.palette-study .applied-grid {
  display: grid;
  grid-template-columns: 1.2fr 0.8fr;
  gap: 28px;
  align-items: start;
}
@media (max-width: 980px) {
  .palette-study .applied-grid { grid-template-columns: 1fr; }
}
.palette-study .applied-main,
.palette-study .applied-side {
  display: grid;
  gap: 22px;
}
.palette-study .applied-block {
  display: grid;
  gap: 10px;
  padding-top: 12px;
  border-top: 1px solid var(--hair);
}
.palette-study .applied-label {
  font-family: var(--font-body);
  font-size: 11px;
  font-weight: 500;
  color: var(--ink-heading);
}
.palette-study .applied-copy {
  font-family: var(--font-body);
  font-size: 12px;
  line-height: 1.45;
  color: var(--ink-body);
  max-width: 50ch;
}
.palette-study .applied-factor-legend {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}
.palette-study .applied-chip {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-height: 22px;
  padding: 0 8px;
  border: 1px solid var(--hair);
  color: rgba(214, 219, 226, 0.78);
  font-family: var(--font-body);
  font-size: 11px;
}
.palette-study .applied-chip-dot {
  width: 6px;
  height: 6px;
  background: currentColor;
}
.palette-study .applied-chip-rail {
  width: 12px;
  height: 2px;
  background: currentColor;
}
.palette-study .applied-factor-list {
  display: grid;
  gap: 0;
  position: relative;
}
.palette-study .applied-factor-list::before {
  content: "";
  position: absolute;
  top: 28px;
  bottom: 10px;
  left: calc(
    var(--applied-factor-label-col)
    + 12px
    + ((100% - var(--applied-factor-label-col) - var(--applied-factor-value-col) - 24px) / 2)
  );
  width: 1px;
  background-image: repeating-linear-gradient(
    to bottom,
    rgba(246, 246, 244, 0.18) 0,
    rgba(246, 246, 244, 0.18) 2px,
    transparent 2px,
    transparent 6px
  );
  pointer-events: none;
  z-index: 1;
}
.palette-study .applied-group-label {
  font-family: var(--font-body);
  font-size: 10px;
  font-weight: 500;
  color: var(--ink-folio);
  letter-spacing: 0.01em;
  padding: 6px 0 1px;
}
.palette-study .applied-factor-row {
  display: grid;
  grid-template-columns: var(--applied-factor-label-col) minmax(260px, 1fr) var(--applied-factor-value-col);
  align-items: center;
  gap: 12px;
  padding: 2px 0;
  border-bottom: none;
  position: relative;
}
.palette-study .applied-factor-row.selected {
  color: var(--ink-heading);
}
.palette-study .applied-factor-meta {
  display: flex;
  align-items: baseline;
  gap: 10px;
  min-width: 0;
}
.palette-study .applied-factor-title {
  font-family: var(--font-body);
  font-size: 12px;
  color: var(--ink-heading);
  white-space: nowrap;
}
.palette-study .applied-factor-id {
  font-family: var(--font-mono);
  font-size: 9px;
  color: var(--ink-folio);
  letter-spacing: 0.08em;
  white-space: nowrap;
}
.palette-study .applied-factor-value {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--ink-heading);
  white-space: nowrap;
}
.palette-study .applied-factor-track {
  position: relative;
  height: 10px;
  min-width: 0;
}
.palette-study .applied-factor-bar {
  position: absolute;
  top: 3px;
  height: 4px;
  z-index: 2;
}
.palette-study .applied-factor-bar.neg { background: rgba(217, 122, 141, 0.94); }
.palette-study .applied-factor-bar.pos { background: rgba(127, 193, 161, 0.94); }
.palette-study .applied-factor-ext {
  position: absolute;
  z-index: 2;
  top: 3px;
  height: 4px;
}
.palette-study .applied-factor-ext.fundamental.neg,
.palette-study .applied-factor-ext.fundamental.pos {
  background: var(--projection-fundamental);
}
.palette-study .applied-factor-ext.returns.neg,
.palette-study .applied-factor-ext.returns.pos {
  background: var(--projection-returns);
}
.palette-study .applied-factor-marker {
  position: absolute;
  top: 1px;
  width: 1.5px;
  height: 8px;
  background: rgba(246, 246, 244, 0.88);
  z-index: 3;
}
@media (max-width: 760px) {
  .palette-study .applied-factor-row {
    grid-template-columns: 1fr;
    gap: 5px;
  }
  .palette-study .applied-factor-meta {
    justify-content: space-between;
  }
  .palette-study .applied-factor-list::before {
    display: none;
  }
}
.palette-study .applied-search-shell {
  display: grid;
  gap: 8px;
}
.palette-study .applied-search-input {
  width: 100%;
  height: 34px;
  border: 1px solid rgba(255, 255, 255, 0.12);
  background: rgba(255, 255, 255, 0.03);
  color: rgba(232, 237, 249, 0.9);
  padding: 6px 9px;
  font-family: var(--font-body);
  font-size: 12px;
}
.palette-study .applied-search-results {
  display: grid;
  gap: 0;
}
.palette-study .applied-search-result {
  display: grid;
  gap: 4px;
  padding: 8px 0;
  border-top: 1px solid var(--hair);
  transition: transform 160ms ease, color 160ms ease, border-color 160ms ease;
}
.palette-study .applied-search-result:hover {
  transform: translateX(3px);
  color: var(--ink-heading);
}
.palette-study .applied-search-result-top {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: baseline;
}
.palette-study .applied-search-ticker {
  font-family: var(--font-body);
  font-size: 13px;
  font-weight: 500;
  color: var(--ink-heading);
}
.palette-study .applied-search-fit {
  font-family: var(--font-body);
  font-size: 10px;
  color: var(--fam-cpar);
}
.palette-study .applied-search-name,
.palette-study .applied-search-meta {
  font-family: var(--font-body);
  font-size: 11px;
  color: var(--ink-body);
}
.palette-study .applied-settings {
  display: grid;
  gap: 8px;
}
.palette-study .applied-setting-row {
  display: grid;
  gap: 8px;
  padding: 8px 0;
  border-top: 1px solid var(--hair);
}
.palette-study .applied-setting-title {
  font-family: var(--font-body);
  font-size: 12px;
  color: var(--ink-heading);
}
.palette-study .applied-setting-help {
  font-family: var(--font-body);
  font-size: 11px;
  line-height: 1.4;
  color: var(--ink-body);
}
.palette-study .applied-options {
  display: flex;
  gap: 14px;
  flex-wrap: wrap;
}
.palette-study .applied-option {
  padding: 0 0 7px;
  border-bottom: 1px solid transparent;
  font-family: var(--font-body);
  font-size: 11px;
  color: rgba(214, 219, 226, 0.74);
}
.palette-study .applied-option.active {
  color: var(--ink-heading);
  border-bottom-color: var(--fam-cmac);
}
.palette-study .applied-statline {
  display: grid;
  gap: 0;
}
.palette-study .applied-stat {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 16px;
  padding: 7px 0;
  border-top: 1px solid var(--hair);
}
.palette-study .applied-stat-key {
  font-family: var(--font-body);
  font-size: 11px;
  color: var(--ink-body);
}
.palette-study .applied-stat-val {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--ink-heading);
}
.palette-study .applied-table {
  display: grid;
  gap: 0;
  border-top: 1px solid var(--hair);
}
.palette-study .applied-table-row {
  display: grid;
  grid-template-columns: 72px minmax(180px, 1.4fr) 78px 110px 78px 126px;
  gap: 12px;
  align-items: center;
  padding: 8px 0;
  border-bottom: 1px solid rgba(255, 255, 255, 0.035);
  transition: background-color 160ms ease;
}
.palette-study .applied-table-row.head {
  padding: 6px 0;
  border-bottom-color: var(--hair);
}
.palette-study .applied-table-row.data:nth-child(odd) {
  background: rgba(255, 255, 255, 0.014);
}
.palette-study .applied-table-row:hover {
  color: var(--ink-heading);
}
.palette-study .applied-th {
  font-family: var(--font-body);
  font-size: 10px;
  font-weight: 500;
  color: var(--ink-folio);
  letter-spacing: 0.03em;
}
.palette-study .applied-th.right,
.palette-study .applied-td.right {
  text-align: right;
}
.palette-study .applied-td {
  font-family: var(--font-body);
  font-size: 12px;
  color: var(--ink-body);
  min-width: 0;
}
.palette-study .applied-td.ticker {
  color: var(--ink-heading);
  font-weight: 500;
}
.palette-study .applied-td.name {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.palette-study .applied-td.mono {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--ink-heading);
}
.palette-study .applied-td.pos {
  color: var(--sig-pos);
}
.palette-study .applied-td.neg {
  color: var(--sig-neg);
}
.palette-study .applied-method {
  display: inline;
  color: rgba(214, 219, 226, 0.74);
  font-family: var(--font-body);
  font-size: 11px;
  white-space: nowrap;
}
.palette-study .applied-method.core {
  color: var(--method-core);
}
.palette-study .applied-method.projection {
  color: var(--method-projection);
}
.palette-study .applied-method.fundamental {
  color: var(--method-projection);
}
.palette-study .applied-method.returns {
  color: var(--method-projection);
}
@media (max-width: 980px) {
  .palette-study .applied-table-row {
    grid-template-columns: 64px minmax(140px, 1fr) 68px 92px 66px 110px;
    gap: 10px;
  }
}
@media (max-width: 760px) {
  .palette-study .applied-table-row {
    grid-template-columns: 64px minmax(120px, 1fr) 68px 92px;
  }
  .palette-study .applied-table-row > :nth-child(5),
  .palette-study .applied-table-row > :nth-child(6) {
    display: none;
  }
}

/* ─── SURFACES ─── */
.palette-study .surfaces {
  display: grid;
  gap: 34px;
}
.palette-study .surfaces-grid {
  display: grid;
  grid-template-columns: 1.05fr 0.95fr;
  gap: 28px;
  align-items: start;
}
@media (max-width: 980px) {
  .palette-study .surfaces-grid { grid-template-columns: 1fr; }
}
.palette-study .surfaces-main,
.palette-study .surfaces-side {
  display: grid;
  gap: 22px;
}
.palette-study .surface-block {
  display: grid;
  gap: 10px;
  padding-top: 12px;
  border-top: 1px solid var(--hair);
}
.palette-study .surface-label {
  font-family: var(--font-body);
  font-size: 11px;
  font-weight: 500;
  color: var(--ink-heading);
}
.palette-study .surface-copy {
  font-family: var(--font-body);
  font-size: 12px;
  line-height: 1.45;
  color: var(--ink-body);
  max-width: 52ch;
}
.palette-study .surface-panel {
  display: grid;
  gap: 8px;
  padding: 14px 16px 12px;
  background: rgba(14, 17, 24, 0.22);
  border: 1px solid rgba(255, 255, 255, 0.045);
}
.palette-study .surface-panel-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
}
.palette-study .surface-panel-title {
  font-family: var(--font-body);
  font-size: 14px;
  color: var(--ink-heading);
}
.palette-study .surface-panel-meta {
  font-family: var(--font-body);
  font-size: 10px;
  color: var(--ink-folio);
  letter-spacing: 0.03em;
}
.palette-study .surface-panel-close {
  font-family: var(--font-body);
  font-size: 10px;
  color: rgba(232, 237, 249, 0.74);
  letter-spacing: 0.04em;
}
.palette-study .surface-panel-text {
  font-family: var(--font-body);
  font-size: 12px;
  line-height: 1.5;
  color: var(--ink-body);
}
.palette-study .surface-mini-table {
  display: grid;
  gap: 0;
  margin-top: 4px;
}
.palette-study .surface-mini-row {
  display: grid;
  grid-template-columns: 1.2fr 0.7fr 0.7fr 0.7fr;
  gap: 10px;
  padding: 6px 0;
  border-top: 1px solid rgba(255, 255, 255, 0.035);
}
.palette-study .surface-mini-row.head {
  padding-top: 2px;
  border-top: none;
}
.palette-study .surface-mini-th {
  font-family: var(--font-body);
  font-size: 10px;
  font-weight: 500;
  color: var(--ink-folio);
  letter-spacing: 0.03em;
}
.palette-study .surface-mini-td {
  font-family: var(--font-body);
  font-size: 11px;
  color: var(--ink-body);
}
.palette-study .surface-mini-td.num {
  font-family: var(--font-mono);
  color: var(--ink-heading);
  text-align: right;
}
.palette-study .surface-hover-wrap {
  position: relative;
  display: grid;
  gap: 8px;
}
.palette-study .surface-hover-line {
  position: relative;
  height: 84px;
  border-top: 1px solid rgba(255, 255, 255, 0.04);
  border-bottom: 1px solid rgba(255, 255, 255, 0.04);
}
.palette-study .surface-hover-line::before {
  content: "";
  position: absolute;
  left: 58%;
  top: 0;
  bottom: 0;
  width: 1px;
  background: rgba(246, 246, 245, 0.18);
}
.palette-study .surface-hover-line::after {
  content: "";
  position: absolute;
  inset: 0;
  background:
    linear-gradient(180deg, transparent 0%, rgba(255,255,255,0.015) 50%, transparent 100%);
}
.palette-study .surface-hover-tip {
  position: absolute;
  left: calc(58% + 8px);
  top: 10px;
  min-width: 144px;
  padding: 8px 10px;
  background: rgba(22, 25, 33, 0.94);
  border: 1px solid rgba(196, 203, 220, 0.22);
  color: var(--chart-tooltip-text);
  font-family: var(--font-body);
  font-size: 11px;
  line-height: 1.4;
}
.palette-study .surface-tip-title {
  color: var(--ink-heading);
  margin-bottom: 4px;
}
.palette-study .surface-form {
  display: grid;
  gap: 10px;
}
.palette-study .surface-input,
.palette-study .surface-select {
  width: 100%;
  height: 34px;
  border: 1px solid rgba(226, 231, 238, 0.14);
  background: rgba(226, 231, 238, 0.035);
  color: rgba(232, 237, 249, 0.9);
  padding: 6px 9px;
  font-family: var(--font-body);
  font-size: 12px;
}
.palette-study .surface-input:focus,
.palette-study .surface-select:focus {
  outline: none;
  border-color: rgba(196, 203, 220, 0.24);
  box-shadow: 0 0 0 1px rgba(196, 203, 220, 0.06);
}
.palette-study .surface-typeahead {
  display: grid;
  gap: 0;
  border-top: 1px solid var(--hair);
}
.palette-study .surface-typeahead-row {
  display: grid;
  grid-template-columns: 64px 1fr auto;
  gap: 10px;
  padding: 7px 0;
  border-bottom: 1px solid rgba(255,255,255,0.03);
}
.palette-study .surface-typeahead-row.active {
  color: var(--ink-heading);
}
.palette-study .surface-typeahead-row .ticker {
  font-family: var(--font-body);
  font-size: 12px;
  font-weight: 500;
  color: var(--ink-heading);
}
.palette-study .surface-typeahead-row .name {
  font-family: var(--font-body);
  font-size: 11px;
  color: var(--ink-body);
}
.palette-study .surface-typeahead-row .meta {
  font-family: var(--font-body);
  font-size: 10px;
  color: var(--ink-folio);
}
.palette-study .surface-modal-backdrop {
  display: grid;
  place-items: center;
  min-height: 240px;
  background: rgba(5, 7, 10, 0.22);
}
.palette-study .surface-modal {
  width: min(100%, 360px);
  display: grid;
  gap: 10px;
  padding: 16px;
  background: rgba(18, 21, 28, 0.94);
  border: 1px solid rgba(255,255,255,0.06);
}
.palette-study .surface-modal-title {
  font-family: var(--font-body);
  font-size: 15px;
  color: var(--ink-heading);
}
.palette-study .surface-modal-body {
  font-family: var(--font-body);
  font-size: 12px;
  line-height: 1.5;
  color: var(--ink-body);
}
.palette-study .surface-modal-label {
  font-family: var(--font-body);
  font-size: 11px;
  color: var(--ink-body);
}
.palette-study .surface-modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  margin-top: 4px;
}

/* ─── LIGHT MODE STUDY ─── */
.palette-study .lightmode {
  display: grid;
  gap: 40px;
}
.palette-study .lightmode-shell {
  margin: 0 -48px;
  padding: 88px 72px;
  background: linear-gradient(180deg, ${LIGHT_PALETTE.shell.top} 0%, ${LIGHT_PALETTE.shell.base} 52%, ${LIGHT_PALETTE.shell.deep} 100%);
  color: ${LIGHT_PALETTE.ink.body};
  position: relative;
  overflow: hidden;
}
@media (max-width: 900px) {
  .palette-study .lightmode-shell {
    margin: 0 -20px;
    padding: 56px 28px;
  }
}
.palette-study .lightmode-shell > * {
  position: relative;
  z-index: 1;
}
.palette-study .lightmode-head {
  display: grid;
  gap: 14px;
  max-width: 760px;
}
.palette-study .lightmode-folio {
  font-family: var(--font-body);
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: ${LIGHT_PALETTE.ink.folio};
}
.palette-study .lightmode-title {
  font-family: var(--font-display);
  font-weight: 320;
  font-size: clamp(36px, 5vw, 68px);
  line-height: 1.06;
  letter-spacing: -0.04em;
  color: ${LIGHT_PALETTE.ink.display};
  margin: 0;
}
.palette-study .lightmode-title em {
  font-style: italic;
  font-weight: 380;
  color: rgba(72, 76, 84, 0.72);
}
.palette-study .lightmode-copy {
  font-family: var(--font-body);
  font-size: 15px;
  line-height: 1.7;
  color: ${LIGHT_PALETTE.ink.body};
  max-width: 58ch;
  margin: 0;
}
.palette-study .lightmode-grid {
  display: grid;
  grid-template-columns: 1.18fr 0.82fr;
  gap: 40px;
  align-items: start;
}
@media (max-width: 980px) {
  .palette-study .lightmode-grid {
    grid-template-columns: 1fr;
    gap: 28px;
  }
}
.palette-study .lightmode-main,
.palette-study .lightmode-side {
  display: grid;
  gap: 26px;
}
.palette-study .lightmode-palette {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 18px;
}
@media (max-width: 980px) {
  .palette-study .lightmode-palette {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}
@media (max-width: 640px) {
  .palette-study .lightmode-palette {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
.palette-study .lightmode-chip {
  display: grid;
  gap: 10px;
}
.palette-study .lightmode-chip-swatch {
  height: 42px;
  border: 1px solid rgba(31, 34, 39, 0.08);
}
.palette-study .lightmode-chip-meta {
  display: grid;
  gap: 5px;
}
.palette-study .lightmode-chip-hex {
  font-family: var(--font-mono);
  font-size: 10px;
  color: ${LIGHT_PALETTE.ink.heading};
  letter-spacing: 0.04em;
}
.palette-study .lightmode-chip-name {
  font-family: var(--font-body);
  font-size: 12px;
  font-weight: 500;
  color: ${LIGHT_PALETTE.ink.heading};
}
.palette-study .lightmode-chip-role {
  font-family: var(--font-body);
  font-size: 11px;
  color: ${LIGHT_PALETTE.ink.muted};
  line-height: 1.45;
}
.palette-study .lightmode-block {
  display: grid;
  gap: 14px;
  padding-top: 14px;
  border-top: 1px solid ${LIGHT_PALETTE.surface.hair};
}
.palette-study .lightmode-label {
  font-family: var(--font-body);
  font-size: 12px;
  font-weight: 500;
  letter-spacing: 0.01em;
  color: ${LIGHT_PALETTE.ink.heading};
}
.palette-study .lightmode-subcopy {
  font-family: var(--font-body);
  font-size: 13px;
  line-height: 1.6;
  color: ${LIGHT_PALETTE.ink.muted};
  max-width: 52ch;
}
.palette-study .lightmode-factor-list {
  position: relative;
  display: grid;
  gap: 8px;
}
.palette-study .lightmode-factor-list::before {
  content: "";
  position: absolute;
  top: 4px;
  bottom: 4px;
  left: calc(50% + 49px);
  border-left: 1px dotted rgba(49, 53, 60, 0.18);
  pointer-events: none;
}
.palette-study .lightmode-factor-row {
  display: grid;
  grid-template-columns: 180px 1fr 82px;
  gap: 14px;
  align-items: center;
  min-height: 24px;
}
.palette-study .lightmode-factor-name {
  font-family: var(--font-body);
  font-size: 13px;
  color: ${LIGHT_PALETTE.ink.heading};
}
.palette-study .lightmode-factor-track {
  position: relative;
  height: 18px;
}
.palette-study .lightmode-factor-track .bar,
.palette-study .lightmode-factor-track .ext,
.palette-study .lightmode-factor-track .mark {
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
}
.palette-study .lightmode-factor-track .bar,
.palette-study .lightmode-factor-track .ext {
  height: 6px;
}
.palette-study .lightmode-factor-track .bar.neg { background: ${LIGHT_PALETTE.signals.negative}; }
.palette-study .lightmode-factor-track .bar.pos { background: ${LIGHT_PALETTE.signals.positive}; }
.palette-study .lightmode-factor-track .ext.fundamental { background: rgba(118, 126, 138, 0.34); }
.palette-study .lightmode-factor-track .ext.returns { background: rgba(118, 126, 138, 0.20); }
.palette-study .lightmode-factor-track .mark {
  width: 2px;
  height: 14px;
  background: rgba(31, 34, 39, 0.78);
}
.palette-study .lightmode-factor-value {
  font-family: var(--font-mono);
  font-size: 12px;
  color: ${LIGHT_PALETTE.ink.heading};
  text-align: right;
  font-feature-settings: "tnum";
}
.palette-study .lightmode-table {
  display: grid;
}
.palette-study .lightmode-table-row {
  display: grid;
  grid-template-columns: 0.52fr 1.5fr 0.72fr 0.86fr 0.9fr;
  gap: 12px;
  align-items: baseline;
  padding: 10px 0;
  border-bottom: 1px solid ${LIGHT_PALETTE.surface.hair};
  transition: background-color 160ms ease, transform 180ms cubic-bezier(0.2, 0.7, 0.2, 1);
}
.palette-study .lightmode-table-row.head {
  padding-top: 0;
}
.palette-study .lightmode-table-row.data:hover {
  background: rgba(255, 255, 255, 0.18);
  transform: translateX(2px);
}
.palette-study .lightmode-table-row.data.selected {
  background: rgba(255, 255, 255, 0.24);
}
.palette-study .lightmode-table-row.head span {
  font-family: var(--font-mono);
  font-size: 9px;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: ${LIGHT_PALETTE.ink.folio};
}
.palette-study .lightmode-table-row .ticker,
.palette-study .lightmode-table-row .method {
  font-family: var(--font-body);
  font-size: 12px;
  color: ${LIGHT_PALETTE.ink.heading};
}
.palette-study .lightmode-table-row .name {
  font-family: var(--font-body);
  font-size: 13px;
  color: ${LIGHT_PALETTE.ink.body};
}
.palette-study .lightmode-table-row .num {
  font-family: var(--font-mono);
  font-size: 12px;
  text-align: right;
  font-feature-settings: "tnum";
}
.palette-study .lightmode-table-row .num.pos { color: ${LIGHT_PALETTE.signals.positive}; }
.palette-study .lightmode-table-row .num.neg { color: ${LIGHT_PALETTE.signals.negative}; }
.palette-study .lightmode-table-row .method { color: rgba(177, 145, 76, 0.92); }
.palette-study .lightmode-panel {
  display: grid;
  gap: 16px;
  padding: 18px 18px 16px;
  background: ${LIGHT_PALETTE.surface.lift};
  border: 1px solid ${LIGHT_PALETTE.surface.hair};
  backdrop-filter: blur(2px);
}
.palette-study .lightmode-panel-head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: baseline;
  padding-bottom: 12px;
  border-bottom: 1px solid ${LIGHT_PALETTE.surface.hair};
}
.palette-study .lightmode-wordmark {
  font-family: var(--font-body);
  font-size: 17px;
  font-weight: 600;
  letter-spacing: -0.01em;
  color: ${LIGHT_PALETTE.ink.display};
}
.palette-study .lightmode-link {
  font-family: var(--font-body);
  font-size: 12px;
  color: ${LIGHT_PALETTE.ink.muted};
}
.palette-study .lightmode-tabs {
  display: flex;
  gap: 18px;
  flex-wrap: wrap;
}
.palette-study .lightmode-tab {
  font-family: var(--font-body);
  font-size: 12px;
  padding: 0 0 8px;
  color: ${LIGHT_PALETTE.ink.muted};
  border-bottom: 1px solid transparent;
}
.palette-study .lightmode-tab.active {
  color: ${LIGHT_PALETTE.ink.heading};
  border-bottom-color: var(--fam-cmac);
}
.palette-study .lightmode-actions {
  display: flex;
  gap: 14px;
  flex-wrap: wrap;
  align-items: center;
}
.palette-study .lightmode-btn-text,
.palette-study .lightmode-btn-rect {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  font-family: var(--font-body);
  font-size: 13px;
  font-weight: 400;
}
.palette-study .lightmode-btn-text {
  color: ${LIGHT_PALETTE.ink.heading};
}
.palette-study .lightmode-btn-rect {
  color: ${LIGHT_PALETTE.ink.heading};
  border: 1px solid ${LIGHT_PALETTE.surface.hairStrong};
  padding: 9px 14px;
  background: rgba(255, 255, 255, 0.26);
}
.palette-study .lightmode-search {
  display: grid;
  gap: 8px;
}
.palette-study .lightmode-input {
  width: 100%;
  padding: 11px 0;
  border: none;
  border-bottom: 1px solid ${LIGHT_PALETTE.surface.hairStrong};
  background: transparent;
  color: ${LIGHT_PALETTE.ink.heading};
  font-family: var(--font-body);
  font-size: 14px;
}
.palette-study .lightmode-search-row {
  display: grid;
  grid-template-columns: auto 1fr auto;
  gap: 12px;
  align-items: baseline;
  padding: 8px 0;
  border-bottom: 1px solid ${LIGHT_PALETTE.surface.hair};
}
.palette-study .lightmode-search-row .tk {
  font-family: var(--font-body);
  font-size: 12px;
  font-weight: 600;
  color: ${LIGHT_PALETTE.ink.heading};
}
.palette-study .lightmode-search-row .nm {
  font-family: var(--font-body);
  font-size: 13px;
  color: ${LIGHT_PALETTE.ink.body};
}
.palette-study .lightmode-search-row .fit {
  font-family: var(--font-body);
  font-size: 12px;
  color: ${LIGHT_PALETTE.ink.muted};
}
.palette-study .lightmode-surface-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 22px;
}
@media (max-width: 980px) {
  .palette-study .lightmode-surface-grid {
    grid-template-columns: 1fr;
  }
}
.palette-study .lightmode-editorial-card {
  display: grid;
  gap: 14px;
  padding: 22px 22px 20px;
  background: #f7f3ec;
  border: 1px solid rgba(72, 60, 42, 0.10);
}
.palette-study .lightmode-editorial-folio {
  font-family: var(--font-body);
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: rgba(117, 108, 93, 0.72);
}
.palette-study .lightmode-editorial-title {
  font-family: var(--font-display);
  font-size: 32px;
  line-height: 1.02;
  letter-spacing: -0.035em;
  color: rgba(30, 31, 34, 0.94);
  margin: 0;
}
.palette-study .lightmode-editorial-copy {
  font-family: var(--font-body);
  font-size: 13px;
  line-height: 1.65;
  color: rgba(67, 63, 58, 0.82);
}
.palette-study .lightmode-hover-demo {
  position: relative;
  display: grid;
  gap: 10px;
  min-height: 132px;
}
.palette-study .lightmode-mini-chart {
  position: relative;
  height: 82px;
  border-top: 1px solid ${LIGHT_PALETTE.surface.hair};
  border-bottom: 1px solid ${LIGHT_PALETTE.surface.hair};
  background:
    linear-gradient(180deg, transparent 0%, rgba(31, 34, 39, 0.02) 100%);
}
.palette-study .lightmode-mini-bar {
  position: absolute;
  left: 10%;
  right: 8%;
  top: 34px;
  height: 8px;
  background: linear-gradient(90deg, ${LIGHT_PALETTE.signals.negative} 0 24%, transparent 24% 48%, ${LIGHT_PALETTE.signals.positive} 48% 100%);
  opacity: 0.9;
}
.palette-study .lightmode-mini-marker {
  position: absolute;
  left: 61%;
  top: 24px;
  width: 2px;
  height: 28px;
  background: rgba(31, 34, 39, 0.76);
}
.palette-study .lightmode-tooltip {
  position: absolute;
  left: calc(61% + 10px);
  top: 12px;
  min-width: 152px;
  padding: 9px 10px;
  background: rgba(252, 251, 248, 0.96);
  border: 1px solid rgba(31, 34, 39, 0.12);
  color: ${LIGHT_PALETTE.ink.body};
  font-family: var(--font-body);
  font-size: 11px;
  line-height: 1.45;
  box-shadow: 0 6px 18px rgba(31, 34, 39, 0.06);
}
.palette-study .lightmode-tooltip-title {
  color: ${LIGHT_PALETTE.ink.heading};
  margin-bottom: 4px;
  font-weight: 500;
}
.palette-study .lightmode-dropdown {
  display: grid;
  gap: 1px;
  padding: 9px 12px 4px;
  background: rgba(255, 255, 255, 0.46);
  border-top: 1px solid ${LIGHT_PALETTE.surface.hairStrong};
  border-bottom: 1px solid ${LIGHT_PALETTE.surface.hair};
}
.palette-study .lightmode-dropdown-label {
  font-family: var(--font-body);
  font-size: 11px;
  font-weight: 500;
  color: ${LIGHT_PALETTE.ink.folio};
  padding-bottom: 4px;
}
.palette-study .lightmode-dropdown-row {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 12px;
  align-items: center;
  padding: 7px 0;
  border-bottom: 1px solid transparent;
  color: ${LIGHT_PALETTE.ink.body};
}
.palette-study .lightmode-dropdown-row.active {
  color: ${LIGHT_PALETTE.ink.heading};
}
.palette-study .lightmode-dropdown-key {
  font-family: var(--font-body);
  font-size: 13px;
}
.palette-study .lightmode-dropdown-meta {
  font-family: var(--font-mono);
  font-size: 10px;
  letter-spacing: 0.08em;
  color: ${LIGHT_PALETTE.ink.folio};
}
.palette-study .lightmode-modal-backdrop {
  display: grid;
  place-items: center;
  min-height: 180px;
  background: rgba(31, 34, 39, 0.04);
}
.palette-study .lightmode-modal {
  width: min(100%, 340px);
  display: grid;
  gap: 10px;
  padding: 16px;
  background: rgba(251, 250, 247, 0.97);
  border: 1px solid rgba(31, 34, 39, 0.12);
  box-shadow: 0 10px 28px rgba(31, 34, 39, 0.08);
}
.palette-study .lightmode-modal-title {
  font-family: var(--font-body);
  font-size: 15px;
  color: ${LIGHT_PALETTE.ink.heading};
}
.palette-study .lightmode-modal-body,
.palette-study .lightmode-modal-label {
  font-family: var(--font-body);
  font-size: 12px;
  line-height: 1.55;
  color: ${LIGHT_PALETTE.ink.body};
}
.palette-study .lightmode-modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
}

/* ─── SYMBOL EXPLORATION ─── */
.palette-study .symbol-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 24px;
}
@media (max-width: 980px) {
  .palette-study .symbol-grid {
    grid-template-columns: 1fr 1fr;
  }
}
@media (max-width: 680px) {
  .palette-study .symbol-grid {
    grid-template-columns: 1fr;
  }
}
.palette-study .symbol-card {
  display: grid;
  gap: 14px;
  padding-top: 16px;
  border-top: 1px solid var(--hair);
}
.palette-study .symbol-plate {
  min-height: 180px;
  display: grid;
  place-items: center;
  background: transparent;
}
.palette-study .symbol-svg {
  width: min(100%, 220px);
  height: auto;
  overflow: visible;
}
.palette-study .symbol-fill {
  fill: rgba(236, 239, 243, 0.94);
}
.palette-study .symbol-outline {
  fill: none;
  stroke: rgba(236, 239, 243, 0.94);
  stroke-width: 4;
  stroke-linejoin: miter;
  vector-effect: non-scaling-stroke;
}
.palette-study .symbol-meta {
  display: grid;
  gap: 6px;
}
.palette-study .symbol-name {
  font-family: var(--font-body);
  font-size: 14px;
  font-weight: 500;
  color: var(--ink-heading);
}
.palette-study .symbol-copy {
  font-family: var(--font-body);
  font-size: 13px;
  line-height: 1.55;
  color: var(--ink-body);
  max-width: 30ch;
}
.palette-study .symbol-note {
  font-family: var(--font-body);
  font-size: 12px;
  color: var(--ink-folio);
}

/* ─── BRAND LOCKUPS ─── */
.palette-study .brand-lockups {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
  margin-top: 48px;
}
@media (max-width: 820px) {
  .palette-study .brand-lockups { grid-template-columns: 1fr; }
}
.palette-study .brand-tile {
  display: grid;
  grid-template-rows: 1fr auto;
  gap: 20px;
  padding: 34px 28px 18px;
  border: 1px solid var(--hair);
  background: rgba(255, 255, 255, 0.012);
  min-height: 220px;
}
.palette-study .brand-tile.wide { grid-column: span 2; }
@media (max-width: 820px) {
  .palette-study .brand-tile.wide { grid-column: auto; }
}
.palette-study .brand-tile-surface {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  min-height: 0;
}
.palette-study .brand-tile-caption {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  padding-top: 12px;
  border-top: 1px solid var(--hair);
}
.palette-study .brand-tile-label {
  font-family: var(--font-body);
  font-size: 10px;
  font-weight: 500;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--ink-folio);
}
.palette-study .brand-tile-note {
  font-family: var(--font-body);
  font-size: 11px;
  color: var(--ink-folio);
}
.palette-study .brand-mark {
  display: block;
  color: rgba(236, 239, 243, 0.96);
  fill: currentColor;
  stroke: currentColor;
}
.palette-study .brand-mark rect[data-outline="true"] {
  fill: none;
  stroke-width: 3;
  vector-effect: non-scaling-stroke;
}
.palette-study .brand-wordmark {
  font-family: var(--font-body);
  font-weight: 500;
  color: var(--ink-heading);
  letter-spacing: -0.01em;
  line-height: 1;
}
.palette-study .brand-primary {
  display: flex;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
}
.palette-study .brand-primary .brand-mark { width: auto; height: 24px; }
.palette-study .brand-primary .brand-wordmark { font-size: 34px; }
.palette-study .brand-primary-tag {
  padding-left: 24px;
  border-left: 1px solid var(--hair);
  font-family: var(--font-body);
  font-size: 12px;
  line-height: 1.5;
  color: var(--ink-body);
  max-width: 30ch;
}
.palette-study .brand-stack {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 10px;
}
.palette-study .brand-stack .brand-mark { width: 46px; }
.palette-study .brand-stack .brand-wordmark { font-size: 22px; }
.palette-study .brand-chrome {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 14px;
  padding: 12px 14px;
  border: 1px solid rgba(255, 255, 255, 0.06);
  background: rgba(255, 255, 255, 0.02);
}
.palette-study .brand-chrome-left {
  display: flex;
  align-items: center;
  gap: 7px;
}
.palette-study .brand-chrome .brand-mark { width: auto; height: 12px; }
.palette-study .brand-chrome .brand-wordmark { font-size: 15px; }
.palette-study .brand-chrome-rule {
  height: 14px;
  width: 1px;
  background: rgba(255, 255, 255, 0.12);
  margin: 0 4px;
}
.palette-study .brand-chrome-nav {
  display: flex;
  gap: 16px;
  font-family: var(--font-body);
  font-size: 12px;
  color: rgba(214, 219, 226, 0.72);
}
.palette-study .brand-chrome-nav .active { color: var(--ink-heading); }
.palette-study .brand-chrome-cta {
  margin-left: auto;
  font-family: var(--font-body);
  font-size: 11px;
  color: var(--ink-heading);
  padding: 5px 10px;
  border: 1px solid rgba(255, 255, 255, 0.14);
}
.palette-study .brand-paper {
  width: 100%;
  padding: 28px 26px 22px;
  background: var(--paper-card);
  color: var(--paper-ink);
  display: grid;
  grid-template-columns: auto 1fr;
  grid-template-rows: auto auto auto;
  row-gap: 16px;
  column-gap: 18px;
  align-items: center;
}
.palette-study .brand-paper .brand-mark { width: auto; height: 22px; color: var(--paper-ink); }
.palette-study .brand-paper-wordmark {
  font-family: var(--font-body);
  font-weight: 500;
  font-size: 26px;
  color: var(--paper-ink);
  letter-spacing: -0.01em;
}
.palette-study .brand-paper-copy {
  grid-column: 1 / -1;
  font-family: var(--font-body);
  font-size: 12px;
  line-height: 1.55;
  color: rgba(45, 40, 32, 0.66);
  max-width: 44ch;
}
.palette-study .brand-paper-meta {
  grid-column: 1 / -1;
  display: flex;
  justify-content: space-between;
  font-family: var(--font-body);
  font-size: 9px;
  font-weight: 500;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: rgba(45, 40, 32, 0.54);
  padding-top: 10px;
  border-top: 1px solid rgba(45, 40, 32, 0.14);
}
.palette-study .brand-scale {
  width: 100%;
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 24px;
  align-items: center;
}
.palette-study .brand-scale-item {
  display: flex;
  align-items: center;
  gap: 10px;
}
.palette-study .brand-scale-item.lg .brand-mark { width: 52px; }
.palette-study .brand-scale-item.lg .brand-wordmark { font-size: 22px; }
.palette-study .brand-scale-item.md .brand-mark { width: 28px; }
.palette-study .brand-scale-item.md .brand-wordmark { font-size: 14px; }
.palette-study .brand-scale-item.sm .brand-mark { width: 16px; }
.palette-study .brand-scale-item.sm .brand-wordmark { font-size: 11px; }

.palette-study .brand-icon {
  width: 112px;
  height: 112px;
  display: grid;
  place-items: center;
  background: rgba(236, 239, 243, 0.96);
  color: var(--shell-base);
}
.palette-study .brand-icon .brand-mark { width: 64px; }
.palette-study .brand-icon-row {
  display: flex;
  align-items: center;
  gap: 22px;
}
.palette-study .brand-icon-meta {
  display: grid;
  gap: 6px;
  font-family: var(--font-body);
  font-size: 12px;
  color: var(--ink-body);
  max-width: 26ch;
  line-height: 1.5;
}
.palette-study .brand-icon-meta strong {
  font-weight: 500;
  color: var(--ink-heading);
}

.palette-study .brand-header {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 18px;
  padding: 16px 22px;
  background: linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0));
  border: 1px solid rgba(255, 255, 255, 0.06);
}
.palette-study .brand-header-left {
  display: flex;
  align-items: center;
  gap: 8px;
}
.palette-study .brand-header .brand-mark { width: auto; height: 14px; }
.palette-study .brand-header .brand-wordmark { font-size: 17px; }
.palette-study .brand-header-rule {
  width: 1px;
  height: 16px;
  background: rgba(255, 255, 255, 0.12);
  margin: 0 8px;
}
.palette-study .brand-header-nav {
  display: flex;
  gap: 20px;
  font-family: var(--font-body);
  font-size: 12px;
  color: rgba(214, 219, 226, 0.72);
}
.palette-study .brand-header-nav .active { color: var(--ink-heading); }
.palette-study .brand-header-right {
  margin-left: auto;
  display: flex;
  align-items: center;
  gap: 14px;
  font-family: var(--font-body);
  font-size: 11px;
  color: var(--ink-body);
}
.palette-study .brand-header-avatar {
  width: 22px;
  height: 22px;
  background: rgba(236, 239, 243, 0.12);
  border: 1px solid rgba(255, 255, 255, 0.12);
  display: grid;
  place-items: center;
  font-family: var(--font-body);
  font-size: 10px;
  font-weight: 500;
  color: var(--ink-heading);
  letter-spacing: 0;
}

.palette-study .brand-light {
  width: 100%;
  padding: 28px 26px 22px;
  background: #e8e6e0;
  color: #1f2024;
  display: flex;
  flex-direction: column;
  gap: 20px;
}
.palette-study .brand-light-row {
  display: flex;
  align-items: center;
  gap: 10px;
}
.palette-study .brand-light .brand-mark { width: auto; height: 22px; color: #1f2024; }
.palette-study .brand-light-wordmark {
  font-family: var(--font-body);
  font-weight: 500;
  font-size: 26px;
  color: #1f2024;
  letter-spacing: -0.01em;
}
.palette-study .brand-light-copy {
  font-family: var(--font-body);
  font-size: 12px;
  line-height: 1.55;
  color: rgba(31, 32, 36, 0.66);
  max-width: 46ch;
}
.palette-study .brand-light-rule {
  height: 1px;
  background: rgba(31, 32, 36, 0.12);
}
.palette-study .brand-light-foot {
  display: flex;
  justify-content: space-between;
  font-family: var(--font-body);
  font-size: 10px;
  font-weight: 500;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: rgba(31, 32, 36, 0.56);
}

.palette-study .brand-favicon-grid {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 16px;
}
.palette-study .brand-favicon {
  display: grid;
  place-items: center;
  background: transparent;
  color: rgba(236, 239, 243, 0.96);
}
.palette-study .brand-favicon .brand-mark {
  color: inherit;
  width: 78%;
}
.palette-study .brand-favicon.xl { width: 64px; height: 64px; }
.palette-study .brand-favicon.lg { width: 40px; height: 40px; }
.palette-study .brand-favicon.md { width: 28px; height: 28px; }
.palette-study .brand-favicon.sm { width: 20px; height: 20px; }
.palette-study .brand-favicon.xs { width: 14px; height: 14px; }
.palette-study .brand-favicon-labels {
  margin-left: auto;
  display: flex;
  gap: 12px;
  font-family: var(--font-mono);
  font-size: 10px;
  color: var(--ink-folio);
  letter-spacing: 0.06em;
}

.palette-study .brand-stamp {
  width: 100%;
  display: grid;
  place-items: center;
  padding: 32px 0 18px;
}
.palette-study .brand-stamp-grid {
  display: flex;
  align-items: end;
  justify-content: center;
  gap: 28px;
  flex-wrap: wrap;
}
.palette-study .brand-stamp-size {
  display: grid;
  justify-items: center;
  gap: 10px;
}
.palette-study .brand-stamp .brand-mark { width: 180px; }
.palette-study .brand-stamp-size.lg .brand-mark { width: 180px; }
.palette-study .brand-stamp-size.md .brand-mark { width: 132px; }
.palette-study .brand-stamp-size.sm .brand-mark { width: 94px; }
.palette-study .brand-stamp-size-label {
  font-family: var(--font-mono);
  font-size: 10px;
  letter-spacing: 0.08em;
  color: var(--ink-folio);
}
.palette-study .brand-stamp-caption {
  margin-top: 16px;
  text-align: center;
  font-family: var(--font-body);
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--ink-folio);
}
.palette-study .brand-outline-lockup {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 22px;
  flex-wrap: wrap;
}
.palette-study .brand-outline-lockup .brand-mark {
  width: auto;
  height: 42px;
}
.palette-study .brand-outline-word {
  font-family: var(--font-body);
  font-weight: 500;
  font-size: 30px;
  line-height: 1;
  letter-spacing: -0.014em;
  color: var(--ink-heading);
}
.palette-study .brand-outline-copy {
  width: 100%;
  max-width: 42ch;
  font-family: var(--font-body);
  font-size: 12px;
  line-height: 1.55;
  color: var(--ink-body);
}
.palette-study .brand-outline-chip {
  width: 72px;
  height: 72px;
  display: grid;
  place-items: center;
  background: rgba(236, 239, 243, 0.96);
  color: var(--shell-base);
}
.palette-study .brand-outline-chip .brand-mark {
  width: 44px;
}
.palette-study .brand-outline-chip-row {
  display: flex;
  align-items: center;
  gap: 18px;
}
.palette-study .brand-outline-chip-meta {
  display: grid;
  gap: 6px;
  max-width: 28ch;
  font-family: var(--font-body);
  font-size: 12px;
  line-height: 1.5;
  color: var(--ink-body);
}
.palette-study .brand-outline-chip-meta strong {
  color: var(--ink-heading);
  font-weight: 500;
}
.palette-study .brand-outline-banner {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 18px;
  padding: 18px 22px;
  border-top: 1px solid rgba(255,255,255,0.08);
  border-bottom: 1px solid rgba(255,255,255,0.08);
}
.palette-study .brand-outline-banner .brand-mark {
  width: auto;
  height: 18px;
}
.palette-study .brand-outline-banner-copy {
  font-family: var(--font-body);
  font-size: 12px;
  color: var(--ink-body);
}
.palette-study .brand-launch {
  width: 100%;
  min-height: 260px;
  display: grid;
  place-items: center;
  background: linear-gradient(180deg, rgba(255,255,255,0.018), rgba(255,255,255,0));
  border: 1px solid rgba(255, 255, 255, 0.06);
}
.palette-study .brand-launch-inner {
  display: grid;
  justify-items: center;
  gap: 20px;
  text-align: center;
}
.palette-study .brand-launch .brand-mark {
  width: auto;
  height: 42px;
}
.palette-study .brand-launch-wordmark {
  font-family: var(--font-body);
  font-weight: 500;
  font-size: 40px;
  line-height: 1;
  letter-spacing: -0.018em;
  color: var(--ink-heading);
}
.palette-study .brand-launch-sub {
  font-family: var(--font-body);
  font-size: 12px;
  line-height: 1.55;
  color: var(--ink-body);
  max-width: 34ch;
}
.palette-study .brand-masthead {
  width: 100%;
  padding: 28px 28px 24px;
  background: var(--paper-card);
  color: var(--paper-ink);
  display: grid;
  gap: 18px;
}
.palette-study .brand-masthead-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 18px;
}
.palette-study .brand-masthead-left {
  display: flex;
  align-items: center;
  gap: 14px;
}
.palette-study .brand-masthead .brand-mark {
  width: auto;
  height: 22px;
  color: var(--paper-ink);
}
.palette-study .brand-masthead-wordmark {
  font-family: var(--font-body);
  font-weight: 500;
  font-size: 22px;
  letter-spacing: -0.01em;
  color: var(--paper-ink);
}
.palette-study .brand-masthead-meta {
  font-family: var(--font-body);
  font-size: 10px;
  font-weight: 500;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: rgba(45, 40, 32, 0.52);
}
.palette-study .brand-masthead-title {
  font-family: var(--font-display);
  font-size: 44px;
  line-height: 0.98;
  letter-spacing: -0.04em;
  color: var(--paper-ink);
}
.palette-study .brand-masthead-title em {
  font-style: italic;
  font-weight: 400;
  color: rgba(45, 40, 32, 0.54);
}
.palette-study .brand-masthead-rule {
  height: 1px;
  background: rgba(45, 40, 32, 0.14);
}
.palette-study .brand-masthead-foot {
  display: flex;
  justify-content: space-between;
  font-family: var(--font-body);
  font-size: 11px;
  color: rgba(45, 40, 32, 0.62);
}
`;

function Folio({ items }: { items: string[] }) {
  return (
    <div className="folio">
      {items.map((it, i) => (
        <span key={i} style={{ display: "contents" }}>
          <span>{it}</span>
          {i < items.length - 1 ? <span className="rule" /> : null}
        </span>
      ))}
    </div>
  );
}

function Cover() {
  return (
    <header className="cover rise d0">
      <Folio items={["Ceiora · Palette Study", "Edition 02"]} />
      <h1 className="cover-title">
        A palette for <em>quiet</em> intelligence.
      </h1>
      <p className="cover-sub">
        A single, coherent system extending the intro canvas — restrained, slightly warm, tuned for portfolio risk, factor decomposition, and the quieter work of hedging.
      </p>
      <div className="cover-meta">
        <div>
          <span className="meta-label">Family</span>
          <span className="meta-value">Graphite, slightly warm</span>
        </div>
        <div>
          <span className="meta-label">Status</span>
          <span className="meta-value">Local study, not applied</span>
        </div>
        <div>
          <span className="meta-label">Typography</span>
          <span className="meta-value">Current header voice · IBM Plex</span>
        </div>
      </div>
    </header>
  );
}

function ColorFieldSection() {
  return (
    <section className="sec rise d1">
      <div className="sec-head">
        <span className="sec-num">01 · Substrate</span>
        <h2 className="sec-title">
          Graphite, with a <em>half-turn</em> of warmth.
        </h2>
        <p className="sec-lede">
          The canvas stays near-neutral — low saturation, quiet — with just enough warmth to keep tables, hedge grids, and factor plates from feeling clinical. Three stops, top to shadow.
        </p>
      </div>
      <div className="field">
        <div className="field-gradient" />
        <div className="field-stops">
          <div className="field-stop">
            <span className="field-stop-hex">{PALETTE.shell.top.toUpperCase()}</span>
            <span className="field-stop-role">Shell · Top</span>
          </div>
          <div className="field-stop">
            <span className="field-stop-hex">{PALETTE.shell.base.toUpperCase()}</span>
            <span className="field-stop-role">Shell · Base</span>
          </div>
          <div className="field-stop">
            <span className="field-stop-hex">{PALETTE.shell.deep.toUpperCase()}</span>
            <span className="field-stop-role">Shell · Deep</span>
          </div>
        </div>
        <div className="field-attrib">Plate I — Canvas gradient, 180°</div>
      </div>
    </section>
  );
}

type ExposureSeed = { label: string; value: number; color: string };

function ExposureBar({ exposure }: { exposure: ExposureSeed }) {
  const pct = Math.min(Math.abs(exposure.value) * 100, 100);
  const isPositive = exposure.value >= 0;
  return (
    <div className="exposure">
      <div className="exp-head">
        <span className="exp-label">{exposure.label}</span>
        <span className="exp-val">{exposure.value >= 0 ? "+" : ""}{exposure.value.toFixed(2)}</span>
      </div>
      <div className="exp-track">
        <div className="exp-axis" />
        <div
          className="exp-fill"
          style={{
            background: exposure.color,
            width: `${pct / 2}%`,
            left: isPositive ? "50%" : undefined,
            right: isPositive ? undefined : "50%",
          }}
        />
      </div>
    </div>
  );
}

function Instrument() {
  const exposures: ExposureSeed[] = [
    { label: "Growth", value: 0.46, color: PALETTE.family.cuse },
    { label: "Quality", value: -0.28, color: PALETTE.family.cpar },
    { label: "Rates", value: 0.14, color: PALETTE.family.cmac },
  ];

  const rows: { idx: string; factor: string; family: FamilyKey; value: number; tone: "pos" | "neg" | "wrn" }[] = [
    { idx: "01", factor: "Market", family: "cuse", value: 0.84, tone: "pos" },
    { idx: "02", factor: "Semiconductors", family: "cpar", value: -0.32, tone: "neg" },
    { idx: "03", factor: "Rates — short end", family: "cmac", value: 0.19, tone: "wrn" },
  ];

  return (
    <section className="sec rise d2">
      <div className="sec-head">
        <span className="sec-num">02 · Instrument</span>
        <h2 className="sec-title">
          Chrome held quiet. <em>Data</em> at full voice.
        </h2>
        <p className="sec-lede">
          Thin chrome, restrained controls, and color reserved for meaning.
        </p>
      </div>

      <div className="instrument">
        <div className="chrome">
          <div className="chrome-top">
            <span className="wordmark">Ceiora</span>
            <nav className="nav">
              <span className="nav-item active">Overview</span>
              <span className="nav-item">Positions</span>
              <span className="nav-item">Explore</span>
            </nav>
            <span className="cta">Open app</span>
          </div>

          <div className="chrome-body">
            <div className="chrome-meta">
              <div className="chrome-meta-left">
                <span className="chrome-meta-eyebrow">Household aggregate</span>
                <span className="chrome-meta-title">April 16 · 09:42 ET</span>
              </div>
              <div className="chrome-meta-right">
                <span className="chrome-meta-dot" />
                <span className="chrome-meta-label">Live</span>
              </div>
            </div>

            <div className="headline">
              Portfolio risk, factor decomposition, and the hedges that hold them in place.
            </div>

            <div className="stats">
              <div className="stat">
                <span className="stat-label">Active Risk</span>
                <span className="stat-val">3.84%</span>
                <span className="stat-delta">+0.12 vs. 30d</span>
              </div>
              <div className="stat">
                <span className="stat-label">Tracking Var</span>
                <span className="stat-val family-cuse">1.27</span>
                <span className="stat-delta">cUSE dominant</span>
              </div>
              <div className="stat">
                <span className="stat-label">Top Sleeve</span>
                <span className="stat-val family-cpar">Semis</span>
                <span className="stat-delta">24.3% gross</span>
              </div>
              <div className="stat">
                <span className="stat-label">Stress Regime</span>
                <span className="stat-val family-cmac">Rates</span>
                <span className="stat-delta">−1.8σ scenario</span>
              </div>
            </div>

            <div className="exposures">
              {exposures.map((e) => <ExposureBar key={e.label} exposure={e} />)}
            </div>

            <div className="table">
              <div className="tr tr-head">
                <span className="th">ID</span>
                <span className="th">Factor</span>
                <span className="th">Family</span>
                <span className="th right">Beta</span>
              </div>
              {rows.map((r) => (
                <div key={r.idx} className="tr">
                  <span className="td-idx">{r.idx}</span>
                  <span className="td">{r.factor}</span>
                  <span className="td-fam" style={{ color: PALETTE.family[r.family] }}>
                    {r.family.toUpperCase()}
                  </span>
                  <span className={`td-num ${r.tone}`}>
                    {r.value >= 0 ? "+" : ""}{r.value.toFixed(2)}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>

        <aside className="aside">
          <div className="aside-eyebrow">Landing · Editorial surface</div>
          <div className="aside-title">
            Barra-style equity risk — unsentimental, and in your hand.
          </div>
          <p className="aside-body">
            Editorial surfaces stay warm and tactile — cream paper, ink, hairlines — but they are reserved for the intro and for narrative callouts. The analytics substrate remains graphite, because the work deserves contrast, not mood.
          </p>
          <div className="aside-foot">Plate II — Paper surface, flat</div>
        </aside>
      </div>
    </section>
  );
}

type ChipSpec = { value: string; name: string; role: string; isLight?: boolean };

function ChipCard({ chip }: { chip: ChipSpec }) {
  return (
    <div className="chip">
      <div
        className={`chip-swatch ${chip.isLight ? "is-light" : ""}`}
        style={{ background: chip.value }}
      />
      <div className="chip-meta">
        <span className="chip-hex">{chip.value.toUpperCase()}</span>
        <span className="chip-name">{chip.name}</span>
        <span className="chip-role">{chip.role}</span>
      </div>
    </div>
  );
}

function Tokens() {
  const chips: ChipSpec[] = [
    { value: PALETTE.shell.top, name: "Shell · Top", role: "Canvas top stop" },
    { value: PALETTE.shell.base, name: "Shell · Base", role: "Default canvas" },
    { value: PALETTE.shell.deep, name: "Shell · Deep", role: "Canvas shadow" },
    { value: "rgba(255, 255, 255, 0.028)", name: "Surface Lift", role: "Elevated panel" },
    { value: "#f4f4f2", name: "Ink · Display", role: "Titles / figures", isLight: true },
    { value: "#dee3e9", name: "Ink · Body", role: "Running text", isLight: true },
    { value: "#b1bac7", name: "Ink · Muted", role: "Labels / meta", isLight: true },
    { value: "#8c94a2", name: "Ink · Folio", role: "Small caps / rules" },
    { value: PALETTE.paper.card, name: "Paper · Card", role: "Editorial surface", isLight: true },
    { value: PALETTE.paper.ink, name: "Paper · Ink", role: "Dark on paper" },
    { value: "rgba(255, 255, 255, 0.06)", name: "Hairline", role: "Structural rule" },
  ];

  return (
    <section className="sec rise d3">
      <div className="sec-head">
        <span className="sec-num">03 · Semantic tokens</span>
        <h2 className="sec-title">
          Twelve stops that render <em>the whole system.</em>
        </h2>
      </div>
      <div className="chips">
        {chips.map((c) => <ChipCard key={c.name} chip={c} />)}
      </div>
    </section>
  );
}

function Families() {
  const families: { key: FamilyKey; code: string; title: string; desc: string }[] = [
    {
      key: "cuse",
      code: "cUSE",
      title: "Core equity",
      desc: "Magenta for the US core. The established family color; reads as a brand, not a signal.",
    },
    {
      key: "cpar",
      code: "cPAR",
      title: "Parsimonious and actional regression",
      desc: "Ink blue. Sits next to cUSE without fighting it. Used for cPAR attribution and hedge-read surfaces.",
    },
    {
      key: "cmac",
      code: "cMAC",
      title: "Macro",
      desc: "Amber. Carries across the intro and the colophon. Anchors macro regimes.",
    },
  ];

  return (
    <section className="sec rise d4">
      <div className="sec-head">
        <span className="sec-num">04 · Model families</span>
        <h2 className="sec-title">
          Three marks, <em>one register.</em>
        </h2>
      </div>
      <div className="families">
        {families.map((f) => (
          <article key={f.key} className="fam">
            <div className="fam-plate" style={{ background: PALETTE.family[f.key] }}>
              <span className="fam-code">{f.code}</span>
              <span className="fam-hex">{PALETTE.family[f.key].toUpperCase()}</span>
            </div>
            <div className="fam-sub">
              <div className="fam-logo" aria-label={`${f.code} logo study`}>
                <span className={`fam-logo-word ${f.key}`}>
                  <span className="lead">c</span>
                  <span>{f.code.slice(1)}</span>
                </span>
              </div>
              <div className="fam-title">{f.title}</div>
              <div className="fam-desc">{f.desc}</div>
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}

function Signals() {
  const items: { key: SignalKey; name: string; hex: string; use: string; example: string; tone: "pos" | "neg" | "wrn" | "ntr" }[] = [
    { key: "positive", name: "Positive", hex: PALETTE.signals.positive, use: "Gains, filled orders, confirmations.", example: "+2.14%", tone: "pos" },
    { key: "negative", name: "Negative", hex: PALETTE.signals.negative, use: "Drawdowns, rejections, rule breaks.", example: "−1.87%", tone: "neg" },
    { key: "warning", name: "Warning", hex: PALETTE.signals.warning, use: "Exposure drift, stress flags, thresholds.", example: "σ 1.9", tone: "wrn" },
    { key: "neutral", name: "Neutral", hex: PALETTE.signals.neutral, use: "Informational state, no portfolio action.", example: "—", tone: "ntr" },
  ];

  const toneColor: Record<"pos" | "neg" | "wrn" | "ntr", string> = {
    pos: PALETTE.signals.positive,
    neg: PALETTE.signals.negative,
    wrn: PALETTE.signals.warning,
    ntr: PALETTE.signals.neutral,
  };

  return (
    <section className="sec rise d4">
      <div className="sec-head">
        <span className="sec-num">05 · Signals</span>
        <h2 className="sec-title">
          Quiet, financial — <em>never alarmed.</em>
        </h2>
      </div>

      <div className="signals-table">
        {items.map((i) => (
          <div key={i.key} className="sig-row">
            <span className="sig-chip" style={{ background: i.hex }} />
            <span className="sig-name">{i.name}</span>
            <span className="sig-hex">{i.hex.toUpperCase()}</span>
            <span className="sig-use">{i.use}</span>
            <span className="sig-example" style={{ color: toneColor[i.tone] }}>{i.example}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function ChartSection() {
  const categories = [
    { label: "cUSE · Core", segs: [42, 18, 12, 14, 8, 6] },
    { label: "cPAR · Rates", segs: [10, 38, 22, 12, 12, 6] },
    { label: "cMAC · Macro", segs: [14, 16, 32, 18, 12, 8] },
    { label: "Hedge overlay", segs: [8, 14, 16, 30, 20, 12] },
  ];
  const chartLabels = ["Equity", "Duration", "Credit", "FX", "Commodity", "Alt. beta"];

  return (
    <section className="sec rise d5">
      <div className="sec-head">
        <span className="sec-num">06 · Chart palette</span>
        <h2 className="sec-title">
          Six pigments, <em>made to share a field.</em>
        </h2>
      </div>

      <div className="chart-wrap">
        <div className="chart-title">Gross exposure by risk kind — 4-sleeve aggregate</div>
        <div className="chart-stack">
          {categories.map((cat) => {
            const total = cat.segs.reduce((s, v) => s + v, 0);
            return (
              <div key={cat.label} className="chart-row">
                <span className="chart-row-label">{cat.label}</span>
                <div className="chart-bar">
                  {cat.segs.map((v, i) => (
                    <div
                      key={i}
                      className="chart-seg"
                      style={{
                        flex: `${v} 0 0`,
                        background: PALETTE.chart[i],
                      }}
                      title={`${chartLabels[i]} — ${v}%`}
                    />
                  ))}
                </div>
                <span className="chart-val">{total}%</span>
              </div>
            );
          })}
        </div>

        <div className="chart-legend">
          {PALETTE.chart.map((c, i) => (
            <div key={c} className="legend">
              <div className="legend-bar" style={{ background: c }} />
              <span className="legend-label">{chartLabels[i]}</span>
              <span className="legend-hex">{c.toUpperCase()}</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function Editorial() {
  return (
    <section className="editorial rise d5">
      <div className="editorial-left">
        <div className="editorial-folio">07 · Landing crossover</div>
        <h2 className="editorial-title">
          Warm, <em>held as a guest.</em>
        </h2>
      </div>
      <div className="editorial-right">
        <span className="editorial-eyebrow">Principle · Restraint of warmth</span>
        <div className="editorial-body">
          <p>
            Editorial whites — <strong>cream, paper, kraft</strong> — belong to moments of narrative. The intro page. A hero callout. The handoff between product and prospectus.
          </p>
          <p>
            They never become the substrate for dense analytics. Beyond the first two screens the product returns to graphite, because a table of seventy factors cannot live on paper without turning into an advertisement.
          </p>
          <p>
            Used this way, the warm whites feel <strong>considered</strong> — a voice, rather than a background.
          </p>
        </div>

        <div className="editorial-pairs">
          <div className="editorial-pair">
            <span className="editorial-pair-label">Warm entry</span>
            <span className="editorial-pair-value">Intro · Onboarding · Hero</span>
          </div>
          <div className="editorial-pair">
            <span className="editorial-pair-label">Graphite work</span>
            <span className="editorial-pair-value">Tables · Factor plates · Hedge</span>
          </div>
        </div>
      </div>
    </section>
  );
}

function Colophon() {
  const monoSample = `+2.14%  −0.38σ
AAPL  NVDA  META
{"family":"cuse"}`;

  return (
    <section className="colophon rise d6">
      <div className="sec-head">
        <span className="sec-num">08 · Colophon</span>
        <h2 className="sec-title">
          Voice, set in <em>three weights.</em>
        </h2>
      </div>

      <div className="specimens">
        <div className="specimen">
          <div className="specimen-label"><span>Display</span><span>Portrait stack</span></div>
          <div className="specimen-display">
            Risk, <em>held in the hand.</em>
          </div>
          <div className="specimen-foot">Variable · reserved for h1 &amp; h2</div>
        </div>
        <div className="specimen">
          <div className="specimen-label"><span>Body</span><span>IBM Plex Sans</span></div>
          <div className="specimen-sans">
            The body face sits under every table, dialog, and caption. It stays thoughtful without becoming decorative. Numbers are tabular.
          </div>
          <div className="specimen-foot">Weights 300 / 400 / 500</div>
        </div>
        <div className="specimen">
          <div className="specimen-label"><span>Data</span><span>IBM Plex Mono</span></div>
          <div className="specimen-mono">{monoSample}</div>
          <div className="specimen-foot">Tnum · Lnum · 10 / 12 / 14 px</div>
        </div>
      </div>

      <div className="credits">
        <div>
          <div className="credit-label">Family</div>
          <div className="credit-body">
            <strong className="wordmark-credit">Ceiora — Edition II.</strong> Graphite canvas, very slightly warm. Paper reserved for narrative.
          </div>
        </div>
        <div>
          <div className="credit-label">Principle</div>
          <div className="credit-body">
            Warmth is a guest. Category color ≠ state color. Paper for narrative; graphite for work. Numbers are tabular.
          </div>
        </div>
        <div>
          <div className="credit-label">Set in</div>
          <div className="credit-body">
            <strong>Portrait stack</strong> display, <strong>IBM Plex Sans</strong> body, <strong>IBM Plex Mono</strong> data.
          </div>
        </div>
      </div>
    </section>
  );
}

function MotionStandards() {
  const specs = [
    {
      key: "Durations",
      value: "160ms for color, border, and opacity. 180ms for micro-translation. Nothing slower unless content itself is entering the page.",
    },
    {
      key: "Movement",
      value: "Use 1-3px of travel at most. Favor lift, slide, and underline over bounce, scale, glow, or springy overshoot.",
    },
    {
      key: "Focus",
      value: "Single-line focus treatment, low-glare. Clear enough for keyboard use, never neon, never thick.",
    },
    {
      key: "Surfaces",
      value: "Most interactions should animate on the graphite field itself. Reserve filled panels for rare moments of separation.",
    },
    {
      key: "Reduced motion",
      value: "When motion is reduced, keep only color and border changes. No content should rely on travel to be understandable.",
    },
  ];

  return (
    <section className="motion rise d6">
      <div className="sec-head">
        <span className="sec-num">09 · Interaction</span>
        <h2 className="sec-title">
          Motion that clarifies, <em>never performs.</em>
        </h2>
        <p className="sec-lede">
          Ceiora interactions should feel precise and light: quiet transitions, minimal travel, no ornamental animation, and no heavy control chrome.
        </p>
      </div>

      <div className="motion-grid">
        <div className="motion-main">
          <div className="motion-block">
            <div className="motion-label">Menu bar and dropdowns</div>
            <div className="motion-copy">
              The top chrome should stay line-based and quiet. Dropdowns should read as architectural lists, not rounded popovers or glowing trays.
            </div>
            <div className="motion-toolbar">
              <div className="motion-toolbar-tools">
                <div className="motion-menu-wrap">
                  <button type="button" className="motion-menu-btn" aria-label="Menu">
                    <svg className="motion-menu-icon" viewBox="0 0 18 18" fill="none" aria-hidden="true">
                      <line className="motion-menu-line-1" x1="3" y1="5" x2="15" y2="5" />
                      <line className="motion-menu-line-2" x1="3" y1="9" x2="15" y2="9" />
                      <line className="motion-menu-line-3" x1="3" y1="13" x2="15" y2="13" />
                    </svg>
                  </button>
                  <span className="motion-menu-text">Menu</span>
                </div>
                <div className="motion-dropdown">
                  <span className="motion-dropdown-label">Background mode</span>
                  <div className="motion-dropdown-item active">
                    <span className="motion-dropdown-key">Neo dot field</span>
                    <span className="motion-dropdown-meta">01</span>
                  </div>
                  <div className="motion-dropdown-item">
                    <span className="motion-dropdown-key">Quiet graphite</span>
                    <span className="motion-dropdown-meta">02</span>
                  </div>
                  <div className="motion-dropdown-item">
                    <span className="motion-dropdown-key">Presentation mode</span>
                    <span className="motion-dropdown-meta">03</span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div className="motion-block">
            <div className="motion-label">Action styles</div>
            <div className="motion-copy">
              Two standards only: a plain text action for low-ceremony moves, and a rectangular action for the main commitment.
            </div>
            <div className="motion-actions">
              <button type="button" className="motion-btn-text">
                View methodology
                <span className="motion-btn-arrow" aria-hidden="true">
                  <svg viewBox="0 0 12 12" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path
                      d="M2 10L10 2M2 2H10V10"
                      stroke="currentColor"
                      strokeWidth="1.2"
                      strokeLinecap="square"
                      strokeLinejoin="miter"
                    />
                  </svg>
                </span>
              </button>
              <button type="button" className="motion-btn-rect">
                Open app
                <span className="motion-btn-arrow" aria-hidden="true">
                  <svg viewBox="0 0 12 12" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path
                      d="M2 10L10 2M2 2H10V10"
                      stroke="currentColor"
                      strokeWidth="1.2"
                      strokeLinecap="square"
                      strokeLinejoin="miter"
                    />
                  </svg>
                </span>
              </button>
            </div>
          </div>

          <div className="motion-block">
            <div className="motion-label">Tabs and selection</div>
            <div className="motion-copy">
              Active state comes through line and contrast, not pills. Hover should sharpen, not decorate.
            </div>
            <div className="motion-tabs">
              <span className="motion-tab active">Overview</span>
              <span className="motion-tab">Positions</span>
              <span className="motion-tab">Explore</span>
            </div>
          </div>

          <div className="motion-block">
            <div className="motion-label">Row behavior</div>
            <div className="motion-copy">
              Dense lists should acknowledge the pointer with a slight shift and text emphasis, not a filled hover slab.
            </div>
            <div className="motion-rows">
              <div className="motion-row">
                <span className="motion-row-title">Semiconductors sleeve</span>
                <span className="motion-row-meta">+0.34</span>
              </div>
              <div className="motion-row">
                <span className="motion-row-title">Rates hedge overlay</span>
                <span className="motion-row-meta">−0.18</span>
              </div>
            </div>
          </div>
        </div>

        <div className="motion-notes">
          <div className="motion-block">
            <div className="motion-label">Global standards</div>
            <div className="motion-specs">
              {specs.map((spec) => (
              <div key={spec.key} className="motion-spec">
                  <div className="motion-spec-key">{spec.key}</div>
                  <div className="motion-spec-val">{spec.value}</div>
                </div>
              ))}
            </div>
          </div>

          <div className="motion-block">
            <div className="motion-label">Current app patterns to unify</div>
            <div className="motion-copy">
              The existing app still has three areas worth normalizing: rounded dropdowns in top-level menus and settings, glowing ready states in what-if actions, and pill-like segmented controls that don’t match the broader square geometry.
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

type AppliedFactor = {
  group: string;
  label: string;
  long: number;
  short: number;
  fundamentalLong: number;
  fundamentalShort: number;
  returnsLong: number;
  returnsShort: number;
  selected?: boolean;
};

function AppliedFragments() {
  const factors: AppliedFactor[] = [
    {
      group: "Market",
      label: "Market beta",
      long: 0.84,
      short: -0.09,
      fundamentalLong: 0.92,
      fundamentalShort: -0.14,
      returnsLong: 1.02,
      returnsShort: -0.18,
    },
    {
      group: "Sector",
      label: "Semiconductors",
      long: 0.18,
      short: -0.60,
      fundamentalLong: 0.24,
      fundamentalShort: -0.68,
      returnsLong: 0.31,
      returnsShort: -0.77,
      selected: true,
    },
    {
      group: "Sector",
      label: "Software",
      long: 0.31,
      short: -0.07,
      fundamentalLong: 0.38,
      fundamentalShort: -0.11,
      returnsLong: 0.46,
      returnsShort: -0.16,
    },
    {
      group: "Style",
      label: "Growth",
      long: 0.36,
      short: -0.05,
      fundamentalLong: 0.43,
      fundamentalShort: -0.08,
      returnsLong: 0.52,
      returnsShort: -0.12,
    },
    {
      group: "Style",
      label: "Value",
      long: 0.08,
      short: -0.27,
      fundamentalLong: 0.13,
      fundamentalShort: -0.33,
      returnsLong: 0.18,
      returnsShort: -0.40,
    },
  ];
  const maxMagnitude = Math.max(
    ...factors.map((factor) => Math.max(
      Math.abs(factor.long),
      Math.abs(factor.short),
      Math.abs(factor.fundamentalLong),
      Math.abs(factor.fundamentalShort),
      Math.abs(factor.returnsLong),
      Math.abs(factor.returnsShort),
    )),
    1,
  );
  const searchRows = [
    { ticker: "NVDA", name: "NVIDIA Corporation", fit: "Complete fit", meta: "Semiconductors · United States" },
    { ticker: "ASML", name: "ASML Holding N.V.", fit: "Package-ready", meta: "Semiconductor equipment · Netherlands" },
    { ticker: "TSM", name: "Taiwan Semiconductor", fit: "Coverage lag", meta: "Foundry · Taiwan" },
  ];
  const tableRows = [
    { ticker: "NVDA", name: "NVIDIA Corporation", weight: "7.42%", marketValue: "$412k", side: "Long", method: "Core", factor: "+0.84", tone: "pos" as const },
    { ticker: "ASML", name: "ASML Holding N.V.", weight: "2.16%", marketValue: "$120k", side: "Long", method: "Fundamental Projection", factor: "+0.31", tone: "pos" as const },
    { ticker: "SMH", name: "VanEck Semiconductor ETF", weight: "-1.94%", marketValue: "$-108k", side: "Short", method: "Returns Projection", factor: "-0.42", tone: "neg" as const },
    { ticker: "IEF", name: "iShares 7-10 Year Treasury", weight: "1.28%", marketValue: "$71k", side: "Long", method: "Core", factor: "-0.18", tone: "neg" as const },
  ];

  return (
    <section className="applied rise d6">
      <div className="sec-head">
        <span className="sec-num">10 · Applied fragments</span>
        <h2 className="sec-title">
          Real screens, <em>restated.</em>
        </h2>
        <p className="sec-lede">
          A few actual Ceiora patterns translated into the revised theme: the long/short factor view, search-driven selection, and lightweight controls that stay mostly on the background.
        </p>
      </div>

      <div className="applied-grid">
        <div className="applied-main">
          <div className="applied-block">
            <div className="applied-label">Long / short factor chart</div>
            <div className="applied-copy">
              Based on the cPAR factor chart: left for short exposure, right for long exposure, center line held quiet, net marker kept crisp.
            </div>
            <div className="applied-factor-legend">
              <span className="applied-chip" style={{ color: PALETTE.signals.negative }}>
                <span className="applied-chip-dot" />
                Left: short
              </span>
              <span className="applied-chip" style={{ color: PALETTE.signals.positive }}>
                <span className="applied-chip-dot" />
                Right: long
              </span>
              <span className="applied-chip">
                <span className="applied-chip-dot" />
                Marker: net beta
              </span>
              <span className="applied-chip" style={{ color: "rgba(196, 204, 215, 0.74)" }}>
                <span className="applied-chip-rail" />
                Fundamental projection
              </span>
              <span className="applied-chip" style={{ color: "rgba(160, 169, 183, 0.54)" }}>
                <span className="applied-chip-rail" />
                Returns projection
              </span>
            </div>
            <div className="applied-factor-list">
              {factors.map((factor, index) => {
                const showGroupLabel = index === 0 || factors[index - 1]?.group !== factor.group;
                const negativeWidth = Math.min(50, (Math.abs(factor.short) / maxMagnitude) * 50);
                const positiveWidth = Math.min(50, (Math.abs(factor.long) / maxMagnitude) * 50);
                const fundamentalNegativeWidth = Math.min(50, (Math.abs(factor.fundamentalShort) / maxMagnitude) * 50);
                const fundamentalPositiveWidth = Math.min(50, (Math.abs(factor.fundamentalLong) / maxMagnitude) * 50);
                const returnsNegativeWidth = Math.min(50, (Math.abs(factor.returnsShort) / maxMagnitude) * 50);
                const returnsPositiveWidth = Math.min(50, (Math.abs(factor.returnsLong) / maxMagnitude) * 50);
                const baseNegativeEdge = negativeWidth;
                const basePositiveEdge = positiveWidth;
                const fundamentalNegativeExtra = Math.max(0, fundamentalNegativeWidth - baseNegativeEdge);
                const fundamentalPositiveExtra = Math.max(0, fundamentalPositiveWidth - basePositiveEdge);
                const returnsNegativeExtra = Math.max(0, returnsNegativeWidth - Math.max(baseNegativeEdge, fundamentalNegativeWidth));
                const returnsPositiveExtra = Math.max(0, returnsPositiveWidth - Math.max(basePositiveEdge, fundamentalPositiveWidth));
                const netBeta = factor.long + factor.short;
                const markerPosition = Math.max(0, Math.min(100, 50 + ((netBeta / maxMagnitude) * 50)));

                return (
                  <div key={`${factor.group}-${factor.label}`}>
                    {showGroupLabel ? <div className="applied-group-label">{factor.group}</div> : null}
                    <div className={`applied-factor-row ${factor.selected ? "selected" : ""}`}>
                      <div className="applied-factor-meta">
                        <span className="applied-factor-title">{factor.label}</span>
                      </div>
                      <div className="applied-factor-track">
                        <span
                          className="applied-factor-bar neg"
                          style={{ left: `${50 - negativeWidth}%`, width: `${negativeWidth}%` }}
                        />
                        <span
                          className="applied-factor-bar pos"
                          style={{ left: "50%", width: `${positiveWidth}%` }}
                        />
                        {fundamentalNegativeExtra > 0 ? (
                          <span
                            className="applied-factor-ext fundamental neg"
                            style={{
                              left: `${50 - fundamentalNegativeWidth}%`,
                              width: `${fundamentalNegativeExtra}%`,
                            }}
                          />
                        ) : null}
                        {fundamentalPositiveExtra > 0 ? (
                          <span
                            className="applied-factor-ext fundamental pos"
                            style={{
                              left: `${50 + basePositiveEdge}%`,
                              width: `${fundamentalPositiveExtra}%`,
                            }}
                          />
                        ) : null}
                        {returnsNegativeExtra > 0 ? (
                          <span
                            className="applied-factor-ext returns neg"
                            style={{
                              left: `${50 - returnsNegativeWidth}%`,
                              width: `${returnsNegativeExtra}%`,
                            }}
                          />
                        ) : null}
                        {returnsPositiveExtra > 0 ? (
                          <span
                            className="applied-factor-ext returns pos"
                            style={{
                              left: `${50 + Math.max(basePositiveEdge, fundamentalPositiveWidth)}%`,
                              width: `${returnsPositiveExtra}%`,
                            }}
                          />
                        ) : null}
                        <span className="applied-factor-marker" style={{ left: `${markerPosition}%` }} />
                      </div>
                      <span className="applied-factor-value">
                        {netBeta >= 0 ? "+" : ""}
                        {netBeta.toFixed(3)}
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          <div className="applied-block">
            <div className="applied-label">Table language</div>
            <div className="applied-copy">
              One dense table style should carry most of the app: light rules, quiet headers, compact numerics, and restrained method tags.
            </div>
            <div className="applied-table">
              <div className="applied-table-row head">
                <span className="applied-th">Ticker</span>
                <span className="applied-th">Name</span>
                <span className="applied-th right">Weight</span>
                <span className="applied-th right">Mkt Value</span>
                <span className="applied-th">Side</span>
                <span className="applied-th">Method</span>
              </div>
              {tableRows.map((row) => (
                <div key={row.ticker} className="applied-table-row data">
                  <span className="applied-td ticker">{row.ticker}</span>
                  <span className="applied-td name">{row.name}</span>
                  <span className={`applied-td mono right ${row.weight.startsWith("-") ? "neg" : "pos"}`}>{row.weight}</span>
                  <span className={`applied-td mono right ${row.marketValue.includes("-") ? "neg" : "pos"}`}>{row.marketValue}</span>
                  <span className="applied-td">{row.side}</span>
                  <span
                    className={[
                      "applied-method",
                      row.method === "Core" ? "core" : "projection",
                      row.method === "Fundamental Projection" ? "fundamental" : "",
                      row.method === "Returns Projection" ? "returns" : "",
                    ].filter(Boolean).join(" ")}
                  >
                    {row.method}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="applied-side">
          <div className="applied-block">
            <div className="applied-label">Search and selection</div>
            <div className="applied-copy">
              Explore and what-if search should feel lighter than the current boxed trays: input at the top, rows acknowledged by movement and type contrast rather than card hover slabs.
            </div>
            <div className="applied-search-shell">
              <input className="applied-search-input" value="semi" readOnly aria-label="Search query" />
              <div className="applied-search-results">
                {searchRows.map((row) => (
                  <div key={row.ticker} className="applied-search-result">
                    <div className="applied-search-result-top">
                      <span className="applied-search-ticker">{row.ticker}</span>
                      <span className="applied-search-fit">{row.fit}</span>
                    </div>
                    <div className="applied-search-name">{row.name}</div>
                    <div className="applied-search-meta">{row.meta}</div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="applied-block">
            <div className="applied-label">Controls and readouts</div>
            <div className="applied-copy">
              Settings and mode controls should move away from pills. Line-based selection and quiet statlines fit the rest of the app better.
            </div>
            <div className="applied-settings">
              <div className="applied-setting-row">
                <div className="applied-setting-title">Background mode</div>
                <div className="applied-setting-help">Pick the substrate without turning the setting itself into a rounded component.</div>
                <div className="applied-options">
                  <span className="applied-option active">Neo dot field</span>
                  <span className="applied-option">Quiet graphite</span>
                  <span className="applied-option">Presentation</span>
                </div>
              </div>
              <div className="applied-statline">
                <div className="applied-stat">
                  <span className="applied-stat-key">Pre-hedge factor variance</span>
                  <span className="applied-stat-val">1.274</span>
                </div>
                <div className="applied-stat">
                  <span className="applied-stat-key">Post-hedge factor variance</span>
                  <span className="applied-stat-val">0.882</span>
                </div>
                <div className="applied-stat">
                  <span className="applied-stat-key">Gross hedge notional</span>
                  <span className="applied-stat-val">0.413</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function WorkflowSurfaces() {
  return (
    <section className="surfaces rise d6">
      <div className="sec-head">
        <span className="sec-num">11 · Workflow surfaces</span>
        <h2 className="sec-title">
          Focus states, <em>made practical.</em>
        </h2>
        <p className="sec-lede">
          These are the remaining surfaces that decide whether the app feels operational: the focused drilldown, the hovered readout, the control stack, and the one true elevated modal.
        </p>
      </div>

      <div className="surfaces-grid">
        <div className="surfaces-main">
          <div className="surface-block">
            <div className="surface-label">Detail panel / drilldown</div>
            <div className="surface-copy">
              A restrained elevated surface is still useful when the user is reading one factor in depth. It should feel quieter than a modal and denser than a dashboard card.
            </div>
            <div className="surface-panel">
              <div className="surface-panel-head">
                <div>
                  <div className="surface-panel-title">Semiconductors</div>
                  <div className="surface-panel-meta">5 positions · 3 unique exposures</div>
                </div>
                <div className="surface-panel-close">Close</div>
              </div>
              <div className="surface-panel-text">
                Position loading on the selected factor. Top contributors and whether negatives are true hedges.
              </div>
              <div className="surface-mini-table">
                <div className="surface-mini-row head">
                  <span className="surface-mini-th">Position</span>
                  <span className="surface-mini-th">Method</span>
                  <span className="surface-mini-th">Exp</span>
                  <span className="surface-mini-th">Contr</span>
                </div>
                <div className="surface-mini-row">
                  <span className="surface-mini-td">NVDA</span>
                  <span className="surface-mini-td">Core</span>
                  <span className="surface-mini-td num">0.842</span>
                  <span className="surface-mini-td num">0.271</span>
                </div>
                <div className="surface-mini-row">
                  <span className="surface-mini-td">ASML</span>
                  <span className="surface-mini-td">Fundamental</span>
                  <span className="surface-mini-td num">0.311</span>
                  <span className="surface-mini-td num">0.084</span>
                </div>
                <div className="surface-mini-row">
                  <span className="surface-mini-td">SMH</span>
                  <span className="surface-mini-td">Returns</span>
                  <span className="surface-mini-td num">-0.420</span>
                  <span className="surface-mini-td num">-0.112</span>
                </div>
              </div>
            </div>
          </div>

          <div className="surface-block">
            <div className="surface-label">Tooltip / hover readout</div>
            <div className="surface-copy">
              Hover states should clarify one point in the data, not theatrically lift the whole chart. The tooltip itself should stay crisp and compact.
            </div>
            <div className="surface-hover-wrap">
              <div className="surface-hover-line" />
              <div className="surface-hover-tip">
                <div className="surface-tip-title">Semiconductors</div>
                <div>Core loading: -0.420</div>
                <div>Fundamental: -0.680</div>
                <div>Returns: -0.770</div>
              </div>
            </div>
          </div>
        </div>

        <div className="surfaces-side">
          <div className="surface-block">
            <div className="surface-label">Compact controls</div>
            <div className="surface-copy">
              Inputs, selects, and typeahead should stay narrow, crisp, and subordinate to the data they support.
            </div>
            <div className="surface-form">
              <input className="surface-input" value="NVDA" readOnly aria-label="Ticker" />
              <select className="surface-select" value="Household Aggregate" aria-label="Account">
                <option>Household Aggregate</option>
              </select>
              <div className="surface-typeahead">
                <div className="surface-typeahead-row active">
                  <span className="ticker">NVDA</span>
                  <span className="name">NVIDIA Corporation</span>
                  <span className="meta">Held</span>
                </div>
                <div className="surface-typeahead-row">
                  <span className="ticker">NVDS</span>
                  <span className="name">Tradr 1.5X Short NVDA</span>
                  <span className="meta">ETF</span>
                </div>
              </div>
            </div>
          </div>

          <div className="surface-block">
            <div className="surface-label">Confirm modal</div>
            <div className="surface-copy">
              This is where a true elevated surface is justified. It should feel exact and contained, not cinematic.
            </div>
            <div className="surface-modal-backdrop">
              <div className="surface-modal">
                <div className="surface-modal-title">Replace live holdings?</div>
                <div className="surface-modal-body">
                  This will overwrite current positions in the selected account with only the rows present in the file.
                </div>
                <label className="surface-modal-label">Type account ID: HH-01</label>
                <input className="surface-input" value="HH-01" readOnly aria-label="Confirm value" />
                <div className="surface-modal-actions">
                  <button type="button" className="motion-btn-text">Cancel</button>
                  <button type="button" className="motion-btn-rect">Replace</button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function LightModeStudy() {
  const chips = [
    { value: LIGHT_PALETTE.shell.top, name: "Shell · Top", role: "Light canvas top" },
    { value: LIGHT_PALETTE.shell.base, name: "Shell · Base", role: "Default substrate" },
    { value: LIGHT_PALETTE.shell.deep, name: "Shell · Deep", role: "Lower field" },
    { value: LIGHT_PALETTE.ink.display, name: "Ink · Display", role: "Display text" },
    { value: LIGHT_PALETTE.ink.heading, name: "Ink · Heading", role: "Section heads / key labels" },
    { value: LIGHT_PALETTE.ink.body, name: "Ink · Body", role: "Running text" },
    { value: LIGHT_PALETTE.ink.muted, name: "Ink · Muted", role: "Secondary labels" },
    { value: LIGHT_PALETTE.ink.folio, name: "Ink · Folio", role: "Small caps / meta" },
    { value: LIGHT_PALETTE.surface.lift, name: "Surface · Lift", role: "Rare elevated panel" },
    { value: LIGHT_PALETTE.surface.hairStrong, name: "Rule · Strong", role: "Borders / inputs" },
    { value: LIGHT_PALETTE.signals.positive, name: "Signal · Positive", role: "Long / gain state" },
    { value: LIGHT_PALETTE.signals.negative, name: "Signal · Negative", role: "Short / loss state" },
    { value: LIGHT_PALETTE.method.core, name: "Method · Core", role: "Default provenance" },
    { value: LIGHT_PALETTE.method.projection, name: "Method · Projection", role: "Projection labels" },
    { value: LIGHT_PALETTE.method.fundamental, name: "Projection · Fundamental", role: "Primary extension rail" },
    { value: LIGHT_PALETTE.method.returns, name: "Projection · Returns", role: "Secondary extension rail" },
  ];
  const factors = [
    { label: "Market beta", long: 0.78, short: -0.06, fundamentalLong: 0.85, fundamentalShort: -0.10, returnsLong: 0.91, returnsShort: -0.13 },
    { label: "Semiconductors", long: 0.22, short: -0.48, fundamentalLong: 0.28, fundamentalShort: -0.56, returnsLong: 0.34, returnsShort: -0.64 },
    { label: "Growth", long: 0.31, short: -0.04, fundamentalLong: 0.37, fundamentalShort: -0.07, returnsLong: 0.44, returnsShort: -0.10 },
  ];
  const maxMagnitude = Math.max(
    ...factors.flatMap((factor) => [
      Math.abs(factor.long),
      Math.abs(factor.short),
      Math.abs(factor.fundamentalLong),
      Math.abs(factor.fundamentalShort),
      Math.abs(factor.returnsLong),
      Math.abs(factor.returnsShort),
    ]),
    1,
  );
  const rows = [
    { ticker: "NVDA", name: "NVIDIA Corporation", weight: "7.42%", factor: "+0.72", method: "Core" },
    { ticker: "ASML", name: "ASML Holding N.V.", weight: "2.16%", factor: "+0.27", method: "Projection" },
    { ticker: "SMH", name: "VanEck Semiconductor ETF", weight: "-1.94%", factor: "-0.26", method: "Projection" },
  ];
  const searchRows = [
    { ticker: "NVDA", name: "NVIDIA Corporation", fit: "Complete fit" },
    { ticker: "ASML", name: "ASML Holding N.V.", fit: "Package-ready" },
    { ticker: "TSM", name: "Taiwan Semiconductor", fit: "Coverage lag" },
  ];

  return (
    <section className="lightmode rise d6">
      <div className="sec-head">
        <span className="sec-num">12 · Light mode</span>
        <h2 className="sec-title">
          Mineral paper, <em>still slightly warm.</em>
        </h2>
        <p className="sec-lede">
          Not an inversion of dark mode. The same Ceiora logic translated into a pale graphite-paper field: warm-neutral, restrained, and still compact where the work gets technical.
        </p>
      </div>

      <div className="lightmode-shell">
        <div className="lightmode-head">
          <div className="lightmode-folio">Light mode · Warm-neutral study</div>
          <h3 className="lightmode-title">
            Light where it helps, <em>never where it softens the work.</em>
          </h3>
          <p className="lightmode-copy">
            The substrate becomes pale mineral paper rather than bright white. Editorial warmth stays subtle, the shell remains mostly background-led, and tables and factor views keep their compact analytical rhythm.
          </p>
        </div>

        <div className="lightmode-grid">
          <div className="lightmode-main">
            <div className="lightmode-block">
              <div className="lightmode-label">Light-mode palette</div>
              <div className="lightmode-subcopy">
                A slightly warm-neutral substrate, darker ink than the dark shell needs, and signal colors tuned to hold up on pale ground.
              </div>
              <div className="lightmode-palette">
                {chips.map((chip) => (
                  <div key={chip.name} className="lightmode-chip">
                    <div className="lightmode-chip-swatch" style={{ background: chip.value }} />
                    <div className="lightmode-chip-meta">
                      <span className="lightmode-chip-hex">{chip.value.toUpperCase()}</span>
                      <span className="lightmode-chip-name">{chip.name}</span>
                      <span className="lightmode-chip-role">{chip.role}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="lightmode-block">
              <div className="lightmode-label">Factor chart</div>
              <div className="lightmode-subcopy">
                Long and short still split from the center. Projection methods remain secondary flank extensions rather than new primary colors.
              </div>
              <div className="lightmode-factor-list">
                {factors.map((factor) => {
                  const negativeWidth = Math.min(50, (Math.abs(factor.short) / maxMagnitude) * 50);
                  const positiveWidth = Math.min(50, (Math.abs(factor.long) / maxMagnitude) * 50);
                  const fundamentalNegativeWidth = Math.min(50, (Math.abs(factor.fundamentalShort) / maxMagnitude) * 50);
                  const fundamentalPositiveWidth = Math.min(50, (Math.abs(factor.fundamentalLong) / maxMagnitude) * 50);
                  const returnsNegativeWidth = Math.min(50, (Math.abs(factor.returnsShort) / maxMagnitude) * 50);
                  const returnsPositiveWidth = Math.min(50, (Math.abs(factor.returnsLong) / maxMagnitude) * 50);
                  const fundamentalNegativeExtra = Math.max(0, fundamentalNegativeWidth - negativeWidth);
                  const fundamentalPositiveExtra = Math.max(0, fundamentalPositiveWidth - positiveWidth);
                  const returnsNegativeExtra = Math.max(0, returnsNegativeWidth - Math.max(negativeWidth, fundamentalNegativeWidth));
                  const returnsPositiveExtra = Math.max(0, returnsPositiveWidth - Math.max(positiveWidth, fundamentalPositiveWidth));
                  const netBeta = factor.long + factor.short;
                  const markerPosition = Math.max(0, Math.min(100, 50 + ((netBeta / maxMagnitude) * 50)));

                  return (
                    <div key={factor.label} className="lightmode-factor-row">
                      <div className="lightmode-factor-name">{factor.label}</div>
                      <div className="lightmode-factor-track">
                        <span className="bar neg" style={{ left: `${50 - negativeWidth}%`, width: `${negativeWidth}%` }} />
                        <span className="bar pos" style={{ left: "50%", width: `${positiveWidth}%` }} />
                        {fundamentalNegativeExtra > 0 ? (
                          <span className="ext fundamental" style={{ left: `${50 - fundamentalNegativeWidth}%`, width: `${fundamentalNegativeExtra}%` }} />
                        ) : null}
                        {fundamentalPositiveExtra > 0 ? (
                          <span className="ext fundamental" style={{ left: `${50 + positiveWidth}%`, width: `${fundamentalPositiveExtra}%` }} />
                        ) : null}
                        {returnsNegativeExtra > 0 ? (
                          <span className="ext returns" style={{ left: `${50 - returnsNegativeWidth}%`, width: `${returnsNegativeExtra}%` }} />
                        ) : null}
                        {returnsPositiveExtra > 0 ? (
                          <span className="ext returns" style={{ left: `${50 + Math.max(positiveWidth, fundamentalPositiveWidth)}%`, width: `${returnsPositiveExtra}%` }} />
                        ) : null}
                        <span className="mark" style={{ left: `${markerPosition}%` }} />
                      </div>
                      <div className="lightmode-factor-value">{netBeta >= 0 ? "+" : ""}{netBeta.toFixed(3)}</div>
                    </div>
                  );
                })}
              </div>
            </div>

            <div className="lightmode-block">
              <div className="lightmode-label">Dense table</div>
              <div className="lightmode-subcopy">
                Light mode should not become spacious or card-heavy. The table still works through rules, numerics, and quiet method labeling.
              </div>
              <div className="lightmode-table">
                <div className="lightmode-table-row head">
                  <span>Ticker</span>
                  <span>Name</span>
                  <span style={{ textAlign: "right" }}>Weight</span>
                  <span style={{ textAlign: "right" }}>Factor</span>
                  <span>Method</span>
                </div>
                {rows.map((row, index) => (
                  <div key={row.ticker} className={`lightmode-table-row data ${index === 1 ? "selected" : ""}`}>
                    <span className="ticker">{row.ticker}</span>
                    <span className="name">{row.name}</span>
                    <span className={`num ${row.weight.startsWith("-") ? "neg" : "pos"}`}>{row.weight}</span>
                    <span className={`num ${row.factor.startsWith("-") ? "neg" : "pos"}`}>{row.factor}</span>
                    <span className="method">{row.method}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="lightmode-side">
            <div className="lightmode-panel">
              <div className="lightmode-panel-head">
                <span className="lightmode-wordmark">Ceiora</span>
                <span className="lightmode-link">Open app</span>
              </div>
              <div className="lightmode-tabs">
                <span className="lightmode-tab active">Overview</span>
                <span className="lightmode-tab">Positions</span>
                <span className="lightmode-tab">Explore</span>
              </div>
              <div className="lightmode-actions">
                <span className="lightmode-btn-text">View methodology</span>
                <span className="lightmode-btn-rect">Open app</span>
              </div>
            </div>

            <div className="lightmode-block">
              <div className="lightmode-label">Search and controls</div>
              <div className="lightmode-subcopy">
                The app can still use light surfaces sparingly, but search and mode controls should continue to feel line-based and architectural.
              </div>
              <div className="lightmode-search">
                <input className="lightmode-input" value="semi" readOnly aria-label="Light mode search query" />
                {searchRows.map((row) => (
                  <div key={row.ticker} className="lightmode-search-row">
                    <span className="tk">{row.ticker}</span>
                    <span className="nm">{row.name}</span>
                    <span className="fit">{row.fit}</span>
                  </div>
                ))}
              </div>
            </div>

            <div className="lightmode-block">
              <div className="lightmode-label">Hover, editorial, and elevated surfaces</div>
              <div className="lightmode-subcopy">
                These surfaces cannot be reused blindly from dark mode. They need a light-ground counterpart so hierarchy remains visible without turning the app soft.
              </div>
              <div className="lightmode-surface-grid">
                <div className="lightmode-hover-demo">
                  <div className="lightmode-mini-chart">
                    <div className="lightmode-mini-bar" />
                    <div className="lightmode-mini-marker" />
                  </div>
                  <div className="lightmode-tooltip">
                    <div className="lightmode-tooltip-title">Semiconductors</div>
                    Net beta +0.27
                    <br />
                    Primary method: projection
                  </div>
                </div>

                <div className="lightmode-editorial-card">
                  <div className="lightmode-editorial-folio">Editorial · Light surface</div>
                  <h4 className="lightmode-editorial-title">Paper for narrative, not the workbench.</h4>
                  <div className="lightmode-editorial-copy">
                    The editorial card stays slightly warmer and slightly richer than the base light substrate so it remains a voice, not the default canvas.
                  </div>
                </div>

                <div className="lightmode-dropdown">
                  <div className="lightmode-dropdown-label">Background mode</div>
                  <div className="lightmode-dropdown-row active">
                    <span className="lightmode-dropdown-key">Neo dot field</span>
                    <span className="lightmode-dropdown-meta">01</span>
                  </div>
                  <div className="lightmode-dropdown-row">
                    <span className="lightmode-dropdown-key">Quiet graphite</span>
                    <span className="lightmode-dropdown-meta">02</span>
                  </div>
                  <div className="lightmode-dropdown-row">
                    <span className="lightmode-dropdown-key">Presentation mode</span>
                    <span className="lightmode-dropdown-meta">03</span>
                  </div>
                </div>

                <div className="lightmode-modal-backdrop">
                  <div className="lightmode-modal">
                    <div className="lightmode-modal-title">Replace live holdings?</div>
                    <div className="lightmode-modal-body">
                      This remains one of the few places where a true elevated surface is justified in light mode.
                    </div>
                    <label className="lightmode-modal-label">Type account ID: HH-01</label>
                    <div className="lightmode-modal-actions">
                      <span className="lightmode-btn-text">Cancel</span>
                      <span className="lightmode-btn-rect">Replace</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function SymbolStudyVariant() {
  const variantBars = [
    { x: 0, width: 95 },
    { x: 31, width: 88 },
    { x: 17, width: 69 },
  ];

  return (
    <section className="sec rise d6">
      <div className="sec-head">
        <span className="sec-num">13 · Ceiora symbol system</span>
        <h2 className="sec-title">
          Branding built around the <em>chosen mark.</em>
        </h2>
        <p className="sec-lede">
          `Variant B` is the resolved Ceiora mark. The symbol keeps the long-short-long logic from the factor chart, but the tighter middle and shorter footing make it more compact in the app header, cleaner in lockups, and better for favicon-scale use.
        </p>
      </div>
      <div className="symbol-grid" style={{ gridTemplateColumns: "minmax(0, 1fr)" }}>
        <article className="symbol-card">
          <div className="symbol-plate">
            <svg className="symbol-svg" viewBox="0 0 120 74" aria-hidden="true">
              {variantBars.map((bar, i) => (
                <rect key={i} className="symbol-fill" x={bar.x} y={i * 27} width={bar.width} height="20" />
              ))}
            </svg>
          </div>
          <div className="symbol-meta">
            <div className="symbol-name">Variant B</div>
            <div className="symbol-copy">
              Resolved from the local tuner. Long top bar, shorter middle, shorter footing. Compact enough for favicon-scale work without losing the offset-bar logic.
            </div>
            <div className="symbol-note">Solid · 3 bars · chosen direction · chosen favicon direction</div>
          </div>
        </article>
      </div>

      <div className="brand-lockups">
        <div className="brand-tile wide">
          <div className="brand-tile-surface">
            <div className="brand-primary">
              <svg className="brand-mark" viewBox="0 0 120 74" aria-hidden="true">
                {variantBars.map((bar, i) => (
                  <rect key={i} x={bar.x} y={i * 27} width={bar.width} height="20" />
                ))}
              </svg>
              <span className="brand-wordmark">Ceiora</span>
              <div className="brand-primary-tag">
                Portfolio risk, factor decomposition, and the hedges that hold them in place.
              </div>
            </div>
          </div>
          <div className="brand-tile-caption">
            <span className="brand-tile-label">Primary lockup · Variant B</span>
            <span className="brand-tile-note">Horizontal lockup for product chrome and editorial surfaces.</span>
          </div>
        </div>

        <div className="brand-tile">
          <div className="brand-tile-surface">
            <div className="brand-stack">
              <svg className="brand-mark" viewBox="0 0 120 74" aria-hidden="true">
                {variantBars.map((bar, i) => (
                  <rect key={i} x={bar.x} y={i * 27} width={bar.width} height="20" />
                ))}
              </svg>
              <span className="brand-wordmark">Ceiora</span>
            </div>
          </div>
          <div className="brand-tile-caption">
            <span className="brand-tile-label">Stacked lockup · Variant B</span>
            <span className="brand-tile-note">For portrait or narrow chrome.</span>
          </div>
        </div>

        <div className="brand-tile">
          <div className="brand-tile-surface">
            <div className="brand-scale">
              {["lg", "md", "sm"].map((size) => (
                <div key={size} className={`brand-scale-item ${size}`}>
                  <svg className="brand-mark" viewBox="0 0 120 74" aria-hidden="true">
                    {variantBars.map((bar, i) => (
                      <rect key={i} x={bar.x} y={i * 27} width={bar.width} height="20" />
                    ))}
                  </svg>
                  <span className="brand-wordmark">Ceiora</span>
                </div>
              ))}
            </div>
          </div>
          <div className="brand-tile-caption">
            <span className="brand-tile-label">Wordmark scale · Variant B</span>
            <span className="brand-tile-note">The same pair, three sizes.</span>
          </div>
        </div>

        <div className="brand-tile">
          <div className="brand-tile-surface">
            <div className="brand-chrome">
              <div className="brand-chrome-left">
                <svg className="brand-mark" viewBox="0 0 120 74" aria-hidden="true">
                  {variantBars.map((bar, i) => (
                    <rect key={i} x={bar.x} y={i * 27} width={bar.width} height="20" />
                  ))}
                </svg>
                <span className="brand-wordmark">Ceiora</span>
              </div>
              <span className="brand-chrome-rule" />
              <div className="brand-chrome-nav">
                <span className="active">Overview</span>
                <span>Positions</span>
                <span>Explore</span>
              </div>
              <span className="brand-chrome-cta">Open app</span>
            </div>
          </div>
          <div className="brand-tile-caption">
            <span className="brand-tile-label">App chrome · Variant B</span>
            <span className="brand-tile-note">Default product-facing lockup.</span>
          </div>
        </div>

        <div className="brand-tile wide">
          <div className="brand-tile-surface">
            <div className="brand-header">
              <div className="brand-header-left">
                <svg className="brand-mark" viewBox="0 0 120 74" aria-hidden="true">
                  {variantBars.map((bar, i) => (
                    <rect key={i} x={bar.x} y={i * 27} width={bar.width} height="20" />
                  ))}
                </svg>
                <span className="brand-wordmark">Ceiora</span>
              </div>
              <span className="brand-header-rule" />
              <div className="brand-header-nav">
                <span className="active">Overview</span>
                <span>Positions</span>
                <span>Explore</span>
              </div>
              <div className="brand-header-right">
                <span>Household Aggregate</span>
                <span className="brand-header-avatar">SK</span>
              </div>
            </div>
          </div>
          <div className="brand-tile-caption">
            <span className="brand-tile-label">Full navigation bar · Variant B</span>
            <span className="brand-tile-note">Header usage with the chosen mark.</span>
          </div>
        </div>

        <div className="brand-tile wide">
          <div className="brand-tile-surface">
            <div className="brand-launch">
              <div className="brand-launch-inner">
                <svg className="brand-mark" viewBox="0 0 120 74" aria-hidden="true">
                  {variantBars.map((bar, i) => (
                    <rect key={i} x={bar.x} y={i * 27} width={bar.width} height="20" />
                  ))}
                </svg>
                <div className="brand-launch-wordmark">Ceiora</div>
                <div className="brand-launch-sub">
                  Quiet intelligence for portfolio risk, factor decomposition, and hedge construction.
                </div>
              </div>
            </div>
          </div>
          <div className="brand-tile-caption">
            <span className="brand-tile-label">Launch screen · Variant B</span>
            <span className="brand-tile-note">Centered moment for boot, loading, or sign-in handoff.</span>
          </div>
        </div>

        <div className="brand-tile">
          <div className="brand-tile-surface">
            <div className="brand-paper">
              <svg className="brand-mark" viewBox="0 0 120 74" aria-hidden="true">
                {variantBars.map((bar, i) => (
                  <rect key={i} x={bar.x} y={i * 27} width={bar.width} height="20" />
                ))}
              </svg>
              <span className="brand-paper-wordmark">Ceiora</span>
              <p className="brand-paper-copy">
                An editorial surface for intros and prospectuses. Warm paper, dark ink, the mark held quiet in the corner.
              </p>
              <div className="brand-paper-meta">
                <span>Edition · Warm surface</span>
                <span>MMXXVI</span>
              </div>
            </div>
          </div>
          <div className="brand-tile-caption">
            <span className="brand-tile-label">Paper card · Variant B</span>
            <span className="brand-tile-note">Dark on cream. Restraint surface.</span>
          </div>
        </div>

        <div className="brand-tile">
          <div className="brand-tile-surface">
            <div className="brand-light">
              <div className="brand-light-row">
                <svg className="brand-mark" viewBox="0 0 120 74" aria-hidden="true">
                  {variantBars.map((bar, i) => (
                    <rect key={i} x={bar.x} y={i * 27} width={bar.width} height="20" />
                  ))}
                </svg>
                <span className="brand-light-wordmark">Ceiora</span>
              </div>
              <p className="brand-light-copy">
                The mark inverts cleanly onto pale mineral ground. Single solid color — no gradients, no adjustments — because that was the constraint from the start.
              </p>
              <div className="brand-light-rule" />
              <div className="brand-light-foot">
                <span>Light surface · Variant B</span>
                <span>#1F2024 on #F6F5F2</span>
              </div>
            </div>
          </div>
          <div className="brand-tile-caption">
            <span className="brand-tile-label">Light / inverted · Variant B</span>
            <span className="brand-tile-note">Same mark, dark ink, pale ground.</span>
          </div>
        </div>

        <div className="brand-tile">
          <div className="brand-tile-surface">
            <div className="brand-favicon-grid">
              {["xl", "lg", "md", "sm", "xs"].map((size) => (
                <div key={size} className={`brand-favicon ${size}`}>
                  <svg className="brand-mark" viewBox="0 0 120 74" aria-hidden="true">
                    {variantBars.map((bar, i) => (
                      <rect key={i} x={bar.x} y={i * 27} width={bar.width} height="20" />
                    ))}
                  </svg>
                </div>
              ))}
              <div className="brand-favicon-labels">
                <span>64</span>
                <span>40</span>
                <span>28</span>
                <span>20</span>
                <span>14</span>
              </div>
            </div>
          </div>
          <div className="brand-tile-caption">
            <span className="brand-tile-label">Favicon scale · Variant B</span>
            <span className="brand-tile-note">Chosen favicon direction. Use this compact stagger for tabs, app icon exports, and small-square brand surfaces.</span>
          </div>
        </div>
      </div>
    </section>
  );
}

export default function PalettePreviewPage() {
  return (
    <div className={`palette-study ${body.variable} ${mono.variable}`}>
      <style>{CSS}</style>
      <main className="root">
        <Cover />
        <ColorFieldSection />
        <Instrument />
        <Tokens />
        <Families />
        <Signals />
        <ChartSection />
        <Editorial />
        <Colophon />
        <MotionStandards />
        <AppliedFragments />
        <WorkflowSurfaces />
        <LightModeStudy />
        <SymbolStudyVariant />
      </main>
    </div>
  );
}
