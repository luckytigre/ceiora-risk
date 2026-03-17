import type { WhatIfScenarioRow } from "@/lib/types";
export { refreshFailureMessage } from "@/lib/refresh";

export type WhatIfMode = "raw" | "sensitivity" | "risk_contribution";

export interface ScenarioDraftRow {
  key: string;
  account_id: string;
  ticker: string;
  quantity_text: string;
  source: string;
}

export interface ExplorePositionSummary {
  shares: number;
  weight: number;
  market_value: number;
  long_short: string;
}

export const WHAT_IF_MODES: Array<{ key: WhatIfMode; label: string }> = [
  { key: "raw", label: "Exposure" },
  { key: "sensitivity", label: "Sensitivity" },
  { key: "risk_contribution", label: "Risk Contrib" },
];

const STRICT_QTY_RE = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/;

export function normalizeAccountId(raw: string | null | undefined): string {
  return String(raw || "").trim().toLowerCase();
}

export function normalizeTicker(raw: string | null | undefined): string {
  return String(raw || "").trim().toUpperCase();
}

export function scenarioKey(accountId: string, ticker: string): string {
  return `${normalizeAccountId(accountId)}::${normalizeTicker(ticker)}`;
}

export function parseQty(raw: string): number | null {
  const clean = String(raw || "").trim().replaceAll(",", "");
  if (!clean) return null;
  if (!STRICT_QTY_RE.test(clean)) return null;
  const out = Number.parseFloat(clean);
  return Number.isFinite(out) ? out : null;
}

export function fmtQty(n: number): string {
  if (!Number.isFinite(n)) return "—";
  const rounded = Number(n.toFixed(6));
  if (Number.isInteger(rounded)) return `${rounded}`;
  return `${rounded}`;
}

export function fmtMarketValue(n: number): string {
  if (!Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

export function formatScenarioCount(count: number): string {
  return `${count} what-if trade${count === 1 ? "" : "s"}`;
}

export function buildScenarioPayloadRows({
  scenarioRows,
  validAccountIds,
  action,
}: {
  scenarioRows: ScenarioDraftRow[];
  validAccountIds: Set<string>;
  action: "preview" | "apply";
}): { rows: WhatIfScenarioRow[] } | { error: string } {
  const rows: WhatIfScenarioRow[] = [];
  for (const row of scenarioRows) {
    const qty = parseQty(row.quantity_text);
    if (qty === null) {
      return { error: `Fix quantity for ${row.ticker} before ${action === "apply" ? "applying" : "previewing"}.` };
    }
    if (validAccountIds.size > 0 && !validAccountIds.has(normalizeAccountId(row.account_id))) {
      return { error: `Choose an existing account for ${row.ticker} before ${action === "apply" ? "applying" : "previewing"}.` };
    }
    rows.push({
      account_id: row.account_id,
      ticker: row.ticker,
      quantity: qty,
      source: row.source || "what_if",
    });
  }
  return { rows };
}
