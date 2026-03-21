"use client";

import type { CparSearchItem } from "@/lib/types/cpar";

export type CparExploreMode = "raw" | "sensitivity" | "risk_contribution";

export interface CparExplorePositionSummary {
  shares: number;
  weight: number;
  market_value: number;
  long_short: string;
}

export interface CparExploreScenarioDraftRow {
  key: string;
  account_id: string;
  ticker: string;
  ric: string;
  quantity_text: string;
  display_name: string | null;
  fit_status: CparSearchItem["fit_status"];
  hq_country_code: string | null | undefined;
  source: string;
}

export const CPAR_EXPLORE_MODES: Array<{ key: CparExploreMode; label: string }> = [
  { key: "raw", label: "Raw" },
  { key: "sensitivity", label: "Sensitivity" },
  { key: "risk_contribution", label: "Risk %" },
];

export function normalizeAccountId(value: string | null | undefined): string {
  return String(value || "").trim().toLowerCase();
}

export function normalizeTicker(value: string | null | undefined): string {
  return String(value || "").trim().toUpperCase();
}

export function normalizeRic(value: string | null | undefined): string {
  return String(value || "").trim().toUpperCase();
}

export function scenarioKey(accountId: string | null | undefined, ric: string | null | undefined): string {
  return `${normalizeAccountId(accountId)}:${normalizeRic(ric)}`;
}

export function parseQty(value: string | null | undefined): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || Math.abs(parsed) <= 1e-12) return null;
  return parsed;
}

export function fmtQty(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return value.toLocaleString(undefined, { maximumFractionDigits: 3 });
}

export function fmtMarketValue(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  const abs = Math.abs(value);
  if (abs >= 1e9) return `$${(value / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `$${(value / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `$${(value / 1e3).toFixed(1)}K`;
  return `$${value.toFixed(0)}`;
}

export function formatScenarioCount(count: number): string {
  return `${count} scenario row${count === 1 ? "" : "s"}`;
}
