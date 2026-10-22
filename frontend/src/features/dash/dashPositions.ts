import type { HoldingsPosition } from "@/lib/types/holdings";
import type { Position } from "@/lib/types/analytics";
import type { CparPortfolioPositionRow } from "@/lib/types/cpar";

export interface DashPosition {
  account: string;
  ric: string;
  ticker: string;
  shares: number;
  price: number | null;
  market_value: number | null;
  trbc_industry_group: string;
  risk_mix: Position["risk_mix"] | null;
}

// Holdings own identity and quantity. Instrument metadata may join by RIC, but
// an aggregate-book risk mix must never be attributed to an individual account.
export function buildDashPositions(
  holdings: HoldingsPosition[],
  analytics: Position[] | CparPortfolioPositionRow[],
): DashPosition[] {
  const byIdentity = new Map<string, Position | CparPortfolioPositionRow>();
  const byRic = new Map<string, Position | CparPortfolioPositionRow>();
  for (const row of analytics) {
    const ric = String(row.ric ?? "").trim().toUpperCase();
    const account = ("account" in row ? row.account : row.account_id).trim().toLowerCase();
    byIdentity.set(`${account}:${ric}`, row);
    if (!byRic.has(ric)) byRic.set(ric, row);
  }
  return holdings.map((holding) => {
    const exact = byIdentity.get(`${holding.account_id.trim().toLowerCase()}:${holding.ric.trim().toUpperCase()}`);
    const metadata = exact ?? byRic.get(holding.ric.trim().toUpperCase());
    const price = typeof metadata?.price === "number" && Number.isFinite(metadata.price) ? metadata.price : null;
    return {
      account: holding.account_id,
      ric: holding.ric,
      ticker: holding.ticker,
      shares: holding.quantity,
      price,
      market_value: price === null ? null : holding.quantity * price,
      trbc_industry_group: metadata?.trbc_industry_group ?? "Unmapped",
      risk_mix: exact && ("shares" in exact ? exact.shares : exact.quantity) === holding.quantity
        ? exact.risk_mix ?? null : null,
    };
  });
}
