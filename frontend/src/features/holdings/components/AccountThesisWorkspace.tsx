"use client";

import { useHoldingsPositions } from "@/hooks/useHoldingsApi";
import ApiErrorState from "@/components/ApiErrorState";
import ThesisPositionsTable from "@/features/holdings/components/ThesisPositionsTable";
import { buildDashPositions } from "@/features/dash/dashPositions";
import type { Position } from "@/lib/types/analytics";
import type { CparPortfolioPositionRow } from "@/lib/types/cpar";

/** Risk pages retain their aggregate instrument table. Their thesis view uses
 * real account holdings and only exact, quantity-matching model risk metadata. */
export default function AccountThesisWorkspace({ analytics, family }: {
  analytics: Position[] | CparPortfolioPositionRow[];
  family: "cuse" | "cpar";
}) {
  const holdings = useHoldingsPositions(null);
  if (holdings.error) return <ApiErrorState surface="account positions" error={holdings.error} />;
  if (!holdings.data) return <p role="status">Loading account positions…</p>;
  return <>
    <p className="section-subtitle">Theses group current account holdings. Prices and risk metadata use the last served model snapshot; unavailable account risk mixes show —. Portfolio risk measures remain in the instrument view.</p>
    <ThesisPositionsTable positions={buildDashPositions(holdings.data.positions, analytics)} orderScope={family === "cpar" ? "cpar-risk" : "cuse-risk"} />
  </>;
}
