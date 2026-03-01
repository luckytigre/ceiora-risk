"use client";

import { useState } from "react";
import { usePortfolio, useRisk } from "@/hooks/useApi";
import KpiCard from "@/components/KpiCard";
import RiskDecompChart from "@/components/RiskDecompChart";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import TableRowToggle from "@/components/TableRowToggle";

const COLLAPSED_ROWS = 10;

function fmt(n: number): string {
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

export default function OverviewPage() {
  const { data: portfolio, isLoading: pLoading } = usePortfolio();
  const { data: risk, isLoading: rLoading } = useRisk();
  const [showAllHoldings, setShowAllHoldings] = useState(false);

  if (pLoading || rLoading) {
    return <AnalyticsLoadingViz message="Loading overview..." />;
  }

  const positions = portfolio?.positions ?? [];
  const totalValue = portfolio?.total_value ?? 0;
  const posCount = portfolio?.position_count ?? 0;
  const rSquared = risk?.r_squared ?? 0;
  const condNum = risk?.condition_number ?? 0;
  const riskShares = risk?.risk_shares ?? { industry: 0, style: 0, idio: 100 };

  const holdings = [...positions].sort((a, b) => b.market_value - a.market_value);
  const visibleHoldings = showAllHoldings ? holdings : holdings.slice(0, COLLAPSED_ROWS);

  return (
    <div>
      <div className="kpi-row">
        <KpiCard label="Total Value" value={fmt(totalValue)} subtitle={`${posCount} positions`} />
        <KpiCard label="Positions" value={String(posCount)} />
        <KpiCard
          label="R-Squared"
          value={`${(rSquared * 100).toFixed(1)}%`}
          subtitle="Model explanatory power"
        />
        <KpiCard
          label="Data Age"
          value={portfolio?._cached ? "Cached" : "Live"}
          subtitle="From SQLite cache"
        />
        <KpiCard
          label="Condition #"
          value={condNum > 1e6 ? condNum.toExponential(1) : condNum.toFixed(1)}
          subtitle="Factor covariance matrix"
        />
      </div>

      <div className="chart-card mb-4">
        <h3>Risk Decomposition</h3>
        <RiskDecompChart shares={riskShares} />
      </div>

      <div className="chart-card">
        <h3>Top Holdings</h3>
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th>Ticker</th>
                <th className="text-right">Value</th>
                <th className="text-right">Weight</th>
                <th>Sector</th>
                <th className="text-right">Risk Contrib</th>
              </tr>
            </thead>
            <tbody>
              {visibleHoldings.map((pos) => (
                <tr key={pos.ticker}>
                  <td><strong>{pos.ticker}</strong></td>
                  <td className="text-right">{fmt(pos.market_value)}</td>
                  <td className="text-right">{(pos.weight * 100).toFixed(2)}%</td>
                  <td>{pos.trbc_sector || "—"}</td>
                  <td className="text-right">{pos.risk_contrib_pct.toFixed(2)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
          <TableRowToggle
            totalRows={holdings.length}
            collapsedRows={COLLAPSED_ROWS}
            expanded={showAllHoldings}
            onToggle={() => setShowAllHoldings((prev) => !prev)}
            label="holdings"
          />
        </div>
      </div>
    </div>
  );
}
