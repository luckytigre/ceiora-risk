"use client";

import { useState } from "react";
import { useRisk } from "@/hooks/useApi";
import CovarianceHeatmap from "@/components/CovarianceHeatmap";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import TableRowToggle from "@/components/TableRowToggle";
import type { FactorDetail } from "@/lib/types";

type SortKey = keyof FactorDetail;
const COLLAPSED_ROWS = 14;

export default function RiskPage() {
  const { data, isLoading } = useRisk();
  const [sortKey, setSortKey] = useState<SortKey>("pct_of_total");
  const [sortAsc, setSortAsc] = useState(false);
  const [showAllRows, setShowAllRows] = useState(false);

  if (isLoading) {
    return <AnalyticsLoadingViz message="Loading risk data..." />;
  }

  const details = data?.factor_details ?? [];
  const cov = data?.cov_matrix ?? { factors: [], correlation: [] };

  const sorted = [...details].sort((a, b) => {
    const av = a[sortKey];
    const bv = b[sortKey];
    if (typeof av === "number" && typeof bv === "number") {
      return sortAsc ? av - bv : bv - av;
    }
    return sortAsc
      ? String(av).localeCompare(String(bv))
      : String(bv).localeCompare(String(av));
  });

  const handleSort = (key: SortKey) => {
    if (key === sortKey) setSortAsc(!sortAsc);
    else { setSortKey(key); setSortAsc(false); }
  };

  const arrow = (key: SortKey) =>
    sortKey === key ? (sortAsc ? " ↑" : " ↓") : "";
  const visibleRows = showAllRows ? sorted : sorted.slice(0, COLLAPSED_ROWS);

  return (
    <div>
      <div className="chart-card mb-4">
        <h3>Variance Attribution</h3>
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th onClick={() => handleSort("factor")}>Factor{arrow("factor")}</th>
                <th onClick={() => handleSort("category")}>Category{arrow("category")}</th>
                <th className="text-right" onClick={() => handleSort("exposure")}>Exposure{arrow("exposure")}</th>
                <th className="text-right" onClick={() => handleSort("factor_vol")}>Factor Vol{arrow("factor_vol")}</th>
                <th className="text-right" onClick={() => handleSort("sensitivity")}>Sensitivity{arrow("sensitivity")}</th>
                <th className="text-right" onClick={() => handleSort("marginal_var_contrib")}>Marg. Var{arrow("marginal_var_contrib")}</th>
                <th className="text-right" onClick={() => handleSort("pct_of_total")}>% Total{arrow("pct_of_total")}</th>
              </tr>
            </thead>
            <tbody>
              {visibleRows.map((d) => (
                <tr key={d.factor}>
                  <td><strong>{d.factor}</strong></td>
                  <td>
                    <span className={`text-xs ${
                      d.category === "style" ? "text-[#f5bae4]" : "text-[#cc3558]"
                    }`}>
                      {d.category}
                    </span>
                  </td>
                  <td className="text-right">{d.exposure.toFixed(4)}</td>
                  <td className="text-right">{(d.factor_vol * 100).toFixed(2)}%</td>
                  <td className="text-right">{d.sensitivity.toFixed(4)}</td>
                  <td className="text-right">{d.marginal_var_contrib.toFixed(6)}</td>
                  <td className="text-right">
                    <span className={d.pct_of_total >= 0 ? "positive" : "negative"}>
                      {d.pct_of_total.toFixed(2)}%
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <TableRowToggle
            totalRows={sorted.length}
            collapsedRows={COLLAPSED_ROWS}
            expanded={showAllRows}
            onToggle={() => setShowAllRows((prev) => !prev)}
            label="factors"
          />
        </div>
      </div>

      <div className="chart-card">
        <h3>Factor Correlation Heatmap</h3>
        <CovarianceHeatmap data={cov} />
      </div>
    </div>
  );
}
