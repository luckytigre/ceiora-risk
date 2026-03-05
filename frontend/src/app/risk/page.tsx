"use client";

import { useState } from "react";
import { useRisk } from "@/hooks/useApi";
import CovarianceHeatmap from "@/components/CovarianceHeatmap";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import TableRowToggle from "@/components/TableRowToggle";
import HelpLabel from "@/components/HelpLabel";
import ApiErrorState from "@/components/ApiErrorState";
import type { FactorDetail } from "@/lib/types";

type SortKey = keyof FactorDetail;
const COLLAPSED_ROWS = 14;

export default function RiskPage() {
  const { data, isLoading, error } = useRisk();
  const [sortKey, setSortKey] = useState<SortKey>("pct_of_total");
  const [sortAsc, setSortAsc] = useState(false);
  const [showAllRows, setShowAllRows] = useState(false);

  if (isLoading) {
    return <AnalyticsLoadingViz message="Loading risk data..." />;
  }
  if (error) {
    return <ApiErrorState title="Risk Data Not Ready" error={error} />;
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
                <th onClick={() => handleSort("factor")}>
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Factor"
                      plain="The named risk driver in your model."
                      math="Each row is one factor f"
                      interpret={{
                        lookFor: "Which factors dominate your table.",
                        good: "Risk is not unintentionally concentrated in one factor.",
                      }}
                    />
                    {arrow("factor")}
                  </span>
                </th>
                <th onClick={() => handleSort("category")}>
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Category"
                      plain="Whether the factor is industry-based or style-based."
                      math="Category ∈ {industry, style}"
                      interpret={{
                        lookFor: "If one category overwhelmingly dominates.",
                        good: "Mix aligns with your intended portfolio construction.",
                      }}
                    />
                    {arrow("category")}
                  </span>
                </th>
                <th className="text-right" onClick={() => handleSort("exposure")}>
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Exposure"
                      plain="Portfolio loading on that factor before volatility scaling."
                      math="h_f = Σ (w_i × x_i,f)"
                      interpret={{
                        lookFor: "Large absolute exposures and sign concentration.",
                        good: "Exposures are intentional and not accidental bets.",
                      }}
                    />
                    {arrow("exposure")}
                  </span>
                </th>
                <th className="text-right" onClick={() => handleSort("factor_vol")}>
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Factor Vol"
                      plain="Annualized volatility of that factor’s return."
                      math="σ_f = sqrt(F_f,f)"
                      interpret={{
                        lookFor: "High-vol factors paired with high exposure.",
                        good: "Highest vol factors are controlled unless intentionally targeted.",
                      }}
                    />
                    {arrow("factor_vol")}
                  </span>
                </th>
                <th className="text-right" onClick={() => handleSort("sensitivity")}>
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Sensitivity"
                      plain="Exposure scaled by factor volatility."
                      math="Sensitivity_f = h_f × σ_f"
                      interpret={{
                        lookFor: "Large signed values; this is first-pass risk direction.",
                        good: "Top sensitivities match your intended factor bets/hedges.",
                      }}
                    />
                    {arrow("sensitivity")}
                  </span>
                </th>
                <th className="text-right" onClick={() => handleSort("marginal_var_contrib")}>
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Marg. Var"
                      plain="Raw contribution of this factor to portfolio variance, including covariance effects."
                      math="MVC_f = h_f × (Fh)_f"
                      interpret={{
                        lookFor: "Very large positives and unexpected negatives.",
                        good: "Signs and magnitude are consistent with your covariance structure.",
                        distribution: "Can be negative for hedging factors due to correlations.",
                      }}
                    />
                    {arrow("marginal_var_contrib")}
                  </span>
                </th>
                <th className="text-right" onClick={() => handleSort("pct_of_total")}>
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="% Total"
                      plain="Share of total portfolio variance attributed to this factor."
                      math="%_f = MVC_f / total variance"
                      interpret={{
                        lookFor: "Top contributors and whether negatives are true hedges.",
                        good: "No unintended single-factor dominance unless by design.",
                        distribution: "A balanced spread usually indicates better diversification.",
                      }}
                    />
                    {arrow("pct_of_total")}
                  </span>
                </th>
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
        <h3>
          <HelpLabel
            label="Factor Correlation Heatmap"
            plain="Shows how factor returns move together."
            math="corr(factor_return_i, factor_return_j)"
            interpret={{
              lookFor: "Large blocks of very high positive or negative correlation.",
              good: "Mostly moderate correlations with intuitive clusters.",
              distribution: "A broad spread around 0 usually means better diversification potential.",
            }}
          />
        </h3>
        <div className="heatmap-centered-70">
          <CovarianceHeatmap data={cov} />
        </div>
      </div>
    </div>
  );
}
