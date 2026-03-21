"use client";

import { useEffect, useMemo, useState } from "react";
import HelpLabel from "@/components/HelpLabel";
import { formatCparNumber, formatCparPercent } from "@/lib/cparTruth";
import type { CparPortfolioHedgeData } from "@/lib/types/cpar";
import CparRiskFactorDrilldown from "./CparRiskFactorDrilldown";
import CparRiskFactorLoadingsChart from "./CparRiskFactorLoadingsChart";

export default function CparRiskFactorSummaryCard({
  portfolio,
}: {
  portfolio: CparPortfolioHedgeData;
}) {
  const factorRows = useMemo(() => (
    [...portfolio.factor_chart].sort((left, right) => (
      left.display_order - right.display_order
      || Math.abs(right.aggregate_beta) - Math.abs(left.aggregate_beta)
      || left.factor_id.localeCompare(right.factor_id)
    ))
  ), [portfolio.factor_chart]);
  const [selectedFactorId, setSelectedFactorId] = useState<string | null>(factorRows[0]?.factor_id || null);

  useEffect(() => {
    setSelectedFactorId((current) => (
      current && factorRows.some((row) => row.factor_id === current)
        ? current
        : factorRows[0]?.factor_id || null
    ));
  }, [factorRows]);

  const selectedFactor = factorRows.find((row) => row.factor_id === selectedFactorId) || factorRows[0] || null;

  return (
    <section className="chart-card" data-testid="cpar-risk-factor-summary">
      <h3>Factor Loadings Profile</h3>
      <div className="section-subtitle">
        This stays cPAR-native. The chart decomposes each factor into negative and positive covered-row contributions,
        keeps the net aggregate beta explicit, and uses the same package-scoped snapshot that drives the hedge preview.
      </div>

      <div className="cpar-badge-row compact">
        <span className="cpar-detail-chip">{factorRows.length} active factors</span>
        <span className="cpar-detail-chip">
          Pre Var {formatCparNumber(portfolio.pre_hedge_factor_variance_proxy, 3)}
        </span>
      </div>

      {factorRows.length === 0 ? (
        <div className="detail-history-empty compact">
          No covered holdings rows contributed to the aggregate thresholded portfolio vector.
        </div>
      ) : (
        <>
          <CparRiskFactorLoadingsChart
            rows={factorRows}
            selectedFactorId={selectedFactor?.factor_id || null}
            onSelectFactor={setSelectedFactorId}
          />

          {selectedFactor ? <CparRiskFactorDrilldown factor={selectedFactor} /> : null}

          <div className="dash-table">
            <table>
              <thead>
                <tr>
                  <th>Factor</th>
                  <th>Group</th>
                  <th className="text-right">
                    <span className="col-help-wrap">
                      <HelpLabel
                        label="Beta"
                        plain="Aggregate thresholded loading on this factor after weighting covered holdings rows by signed market value."
                        math="β_f = Σ (w_i × thresholded_loading_i,f)"
                        interpret={{
                          lookFor: "Large signed portfolio factor bets.",
                          good: "Top betas align with the account’s intended cPAR hedge posture.",
                        }}
                      />
                    </span>
                  </th>
                  <th className="text-right">
                    <span className="col-help-wrap">
                      <HelpLabel
                        label="% Pre Var"
                        plain="Share of the pre-hedge factor variance proxy attributed to this factor within the factor-only cPAR surface."
                        math="share_f = (β_f × (Fβ)_f) / (βᵀFβ)"
                        interpret={{
                          lookFor: "Which factors dominate the portfolio’s pre-hedge factor-only variance.",
                          good: "Dominant factors are intentional, not accidental concentration.",
                        }}
                      />
                    </span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {factorRows.map((row) => (
                  <tr key={row.factor_id}>
                    <td>
                      <strong>{row.label}</strong>
                      <span className="cpar-table-sub">{row.factor_id}</span>
                    </td>
                    <td>{row.group}</td>
                    <td className="text-right cpar-number-cell">{formatCparNumber(row.aggregate_beta, 3)}</td>
                    <td className="text-right cpar-number-cell">{formatCparPercent(row.variance_share, 1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  );
}
