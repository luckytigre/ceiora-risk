"use client";

import HelpLabel from "@/components/HelpLabel";
import { formatCparNumber, formatCparPercent } from "@/lib/cparTruth";
import type { CparPortfolioHedgeData } from "@/lib/types/cpar";

export default function CparRiskFactorSummaryCard({
  portfolio,
}: {
  portfolio: CparPortfolioHedgeData;
}) {
  const factorRows = [...portfolio.factor_variance_contributions].sort(
    (left, right) => Math.abs(right.variance_share || 0) - Math.abs(left.variance_share || 0),
  );

  return (
    <section className="chart-card" data-testid="cpar-risk-factor-summary">
      <h3>Factor Contribution Profile</h3>
      <div className="section-subtitle">
        This summary stays factor-only. It uses the aggregate thresholded portfolio vector from covered rows plus
        the active-package covariance surface. It still does not introduce cUSE-style risk shares, specific risk,
        or covariance heatmaps.
      </div>

      <div className="cpar-badge-row compact">
        <span className="cpar-detail-chip">{portfolio.aggregate_thresholded_loadings.length} active factors</span>
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
          <div className="cpar-risk-share-stack" aria-hidden="true">
            {factorRows.map((row) => (
              <div key={row.factor_id} className="cpar-risk-share-row">
                <div className="cpar-risk-share-header">
                  <span className="cpar-risk-share-label">{row.label}</span>
                  <span className="cpar-risk-share-value">{formatCparPercent(row.variance_share, 1)}</span>
                </div>
                <div className="cpar-risk-share-bar">
                  <span
                    className="cpar-risk-share-fill"
                    style={{ width: `${Math.max(4, Math.min(100, Math.abs((row.variance_share || 0) * 100)))}%` }}
                  />
                </div>
              </div>
            ))}
          </div>

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
                    <td className="text-right cpar-number-cell">{formatCparNumber(row.beta, 3)}</td>
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
