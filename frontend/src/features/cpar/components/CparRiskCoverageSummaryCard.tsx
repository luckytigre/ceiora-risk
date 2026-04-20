"use client";

import { formatCparNumber, formatCparPercent } from "@/lib/cparTruth";
import type {
  CparCoverageBreakdown,
  CparPortfolioHedgeData,
  CparPortfolioHedgeRecommendationData,
  CparPortfolioStatus,
} from "@/lib/types/cpar";

const STATUS_LABELS: Record<CparPortfolioStatus, { label: string; tone: "success" | "warning" | "error" }> = {
  ok: { label: "Coverage OK", tone: "success" },
  partial: { label: "Partial Coverage", tone: "warning" },
  empty: { label: "Empty Account", tone: "error" },
  unavailable: { label: "Coverage Unavailable", tone: "error" },
};

const BREAKDOWN_LABELS = {
  covered: "Covered",
  missing_price: "Missing Price",
  missing_cpar_fit: "Missing cPAR Fit",
  insufficient_history: "Insufficient History",
} as const;

export default function CparRiskCoverageSummaryCard({
  portfolio,
}: {
  portfolio: CparPortfolioHedgeData | CparPortfolioHedgeRecommendationData;
}) {
  const status = STATUS_LABELS[portfolio.portfolio_status];
  const breakdownEntries = Object.entries(portfolio.coverage_breakdown as CparCoverageBreakdown) as Array<
    [keyof CparCoverageBreakdown, CparCoverageBreakdown[keyof CparCoverageBreakdown]]
  >;

  return (
    <section className="chart-card" data-testid="cpar-portfolio-overview">
      <h3>Coverage Summary</h3>
      <div className="section-subtitle">
        Current holdings rows are valued at the latest shared-source price on or before the active package date.
        Only covered rows contribute to the aggregate cPAR risk vector.
      </div>
      <div className="cpar-badge-row compact">
        <span className={`cpar-badge ${status.tone}`}>{status.label}</span>
        <span className="cpar-detail-chip">{portfolio.covered_positions_count} covered</span>
        <span className="cpar-detail-chip">{portfolio.excluded_positions_count} excluded</span>
        <span className="cpar-detail-chip">Priced coverage {formatCparPercent(portfolio.coverage_ratio, 1)}</span>
      </div>

      <div className="cpar-package-grid compact">
        <div className="cpar-package-metric">
          <div className="cpar-package-label">Gross MV</div>
          <div className="cpar-package-value">{formatCparNumber(portfolio.gross_market_value, 2)}</div>
          <div className="cpar-package-detail">Priced holdings rows only</div>
        </div>
        <div className="cpar-package-metric">
          <div className="cpar-package-label">Covered Gross</div>
          <div className="cpar-package-value">{formatCparNumber(portfolio.covered_gross_market_value, 2)}</div>
          <div className="cpar-package-detail">Rows included in the hedge vector</div>
        </div>
        <div className="cpar-package-metric">
          <div className="cpar-package-label">Net MV</div>
          <div className="cpar-package-value">{formatCparNumber(portfolio.net_market_value, 2)}</div>
          <div className="cpar-package-detail">Covered rows only</div>
        </div>
      </div>

      <div className="cpar-risk-breakdown-grid" data-testid="cpar-risk-coverage-breakdown">
        {breakdownEntries.map(([key, value]) => (
          <div key={key} className="cpar-risk-breakdown-card">
            <div className="cpar-risk-breakdown-label">{BREAKDOWN_LABELS[key]}</div>
            <div className="cpar-risk-breakdown-value">{value.positions_count}</div>
            <div className="cpar-risk-breakdown-detail">
              Gross MV {formatCparNumber(value.gross_market_value, 2)}
            </div>
          </div>
        ))}
      </div>

      {portfolio.portfolio_reason ? (
        <div className="cpar-inline-message warning">
          <strong>Workflow note.</strong>
          <span>{portfolio.portfolio_reason}</span>
        </div>
      ) : null}
    </section>
  );
}
