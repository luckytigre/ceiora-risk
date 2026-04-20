"use client";

import { describeCparHedgeStatus, formatCparPercent } from "@/lib/cparTruth";
import type { CparPortfolioHedgeRecommendationData } from "@/lib/types/cpar";
import CparPostHedgeTable from "./CparPostHedgeTable";

function formatMoney(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "—";
  return value.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

function formatQty(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "—";
  const decimals = Math.abs(value) < 10 ? 1 : 0;
  return value.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function signTone(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value) || value === 0) return "";
  return value > 0 ? "positive" : "negative";
}

function directionLabel(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value) || value === 0) return "Flat";
  return value > 0 ? "Long" : "Short";
}

export default function CparPortfolioHedgeRecommendationPanel({
  data,
}: {
  data: CparPortfolioHedgeRecommendationData;
}) {
  const recommendation = data.hedge_recommendation;
  const status = describeCparHedgeStatus(recommendation.hedge_status);

  return (
    <section className="cpar-hedge-panel" data-testid="cpar-portfolio-hedge-recommendation">
      <div className="chart-card">
        <h3>Factor-Neutral Recommendation</h3>
        <div className="section-subtitle">
          The server sizes ETF legs off covered gross market value for the selected scope and returns signed
          fractional quantities.
        </div>
        <div className="cpar-badge-row compact">
          <span className={`cpar-badge ${status.tone}`}>{status.label}</span>
          {recommendation.hedge_reason ? <span className="cpar-detail-chip">{recommendation.hedge_reason}</span> : null}
          <span className="cpar-detail-chip">Up to {recommendation.max_hedge_legs} legs</span>
        </div>
        <div className="cpar-package-grid compact">
          <div className="cpar-package-metric">
            <div className="cpar-package-label">Base Notional</div>
            <div className="cpar-package-value">{formatMoney(recommendation.base_notional)}</div>
          </div>
          <div className="cpar-package-metric">
            <div className="cpar-package-label">Reduction</div>
            <div className="cpar-package-value">{formatCparPercent(recommendation.non_market_reduction_ratio, 1)}</div>
          </div>
        </div>
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th>ETF</th>
                <th>Factor</th>
                <th>Direction</th>
                <th className="text-right">Quantity</th>
                <th className="text-right">Value</th>
              </tr>
            </thead>
            <tbody>
              {recommendation.trade_rows.length === 0 ? (
                <tr>
                  <td colSpan={5} className="cpar-empty-row">No factor-neutral hedge trades were required for this scope.</td>
                </tr>
              ) : (
                recommendation.trade_rows.map((row) => {
                  const tone = signTone(row.quantity);
                  return (
                  <tr key={row.factor_id}>
                    <td>
                      <strong>{row.proxy_ticker}</strong>
                      <span className="cpar-table-sub">{row.proxy_ric}</span>
                    </td>
                    <td>{row.label || row.factor_id}</td>
                    <td>
                      <span className={tone}>
                        {directionLabel(row.quantity)}
                      </span>
                    </td>
                    <td className={`text-right cpar-number-cell ${tone}`.trim()}>{formatQty(row.quantity)}</td>
                    <td className="text-right cpar-number-cell">{formatMoney(row.dollar_notional)}</td>
                  </tr>
                )})
              )}
            </tbody>
          </table>
        </div>
      </div>

      <CparPostHedgeTable rows={recommendation.post_hedge_exposures} />
    </section>
  );
}
