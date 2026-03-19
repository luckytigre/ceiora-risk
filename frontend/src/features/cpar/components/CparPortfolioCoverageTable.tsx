"use client";

import { formatCparNumber, formatCparPercent } from "@/lib/cparTruth";
import type { CparPortfolioPositionRow } from "@/lib/types";
import CparWarningsBar from "./CparWarningsBar";

const COVERAGE_STYLE: Record<string, { label: string; tone: "success" | "warning" | "error" }> = {
  covered: { label: "Covered", tone: "success" },
  missing_price: { label: "Missing Price", tone: "warning" },
  missing_cpar_fit: { label: "Missing cPAR Fit", tone: "warning" },
  insufficient_history: { label: "Insufficient History", tone: "error" },
};

export default function CparPortfolioCoverageTable({
  rows,
}: {
  rows: CparPortfolioPositionRow[];
}) {
  return (
    <section className="chart-card" data-testid="cpar-portfolio-coverage">
      <h3>Coverage By Holding</h3>
      <div className="section-subtitle">
        Each row is valued at the latest shared-source price on or before the active package date, then checked against the active cPAR package.
      </div>
      <div className="dash-table">
        <table>
          <thead>
            <tr>
              <th>Instrument</th>
              <th className="text-right">Qty</th>
              <th className="text-right">Price</th>
              <th className="text-right">Mkt Value</th>
              <th className="text-right">Weight</th>
              <th>Coverage</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={6} className="cpar-empty-row">No holdings rows are available for this account.</td>
              </tr>
            ) : (
              rows.map((row) => {
                const coverage = COVERAGE_STYLE[row.coverage] || { label: row.coverage, tone: "neutral" as const };
                return (
                  <tr key={`${row.account_id}:${row.ric}`}>
                    <td>
                      <strong>{row.display_name || row.ticker || row.ric}</strong>
                      <span className="cpar-table-sub">
                        {row.ticker || "—"} · {row.ric}
                      </span>
                    </td>
                    <td className="text-right cpar-number-cell">{formatCparNumber(row.quantity, 3)}</td>
                    <td className="text-right cpar-number-cell">{formatCparNumber(row.price, 2)}</td>
                    <td className="text-right cpar-number-cell">{formatCparNumber(row.market_value, 2)}</td>
                    <td className="text-right cpar-number-cell">{formatCparPercent(row.portfolio_weight, 1)}</td>
                    <td>
                      <div className="cpar-badge-row compact">
                        <span className={`cpar-badge ${coverage.tone}`}>{coverage.label}</span>
                      </div>
                      {row.fit_status ? <CparWarningsBar fitStatus={row.fit_status} warnings={row.warnings} compact /> : null}
                      {row.coverage_reason ? (
                        <span className="cpar-table-sub">{row.coverage_reason}</span>
                      ) : row.price_date ? (
                        <span className="cpar-table-sub">
                          {row.price_field_used || "price"} @ {row.price_date}
                        </span>
                      ) : null}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}
