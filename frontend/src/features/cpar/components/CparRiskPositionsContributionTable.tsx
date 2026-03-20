"use client";

import { useMemo, useState } from "react";
import TableRowToggle from "@/components/TableRowToggle";
import { describeCparFitStatus, formatCparNumber, formatCparPercent } from "@/lib/cparTruth";
import type { CparPortfolioPositionRow } from "@/lib/types/cpar";
import CparWarningsBar from "./CparWarningsBar";

const COLLAPSED_ROWS = 8;

function topContributions(row: CparPortfolioPositionRow) {
  return [...row.thresholded_contributions]
    .sort((left, right) => Math.abs(right.beta) - Math.abs(left.beta))
    .slice(0, 3);
}

export default function CparRiskPositionsContributionTable({
  rows,
}: {
  rows: CparPortfolioPositionRow[];
}) {
  const [expanded, setExpanded] = useState(false);
  const visibleRows = expanded ? rows : rows.slice(0, COLLAPSED_ROWS);
  const coveredCount = useMemo(
    () => rows.filter((row) => row.coverage === "covered").length,
    [rows],
  );

  return (
    <section className="chart-card" data-testid="cpar-risk-positions">
      <h3>Positions (Contribution Mix)</h3>
      <div className="section-subtitle">
        This table stays cPAR-native: it shows the weighted thresholded factor contribution mix for covered rows,
        while excluded rows remain explicit with the reason they were withheld from the account vector.
      </div>
      <div className="cpar-badge-row compact">
        <span className="cpar-detail-chip">{coveredCount} covered rows</span>
        <span className="cpar-detail-chip">{rows.length - coveredCount} excluded rows</span>
      </div>

      <div className="dash-table">
        <table>
          <thead>
            <tr>
              <th>Instrument</th>
              <th className="text-right">Mkt Value</th>
              <th className="text-right">Weight</th>
              <th>Contribution Mix</th>
              <th>Coverage</th>
            </tr>
          </thead>
          <tbody>
            {visibleRows.length === 0 ? (
              <tr>
                <td colSpan={5} className="cpar-empty-row">No holdings rows are available for this account.</td>
              </tr>
            ) : (
              visibleRows.map((row) => {
                const fit = row.fit_status ? describeCparFitStatus(row.fit_status) : null;
                const contributions = topContributions(row);
                return (
                  <tr key={`${row.account_id}:${row.ric}`}>
                    <td>
                      <strong>{row.display_name || row.ticker || row.ric}</strong>
                      <span className="cpar-table-sub">
                        {row.ticker || "—"} · {row.ric}
                      </span>
                      <span className="cpar-table-sub">
                        Qty {formatCparNumber(row.quantity, 3)}
                        {row.price_date ? ` · ${row.price_field_used || "price"} @ ${row.price_date}` : ""}
                      </span>
                    </td>
                    <td className="text-right cpar-number-cell">{formatCparNumber(row.market_value, 2)}</td>
                    <td className="text-right cpar-number-cell">{formatCparPercent(row.portfolio_weight, 1)}</td>
                    <td>
                      {contributions.length === 0 ? (
                        <span className="cpar-table-sub">Excluded from the aggregate cPAR vector.</span>
                      ) : (
                        <div className="cpar-risk-mix-list">
                          {contributions.map((item) => (
                            <div key={`${row.ric}:${item.factor_id}`} className="cpar-risk-mix-item">
                              <span className="cpar-risk-mix-factor">{item.label}</span>
                              <span className="cpar-risk-mix-value">{formatCparNumber(item.beta, 3)}</span>
                            </div>
                          ))}
                        </div>
                      )}
                    </td>
                    <td>
                      <div className="cpar-badge-row compact">
                        <span className={`cpar-badge ${
                          row.coverage === "covered"
                            ? "success"
                            : row.coverage === "insufficient_history"
                              ? "error"
                              : "warning"
                        }`}>
                          {row.coverage === "covered"
                            ? "Covered"
                            : row.coverage === "missing_price"
                              ? "Missing Price"
                              : row.coverage === "missing_cpar_fit"
                                ? "Missing cPAR Fit"
                                : "Insufficient History"}
                        </span>
                        {fit ? <span className={`cpar-badge ${fit.tone}`}>{fit.label}</span> : null}
                      </div>
                      {row.fit_status ? <CparWarningsBar fitStatus={row.fit_status} warnings={row.warnings} compact /> : null}
                      {row.coverage_reason ? <span className="cpar-table-sub">{row.coverage_reason}</span> : null}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      <TableRowToggle
        totalRows={rows.length}
        collapsedRows={COLLAPSED_ROWS}
        expanded={expanded}
        onToggle={() => setExpanded((current) => !current)}
        label="positions"
      />
    </section>
  );
}
