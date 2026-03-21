"use client";

import { useMemo, useState } from "react";
import TableRowToggle from "@/components/TableRowToggle";
import { describeCparFitStatus, formatCparNumber, formatCparPercent } from "@/lib/cparTruth";
import type { CparFactorChartRow } from "@/lib/types/cpar";
import CparWarningsBar from "./CparWarningsBar";

const COLLAPSED_ROWS = 8;

type SortKey = "contribution" | "beta" | "weight";

export default function CparRiskFactorDrilldown({
  factor,
}: {
  factor: CparFactorChartRow;
}) {
  const [expanded, setExpanded] = useState(false);
  const [sortKey, setSortKey] = useState<SortKey>("contribution");

  const sortedRows = useMemo(() => {
    const rows = [...factor.drilldown];
    rows.sort((left, right) => {
      if (sortKey === "beta") return Math.abs(right.factor_beta || 0) - Math.abs(left.factor_beta || 0);
      if (sortKey === "weight") return Math.abs((right.portfolio_weight || 0)) - Math.abs((left.portfolio_weight || 0));
      return Math.abs(right.contribution_beta) - Math.abs(left.contribution_beta);
    });
    return rows;
  }, [factor.drilldown, sortKey]);
  const visibleRows = expanded ? sortedRows : sortedRows.slice(0, COLLAPSED_ROWS);

  return (
    <section className="chart-card" data-testid="cpar-risk-factor-drilldown">
      <h3>{factor.label} Drilldown</h3>
      <div className="section-subtitle">
        Covered rows with a non-zero thresholded contribution to this factor are decomposed below. Excluded rows stay
        explicit in the positions table because they do not participate in the weighted account vector.
      </div>
      <div className="cpar-badge-row compact">
        <span className="cpar-detail-chip">{formatCparNumber(factor.aggregate_beta, 3)} aggregate beta</span>
        <span className="cpar-detail-chip">{formatCparPercent(factor.variance_share, 1)} pre var</span>
        <span className="cpar-detail-chip">{factor.drilldown.length} contributing rows</span>
      </div>
      <div className="cpar-factor-drilldown-toolbar">
        <span className="cpar-package-label">Sort</span>
        <div className="cpar-mode-toggle compact">
          <button
            type="button"
            className={`cpar-mode-btn ${sortKey === "contribution" ? "active" : ""}`}
            onClick={() => setSortKey("contribution")}
          >
            Contribution
          </button>
          <button
            type="button"
            className={`cpar-mode-btn ${sortKey === "beta" ? "active" : ""}`}
            onClick={() => setSortKey("beta")}
          >
            Beta
          </button>
          <button
            type="button"
            className={`cpar-mode-btn ${sortKey === "weight" ? "active" : ""}`}
            onClick={() => setSortKey("weight")}
          >
            Weight
          </button>
        </div>
      </div>

      <div className="dash-table">
        <table>
          <thead>
            <tr>
              <th>Instrument</th>
              <th>Coverage</th>
              <th className="text-right">Mkt Value</th>
              <th className="text-right">Weight</th>
              <th className="text-right">Factor Beta</th>
              <th className="text-right">Contribution</th>
            </tr>
          </thead>
          <tbody>
            {visibleRows.length === 0 ? (
              <tr>
                <td colSpan={6} className="cpar-empty-row">
                  No covered positions contributed to this factor.
                </td>
              </tr>
            ) : (
              visibleRows.map((row) => {
                const fit = row.fit_status ? describeCparFitStatus(row.fit_status) : null;
                return (
                  <tr key={`${factor.factor_id}:${row.ric}`}>
                    <td>
                      <strong>{row.display_name || row.ticker || row.ric}</strong>
                      <span className="cpar-table-sub">
                        {row.ticker || "—"} · {row.ric}
                      </span>
                    </td>
                    <td>
                      <div className="cpar-badge-row compact">
                        <span className="cpar-badge success">Covered</span>
                        {fit ? <span className={`cpar-badge ${fit.tone}`}>{fit.label}</span> : null}
                      </div>
                      {row.fit_status ? <CparWarningsBar fitStatus={row.fit_status} warnings={row.warnings} compact /> : null}
                    </td>
                    <td className="text-right cpar-number-cell">{formatCparNumber(row.market_value, 2)}</td>
                    <td className="text-right cpar-number-cell">{formatCparPercent(row.portfolio_weight, 1)}</td>
                    <td className="text-right cpar-number-cell">{formatCparNumber(row.factor_beta, 3)}</td>
                    <td className="text-right cpar-number-cell">{formatCparNumber(row.contribution_beta, 3)}</td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      <TableRowToggle
        totalRows={sortedRows.length}
        collapsedRows={COLLAPSED_ROWS}
        expanded={expanded}
        onToggle={() => setExpanded((current) => !current)}
        label="factor rows"
      />
    </section>
  );
}
