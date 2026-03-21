"use client";

import { formatCparNumber, formatCparPercent } from "@/lib/cparTruth";
import type { CparFactorChartRow } from "@/lib/types/cpar";

const GROUP_LABELS: Record<string, string> = {
  market: "Market",
  sector: "Sector",
  style: "Style",
};

export default function CparRiskFactorLoadingsChart({
  rows,
  selectedFactorId,
  onSelectFactor,
}: {
  rows: CparFactorChartRow[];
  selectedFactorId: string | null;
  onSelectFactor: (factorId: string) => void;
}) {
  const maxMagnitude = rows.reduce((maxValue, row) => (
    Math.max(
      maxValue,
      Math.abs(row.positive_contribution_beta),
      Math.abs(row.negative_contribution_beta),
      Math.abs(row.aggregate_beta),
    )
  ), 0) || 1;

  return (
    <div className="cpar-factor-chart-shell" data-testid="cpar-risk-factor-chart">
      <div className="cpar-factor-chart-legend">
        <span className="cpar-detail-chip">Left: negative contribution</span>
        <span className="cpar-detail-chip">Right: positive contribution</span>
        <span className="cpar-detail-chip">Marker: net aggregate beta</span>
      </div>

      <div className="cpar-factor-chart-grid" role="list" aria-label="cPAR factor loadings chart">
        {rows.map((row, index) => {
          const previousGroup = index > 0 ? rows[index - 1]?.group : null;
          const showGroupLabel = index === 0 || previousGroup !== row.group;
          const negativeWidth = Math.min(50, (Math.abs(row.negative_contribution_beta) / maxMagnitude) * 50);
          const positiveWidth = Math.min(50, (Math.abs(row.positive_contribution_beta) / maxMagnitude) * 50);
          const markerPosition = Math.max(
            0,
            Math.min(100, 50 + ((row.aggregate_beta / maxMagnitude) * 50)),
          );
          return (
            <div key={row.factor_id} role="listitem">
              {showGroupLabel ? (
                <div className="cpar-factor-chart-group-label">{GROUP_LABELS[row.group] || row.group}</div>
              ) : null}
              <button
                type="button"
                className={`cpar-factor-chart-row ${selectedFactorId === row.factor_id ? "selected" : ""}`}
                onClick={() => onSelectFactor(row.factor_id)}
                aria-pressed={selectedFactorId === row.factor_id}
              >
                <div className="cpar-factor-chart-meta">
                  <div>
                    <div className="cpar-factor-chart-label">{row.label}</div>
                    <div className="cpar-table-sub">{row.factor_id}</div>
                  </div>
                  <div className="cpar-factor-chart-values">
                    <span>{formatCparNumber(row.aggregate_beta, 3)} beta</span>
                    <span>{formatCparPercent(row.variance_share, 1)} pre var</span>
                  </div>
                </div>
                <div className="cpar-factor-chart-track">
                  <span className="cpar-factor-chart-axis" aria-hidden="true" />
                  <span
                    className="cpar-factor-chart-bar negative"
                    aria-hidden="true"
                    style={{
                      left: `${50 - negativeWidth}%`,
                      width: `${negativeWidth}%`,
                    }}
                  />
                  <span
                    className="cpar-factor-chart-bar positive"
                    aria-hidden="true"
                    style={{
                      left: "50%",
                      width: `${positiveWidth}%`,
                    }}
                  />
                  <span
                    className="cpar-factor-chart-marker"
                    aria-hidden="true"
                    style={{ left: `${markerPosition}%` }}
                  />
                </div>
              </button>
            </div>
          );
        })}
      </div>
    </div>
  );
}
