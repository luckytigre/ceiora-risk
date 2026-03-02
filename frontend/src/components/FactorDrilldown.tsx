"use client";

import { useState } from "react";
import type { FactorDrilldownItem } from "@/lib/types";
import TableRowToggle from "@/components/TableRowToggle";
import FactorHistoryChart from "@/components/FactorHistoryChart";
import HelpLabel from "@/components/HelpLabel";
import { useFactorHistory } from "@/hooks/useApi";

interface FactorDrilldownProps {
  factor: string;
  items: FactorDrilldownItem[];
  mode?: string;
  factorVol?: number;
  onClose: () => void;
}
const COLLAPSED_ROWS = 12;
type SortKey = "ticker" | "weight" | "exposure" | "sensitivity" | "contribution";

export default function FactorDrilldown({ factor, items, mode, factorVol, onClose }: FactorDrilldownProps) {
  const isSensitivity = mode === "sensitivity";
  const isRiskContribution = mode === "risk_contribution";
  const hints = {
    weight: {
      plain: "Position Market Value ÷ Total Portfolio Market Value.",
      math: "wᵢ = MVᵢ / ΣMV",
    },
    loading: {
      plain: "Position loading on the selected factor.",
      math: "xᵢ,ᶠ",
    },
    sensitivity: isRiskContribution
      ? {
          plain: "Loading × covariance adjustment for this factor.",
          math: "xᵢ,ᶠ × (Fh)ᶠ",
        }
      : {
          plain: "Loading × 1σ factor volatility.",
          math: "xᵢ,ᶠ × σᶠ",
        },
    contribution: isRiskContribution
      ? {
          plain: "Weight × Loading × covariance adjustment, then scaled to factor % of total risk.",
          math: "((wᵢ × xᵢ,ᶠ × (Fh)ᶠ) / marginalᶠ) × factor % total",
        }
      : isSensitivity
        ? {
            plain: "Weight × Loading × 1σ factor volatility.",
            math: "wᵢ × (xᵢ,ᶠ × σᶠ)",
          }
        : {
            plain: "Weight × Loading.",
            math: "wᵢ × xᵢ,ᶠ",
          },
  };
  const [sortKey, setSortKey] = useState<SortKey>(
    isSensitivity ? "sensitivity" : isRiskContribution ? "contribution" : "exposure",
  );
  const [sortAsc, setSortAsc] = useState(false);
  const [showAllRows, setShowAllRows] = useState(false);
  const { data: historyData, isLoading: historyLoading } = useFactorHistory(factor, 5);
  const sorted = [...items].sort((a, b) => {
    if (sortKey === "ticker") {
      return sortAsc
        ? a.ticker.localeCompare(b.ticker)
        : b.ticker.localeCompare(a.ticker);
    }
    const av = sortKey === "sensitivity" ? (a.sensitivity ?? 0) : a[sortKey];
    const bv = sortKey === "sensitivity" ? (b.sensitivity ?? 0) : b[sortKey];
    return sortAsc ? av - bv : bv - av;
  });
  const uniqueExposureCount = new Set(items.map((item) => item.exposure.toFixed(6))).size;
  const handleSort = (key: SortKey) => {
    if (key === sortKey) setSortAsc((prev) => !prev);
    else {
      setSortKey(key);
      setSortAsc(false);
    }
  };
  const arrow = (key: SortKey) => (sortKey === key ? (sortAsc ? " ↑" : " ↓") : "");
  const visibleRows = showAllRows ? sorted : sorted.slice(0, COLLAPSED_ROWS);
  return (
    <div className="detail-panel">
      <div className="detail-panel-header">
        <h4>
          {factor} — {isSensitivity
            ? "Sensitivity Breakdown"
            : isRiskContribution
              ? "Risk Contribution Breakdown"
              : "Position Breakdown"}
        </h4>
        <button
          onClick={onClose}
          className="detail-panel-close"
        >
          CLOSE
        </button>
      </div>
      <div className="detail-history">
        <div className="detail-history-header">
          <h5>5Y Historical Return — {factor}</h5>
          {!historyLoading && historyData?.points && historyData.points.length > 0 && (() => {
            const vals = historyData.points.map((p) => p.cum_return * 100);
            const latest = vals[vals.length - 1] ?? 0;
            const pos = latest >= 0;
            const s = latest >= 0 ? "+" : "";
            return (
              <div className="detail-history-stats">
                <span
                  className="detail-history-stat"
                  style={{ color: pos ? "rgba(107, 207, 154, 0.85)" : "rgba(224, 87, 127, 0.85)" }}
                >
                  {s}{latest.toFixed(1)}%
                </span>
                {factorVol != null && (
                  <span className="detail-history-stat muted">
                    σ {(factorVol * 100).toFixed(1)}%
                  </span>
                )}
              </div>
            );
          })()}
        </div>
        {historyLoading
          ? <div className="detail-history-empty loading-pulse">Loading 5Y history...</div>
          : <FactorHistoryChart factor={factor} points={historyData?.points ?? []} factorVol={factorVol} />}
      </div>
      <p className="detail-panel-meta">
        {items.length} positions, {uniqueExposureCount} unique exposure values
        {isRiskContribution ? ", covariance-adjusted contributions" : ""}
      </p>
      <div className="dash-table">
        <table>
          <thead>
            <tr>
              <th onClick={() => handleSort("ticker")}>Ticker{arrow("ticker")}</th>
              <th className="text-right" onClick={() => handleSort("weight")}>
                <span className="col-help-wrap">
                  <HelpLabel label="Weight" plain={hints.weight.plain} math={hints.weight.math} />
                  {arrow("weight")}
                </span>
              </th>
              <th className="text-right" onClick={() => handleSort("exposure")}>
                <span className="col-help-wrap">
                  <HelpLabel
                    label={isRiskContribution ? "Raw Loading" : "Loading"}
                    plain={hints.loading.plain}
                    math={hints.loading.math}
                  />
                  {arrow("exposure")}
                </span>
              </th>
              {(isSensitivity || isRiskContribution) && (
                <th className="text-right" onClick={() => handleSort("sensitivity")}>
                  <span className="col-help-wrap">
                    <HelpLabel
                      label={isRiskContribution ? "Loading × CovAdj" : "Loading × 1σ Vol"}
                      plain={hints.sensitivity.plain}
                      math={hints.sensitivity.math}
                    />
                    {arrow("sensitivity")}
                  </span>
                </th>
              )}
              <th className="text-right" onClick={() => handleSort("contribution")}>
                <span className="col-help-wrap">
                  <HelpLabel
                    label={isRiskContribution
                      ? "% Risk Contrib (w×x×CovAdj)"
                      : isSensitivity
                        ? "Contribution (w×x×σ)"
                        : "Contribution (w×x)"}
                    plain={hints.contribution.plain}
                    math={hints.contribution.math}
                  />
                  {arrow("contribution")}
                </span>
              </th>
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((item) => (
              <tr key={item.ticker}>
                <td><strong>{item.ticker}</strong></td>
                <td className="text-right">{(item.weight * 100).toFixed(2)}%</td>
                <td className="text-right">
                  <span className={item.exposure >= 0 ? "positive" : "negative"}>
                    {item.exposure.toFixed(4)}
                  </span>
                </td>
                {(isSensitivity || isRiskContribution) && (
                  <td className="text-right">
                    <span className={(item.sensitivity ?? 0) >= 0 ? "positive" : "negative"}>
                      {(item.sensitivity ?? 0).toFixed(4)}
                    </span>
                  </td>
                )}
                <td className="text-right">
                  <span className={item.contribution >= 0 ? "positive" : "negative"}>
                    {isRiskContribution ? `${item.contribution.toFixed(4)}%` : item.contribution.toFixed(6)}
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
          label="positions"
        />
      </div>
    </div>
  );
}
