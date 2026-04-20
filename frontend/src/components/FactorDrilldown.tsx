"use client";

import { useEffect, useState } from "react";
import type { FactorDrilldownItem } from "@/lib/types/cuse4";
import TableRowToggle from "@/components/TableRowToggle";
import FactorHistoryChart from "@/components/FactorHistoryChart";
import HelpLabel from "@/components/HelpLabel";
import MethodLabel from "@/components/MethodLabel";
import { useFactorHistory } from "@/hooks/useCuse4Api";
import { shortFactorLabel } from "@/lib/factorLabels";
import type { FactorCatalogEntry } from "@/lib/types/cuse4";
import { exposureMethodDisplayLabel, exposureMethodRank, exposureMethodTone } from "@/lib/exposureOrigin";

interface FactorDrilldownProps {
  factorId: string;
  factorName: string;
  items: FactorDrilldownItem[];
  mode?: string;
  factorVol?: number;
  factorCatalog?: FactorCatalogEntry[];
  onClose: () => void;
}
const COLLAPSED_ROWS = 10;
type SortKey = "ticker" | "method" | "weight" | "exposure" | "sensitivity" | "contribution";
type ContributionSortMode = "signed" | "abs";

export default function FactorDrilldown({
  factorId,
  factorName,
  items,
  mode,
  factorVol,
  factorCatalog,
  onClose,
}: FactorDrilldownProps) {
  const isSensitivity = mode === "sensitivity";
  const isRiskContribution = mode === "risk_contribution";
  const hints = {
    weight: {
      plain: "Signed position market value divided by gross portfolio market value.",
      math: "wᵢ = MVᵢ / Σ|MV|",
    },
    loading: {
      plain: "Position loading on the selected factor.",
      math: "xᵢ,ᶠ",
    },
    sensitivity: isRiskContribution
      ? {
          plain: "Loading multiplied by the factor's portfolio covariance adjustment.",
          math: "xᵢ,ᶠ × (Fh)ᶠ",
        }
      : {
          plain: "Loading × 1σ factor volatility.",
          math: "xᵢ,ᶠ × σᶠ",
        },
    contribution: isRiskContribution
      ? {
          plain: "Normalized share of the factor's portfolio risk contribution, not the raw variance term itself.",
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
  const [sortKey, setSortKey] = useState<SortKey>("contribution");
  const [sortAsc, setSortAsc] = useState(false);
  const [contributionSortMode, setContributionSortMode] = useState<ContributionSortMode>("signed");
  const [showAllRows, setShowAllRows] = useState(false);
  const { data: historyData, error: historyError, isLoading: historyLoading } = useFactorHistory(factorId, 5);
  const displayFactor = shortFactorLabel(factorId, factorCatalog);
  const contributionValue = (item: FactorDrilldownItem) => item.contribution;

  useEffect(() => {
    setSortKey("contribution");
    setSortAsc(false);
    setContributionSortMode("signed");
  }, [factorId, mode]);

  const sorted = [...items].sort((a, b) => {
    if (sortKey === "ticker") {
      return sortAsc
        ? a.ticker.localeCompare(b.ticker)
        : b.ticker.localeCompare(a.ticker);
    }
    if (sortKey === "method") {
      const rankA = exposureMethodRank(a.exposure_origin, a.model_status);
      const rankB = exposureMethodRank(b.exposure_origin, b.model_status);
      if (rankA !== rankB) return sortAsc ? rankA - rankB : rankB - rankA;
      const labelA = exposureMethodDisplayLabel(a.exposure_origin, a.model_status);
      const labelB = exposureMethodDisplayLabel(b.exposure_origin, b.model_status);
      return sortAsc ? labelA.localeCompare(labelB) : labelB.localeCompare(labelA);
    }
    if (sortKey === "contribution") {
      const leftValue = contributionValue(a);
      const rightValue = contributionValue(b);
      const leftComparable = contributionSortMode === "abs" ? Math.abs(leftValue) : leftValue;
      const rightComparable = contributionSortMode === "abs" ? Math.abs(rightValue) : rightValue;
      return sortAsc ? leftComparable - rightComparable : rightComparable - leftComparable;
    }
    const av = sortKey === "sensitivity" ? (a.sensitivity ?? 0) : a[sortKey];
    const bv = sortKey === "sensitivity" ? (b.sensitivity ?? 0) : b[sortKey];
    return sortAsc ? av - bv : bv - av;
  });
  const uniqueExposureCount = new Set(items.map((item) => item.exposure.toFixed(6))).size;
  const handleSort = (key: SortKey) => {
    if (key === "contribution") {
      if (sortKey !== "contribution") {
        setSortKey("contribution");
        setContributionSortMode("signed");
        setSortAsc(false);
        return;
      }
      if (contributionSortMode === "signed" && !sortAsc) {
        setSortAsc(true);
        return;
      }
      if (contributionSortMode === "signed" && sortAsc) {
        setContributionSortMode("abs");
        setSortAsc(false);
        return;
      }
      if (contributionSortMode === "abs" && !sortAsc) {
        setSortAsc(true);
        return;
      }
      setContributionSortMode("signed");
      setSortAsc(false);
      return;
    }
    if (key === sortKey) setSortAsc((prev) => !prev);
    else {
      setSortKey(key);
      setSortAsc(false);
    }
  };
  const arrow = (key: SortKey) => {
    if (sortKey !== key) return "";
    if (key === "contribution") {
      const modeLabel = contributionSortMode === "abs" ? "abs" : "signed";
      return sortAsc ? ` (${modeLabel}) ↑` : ` (${modeLabel}) ↓`;
    }
    return sortAsc ? " ↑" : " ↓";
  };
  const visibleRows = showAllRows ? sorted : sorted.slice(0, COLLAPSED_ROWS);
  return (
    <div className="detail-panel">
      <div className="detail-panel-header">
        <h4>
          {displayFactor} — {isSensitivity
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
          <h5>5Y Historical Return — {displayFactor}</h5>
          {!historyLoading && historyData?.points && historyData.points.length > 0 && (() => {
            const vals = historyData.points.map((p) => p.cum_return * 100);
            const latest = vals[vals.length - 1] ?? 0;
            const pos = latest >= 0;
            const s = latest >= 0 ? "+" : "";
            return (
              <div className="detail-history-stats">
                <span className={`detail-history-stat ${pos ? "positive" : "negative"}`}>
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
          : historyError
            ? <div className="detail-history-empty">5Y factor-return history is temporarily unavailable for {displayFactor}.</div>
            : <FactorHistoryChart factor={factorName} points={historyData?.points ?? []} factorVol={factorVol} />}
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
              <th onClick={() => handleSort("method")}>Method{arrow("method")}</th>
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
                      ? "% Risk Contrib (normalized)"
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
                <td>
                  <MethodLabel
                    label={exposureMethodDisplayLabel(item.exposure_origin, item.model_status)}
                    tone={exposureMethodTone(item.exposure_origin, item.model_status)}
                  />
                </td>
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
