"use client";

import { useEffect, useMemo, useState } from "react";
import { useAppSettings } from "@/components/AppSettingsContext";
import HelpLabel from "@/components/HelpLabel";
import TableRowToggle from "@/components/TableRowToggle";
import CparFactorHistoryChart from "@/features/cpar/components/CparFactorHistoryChart";
import { useCparFactorHistory } from "@/hooks/useCparApi";
import {
  describeCparFitStatus,
  formatCparMarketValueThousands,
  formatCparNumber,
  formatCparPercent,
  readCparError,
} from "@/lib/cparTruth";
import type { CparFactorChartRow, CparRiskExposureMode } from "@/lib/types/cpar";

const COLLAPSED_ROWS = 8;

type SortKey = "ticker" | "coverage" | "market_value" | "weight" | "beta" | "sensitivity" | "contribution";
type ContributionSortMode = "signed" | "abs";

export default function CparRiskFactorDrilldown({
  factor,
  mode,
}: {
  factor: CparFactorChartRow;
  mode: CparRiskExposureMode;
}) {
  const [expanded, setExpanded] = useState(false);
  const [sortKey, setSortKey] = useState<SortKey>("contribution");
  const [sortAsc, setSortAsc] = useState(false);
  const [contributionSortMode, setContributionSortMode] = useState<ContributionSortMode>("signed");
  const { cparFactorHistoryMode } = useAppSettings();
  const {
    data: historyData,
    error: historyError,
    isLoading: historyLoading,
  } = useCparFactorHistory(factor.factor_id, 5, cparFactorHistoryMode);
  const historyState = historyError ? readCparError(historyError) : null;

  const isSensitivity = mode === "sensitivity";
  const isRiskContribution = mode === "risk_contribution";
  const isMarketFactor = factor.factor_id === "SPY" || factor.group === "market";
  const usesMarketAdjustedHistory = !isMarketFactor && cparFactorHistoryMode === "market_adjusted";
  const historyLabel = isMarketFactor
    ? "5Y Daily Return"
    : usesMarketAdjustedHistory
      ? "5Y Daily Market-Adjusted Return"
      : "5Y Daily Residual Return";
  const contributionValue = (row: CparFactorChartRow["drilldown"][number]) => (
    isRiskContribution ? row.risk_contribution_pct : isSensitivity ? row.vol_scaled_contribution : row.contribution_beta
  );
  const hints = {
    weight: {
      plain: "Signed position market value divided by gross portfolio market value.",
      math: "w_i = MV_i / Σ|MV|",
    },
    loading: {
      plain: "Position loading on the selected factor from the active cPAR package.",
      math: "x_i,f",
    },
    sensitivity: isRiskContribution
      ? {
          plain: "Loading multiplied by the factor's covariance adjustment from the portfolio factor vector.",
          math: "x_i,f × (Fh)_f",
        }
      : {
          plain: "Loading scaled by one standard deviation of factor volatility.",
          math: "x_i,f × σ_f",
        },
    contribution: isRiskContribution
      ? {
          plain: "Normalized share of this factor's total portfolio risk contribution.",
          math: "((w_i × x_i,f × (Fh)_f) / h'Fh) × 100",
        }
      : isSensitivity
        ? {
            plain: "Weight × loading × one standard deviation of factor volatility.",
            math: "w_i × x_i,f × σ_f",
          }
        : {
            plain: "Weight × loading.",
            math: "w_i × x_i,f",
          },
  };

  useEffect(() => {
    setSortKey("contribution");
    setSortAsc(false);
    setContributionSortMode("signed");
  }, [factor.factor_id, mode]);

  const sortedRows = useMemo(() => {
    const rows = [...factor.drilldown];
    rows.sort((left, right) => {
      if (sortKey === "ticker") {
        const leftTicker = String(left.ticker || left.ric || "");
        const rightTicker = String(right.ticker || right.ric || "");
        return sortAsc ? leftTicker.localeCompare(rightTicker) : rightTicker.localeCompare(leftTicker);
      }
      if (sortKey === "coverage") {
        const leftFit = left.fit_status ? describeCparFitStatus(left.fit_status).label : "";
        const rightFit = right.fit_status ? describeCparFitStatus(right.fit_status).label : "";
        const leftCoverage = `Covered ${leftFit}`.trim();
        const rightCoverage = `Covered ${rightFit}`.trim();
        return sortAsc ? leftCoverage.localeCompare(rightCoverage) : rightCoverage.localeCompare(leftCoverage);
      }
      if (sortKey === "contribution") {
        const leftValue = contributionValue(left);
        const rightValue = contributionValue(right);
        const leftComparable = contributionSortMode === "abs" ? Math.abs(leftValue) : leftValue;
        const rightComparable = contributionSortMode === "abs" ? Math.abs(rightValue) : rightValue;
        return sortAsc ? leftComparable - rightComparable : rightComparable - leftComparable;
      }
      const leftValue = sortKey === "sensitivity"
        ? (isRiskContribution ? left.covariance_adjusted_loading : left.vol_scaled_loading)
        : sortKey === "beta"
            ? (left.factor_beta || 0)
            : sortKey === "weight"
              ? (left.portfolio_weight || 0)
              : (left.market_value || 0);
      const rightValue = sortKey === "sensitivity"
        ? (isRiskContribution ? right.covariance_adjusted_loading : right.vol_scaled_loading)
        : sortKey === "beta"
            ? (right.factor_beta || 0)
            : sortKey === "weight"
              ? (right.portfolio_weight || 0)
              : (right.market_value || 0);
      return sortAsc ? leftValue - rightValue : rightValue - leftValue;
    });
    return rows;
  }, [contributionSortMode, factor.drilldown, isRiskContribution, isSensitivity, sortAsc, sortKey]);
  const visibleRows = expanded ? sortedRows : sortedRows.slice(0, COLLAPSED_ROWS);

  const handleSort = (nextKey: SortKey) => {
    if (nextKey === "contribution") {
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
    if (sortKey === nextKey) {
      setSortAsc((current) => !current);
      return;
    }
    setSortKey(nextKey);
    setSortAsc(false);
  };
  const arrow = (key: SortKey) => {
    if (sortKey !== key) return "";
    if (key === "contribution") {
      const modeLabel = contributionSortMode === "abs" ? "abs" : "signed";
      return sortAsc ? ` (${modeLabel}) ↑` : ` (${modeLabel}) ↓`;
    }
    return sortAsc ? " ↑" : " ↓";
  };
  const title = isRiskContribution
    ? "Risk Contribution Breakdown"
    : isSensitivity
      ? "Sensitivity Breakdown"
      : "Position Breakdown";

  return (
    <div className="detail-panel" data-testid="cpar-risk-factor-drilldown">
      <div className="detail-panel-header">
        <h4>{factor.label} — {title}</h4>
      </div>
      <div className="detail-history">
        <div className="detail-history-header">
          <h5>{historyLabel} — {factor.label}</h5>
          {!historyLoading && historyData?.points && historyData.points.length > 0 ? (
            <div className="detail-history-stats">
              <span
                className={`detail-history-stat ${
                  (historyData.points[historyData.points.length - 1]?.cum_return ?? 0) >= 0
                    ? "positive"
                    : "negative"
                }`}
              >
                {`${(historyData.points[historyData.points.length - 1]?.cum_return ?? 0) >= 0 ? "+" : ""}${(
                  (historyData.points[historyData.points.length - 1]?.cum_return ?? 0) * 100
                ).toFixed(1)}%`}
              </span>
              <span className="detail-history-stat muted">
                σ {(factor.factor_volatility * 100).toFixed(1)}%
              </span>
            </div>
          ) : null}
        </div>
        {historyLoading ? (
          <div className="detail-history-empty loading-pulse">
            {isMarketFactor
              ? "Loading 5Y daily factor history..."
              : usesMarketAdjustedHistory
                ? "Loading 5Y daily market-adjusted history..."
                : "Loading 5Y daily residual history..."}
          </div>
        ) : historyState ? (
          <div className="detail-history-empty">
            {historyState.kind === "not_ready"
              ? `${isMarketFactor ? "Daily" : usesMarketAdjustedHistory ? "Daily market-adjusted" : "Daily residual"} cPAR factor returns are not ready for ${factor.label} yet.`
              : `5Y daily ${isMarketFactor ? "factor" : usesMarketAdjustedHistory ? "market-adjusted factor" : "residual factor"}-return history is temporarily unavailable for ${factor.label}.`}
          </div>
        ) : (
          <CparFactorHistoryChart factor={factor.label} points={historyData?.points ?? []} factorVol={factor.factor_volatility} />
        )}
      </div>
      <p className="detail-panel-meta">
        {factor.drilldown.length} positions, {formatCparNumber(factor.aggregate_beta, 3)} aggregate beta,{" "}
        {formatCparPercent(factor.variance_share, 1)} pre var
      </p>
      <div className="dash-table">
        <table>
          <thead>
            <tr>
              <th onClick={() => handleSort("ticker")}>Ticker{arrow("ticker")}</th>
              <th onClick={() => handleSort("coverage")}>Coverage{arrow("coverage")}</th>
              <th className="text-right" onClick={() => handleSort("market_value")}>Mkt Value{arrow("market_value")}</th>
              <th className="text-right" onClick={() => handleSort("weight")}>
                <span className="col-help-wrap">
                  <HelpLabel label="Weight" plain={hints.weight.plain} math={hints.weight.math} />
                  {arrow("weight")}
                </span>
              </th>
              <th className="text-right" onClick={() => handleSort("beta")}>
                <span className="col-help-wrap">
                  <HelpLabel
                    label={isRiskContribution ? "Raw Loading" : "Loading"}
                    plain={hints.loading.plain}
                    math={hints.loading.math}
                  />
                  {arrow("beta")}
                </span>
              </th>
              {(isSensitivity || isRiskContribution) ? (
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
              ) : null}
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
            {visibleRows.length === 0 ? (
              <tr>
                <td colSpan={isSensitivity || isRiskContribution ? 7 : 6} className="cpar-empty-row">
                  No covered positions contributed to this factor.
                </td>
              </tr>
            ) : (
              visibleRows.map((row) => {
                const fit = row.fit_status ? describeCparFitStatus(row.fit_status) : null;
                const sensitivityValue = isRiskContribution ? row.covariance_adjusted_loading : row.vol_scaled_loading;
                const contributionValue = isRiskContribution
                  ? row.risk_contribution_pct
                  : isSensitivity
                    ? row.vol_scaled_contribution
                    : row.contribution_beta;
                return (
                  <tr key={`${factor.factor_id}:${row.ric}`}>
                    <td><strong>{row.ticker || row.ric}</strong></td>
                    <td>
                      <div className="cpar-badge-row compact">
                        <span className="cpar-badge success">Covered</span>
                        {fit ? <span className={`cpar-badge ${fit.tone}`}>{fit.label}</span> : null}
                      </div>
                    </td>
                    <td className="text-right cpar-number-cell">{formatCparMarketValueThousands(row.market_value)}</td>
                    <td className="text-right">{`${((row.portfolio_weight || 0) * 100).toFixed(2)}%`}</td>
                    <td className="text-right">
                      <span className={(row.factor_beta || 0) >= 0 ? "positive" : "negative"}>
                        {formatCparNumber(row.factor_beta, 4)}
                      </span>
                    </td>
                    {(isSensitivity || isRiskContribution) ? (
                      <td className="text-right">
                        <span className={sensitivityValue >= 0 ? "positive" : "negative"}>
                          {formatCparNumber(sensitivityValue, 4)}
                        </span>
                      </td>
                    ) : null}
                    <td className="text-right">
                      <span className={contributionValue >= 0 ? "positive" : "negative"}>
                        {isRiskContribution
                          ? `${formatCparNumber(contributionValue, 4)}%`
                          : formatCparNumber(contributionValue, 6)}
                      </span>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
        <TableRowToggle
          totalRows={sortedRows.length}
          collapsedRows={COLLAPSED_ROWS}
          expanded={expanded}
          onToggle={() => setExpanded((current) => !current)}
          label="positions"
        />
      </div>
    </div>
  );
}
