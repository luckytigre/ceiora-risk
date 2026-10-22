"use client";

import { useEffect, useMemo, useState } from "react";
import { formatCparNumber } from "@/lib/cparTruth";
import type {
  CparFactorVarianceContribution,
  CparPortfolioHedgeRecommendationData,
  CparRiskData,
  CparRiskExposureMode,
} from "@/lib/types/cpar";
import CparRiskFactorDrilldown from "./CparRiskFactorDrilldown";
import CparRiskFactorLoadingsChart from "./CparRiskFactorLoadingsChart";

const MODES: Array<{ key: CparRiskExposureMode; label: string }> = [
  { key: "raw", label: "Exposure" },
  { key: "sensitivity", label: "Sensitivity" },
  { key: "risk_contribution", label: "Risk Contrib" },
];

export default function CparRiskFactorSummaryCard({
  portfolio,
  space = "display",
}: {
  portfolio: CparRiskData | CparPortfolioHedgeRecommendationData;
  space?: "display" | "trade";
}) {
  const factorChartRows = space === "trade" ? portfolio.factor_chart : portfolio.display_factor_chart;
  const varianceRows = space === "trade"
    ? portfolio.factor_variance_contributions
    : portfolio.display_factor_variance_contributions;
  const varianceProxy = space === "trade"
    ? portfolio.factor_variance_proxy
    : portfolio.pre_hedge_factor_variance_proxy;

  const factorRows = useMemo(() => (
    [...factorChartRows].sort((left, right) => (
      left.display_order - right.display_order
      || Math.abs(right.aggregate_beta) - Math.abs(left.aggregate_beta)
      || left.factor_id.localeCompare(right.factor_id)
    ))
  ), [factorChartRows]);
  const [mode, setMode] = useState<CparRiskExposureMode>("raw");
  const modeLabel = MODES.find((option) => option.key === mode)?.label ?? "Exposure";
  const [selectedFactorId, setSelectedFactorId] = useState<string | null>(null);
  const preHedgeVarianceProxy = useMemo(() => (
    typeof varianceProxy === "number"
      ? varianceProxy
      : varianceRows.reduce(
          (sum: number, row: CparFactorVarianceContribution) => sum + (row.variance_contribution || 0),
          0,
        )
  ), [varianceProxy, varianceRows]);

  useEffect(() => {
    setSelectedFactorId((current) => (
      current && factorRows.some((row) => row.factor_id === current)
        ? current
        : null
    ));
  }, [factorRows]);

  const selectedFactor = factorRows.find((row) => row.factor_id === selectedFactorId) || null;

  return (
    <section className="chart-card" data-testid="cpar-risk-factor-summary">
      <div className="thesis-section-heading risk-module-heading">
        <div className="risk-module-title-line">
          <h3>Risk — {modeLabel}</h3>
          <span className="risk-module-meta">{factorRows.length} factors · Pre Var {formatCparNumber(preHedgeVarianceProxy, 3)}</span>
        </div>
        <div className="settings-segmented-control" role="tablist" aria-label="cPAR exposure mode">
          {MODES.map((option) => {
            const active = mode === option.key;
            return (
              <button
                key={option.key}
                type="button"
                role="tab"
                aria-selected={active}
                className={`settings-segmented-option${active ? " active" : ""}`}
                onClick={() => {
                  setMode(option.key);
                  setSelectedFactorId(null);
                }}
              >
                {option.label}
              </button>
            );
          })}
        </div>
      </div>

      {factorRows.length === 0 ? (
        <div className="detail-history-empty compact">
          No covered holdings rows contributed to the aggregate display-loading vector.
        </div>
      ) : (
        <>
          <CparRiskFactorLoadingsChart
            rows={factorRows}
            mode={mode}
            selectedFactorId={selectedFactor?.factor_id || null}
            onSelectFactor={(factorId) => {
              setSelectedFactorId((current) => (current === factorId ? null : factorId));
            }}
          />

          {selectedFactor ? <CparRiskFactorDrilldown factor={selectedFactor} mode={mode} /> : null}
        </>
      )}
    </section>
  );
}
