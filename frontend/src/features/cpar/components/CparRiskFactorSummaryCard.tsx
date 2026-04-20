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
      <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 10 }}>
        <h3 style={{ margin: 0 }}>Risk — {modeLabel}</h3>
        <span
          style={{
            fontSize: 10,
            letterSpacing: "0.04em",
            color: "var(--text-muted)",
            fontVariantNumeric: "tabular-nums",
          }}
        >
          {factorRows.length} factors · Pre Var {formatCparNumber(preHedgeVarianceProxy, 3)}
        </span>
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

      <div className="floating-mode-toggle">
        {MODES.map((option) => (
          <button
            key={option.key}
            type="button"
            className={mode === option.key ? "active" : ""}
            onClick={() => {
              setMode(option.key);
              setSelectedFactorId(null);
            }}
          >
            {option.label}
          </button>
        ))}
      </div>
    </section>
  );
}
