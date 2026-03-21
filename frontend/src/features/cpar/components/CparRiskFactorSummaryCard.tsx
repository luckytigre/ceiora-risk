"use client";

import { useEffect, useMemo, useState } from "react";
import { formatCparNumber } from "@/lib/cparTruth";
import type {
  CparFactorVarianceContribution,
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
}: {
  portfolio: CparRiskData;
}) {
  const factorRows = useMemo(() => (
    [...portfolio.factor_chart].sort((left, right) => (
      left.display_order - right.display_order
      || Math.abs(right.aggregate_beta) - Math.abs(left.aggregate_beta)
      || left.factor_id.localeCompare(right.factor_id)
    ))
  ), [portfolio.factor_chart]);
  const [mode, setMode] = useState<CparRiskExposureMode>("raw");
  const modeLabel = MODES.find((option) => option.key === mode)?.label ?? "Exposure";
  const [selectedFactorId, setSelectedFactorId] = useState<string | null>(factorRows[0]?.factor_id || null);
  const preHedgeVarianceProxy = useMemo(() => (
    typeof portfolio.pre_hedge_factor_variance_proxy === "number"
      ? portfolio.pre_hedge_factor_variance_proxy
      : portfolio.factor_variance_contributions.reduce(
          (sum: number, row: CparFactorVarianceContribution) => sum + (row.variance_contribution || 0),
          0,
        )
  ), [portfolio.factor_variance_contributions, portfolio.pre_hedge_factor_variance_proxy]);

  useEffect(() => {
    setSelectedFactorId((current) => (
      current && factorRows.some((row) => row.factor_id === current)
        ? current
        : factorRows[0]?.factor_id || null
    ));
  }, [factorRows]);

  const selectedFactor = factorRows.find((row) => row.factor_id === selectedFactorId) || factorRows[0] || null;

  return (
    <section className="chart-card" data-testid="cpar-risk-factor-summary">
      <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 10 }}>
        <h3 style={{ margin: 0 }}>Risk — {modeLabel}</h3>
        <span
          style={{
            fontSize: 10,
            letterSpacing: "0.04em",
            color: "rgba(169, 182, 210, 0.5)",
            fontVariantNumeric: "tabular-nums",
          }}
        >
          {factorRows.length} factors · Pre Var {formatCparNumber(preHedgeVarianceProxy, 3)}
        </span>
      </div>

      {factorRows.length === 0 ? (
        <div className="detail-history-empty compact">
          No covered holdings rows contributed to the aggregate thresholded portfolio vector.
        </div>
      ) : (
        <>
          <CparRiskFactorLoadingsChart
            rows={factorRows}
            mode={mode}
            selectedFactorId={selectedFactor?.factor_id || null}
            onSelectFactor={setSelectedFactorId}
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
