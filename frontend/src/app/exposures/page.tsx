"use client";

import { useMemo, useState } from "react";
import { useExposures, usePortfolio } from "@/hooks/useApi";
import ExposureBarChart from "@/components/ExposureBarChart";
import FactorDrilldown from "@/components/FactorDrilldown";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ExposurePositionsTable from "@/components/ExposurePositionsTable";

const MODES = [
  { key: "raw", label: "Exposure" },
  { key: "sensitivity", label: "Sensitivity" },
  { key: "risk_contribution", label: "Risk Contrib" },
] as const;

export default function ExposuresPage() {
  const [mode, setMode] = useState<string>("raw");
  const [selectedFactor, setSelectedFactor] = useState<string | null>(null);
  const { data, isLoading } = useExposures(mode);
  const { data: portfolioData, isLoading: portfolioLoading } = usePortfolio();
  const factors = data?.factors ?? [];
  const positions = portfolioData?.positions ?? [];

  // Extract cross-section summary from the factor data
  const crossSection = useMemo(() => {
    const ns = factors
      .map((f) => Number(f.cross_section_n || 0))
      .filter((n) => n > 0);
    if (ns.length === 0) return null;
    const min = Math.min(...ns);
    const max = Math.max(...ns);
    const date = factors.find((f) => f.coverage_date)?.coverage_date ?? null;
    return { min, max, date };
  }, [factors]);

  if (isLoading) {
    return <AnalyticsLoadingViz message="Loading exposures..." />;
  }

  const selected = selectedFactor
    ? factors.find((f) => f.factor === selectedFactor)
    : null;

  return (
    <div>
      <div className="chart-card">
        <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 10 }}>
          <h3 style={{ margin: 0 }}>
            Factor Exposures — {MODES.find((m) => m.key === mode)?.label}
          </h3>
          {crossSection && (
            <span style={{
              fontSize: 10,
              letterSpacing: "0.04em",
              color: "rgba(169, 182, 210, 0.5)",
              fontVariantNumeric: "tabular-nums",
            }}>
              {crossSection.min === crossSection.max
                ? `N = ${crossSection.min.toLocaleString()}`
                : `N = ${crossSection.min.toLocaleString()}–${crossSection.max.toLocaleString()}`}
              {crossSection.date && ` · ${crossSection.date}`}
            </span>
          )}
        </div>
        <ExposureBarChart
          factors={factors}
          mode={mode as "raw" | "sensitivity" | "risk_contribution"}
          onBarClick={(f) => setSelectedFactor(f === selectedFactor ? null : f)}
        />
      </div>

      {selected && (
        <FactorDrilldown
          factor={selected.factor}
          items={selected.drilldown}
          mode={mode}
          factorVol={selected.factor_vol}
          onClose={() => setSelectedFactor(null)}
        />
      )}

      <div className="chart-card" style={{ marginTop: 12 }}>
        <h3>Positions (Barra Risk Mix)</h3>
        {portfolioLoading ? (
          <div className="detail-history-empty loading-pulse">Loading positions...</div>
        ) : (
          <ExposurePositionsTable positions={positions} />
        )}
      </div>

      <div className="floating-mode-toggle">
        {MODES.map((m) => (
          <button
            key={m.key}
            className={mode === m.key ? "active" : ""}
            onClick={() => {
              setMode(m.key);
              setSelectedFactor(null);
            }}
          >
            {m.label}
          </button>
        ))}
      </div>
    </div>
  );
}
