"use client";

import { useState } from "react";
import { useExposures } from "@/hooks/useApi";
import ExposureBarChart from "@/components/ExposureBarChart";
import FactorDrilldown from "@/components/FactorDrilldown";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";

const MODES = [
  { key: "raw", label: "Raw Loading" },
  { key: "sensitivity", label: "Factor Sensitivity" },
  { key: "risk_contribution", label: "Risk Contribution" },
] as const;

export default function ExposuresPage() {
  const [mode, setMode] = useState<string>("raw");
  const [selectedFactor, setSelectedFactor] = useState<string | null>(null);
  const { data, isLoading } = useExposures(mode);

  if (isLoading) {
    return <AnalyticsLoadingViz message="Loading exposures..." />;
  }

  const factors = data?.factors ?? [];
  const selected = selectedFactor
    ? factors.find((f) => f.factor === selectedFactor)
    : null;

  return (
    <div>
      <div className="mode-toggle">
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

      <div className="chart-card">
        <h3>Factor Exposures — {MODES.find((m) => m.key === mode)?.label}</h3>
        <ExposureBarChart
          factors={factors}
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
    </div>
  );
}
