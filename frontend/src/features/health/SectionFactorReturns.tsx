"use client";

import { useState } from "react";
import LazyMountOnVisible from "@/components/LazyMountOnVisible";
import CovarianceHeatmap from "@/features/cuse4/components/CovarianceHeatmap";
import HelpLabel from "@/components/HelpLabel";
import { chartLongColor, chartShortColor, chartTextColor } from "@/lib/charts/chartTheme";
import { shortFactorLabel } from "@/lib/factorLabels";
import type { HealthDiagnosticsData } from "@/lib/types/cuse4";
import { Bar, Line } from "./charts";
import {
  buildHistogramData,
  commonLineOptions,
  histogramOptions,
  seriesData,
} from "./utils";

export default function SectionFactorReturns({ data }: { data: HealthDiagnosticsData }) {
  const [selectedReturnFactor, setSelectedReturnFactor] = useState<string>("");
  const returnFactors = data.section3.factors ?? [];
  const returnFactor = selectedReturnFactor || returnFactors[0] || "";
  const cumulativeRows = data.section3.cumulative_returns[returnFactor] ?? [];
  const rollingVolRows = data.section3.rolling_vol_60d[returnFactor] ?? [];
  const returnDist = data.section3.return_dist[returnFactor] ?? { centers: [], counts: [] };

  return (
    <div className="chart-card">
      <h3>
        <HelpLabel
          label="Section 3 — Factor Return Health"
          plain="Checks if factor returns look reasonable: trend, volatility, co-movement, and outliers."
          math="Uses cumulative return, rolling vol, return correlation, and return distribution"
        />
      </h3>
      <div className="health-picker-row" style={{ marginBottom: 8 }}>
        <span className="health-picker-label">Selected Factor</span>
        <select
          className="health-select"
          value={returnFactor}
          onChange={(e) => setSelectedReturnFactor(e.target.value)}
        >
          {returnFactors.map((f) => (
            <option key={f} value={f}>{shortFactorLabel(f, data.factor_catalog)}</option>
          ))}
        </select>
      </div>
      <div className="health-grid-2-half">
        <div>
          <h4>Cumulative Return</h4>
          <div className="health-chart-sm">
            <Line
              data={seriesData(cumulativeRows, "Cumulative Return", chartLongColor(0.88), 100)}
              options={{
                ...commonLineOptions,
                scales: {
                  ...commonLineOptions.scales,
                  y: {
                    ...(commonLineOptions.scales?.y || {}),
                    ticks: {
                      color: chartTextColor("secondary", 0.5),
                      callback: (v) => `${Number(v).toFixed(0)}%`,
                      font: { size: 9 },
                    },
                  },
                },
              }}
            />
          </div>
        </div>
        <div>
          <h4>Rolling 60d Volatility</h4>
          <div className="health-chart-sm">
            <Line
              data={seriesData(rollingVolRows, "Rolling Volatility", chartShortColor(0.88), 100)}
              options={{
                ...commonLineOptions,
                scales: {
                  ...commonLineOptions.scales,
                  y: {
                    ...(commonLineOptions.scales?.y || {}),
                    ticks: {
                      color: chartTextColor("secondary", 0.5),
                      callback: (v) => `${Number(v).toFixed(1)}%`,
                      font: { size: 9 },
                    },
                  },
                },
              }}
            />
          </div>
        </div>
      </div>
      <div className="health-grid-2">
        <div>
          <h4>Daily Return Distribution</h4>
          <div className="health-chart-sm">
            <Bar data={buildHistogramData(returnDist)} options={histogramOptions("Daily Return", "Days")} />
          </div>
        </div>
        <div>
          <h4>Factor Return Correlation Heatmap</h4>
          <LazyMountOnVisible
            minHeight={320}
            fallback={<div className="detail-history-empty">Scroll to load the factor return heatmap.</div>}
          >
            <CovarianceHeatmap data={data.section3.return_corr} factorCatalog={data.factor_catalog} />
          </LazyMountOnVisible>
        </div>
      </div>
    </div>
  );
}
