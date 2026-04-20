"use client";

import { useMemo } from "react";
import HelpLabel from "@/components/HelpLabel";
import { compareNumber, compareText, useSortableRows } from "@/hooks/useSortableRows";
import { chartTextColor } from "@/lib/charts/chartTheme";
import type { HealthDiagnosticsData } from "@/lib/types/cuse4";
import { Bar, Line } from "./charts";
import { commonLineOptions, fmtPct, seriesData } from "./utils";

type SortKey = "name" | "forecast" | "realized" | "gap";

export default function SectionCovarianceQuality({ data }: { data: HealthDiagnosticsData }) {
  const eigenvalues = data.section4.eigenvalues ?? [];
  const eigenData = {
    labels: eigenvalues.map((_v, i) => `λ${i + 1}`),
    datasets: [
      {
        label: "Eigenvalue",
        data: eigenvalues,
        backgroundColor: chartTextColor("secondary", 0.55),
        borderWidth: 0,
      },
    ],
  };
  const forecastRows = useMemo(
    () => (data.section4.forecast_vs_realized || []).map((row) => ({
      ...row,
      gap: (Number(row.realized_vol_60d) || 0) - (Number(row.forecast_vol) || 0),
    })),
    [data.section4.forecast_vs_realized],
  );
  const comparators = useMemo<Record<SortKey, (left: (typeof forecastRows)[number], right: (typeof forecastRows)[number]) => number>>(
    () => ({
      name: (left, right) => compareText(left.name, right.name),
      forecast: (left, right) => compareNumber(left.forecast_vol, right.forecast_vol),
      realized: (left, right) => compareNumber(left.realized_vol_60d, right.realized_vol_60d),
      gap: (left, right) => compareNumber(left.gap, right.gap),
    }),
    [],
  );
  const { sortedRows, handleSort, arrow } = useSortableRows<(typeof forecastRows)[number], SortKey>({
    rows: forecastRows,
    comparators,
  });

  return (
    <div className="chart-card">
      <h3>
        <HelpLabel
          label="Section 4 — Covariance Quality"
          plain="Checks whether covariance forecasts are stable and close to what actually happened."
          math="Uses eigenvalues and forecast-vs-realized volatility"
        />
      </h3>
      <div className="health-grid-2-half">
        <div>
          <h4>Eigenvalue Spectrum</h4>
          <div className="health-chart-sm">
            <Bar data={eigenData} options={{ responsive: true, maintainAspectRatio: false }} />
          </div>
        </div>
        <div>
          <h4>Rolling Average Factor Vol</h4>
          <div className="health-chart-sm">
            <Line
              data={seriesData(data.section4.rolling_avg_factor_vol, "Avg Factor Vol", chartTextColor("secondary", 0.85), 100)}
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
      <div className="dash-table health-table">
        <table>
          <thead>
            <tr>
              <th onClick={() => handleSort("name")}>Portfolio Sample{arrow("name")}</th>
              <th className="text-right" onClick={() => handleSort("forecast")}>Forecast Vol{arrow("forecast")}</th>
              <th className="text-right" onClick={() => handleSort("realized")}>Realized Vol (60d){arrow("realized")}</th>
              <th className="text-right" onClick={() => handleSort("gap")}>Gap{arrow("gap")}</th>
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row) => {
              const gap = row.gap;
              return (
                <tr key={row.name}>
                  <td>{row.name}</td>
                  <td className="text-right">{fmtPct(row.forecast_vol, 2)}</td>
                  <td className="text-right">{fmtPct(row.realized_vol_60d, 2)}</td>
                  <td className={`text-right ${gap >= 0 ? "positive" : "negative"}`}>{fmtPct(gap, 2)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
