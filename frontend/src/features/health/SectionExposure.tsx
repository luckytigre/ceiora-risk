"use client";

import { useMemo, useState } from "react";
import LazyMountOnVisible from "@/components/LazyMountOnVisible";
import CovarianceHeatmap from "@/features/cuse4/components/CovarianceHeatmap";
import HelpLabel from "@/components/HelpLabel";
import TableRowToggle from "@/components/TableRowToggle";
import { chartLongColor, chartTextColor } from "@/lib/charts/chartTheme";
import { factorFamily, shortFactorLabel } from "@/lib/factorLabels";
import type { HealthDiagnosticsData, HealthExposureStats } from "@/lib/types/cuse4";
import { Bar, Line } from "./charts";
import {
  buildHistogramData,
  COLLAPSED_ROWS,
  commonLineOptions,
  fmtNum,
  histogramOptions,
  sortExposureRows,
} from "./utils";

export default function SectionExposure({ data }: { data: HealthDiagnosticsData }) {
  const [showAllExposureRows, setShowAllExposureRows] = useState(false);
  const [expSortKey, setExpSortKey] = useState<keyof HealthExposureStats>("max_abs");
  const [expSortAsc, setExpSortAsc] = useState(false);
  const [selectedExposureFactor, setSelectedExposureFactor] = useState<string>("");

  const exposureFactors = useMemo(() => {
    const stats = data.section2.factor_stats || [];
    const EPS = 1e-6;
    return stats
      .filter((s) => factorFamily(s.factor_id, data.factor_catalog) === "style")
      .filter((s) => !(Math.abs(s.p1) <= EPS && Math.abs(s.p99 - 1.0) <= EPS))
      .map((s) => s.factor_id)
      .sort((a, b) => shortFactorLabel(a, data.factor_catalog).localeCompare(shortFactorLabel(b, data.factor_catalog)));
  }, [data]);
  const exposureFactor = selectedExposureFactor || exposureFactors[0] || "";

  const sortedExposureRows = useMemo(() => {
    const rows = data.section2.factor_stats ?? [];
    return sortExposureRows(rows, expSortKey, expSortAsc, data.factor_catalog);
  }, [data, expSortKey, expSortAsc]);

  const turnoverSeries = data.section2.turnover_series ?? [];
  const turnoverData = {
    labels: turnoverSeries.map((r) => r.date),
    datasets: [
      {
        label: "Daily Exposure Turnover",
        data: turnoverSeries.map((r) => Number(r.turnover) || 0),
        borderColor: chartTextColor("secondary", 0.56),
        pointRadius: 0,
        borderWidth: 1.2,
        tension: 0.2,
      },
      {
        label: "Rolling 60",
        data: turnoverSeries.map((r) => Number(r.roll60) || 0),
        borderColor: chartLongColor(0.9),
        pointRadius: 0,
        borderWidth: 1.5,
        tension: 0.2,
      },
    ],
  };

  const showExposureRows = showAllExposureRows ? sortedExposureRows : sortedExposureRows.slice(0, COLLAPSED_ROWS);

  return (
    <div className="chart-card">
      <h3>
        <HelpLabel
          label="Section 2 — Exposure Diagnostics"
          plain="Checks if exposures are centered, properly scaled, and not drifting too fast."
          math="Uses mean, std, tails, exposure correlation, and turnover"
        />
      </h3>
      <div className="health-grid-2-half">
        <div>
          <h4>Exposure Turnover (Rolling 60)</h4>
          <div className="health-chart-sm">
            <Line data={turnoverData} options={commonLineOptions} />
          </div>
        </div>
        <div>
          <div className="health-picker-row">
            <h4>Exposure Histogram</h4>
            <select
              className="health-select"
              value={exposureFactor}
              onChange={(e) => setSelectedExposureFactor(e.target.value)}
            >
              {exposureFactors.map((f) => (
                <option key={f} value={f}>{shortFactorLabel(f)}</option>
              ))}
            </select>
          </div>
          <div className="health-chart-sm">
            <Bar
              data={buildHistogramData(data.section2.factor_histograms[exposureFactor] || { centers: [], counts: [] })}
              options={histogramOptions("Exposure", "Names")}
            />
          </div>
        </div>
      </div>

      <h4 style={{ marginTop: 10 }}>Exposure Correlation Heatmap</h4>
      <LazyMountOnVisible
        minHeight={360}
        fallback={<div className="detail-history-empty">Scroll to load the exposure correlation heatmap.</div>}
      >
        <div className="heatmap-centered-70">
          <CovarianceHeatmap data={data.section2.exposure_corr} factorCatalog={data.factor_catalog} />
        </div>
      </LazyMountOnVisible>

      <div className="dash-table health-table">
        <table>
          <thead>
            <tr>
              <th onClick={() => {
                if (expSortKey === "factor_id") setExpSortAsc((s) => !s);
                else { setExpSortKey("factor_id"); setExpSortAsc(true); }
              }}>Factor{expSortKey === "factor_id" ? (expSortAsc ? " ↑" : " ↓") : ""}</th>
              <th className="text-right" onClick={() => {
                if (expSortKey === "mean") setExpSortAsc((s) => !s);
                else { setExpSortKey("mean"); setExpSortAsc(false); }
              }}>Mean{expSortKey === "mean" ? (expSortAsc ? " ↑" : " ↓") : ""}</th>
              <th className="text-right" onClick={() => {
                if (expSortKey === "std") setExpSortAsc((s) => !s);
                else { setExpSortKey("std"); setExpSortAsc(false); }
              }}>Std Dev{expSortKey === "std" ? (expSortAsc ? " ↑" : " ↓") : ""}</th>
              <th className="text-right">p1</th>
              <th className="text-right">p99</th>
              <th className="text-right" onClick={() => {
                if (expSortKey === "max_abs") setExpSortAsc((s) => !s);
                else { setExpSortKey("max_abs"); setExpSortAsc(false); }
              }}>Max |x|{expSortKey === "max_abs" ? (expSortAsc ? " ↑" : " ↓") : ""}</th>
            </tr>
          </thead>
          <tbody>
            {showExposureRows.map((row) => (
              <tr key={row.factor_id}>
                <td>{shortFactorLabel(row.factor_id, data.factor_catalog)}</td>
                <td className="text-right">{fmtNum(row.mean, 3)}</td>
                <td className="text-right">{fmtNum(row.std, 3)}</td>
                <td className="text-right">{fmtNum(row.p1, 3)}</td>
                <td className="text-right">{fmtNum(row.p99, 3)}</td>
                <td className="text-right">{fmtNum(row.max_abs, 3)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <TableRowToggle
          totalRows={sortedExposureRows.length}
          collapsedRows={COLLAPSED_ROWS}
          expanded={showAllExposureRows}
          onToggle={() => setShowAllExposureRows((p) => !p)}
          label="factors"
        />
      </div>
    </div>
  );
}
