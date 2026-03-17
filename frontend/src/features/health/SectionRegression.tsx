"use client";

import { useMemo, useRef, useState } from "react";
import HelpLabel from "@/components/HelpLabel";
import TableRowToggle from "@/components/TableRowToggle";
import { shortFactorLabel } from "@/lib/factorLabels";
import type { HealthDiagnosticsData } from "@/lib/types";
import { Bar, ChartJS, Line, type ChartData } from "./charts";
import {
  buildHistogramData,
  COLLAPSED_ROWS,
  commonLineOptions,
  r2ChartData,
  sortTStatRows,
} from "./utils";

export default function SectionRegression({ data }: { data: HealthDiagnosticsData }) {
  const [showAllTStatRows, setShowAllTStatRows] = useState(false);
  const [tSortAsc, setTSortAsc] = useState(false);
  const blockChartRef = useRef<ChartJS<"line"> | null>(null);

  const sortedTStatRows = useMemo(() => {
    const rows = data.section1.pct_days_abs_t_gt_2 ?? [];
    return sortTStatRows(rows, tSortAsc);
  }, [data, tSortAsc]);

  const r2Data = r2ChartData(data);
  const tHistData = buildHistogramData(data.section1.t_stat_hist);
  const blockRows = data.section1.incremental_block_r2_series ?? [];
  const blockData: ChartData<"line", number[], string> = {
    labels: blockRows.map((r) => r.date),
    datasets: [
      {
        label: "Structural R²",
        data: blockRows.map((r) => (Number(r.r2_structural) || 0) * 100),
        borderColor: "rgba(204, 53, 88, 0.45)",
        backgroundColor: (ctx) => {
          const chart = ctx.chart;
          if (!chart.chartArea) return "rgba(204, 53, 88, 0.15)";
          const grad = chart.ctx.createLinearGradient(0, chart.chartArea.top, 0, chart.chartArea.bottom);
          grad.addColorStop(0, "rgba(204, 53, 88, 0.38)");
          grad.addColorStop(0.6, "rgba(204, 53, 88, 0.14)");
          grad.addColorStop(1, "rgba(204, 53, 88, 0.04)");
          return grad;
        },
        borderWidth: 1,
        pointRadius: 0,
        pointHoverRadius: 3,
        pointHoverBackgroundColor: "rgba(204, 53, 88, 0.9)",
        pointHoverBorderColor: "#fff",
        pointHoverBorderWidth: 1.5,
        fill: "origin",
        stack: "r2_blocks",
        tension: 0.25,
      },
      {
        label: "Style ΔR²",
        data: blockRows.map((r) => (Number(r.r2_style_incremental) || 0) * 100),
        borderColor: "rgba(107, 207, 154, 0.45)",
        backgroundColor: (ctx) => {
          const chart = ctx.chart;
          if (!chart.chartArea) return "rgba(107, 207, 154, 0.12)";
          const grad = chart.ctx.createLinearGradient(0, chart.chartArea.top, 0, chart.chartArea.bottom);
          grad.addColorStop(0, "rgba(107, 207, 154, 0.32)");
          grad.addColorStop(0.5, "rgba(107, 207, 154, 0.12)");
          grad.addColorStop(1, "rgba(107, 207, 154, 0.03)");
          return grad;
        },
        borderWidth: 1,
        pointRadius: 0,
        pointHoverRadius: 3,
        pointHoverBackgroundColor: "rgba(107, 207, 154, 0.9)",
        pointHoverBorderColor: "#fff",
        pointHoverBorderWidth: 1.5,
        fill: "-1",
        stack: "r2_blocks",
        tension: 0.25,
      },
      {
        label: "Full R²",
        data: blockRows.map((r) => (Number(r.r2_full) || 0) * 100),
        borderColor: "rgba(232, 237, 249, 0.35)",
        borderWidth: 1.2,
        pointRadius: 0,
        pointHoverRadius: 3,
        pointHoverBackgroundColor: "rgba(232, 237, 249, 0.9)",
        pointHoverBorderColor: "#fff",
        pointHoverBorderWidth: 1.5,
        fill: false,
        stack: "r2_full_overlay",
        tension: 0.25,
      },
    ],
  };

  const breadthRows = data.section1.bucket_breadth_series ?? [];
  const breadthData: ChartData<"line", number[], string> = {
    labels: breadthRows.map((r) => r.date),
    datasets: [
      {
        label: "Industry Mean |t|",
        data: breadthRows.map((r) => Number(r.industry_mean_abs_t) || 0),
        borderColor: "rgba(224, 87, 127, 0.9)",
        borderWidth: 1.5,
        pointRadius: 0,
        tension: 0.2,
      },
      {
        label: "Style Mean |t|",
        data: breadthRows.map((r) => Number(r.style_mean_abs_t) || 0),
        borderColor: "rgba(107, 207, 154, 0.9)",
        borderWidth: 1.5,
        pointRadius: 0,
        tension: 0.2,
      },
    ],
  };

  const breadthSummary = data.section1.bucket_breadth_summary ?? {
    industry_mean_abs_t: 0,
    style_mean_abs_t: 0,
  };
  const varianceSplit = data.section1.portfolio_variance_split ?? {
    market_pct_total: 0,
    industry_pct_total: 0,
    style_pct_total: 0,
    idio_pct_total: 0,
    market_pct_factor_only: 0,
    industry_pct_factor_only: 0,
    style_pct_factor_only: 0,
  };
  const showTStatRows = showAllTStatRows ? sortedTStatRows : sortedTStatRows.slice(0, COLLAPSED_ROWS);

  return (
    <div className="chart-card">
      <h3>
        <HelpLabel
          label="Section 1 — Cross-Sectional Regression Health"
          plain="Checks whether daily regressions are stable and explain returns consistently."
          math="Tracks R², t-signal strength, and industry/style signal share"
          interpret={{
            lookFor: "Stable rolling R² and no prolonged collapses in daily R².",
            good: "Rolling R² is steady and daily R² stays meaningfully above zero most days.",
            distribution: "t-stats are centered near zero with controlled tails.",
          }}
        />
      </h3>
      <div className="health-meta-row" style={{ marginBottom: 6 }}>
        <span>Section 1 sampling: {data.section1.sampling === "weekly_week_end" ? "Week-end sample (10Y)" : "Daily (10Y)"}</span>
        <span>Heavy diagnostics are sampled at week-end for speed</span>
      </div>
      <div className="health-chart-lg">
        <Line
          data={r2Data}
          options={{
            ...commonLineOptions,
            scales: {
              ...commonLineOptions.scales,
              y: {
                ...(commonLineOptions.scales?.y || {}),
                ticks: {
                  color: "rgba(169, 182, 210, 0.5)",
                  font: { size: 9 },
                  callback: (v) => `${Number(v).toFixed(0)}%`,
                },
              },
            },
          }}
        />
      </div>
      <div className="detail-history-header" style={{ marginBottom: 2 }}>
        <h4 style={{ margin: 0 }}>
          <HelpLabel
            label="Incremental R² By Block"
            plain="Shows how much fit comes from the structural block first, then extra fit added by style."
            math="Structural R², Style ΔR² = Full R² − Structural R²"
            interpret={{
              lookFor: "Stable block contributions with style adding incremental fit.",
              good: "Structural R² is stable, and style ΔR² is positive and persistent.",
              distribution: "Avoid abrupt structural jumps or long flatline in style ΔR².",
            }}
          />
        </h4>
        {blockRows.length > 0 && (() => {
          const last = blockRows[blockRows.length - 1];
          const structuralR2 = (Number(last.r2_structural) || 0) * 100;
          const styleR2 = (Number(last.r2_style_incremental) || 0) * 100;
          const fullR2 = (Number(last.r2_full) || 0) * 100;
          return (
            <div className="detail-history-stats">
              <span className="detail-history-stat" style={{ color: "rgba(204, 53, 88, 0.85)" }}>
                Struct {structuralR2.toFixed(1)}%
              </span>
              <span className="detail-history-stat" style={{ color: "rgba(107, 207, 154, 0.85)" }}>
                +Style {styleR2.toFixed(1)}%
              </span>
              <span className="detail-history-stat" style={{ color: "rgba(232, 237, 249, 0.7)" }}>
                = {fullR2.toFixed(1)}%
              </span>
            </div>
          );
        })()}
      </div>
      <div className="health-chart-lg" style={{ marginBottom: 10 }}>
        <Line
          ref={blockChartRef}
          data={blockData}
          options={{
            ...commonLineOptions,
            plugins: {
              ...commonLineOptions.plugins,
              tooltip: {
                ...commonLineOptions.plugins?.tooltip,
                callbacks: {
                  label: (ctx) => {
                    const val = Number(ctx.parsed.y ?? 0);
                    return ` ${ctx.dataset.label}: ${val.toFixed(1)}%`;
                  },
                },
              },
            },
            scales: {
              ...commonLineOptions.scales,
              x: {
                ...(commonLineOptions.scales?.x || {}),
                stacked: true,
              },
              y: {
                ...(commonLineOptions.scales?.y || {}),
                stacked: true,
                grid: { color: "rgba(154, 171, 214, 0.08)" },
                ticks: {
                  color: "rgba(169, 182, 210, 0.5)",
                  callback: (v) => `${Number(v).toFixed(0)}%`,
                  font: { size: 9 },
                },
              },
            },
          }}
        />
      </div>
      <div className="health-grid-2-half">
        <div>
          <h4>
            <HelpLabel
              label="Factor t-stat Distribution"
              plain="Shows how often factor signals are small versus extreme on a day."
              math="t = factor return / heteroskedasticity-robust SE"
              interpret={{
                lookFor: "Most observations near the center with limited extreme spikes.",
                good: "Majority within about -2 to +2, with occasional tail observations.",
                distribution: "Roughly symmetric around 0; heavy one-sided tails are a warning.",
              }}
            />
          </h4>
          <div className="health-chart-sm">
            <Bar data={tHistData} options={{
              responsive: true,
              maintainAspectRatio: false,
              plugins: { legend: { display: false } },
            }} />
          </div>
        </div>
        <div>
          <h4>
            <HelpLabel
              label="Breadth-Adjusted Signal Strength"
              plain="Compares average per-factor signal strength, so bucket size does not dominate the result."
              math="Mean |t| per bucket by day"
              interpret={{
                lookFor: "Whether industry still dominates after adjusting for factor count.",
                good: "Both buckets contribute signal over time, not just one.",
                distribution: "Stable spread between lines is normal; extreme divergence is a warning.",
              }}
            />
          </h4>
          <div className="health-chart-sm">
            <Line data={breadthData} options={commonLineOptions} />
          </div>
        </div>
      </div>

      <div className="dash-table health-table">
        <table>
          <thead>
            <tr>
              <th>Factor</th>
              <th className="text-right" onClick={() => setTSortAsc((s) => !s)}>% Days |t| &gt; 2{tSortAsc ? " ↑" : " ↓"}</th>
            </tr>
          </thead>
          <tbody>
            {showTStatRows.map((row) => (
              <tr key={row.factor_id}>
                <td>{shortFactorLabel(row.factor_id, data.factor_catalog)}</td>
                <td className="text-right">{row.value.toFixed(2)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
        <TableRowToggle
          totalRows={sortedTStatRows.length}
          collapsedRows={COLLAPSED_ROWS}
          expanded={showAllTStatRows}
          onToggle={() => setShowAllTStatRows((p) => !p)}
          label="factors"
        />
      </div>

      <div className="health-kpi-strip">
        <div className="health-kpi">
          <div className="health-kpi-label">Portfolio Variance Split (Total)</div>
          <div className="health-kpi-subrow"><span>Market</span><strong>{varianceSplit.market_pct_total.toFixed(1)}%</strong></div>
          <div className="health-kpi-subrow"><span>Industry</span><strong>{varianceSplit.industry_pct_total.toFixed(1)}%</strong></div>
          <div className="health-kpi-subrow"><span>Style</span><strong>{varianceSplit.style_pct_total.toFixed(1)}%</strong></div>
          <div className="health-kpi-subrow"><span>Idio</span><strong>{varianceSplit.idio_pct_total.toFixed(1)}%</strong></div>
        </div>
        <div className="health-kpi">
          <div className="health-kpi-label">Portfolio Variance Split (Factor-Only)</div>
          <div className="health-kpi-subrow"><span>Market</span><strong>{varianceSplit.market_pct_factor_only.toFixed(1)}%</strong></div>
          <div className="health-kpi-subrow"><span>Industry</span><strong>{varianceSplit.industry_pct_factor_only.toFixed(1)}%</strong></div>
          <div className="health-kpi-subrow"><span>Style</span><strong>{varianceSplit.style_pct_factor_only.toFixed(1)}%</strong></div>
          <div className="health-kpi-subrow">
            <span>Summary Mean |t|</span>
            <strong>{breadthSummary.industry_mean_abs_t.toFixed(2)} / {breadthSummary.style_mean_abs_t.toFixed(2)}</strong>
          </div>
        </div>
      </div>
    </div>
  );
}
