"use client";

import { useMemo, useRef, useState } from "react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Tooltip,
  Legend,
  Filler,
  type ChartData,
  type ChartOptions,
} from "chart.js";
import { Bar, Line } from "react-chartjs-2";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import CovarianceHeatmap from "@/components/CovarianceHeatmap";
import HelpLabel from "@/components/HelpLabel";
import TableRowToggle from "@/components/TableRowToggle";
import { useHealthDiagnostics } from "@/hooks/useApi";
import { shortFactorLabel, STYLE_FACTORS } from "@/lib/factorLabels";
import type {
  HealthDiagnosticsData,
  HealthExposureStats,
  HealthFactorPctRow,
  HealthHistogram,
  SeriesPoint,
} from "@/lib/types";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, Tooltip, Legend, Filler);

const COLLAPSED_ROWS = 12;

function fmtPct(v: number, digits = 2): string {
  return `${(Number(v) * 100).toFixed(digits)}%`;
}

function fmtNum(v: number, digits = 3): string {
  return Number(v).toFixed(digits);
}

function compactDateLabel(s: string): string {
  return s.length >= 7 ? s.slice(0, 7) : s;
}

function buildHistogramData(hist: HealthHistogram): ChartData<"bar", number[], string> {
  const labels = hist.centers.map((c) => c.toFixed(2));
  return {
    labels,
    datasets: [
      {
        label: "Count",
        data: hist.counts.map((v) => Number(v) || 0),
        backgroundColor: "rgba(169, 182, 210, 0.52)",
        borderWidth: 0,
        borderRadius: 0,
      },
    ],
  };
}

function histogramOptions(xLabel = "", yLabel = "Count"): ChartOptions<"bar"> {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: "rgba(20, 22, 30, 0.92)",
        borderColor: "rgba(154, 171, 214, 0.18)",
        borderWidth: 1,
        cornerRadius: 4,
        displayColors: false,
      },
    },
    scales: {
      x: {
        border: { display: false },
        grid: { display: false },
        ticks: {
          color: "rgba(169, 182, 210, 0.5)",
          maxTicksLimit: 8,
          font: { size: 9 },
        },
        title: xLabel ? { display: true, text: xLabel, color: "rgba(169, 182, 210, 0.55)", font: { size: 10 } } : undefined,
      },
      y: {
        border: { display: false },
        grid: { color: "rgba(154, 171, 214, 0.12)" },
        ticks: { color: "rgba(169, 182, 210, 0.5)", font: { size: 9 } },
        title: yLabel ? { display: true, text: yLabel, color: "rgba(169, 182, 210, 0.55)", font: { size: 10 } } : undefined,
      },
    },
  };
}

function r2ChartData(data: HealthDiagnosticsData): ChartData<"line", number[], string> {
  const rows = data.section1.r2_series;
  const labels = rows.map((r) => r.date);
  return {
    labels,
    datasets: [
      {
        label: "Week-End R²",
        data: rows.map((r) => (Number(r.r2) || 0) * 100),
        borderColor: "rgba(169, 182, 210, 0.72)",
        borderWidth: 1.3,
        pointRadius: 0,
        tension: 0.2,
      },
      {
        label: "12w Mean",
        data: rows.map((r) => (Number(r.roll60) || 0) * 100),
        borderColor: "rgba(107, 207, 154, 0.9)",
        borderWidth: 1.6,
        pointRadius: 0,
        tension: 0.24,
      },
      {
        label: "52w Mean",
        data: rows.map((r) => (Number(r.roll252) || 0) * 100),
        borderColor: "rgba(224, 87, 127, 0.84)",
        borderWidth: 1.6,
        pointRadius: 0,
        tension: 0.24,
      },
    ],
  };
}

const commonLineOptions: ChartOptions<"line"> = {
  responsive: true,
  maintainAspectRatio: false,
  interaction: { mode: "index", intersect: false },
  plugins: {
    legend: {
      display: true,
      labels: {
        boxWidth: 9,
        boxHeight: 9,
        color: "rgba(169, 182, 210, 0.72)",
        font: { size: 10 },
      },
    },
    tooltip: {
      backgroundColor: "rgba(20, 22, 30, 0.92)",
      borderColor: "rgba(154, 171, 214, 0.18)",
      borderWidth: 1,
      cornerRadius: 4,
    },
  },
  scales: {
    x: {
      border: { display: false },
      grid: { display: false },
      ticks: {
        color: "rgba(169, 182, 210, 0.5)",
        autoSkip: true,
        maxTicksLimit: 8,
        callback: (_v, idx, ticks) => compactDateLabel(String(ticks[idx]?.label ?? "")),
        font: { size: 9 },
      },
    },
    y: {
      border: { display: false },
      grid: { color: "rgba(154, 171, 214, 0.12)" },
      ticks: { color: "rgba(169, 182, 210, 0.5)", font: { size: 9 } },
    },
  },
};

function seriesData(rows: SeriesPoint[], label: string, color: string, multiply = 1): ChartData<"line", number[], string> {
  return {
    labels: rows.map((r) => r.date),
    datasets: [
      {
        label,
        data: rows.map((r) => (Number(r.value) || 0) * multiply),
        borderColor: color,
        borderWidth: 1.6,
        pointRadius: 0,
        tension: 0.24,
      },
    ],
  };
}

function sortNumber<T>(rows: T[], getter: (r: T) => number, asc: boolean): T[] {
  return [...rows].sort((a, b) => (asc ? getter(a) - getter(b) : getter(b) - getter(a)));
}

export default function HealthPage() {
  const { data, isLoading } = useHealthDiagnostics();
  const [showAllTStatRows, setShowAllTStatRows] = useState(false);
  const [showAllExposureRows, setShowAllExposureRows] = useState(false);
  const [tSortAsc, setTSortAsc] = useState(false);
  const [expSortKey, setExpSortKey] = useState<keyof HealthExposureStats>("max_abs");
  const [expSortAsc, setExpSortAsc] = useState(false);
  const [selectedExposureFactor, setSelectedExposureFactor] = useState<string>("");
  const [selectedReturnFactor, setSelectedReturnFactor] = useState<string>("");

  const exposureFactors = useMemo(() => {
    if (!data) return [];
    const stats = data.section2.factor_stats || [];
    const EPS = 1e-6;
    return stats
      .filter((s) => STYLE_FACTORS.has(s.factor))
      .filter((s) => !(Math.abs(s.p1) <= EPS && Math.abs(s.p99 - 1.0) <= EPS))
      .map((s) => s.factor)
      .sort();
  }, [data]);
  const returnFactors = data?.section3.factors ?? [];

  const exposureFactor = selectedExposureFactor || exposureFactors[0] || "";
  const returnFactor = selectedReturnFactor || returnFactors[0] || "";

  const sortedTStatRows = useMemo(() => {
    const rows = data?.section1.pct_days_abs_t_gt_2 ?? [];
    return sortNumber(rows, (r: HealthFactorPctRow) => Number(r.value) || 0, tSortAsc);
  }, [data, tSortAsc]);

  const sortedExposureRows = useMemo(() => {
    const rows = data?.section2.factor_stats ?? [];
    if (expSortKey === "factor") {
      return [...rows].sort((a, b) =>
        expSortAsc ? a.factor.localeCompare(b.factor) : b.factor.localeCompare(a.factor),
      );
    }
    return sortNumber(rows, (r: HealthExposureStats) => Number(r[expSortKey]) || 0, expSortAsc);
  }, [data, expSortKey, expSortAsc]);

  const blockChartRef = useRef<ChartJS<"line"> | null>(null);

  if (isLoading) {
    return <AnalyticsLoadingViz message="Loading model health..." />;
  }

  if (!data || data.status !== "ok") {
    return (
      <div className="chart-card">
        <h3>Health Diagnostics</h3>
        <div className="detail-history-empty">
          No diagnostics payload is available yet. Run refresh and reload this page.
        </div>
      </div>
    );
  }

  const r2Data = r2ChartData(data);
  const tHistData = buildHistogramData(data.section1.t_stat_hist);
  const blockRows = data.section1.incremental_block_r2_series ?? [];
  const blockData: ChartData<"line", number[], string> = {
    labels: blockRows.map((r) => r.date),
    datasets: [
      {
        label: "Industry R²",
        data: blockRows.map((r) => (Number(r.r2_industry) || 0) * 100),
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
    industry_pct_total: 0,
    style_pct_total: 0,
    idio_pct_total: 0,
    industry_pct_factor_only: 0,
    style_pct_factor_only: 0,
  };

  const turnoverSeries = data.section2.turnover_series ?? [];
  const turnoverData: ChartData<"line", number[], string> = {
    labels: turnoverSeries.map((r) => r.date),
    datasets: [
      {
        label: "Daily Exposure Turnover",
        data: turnoverSeries.map((r) => Number(r.turnover) || 0),
        borderColor: "rgba(169, 182, 210, 0.56)",
        pointRadius: 0,
        borderWidth: 1.2,
        tension: 0.2,
      },
      {
        label: "Rolling 60",
        data: turnoverSeries.map((r) => Number(r.roll60) || 0),
        borderColor: "rgba(107, 207, 154, 0.9)",
        pointRadius: 0,
        borderWidth: 1.5,
        tension: 0.2,
      },
    ],
  };

  const cumulativeRows = data.section3.cumulative_returns[returnFactor] ?? [];
  const rollingVolRows = data.section3.rolling_vol_60d[returnFactor] ?? [];
  const returnDist = data.section3.return_dist[returnFactor] ?? { centers: [], counts: [] };

  const eigenvalues = data.section4.eigenvalues ?? [];
  const eigenData: ChartData<"bar", number[], string> = {
    labels: eigenvalues.map((_v, i) => `λ${i + 1}`),
    datasets: [
      {
        label: "Eigenvalue",
        data: eigenvalues,
        backgroundColor: "rgba(169, 182, 210, 0.55)",
        borderWidth: 0,
      },
    ],
  };

  const showTStatRows = showAllTStatRows ? sortedTStatRows : sortedTStatRows.slice(0, COLLAPSED_ROWS);
  const showExposureRows = showAllExposureRows ? sortedExposureRows : sortedExposureRows.slice(0, COLLAPSED_ROWS);

  return (
    <div className="health-wrap">
      <div className="chart-card">
        <h3 style={{ marginBottom: 6 }}>Model Health Diagnostics</h3>
        <div className="health-meta-row">
          <span>As of {data.as_of ?? "—"}</span>
          <span>{data._cached ? "Cached" : "Freshly Computed"}</span>
        </div>
        {data.notes?.length > 0 && (
          <ul className="health-notes">
            {data.notes.map((n) => <li key={n}>{n}</li>)}
          </ul>
        )}
      </div>

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
          <span>Section 1 sampling: {data.section1.sampling === "weekly_week_end" ? "Week-end (10Y)" : "Daily (10Y)"}</span>
          <span>Heavy diagnostics downsampled for speed</span>
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
              plain="Shows how much fit comes from industry first, then extra fit added by style."
              math="Industry R², Style ΔR² = Full R² − Industry R²"
              interpret={{
                lookFor: "Stable block contributions with style adding incremental fit.",
                good: "Industry R² is stable, and style ΔR² is positive and persistent.",
                distribution: "Avoid abrupt structural jumps or long flatline in style ΔR².",
              }}
            />
          </h4>
          {blockRows.length > 0 && (() => {
            const last = blockRows[blockRows.length - 1];
            const indR2 = (Number(last.r2_industry) || 0) * 100;
            const styleR2 = (Number(last.r2_style_incremental) || 0) * 100;
            const fullR2 = (Number(last.r2_full) || 0) * 100;
            return (
              <div className="detail-history-stats">
                <span className="detail-history-stat" style={{ color: "rgba(204, 53, 88, 0.85)" }}>
                  Ind {indR2.toFixed(1)}%
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
        <div className="health-grid-2">
          <div>
            <h4>
              <HelpLabel
                label="Factor t-stat Distribution"
                plain="Shows how often factor signals are small versus extreme on a day."
                math="Approx t = daily factor return / daily residual volatility"
                interpret={{
                  lookFor: "Most observations near the center with limited extreme spikes.",
                  good: "Majority within about -2 to +2, with occasional tail observations.",
                  distribution: "Roughly symmetric around 0; heavy one-sided tails are a warning.",
                }}
              />
            </h4>
            <div className="health-chart-sm">
              <Bar data={tHistData} options={histogramOptions("t-stat", "Days")} />
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
              <Line
                data={breadthData}
                options={commonLineOptions}
              />
            </div>
          </div>
        </div>

        <div className="dash-table health-table">
          <table>
            <thead>
              <tr>
                <th>
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Factor"
                      plain="Name of the factor being evaluated."
                      math="Each row corresponds to one model factor"
                      interpret={{
                        lookFor: "Which factors consistently show statistical activity.",
                        good: "A diversified set of active factors rather than one or two dominating.",
                      }}
                    />
                  </span>
                </th>
                <th
                  className="text-right"
                  onClick={() => setTSortAsc((s) => !s)}
                >
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="% Days |t| > 2"
                      plain="How often this factor had a meaningfully strong daily signal."
                      math="% days = count(|t| > 2) / total days"
                      interpret={{
                        lookFor: "Factors with persistent but not extreme hit rates.",
                        good: "Roughly low-single-digit to low-teens % is common for useful factors.",
                        distribution: "Very near 0% can indicate weak signal; extremely high values can indicate instability.",
                      }}
                    />
                    {tSortAsc ? " ↑" : " ↓"}
                  </span>
                </th>
              </tr>
            </thead>
            <tbody>
              {showTStatRows.map((row) => (
                <tr key={row.factor}>
                  <td>{shortFactorLabel(row.factor)}</td>
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
            <div className="health-kpi-label">
              <HelpLabel
                label="Portfolio Variance Split (Total)"
                plain="Current portfolio variance share from industry, style, and idiosyncratic risk."
                math="Total variance split from risk decomposition"
                interpret={{
                  lookFor: "Whether realized portfolio risk source matches your strategy design.",
                  good: "Mix is intentional: not accidentally concentrated in one block.",
                }}
              />
            </div>
            <div className="health-kpi-subrow">
              <span>Industry</span><strong>{varianceSplit.industry_pct_total.toFixed(1)}%</strong>
            </div>
            <div className="health-kpi-subrow">
              <span>Style</span><strong>{varianceSplit.style_pct_total.toFixed(1)}%</strong>
            </div>
            <div className="health-kpi-subrow">
              <span>Idio</span><strong>{varianceSplit.idio_pct_total.toFixed(1)}%</strong>
            </div>
          </div>
          <div className="health-kpi">
            <div className="health-kpi-label">
              <HelpLabel
                label="Portfolio Variance Split (Factor-Only)"
                plain="Within factor risk only, this shows industry vs style share."
                math="Industry% and style% normalized within factor variance"
                interpret={{
                  lookFor: "Whether factor block dominance aligns with your intended exposures.",
                  good: "A stable, explainable mix over time.",
                }}
              />
            </div>
            <div className="health-kpi-subrow">
              <span>Industry</span><strong>{varianceSplit.industry_pct_factor_only.toFixed(1)}%</strong>
            </div>
            <div className="health-kpi-subrow">
              <span>Style</span><strong>{varianceSplit.style_pct_factor_only.toFixed(1)}%</strong>
            </div>
            <div className="health-kpi-subrow">
              <span>Summary Mean |t|</span>
              <strong>{breadthSummary.industry_mean_abs_t.toFixed(2)} / {breadthSummary.style_mean_abs_t.toFixed(2)}</strong>
            </div>
          </div>
        </div>
      </div>

      <div className="chart-card">
        <h3>
          <HelpLabel
            label="Section 2 — Exposure Diagnostics"
            plain="Checks if exposures are centered, properly scaled, and not drifting too fast."
            math="Uses mean, std, tails, exposure correlation, and turnover"
            interpret={{
              lookFor: "Means near zero, std near one (for standardized factors), and stable turnover.",
              good: "No persistent drift in moments and no sudden jumps in turnover.",
              distribution: "Mostly centered distributions with moderate tails.",
            }}
          />
        </h3>
        <div className="health-grid-2">
          <div>
            <h4>
              <HelpLabel
                label="Exposure Turnover (Rolling 60)"
                plain="Average day-to-day change in exposures. Higher means less stability."
                math="Turnover_t = mean(|X_t − X_t-1|)"
                interpret={{
                  lookFor: "A stable or falling rolling line, not a persistent climb.",
                  good: "Relatively low and stable compared with its own history.",
                  distribution: "Short spikes are normal around rebalances; persistent high levels are not.",
                }}
              />
            </h4>
            <div className="health-chart-sm">
              <Line data={turnoverData} options={commonLineOptions} />
            </div>
          </div>
          <div>
            <div className="health-picker-row">
              <h4>
                <HelpLabel
                  label="Exposure Histogram"
                  plain="Shows where most stocks sit for this factor: middle or extremes."
                  math="Histogram of cross-sectional exposures for selected factor"
                  interpret={{
                    lookFor: "Mass concentrated around center with limited extreme tails.",
                    good: "Centered around 0 for most style factors; tails present but not dominating.",
                    distribution: "Mild skew can be fine, but strong one-sided skew is a red flag.",
                  }}
                />
              </h4>
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

        <h4 style={{ marginTop: 10 }}>
          <HelpLabel
            label="Exposure Correlation Heatmap"
            plain="Shows which factors tend to move together in exposure space."
            math="corr(exposure_f1, exposure_f2)"
            interpret={{
              lookFor: "Limited blocks of very high absolute correlation.",
              good: "Most pairs are moderate; only conceptually related pairs are high.",
              distribution: "A wide spread around 0 is healthier than many values near ±1.",
            }}
          />
        </h4>
        <CovarianceHeatmap data={data.section2.exposure_corr} />

        <div className="dash-table health-table">
          <table>
            <thead>
              <tr>
                <th
                  onClick={() => {
                    if (expSortKey === "factor") setExpSortAsc((s) => !s);
                    else { setExpSortKey("factor"); setExpSortAsc(true); }
                  }}
                >
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Factor"
                      plain="Name of the exposure factor."
                      math="Each row corresponds to one cross-sectional factor"
                      interpret={{
                        lookFor: "Factors with drifting moments or extreme tails.",
                        good: "Most factors have stable moments and controlled tails.",
                      }}
                    />
                    {expSortKey === "factor" ? (expSortAsc ? " ↑" : " ↓") : ""}
                  </span>
                </th>
                <th className="text-right" onClick={() => {
                  if (expSortKey === "mean") setExpSortAsc((s) => !s);
                  else { setExpSortKey("mean"); setExpSortAsc(false); }
                }}>
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Mean"
                      plain="Average exposure across stocks. Closer to zero is usually healthier."
                      math="mean(exposure_f)"
                      interpret={{
                        lookFor: "Persistent positive/negative drift.",
                        good: "Close to 0 over time.",
                      }}
                    />
                    {expSortKey === "mean" ? (expSortAsc ? " ↑" : " ↓") : ""}
                  </span>
                </th>
                <th className="text-right" onClick={() => {
                  if (expSortKey === "std") setExpSortAsc((s) => !s);
                  else { setExpSortKey("std"); setExpSortAsc(false); }
                }}>
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Std Dev"
                      plain="Typical spread of exposures. Standardized factors are often near one."
                      math="std(exposure_f)"
                      interpret={{
                        lookFor: "Large drift away from the normal scale.",
                        good: "Around 1 for standardized style factors.",
                      }}
                    />
                    {expSortKey === "std" ? (expSortAsc ? " ↑" : " ↓") : ""}
                  </span>
                </th>
                <th className="text-right">
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="p1"
                      plain="Low tail cutoff: only 1% of stocks are below this."
                      math="1st percentile(exposure_f)"
                      interpret={{
                        lookFor: "Very negative tail values expanding over time.",
                        good: "Reasonably stable left tail without runaway extremes.",
                      }}
                    />
                  </span>
                </th>
                <th className="text-right">
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="p99"
                      plain="High tail cutoff: only 1% of stocks are above this."
                      math="99th percentile(exposure_f)"
                      interpret={{
                        lookFor: "Very positive tail values expanding over time.",
                        good: "Reasonably stable right tail without runaway extremes.",
                      }}
                    />
                  </span>
                </th>
                <th className="text-right" onClick={() => {
                  if (expSortKey === "max_abs") setExpSortAsc((s) => !s);
                  else { setExpSortKey("max_abs"); setExpSortAsc(false); }
                }}>
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Max |x|"
                      plain="Most extreme exposure in either direction."
                      math="max(|exposure_f|)"
                      interpret={{
                        lookFor: "Outlier growth in single names.",
                        good: "Extreme values are occasional and not structurally rising.",
                      }}
                    />
                    {expSortKey === "max_abs" ? (expSortAsc ? " ↑" : " ↓") : ""}
                  </span>
                </th>
              </tr>
            </thead>
            <tbody>
              {showExposureRows.map((row) => (
                <tr key={row.factor}>
                  <td>{shortFactorLabel(row.factor)}</td>
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

      <div className="chart-card">
        <h3>
          <HelpLabel
            label="Section 3 — Factor Return Health"
            plain="Checks if factor returns look reasonable: trend, volatility, co-movement, and outliers."
            math="Uses cumulative return, rolling vol, return correlation, and return distribution"
            interpret={{
              lookFor: "Stable return behavior without structural breaks.",
              good: "No single factor dominates all risk/return behavior across long windows.",
              distribution: "Returns centered near 0 with manageable tails.",
            }}
          />
        </h3>
        <div className="health-picker-row" style={{ marginBottom: 8 }}>
          <span className="health-picker-label">
            <HelpLabel
              label="Selected Factor"
              plain="Choose the factor used in the return charts below."
              math="All charts below use this selected factor"
              interpret={{
                lookFor: "Compare high-impact portfolio factors first.",
                good: "Prioritize factors with high current exposure or risk contribution.",
              }}
            />
          </span>
          <select
            className="health-select"
            value={returnFactor}
            onChange={(e) => setSelectedReturnFactor(e.target.value)}
          >
            {returnFactors.map((f) => (
              <option key={f} value={f}>{shortFactorLabel(f)}</option>
            ))}
          </select>
        </div>
        <div className="health-grid-2">
          <div>
            <h4>
              <HelpLabel
                label="Cumulative Return"
                plain="How a $1 factor exposure would have grown over time."
                math="Cumulative = Π(1 + daily factor return) − 1"
                interpret={{
                  lookFor: "Long flat periods, sudden jumps, or repeated reversals.",
                  good: "Plausible long-run path without unexplained discontinuities.",
                  distribution: "More continuous paths are generally more robust than jump-driven paths.",
                }}
              />
            </h4>
            <div className="health-chart-sm">
              <Line
                data={seriesData(cumulativeRows, "Cumulative Return", "rgba(107, 207, 154, 0.88)", 100)}
                options={{
                  ...commonLineOptions,
                  scales: {
                    ...commonLineOptions.scales,
                    y: {
                      ...(commonLineOptions.scales?.y || {}),
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
          </div>
          <div>
            <h4>
              <HelpLabel
                label="Rolling 60d Volatility"
                plain="How noisy this factor has been recently (last 60 trading days)."
                math="Rolling vol = std(60d returns) × √252"
                interpret={{
                  lookFor: "Volatility regime shifts and sustained elevation.",
                  good: "A stable range relative to its own history.",
                  distribution: "Occasional spikes are normal; persistently high plateaus are riskier.",
                }}
              />
            </h4>
            <div className="health-chart-sm">
              <Line
                data={seriesData(rollingVolRows, "Rolling Volatility", "rgba(224, 87, 127, 0.88)", 100)}
                options={{
                  ...commonLineOptions,
                  scales: {
                    ...commonLineOptions.scales,
                    y: {
                      ...(commonLineOptions.scales?.y || {}),
                      ticks: {
                        color: "rgba(169, 182, 210, 0.5)",
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
            <h4>
              <HelpLabel
                label="Daily Return Distribution"
                plain="How often daily returns are small, large positive, or large negative."
                math="Histogram of daily factor returns"
                interpret={{
                  lookFor: "Fat tails and strong skew to one side.",
                  good: "Center near 0 with tails that are not excessively heavy.",
                  distribution: "Roughly symmetric with moderate kurtosis is healthier.",
                }}
              />
            </h4>
            <div className="health-chart-sm">
              <Bar data={buildHistogramData(returnDist)} options={histogramOptions("Daily Return", "Days")} />
            </div>
          </div>
          <div>
            <h4>
              <HelpLabel
                label="Factor Return Correlation Heatmap"
                plain="Shows which factors tend to rise and fall together."
                math="corr(daily return_f1, daily return_f2)"
                interpret={{
                  lookFor: "Large blocks of near-1 or near-(-1) correlation.",
                  good: "Mostly moderate correlations with intuitive clusters.",
                  distribution: "Spread around 0 indicates better diversification across factors.",
                }}
              />
            </h4>
            <CovarianceHeatmap data={data.section3.return_corr} />
          </div>
        </div>
      </div>

      <div className="chart-card">
        <h3>
          <HelpLabel
            label="Section 4 — Covariance Quality"
            plain="Checks whether covariance forecasts are stable and close to what actually happened."
            math="Uses eigenvalues, condition number, and forecast-vs-realized volatility"
            interpret={{
              lookFor: "Numerical instability and persistent forecast error.",
              good: "Reasonable conditioning and forecast vol close to realized vol.",
              distribution: "Risk should not concentrate in too few latent dimensions.",
            }}
          />
        </h3>
        <div className="health-grid-2">
          <div>
            <h4>
              <HelpLabel
                label="Eigenvalue Spectrum"
                plain="If early bars dominate, a few factors are driving most risk."
                math="Eigenvalues of factor covariance matrix"
                interpret={{
                  lookFor: "Over-concentration in the first one or two eigenvalues.",
                  good: "Gradual decay, not a cliff after the first eigenvalue.",
                  distribution: "A smoother spectrum means more distributed risk structure.",
                }}
              />
            </h4>
            <div className="health-chart-sm">
              <Bar data={eigenData} options={histogramOptions("Eigenvalue Rank", "Eigenvalue")} />
            </div>
          </div>
          <div>
            <h4>
              <HelpLabel
                label="Rolling Average Factor Vol"
                plain="Average factor volatility level through time."
                math="Mean across factors of 60d annualized vol"
                interpret={{
                  lookFor: "Step-changes or sustained elevated regimes.",
                  good: "Stable bands with temporary spikes during stress.",
                  distribution: "Long high-vol plateaus suggest unstable covariance forecasts.",
                }}
              />
            </h4>
            <div className="health-chart-sm">
              <Line
                data={seriesData(data.section4.rolling_avg_factor_vol, "Avg Factor Vol", "rgba(169, 182, 210, 0.85)", 100)}
                options={{
                  ...commonLineOptions,
                  scales: {
                    ...commonLineOptions.scales,
                    y: {
                      ...(commonLineOptions.scales?.y || {}),
                      ticks: {
                        color: "rgba(169, 182, 210, 0.5)",
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

        <div className="health-kpi-strip">
          <div className="health-kpi">
            <div className="health-kpi-label">
              <HelpLabel
                label="Condition Number"
                plain="Numerical stability score for the covariance matrix. Lower is usually safer."
                math="cond(F) = ||F|| · ||F⁻¹||"
                interpret={{
                  lookFor: "Persistent very high values.",
                  good: "Lower is better; very large values indicate near-singularity.",
                }}
              />
            </div>
            <div className="health-kpi-value">
              {Number(data.section4.condition_number || 0).toFixed(2)}
            </div>
          </div>
        </div>

        <div className="dash-table health-table">
          <table>
            <thead>
              <tr>
                <th>
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Portfolio Sample"
                      plain="Reference portfolio used to compare model forecast vs realized risk."
                      math="Examples: current portfolio, equal-weight variants, SPY proxy"
                      interpret={{
                        lookFor: "Whether errors are broad (all samples) or specific to one construction.",
                        good: "No persistent forecast bias across samples.",
                      }}
                    />
                  </span>
                </th>
                <th className="text-right">
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Forecast Vol"
                      plain="Model-predicted annual risk for this portfolio sample."
                      math="Forecast vol = √(hᵀFh + specific risk)"
                      interpret={{
                        lookFor: "Large persistent gap versus realized vol.",
                        good: "Close to realized vol on average.",
                      }}
                    />
                  </span>
                </th>
                <th className="text-right">
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Realized Vol (60d)"
                      plain="Observed annual risk from the last 60 trading days."
                      math="Realized vol = std(60d portfolio returns) × √252"
                      interpret={{
                        lookFor: "Regime jumps relative to forecast.",
                        good: "Moves in line with forecast direction and magnitude.",
                      }}
                    />
                  </span>
                </th>
                <th className="text-right">
                  <span className="col-help-wrap">
                    <HelpLabel
                      label="Gap"
                      plain="How far observed risk is from the model forecast."
                      math="Gap = realized vol − forecast vol"
                      interpret={{
                        lookFor: "Bias that is mostly positive or mostly negative.",
                        good: "Centered near 0 across samples and over time.",
                        distribution: "Tight spread around 0 means better calibration.",
                      }}
                    />
                  </span>
                </th>
              </tr>
            </thead>
            <tbody>
              {(data.section4.forecast_vs_realized || []).map((row) => {
                const gap = (Number(row.realized_vol_60d) || 0) - (Number(row.forecast_vol) || 0);
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
    </div>
  );
}
