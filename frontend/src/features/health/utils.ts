"use client";

import type {
  HealthCoverageFieldRow,
  HealthDiagnosticsData,
  HealthExposureStats,
  HealthFactorPctRow,
  HealthHistogram,
  OperatorLaneStatus,
  SeriesPoint,
  FactorCatalogEntry,
} from "@/lib/types/cuse4";
import { factorDisplayName } from "@/lib/factorLabels";
import {
  chartGridColor,
  chartLongColor,
  chartShortColor,
  chartTextColor,
  tooltipOptions,
} from "@/lib/charts/chartTheme";
import type { ChartData, ChartOptions } from "./charts";

export const COLLAPSED_ROWS = 12;

export function fmtPct(v: number, digits = 2): string {
  return `${(Number(v) * 100).toFixed(digits)}%`;
}

export function fmtNum(v: number, digits = 3): string {
  return Number(v).toFixed(digits);
}

export function fmtInt(v: number): string {
  return Number(v || 0).toLocaleString();
}

export function compactDateLabel(s: string): string {
  return s.length >= 7 ? s.slice(0, 7) : s;
}

export function fmtTs(v: string | null | undefined): string {
  if (!v) return "—";
  const dt = new Date(v);
  if (Number.isNaN(dt.getTime())) return v;
  return dt.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function operatorTone(status: string | null | undefined): "success" | "warning" | "error" {
  const clean = String(status || "").toLowerCase();
  if (clean === "ok" || clean === "completed") return "success";
  if (clean === "running") return "warning";
  if (clean === "missing" || clean === "unknown" || clean === "skipped") return "warning";
  return "error";
}

export function laneSummary(lane: OperatorLaneStatus): string {
  const run = lane.latest_run;
  if (run.status === "missing") return "No runs yet";
  if (run.status === "running") return `Running since ${fmtTs(run.started_at)}`;
  const ts = run.finished_at || run.updated_at;
  return `${run.status.toUpperCase()} · ${fmtTs(ts)}`;
}

export function buildHistogramData(hist: HealthHistogram): ChartData<"bar", number[], string> {
  const labels = hist.centers.map((c) => c.toFixed(2));
  return {
    labels,
    datasets: [
      {
        label: "Count",
        data: hist.counts.map((v) => Number(v) || 0),
        backgroundColor: chartTextColor("secondary", 0.52),
        borderWidth: 0,
        borderRadius: 0,
      },
    ],
  };
}

export function histogramOptions(xLabel = "", yLabel = "Count"): ChartOptions<"bar"> {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      tooltip: {
        ...tooltipOptions(),
        borderWidth: 1,
        displayColors: false,
      },
    },
    scales: {
      x: {
        border: { display: false },
        grid: { display: false },
        ticks: {
          color: chartTextColor("secondary", 0.5),
          maxTicksLimit: 8,
          font: { size: 9 },
        },
        title: xLabel ? { display: true, text: xLabel, color: chartTextColor("secondary", 0.55), font: { size: 10 } } : undefined,
      },
      y: {
        border: { display: false },
        grid: { color: chartGridColor(0.6) },
        ticks: { color: chartTextColor("secondary", 0.5), font: { size: 9 } },
        title: yLabel ? { display: true, text: yLabel, color: chartTextColor("secondary", 0.55), font: { size: 10 } } : undefined,
      },
    },
  };
}

export function r2ChartData(data: HealthDiagnosticsData): ChartData<"line", number[], string> {
  const rows = data.section1.r2_series;
  const labels = rows.map((r) => r.date);
  return {
    labels,
    datasets: [
      {
        label: "Week-End R²",
        data: rows.map((r) => (Number(r.r2) || 0) * 100),
        borderColor: chartTextColor("secondary", 0.72),
        borderWidth: 1.3,
        pointRadius: 0,
        tension: 0.2,
      },
      {
        label: "12w Mean",
        data: rows.map((r) => (Number(r.roll60) || 0) * 100),
        borderColor: chartLongColor(0.9),
        borderWidth: 1.6,
        pointRadius: 0,
        tension: 0.24,
      },
      {
        label: "52w Mean",
        data: rows.map((r) => (Number(r.roll252) || 0) * 100),
        borderColor: chartShortColor(0.84),
        borderWidth: 1.6,
        pointRadius: 0,
        tension: 0.24,
      },
    ],
  };
}

export const commonLineOptions: ChartOptions<"line"> = {
  responsive: true,
  maintainAspectRatio: false,
  interaction: { mode: "index", intersect: false },
  plugins: {
    legend: {
      display: true,
        labels: {
          boxWidth: 9,
          boxHeight: 9,
          color: chartTextColor("secondary", 0.72),
          font: { size: 10 },
        },
      },
    tooltip: tooltipOptions(),
  },
  scales: {
    x: {
      border: { display: false },
      grid: { display: false },
      ticks: {
        color: chartTextColor("secondary", 0.5),
        autoSkip: true,
        maxTicksLimit: 8,
        callback: (_v, idx, ticks) => compactDateLabel(String(ticks[idx]?.label ?? "")),
        font: { size: 9 },
      },
    },
    y: {
      border: { display: false },
      grid: { color: chartGridColor(0.6) },
      ticks: { color: chartTextColor("secondary", 0.5), font: { size: 9 } },
    },
  },
};

export function seriesData(rows: SeriesPoint[], label: string, color: string, multiply = 1): ChartData<"line", number[], string> {
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

export function sortNumber<T>(rows: T[], getter: (r: T) => number, asc: boolean): T[] {
  return [...rows].sort((a, b) => (asc ? getter(a) - getter(b) : getter(b) - getter(a)));
}

export type CoverageSortKey =
  | "field"
  | "coverage_score_pct"
  | "row_coverage_pct"
  | "avg_ticker_lifecycle_coverage_pct"
  | "p10_ticker_lifecycle_coverage_pct"
  | "avg_date_coverage_pct";

export function sortCoverageRows(rows: HealthCoverageFieldRow[], key: CoverageSortKey, asc: boolean): HealthCoverageFieldRow[] {
  if (key === "field") {
    return [...rows].sort((a, b) => (asc ? a.field.localeCompare(b.field) : b.field.localeCompare(a.field)));
  }
  return sortNumber(rows, (r) => Number(r[key]) || 0, asc);
}

export function sortTStatRows(rows: HealthFactorPctRow[], asc: boolean): HealthFactorPctRow[] {
  return sortNumber(rows, (r) => Number(r.value) || 0, asc);
}

export function sortExposureRows(
  rows: HealthExposureStats[],
  key: keyof HealthExposureStats,
  asc: boolean,
  factorCatalog?: FactorCatalogEntry[],
): HealthExposureStats[] {
  if (key === "factor_id") {
    return [...rows].sort((a, b) =>
      asc
        ? factorDisplayName(a.factor_id, factorCatalog).localeCompare(factorDisplayName(b.factor_id, factorCatalog))
        : factorDisplayName(b.factor_id, factorCatalog).localeCompare(factorDisplayName(a.factor_id, factorCatalog)),
    );
  }
  return sortNumber(rows, (r) => Number(r[key]) || 0, asc);
}
