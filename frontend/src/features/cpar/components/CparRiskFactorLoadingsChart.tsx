"use client";

import {
  ActiveElement,
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  ChartData,
  ChartEvent,
  ChartOptions,
  Legend,
  LinearScale,
  LineElement,
  Plugin,
  PointElement,
  Tooltip,
} from "chart.js";
import { Chart } from "react-chartjs-2";
import { formatCparNumber, formatCparPercent } from "@/lib/cparTruth";
import { shortFactorLabel } from "@/lib/factorLabels";
import type { CparFactorChartRow, CparRiskExposureMode } from "@/lib/types/cpar";

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Tooltip,
  Legend,
);

const GROUP_LABELS: Record<string, string> = {
  market: "MARKET",
  sector: "INDUSTRY",
  style: "STYLE",
};

const EPSILON = 1e-12;

const zeroLinePlugin: Plugin<"bar" | "line"> = {
  id: "cparRiskBarZeroLine",
  afterDraw(chart) {
    const xScale = chart.scales.x;
    if (!xScale) return;
    const xPixel = xScale.getPixelForValue(0);
    if (xPixel < chart.chartArea.left || xPixel > chart.chartArea.right) return;
    const ctx = chart.ctx;
    ctx.save();
    ctx.beginPath();
    ctx.setLineDash([3, 3]);
    ctx.strokeStyle = "rgba(169, 182, 210, 0.32)";
    ctx.lineWidth = 1;
    ctx.moveTo(xPixel, chart.chartArea.top);
    ctx.lineTo(xPixel, chart.chartArea.bottom);
    ctx.stroke();
    ctx.restore();
  },
};

const netMarkerPlugin: Plugin<"bar" | "line"> = {
  id: "cparRiskNetMarker",
  afterDatasetsDraw(chart) {
    const netIndex = chart.data.datasets.findIndex((dataset) => String(dataset.label || "") === "Net");
    if (netIndex < 0) return;
    const meta = chart.getDatasetMeta(netIndex);
    if (!meta?.data?.length) return;
    const ctx = chart.ctx;
    ctx.save();
    ctx.lineCap = "round";
    ctx.strokeStyle = "rgba(232, 237, 249, 0.88)";
    ctx.lineWidth = 2;
    ctx.shadowColor = "rgba(232, 237, 249, 0.25)";
    ctx.shadowBlur = 4;

    for (const point of meta.data) {
      const x = point.x;
      const y = point.y;
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
      ctx.beginPath();
      ctx.moveTo(x, y - 7);
      ctx.lineTo(x, y + 7);
      ctx.stroke();
    }
    ctx.restore();
  },
};

function buildTierBoundaries(rows: CparFactorChartRow[]): number[] {
  const boundaries: number[] = [];
  for (let index = 0; index < rows.length - 1; index += 1) {
    if (rows[index].group !== rows[index + 1].group) boundaries.push(index);
  }
  return boundaries;
}

function finiteNumber(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function sumNegativeDrilldown(
  row: CparFactorChartRow,
  selector: (item: CparFactorChartRow["drilldown"][number]) => number,
): number {
  return row.drilldown.reduce((sum, item) => sum + Math.min(selector(item), 0), 0);
}

function sumPositiveDrilldown(
  row: CparFactorChartRow,
  selector: (item: CparFactorChartRow["drilldown"][number]) => number,
): number {
  return row.drilldown.reduce((sum, item) => sum + Math.max(selector(item), 0), 0);
}

function hasNonZeroDrilldownValues(
  row: CparFactorChartRow,
  selector: (item: CparFactorChartRow["drilldown"][number]) => number,
): boolean {
  return row.drilldown.some((item) => Math.abs(selector(item)) > EPSILON);
}

function fallbackSensitivitySplit(row: CparFactorChartRow) {
  const factorVolatility = finiteNumber(row.factor_volatility);
  return {
    negative: finiteNumber(row.negative_contribution_beta) * factorVolatility,
    positive: finiteNumber(row.positive_contribution_beta) * factorVolatility,
  };
}

function fallbackRiskContributionSplit(row: CparFactorChartRow) {
  const aggregateBeta = finiteNumber(row.aggregate_beta);
  const covarianceAdjustment = finiteNumber(row.covariance_adjustment);
  const riskContributionPct = finiteNumber(row.risk_contribution_pct);
  const denominator = aggregateBeta * covarianceAdjustment;
  if (Math.abs(denominator) <= EPSILON || Math.abs(riskContributionPct) <= EPSILON) {
    return { negative: 0, positive: 0 };
  }
  const scale = riskContributionPct / denominator;
  return {
    negative: finiteNumber(row.negative_contribution_beta) * covarianceAdjustment * scale,
    positive: finiteNumber(row.positive_contribution_beta) * covarianceAdjustment * scale,
  };
}

export default function CparRiskFactorLoadingsChart({
  rows,
  mode,
  selectedFactorId,
  onSelectFactor,
}: {
  rows: CparFactorChartRow[];
  mode: CparRiskExposureMode;
  selectedFactorId: string | null;
  onSelectFactor: (factorId: string) => void;
}) {
  const axisLabel = mode === "risk_contribution"
    ? "% of total risk"
    : mode === "sensitivity"
      ? "vol-scaled loading"
      : "factor loading";
  const leftLabel = mode === "risk_contribution" ? "Hedging" : "Short";
  const rightLabel = mode === "risk_contribution" ? "Risk-adding" : "Long";
  const labels = rows.map((row) => shortFactorLabel(row.label));
  const negativeValues = rows.map((row) => {
    if (mode === "risk_contribution") {
      if (hasNonZeroDrilldownValues(row, (item) => finiteNumber(item.risk_contribution_pct))) {
        return sumNegativeDrilldown(row, (item) => finiteNumber(item.risk_contribution_pct));
      }
      return fallbackRiskContributionSplit(row).negative;
    }
    if (mode === "sensitivity") {
      if (hasNonZeroDrilldownValues(row, (item) => finiteNumber(item.vol_scaled_contribution))) {
        return sumNegativeDrilldown(row, (item) => finiteNumber(item.vol_scaled_contribution));
      }
      return fallbackSensitivitySplit(row).negative;
    }
    return finiteNumber(row.negative_contribution_beta);
  });
  const positiveValues = rows.map((row) => {
    if (mode === "risk_contribution") {
      if (hasNonZeroDrilldownValues(row, (item) => finiteNumber(item.risk_contribution_pct))) {
        return sumPositiveDrilldown(row, (item) => finiteNumber(item.risk_contribution_pct));
      }
      return fallbackRiskContributionSplit(row).positive;
    }
    if (mode === "sensitivity") {
      if (hasNonZeroDrilldownValues(row, (item) => finiteNumber(item.vol_scaled_contribution))) {
        return sumPositiveDrilldown(row, (item) => finiteNumber(item.vol_scaled_contribution));
      }
      return fallbackSensitivitySplit(row).positive;
    }
    return finiteNumber(row.positive_contribution_beta);
  });
  const netValues = rows.map((row) => (
    mode === "risk_contribution"
      ? finiteNumber(row.risk_contribution_pct)
      : mode === "sensitivity"
        ? finiteNumber(row.sensitivity_beta)
        : finiteNumber(row.aggregate_beta)
  ));
  const height = Math.max(320, rows.length * 24 + 36);
  const tierBoundaries = buildTierBoundaries(rows);
  const tierSeparatorPlugin: Plugin<"bar" | "line"> = {
    id: "cparRiskTierSeparator",
    afterDraw(chart) {
      const yScale = chart.scales.y;
      if (!yScale) return;
      const ctx = chart.ctx;
      ctx.save();

      for (const boundaryIndex of tierBoundaries) {
        const y1 = yScale.getPixelForValue(boundaryIndex);
        const y2 = yScale.getPixelForValue(boundaryIndex + 1);
        const yMid = (y1 + y2) / 2;
        ctx.beginPath();
        ctx.setLineDash([]);
        ctx.strokeStyle = "rgba(154, 171, 214, 0.16)";
        ctx.lineWidth = 1;
        ctx.moveTo(chart.chartArea.left, yMid);
        ctx.lineTo(chart.chartArea.right, yMid);
        ctx.stroke();

        const nextLabel = GROUP_LABELS[rows[boundaryIndex + 1]?.group || ""];
        if (nextLabel) {
          ctx.font = "600 9px -apple-system, BlinkMacSystemFont, sans-serif";
          ctx.fillStyle = "rgba(169, 182, 210, 0.7)";
          ctx.textAlign = "right";
          ctx.textBaseline = "top";
          ctx.fillText(nextLabel, chart.chartArea.right - 1, yMid + 4);
        }
      }

      if (rows.length > 0) {
        const firstLabel = GROUP_LABELS[rows[0].group];
        if (firstLabel) {
          ctx.font = "600 9px -apple-system, BlinkMacSystemFont, sans-serif";
          ctx.fillStyle = "rgba(169, 182, 210, 0.7)";
          ctx.textAlign = "right";
          ctx.textBaseline = "top";
          ctx.fillText(firstLabel, chart.chartArea.right - 1, chart.chartArea.top + 2);
        }
      }

      ctx.restore();
    },
  };

  const data: ChartData<"bar" | "line", number[], string> = {
    labels,
    datasets: [
      {
        type: "bar",
        label: "Short",
        data: negativeValues,
        backgroundColor: rows.map((row) => (
          row.factor_id === selectedFactorId
            ? "rgba(224, 87, 127, 1.0)"
            : "rgba(224, 87, 127, 0.96)"
        )),
        hoverBackgroundColor: "rgba(224, 87, 127, 1.0)",
        borderWidth: 0,
        borderRadius: 0,
        borderSkipped: false,
        inflateAmount: 0,
        stack: "exposure",
        barThickness: 10,
      },
      {
        type: "bar",
        label: "Long",
        data: positiveValues,
        backgroundColor: rows.map((row) => (
          row.factor_id === selectedFactorId
            ? "rgba(105, 207, 154, 1.0)"
            : "rgba(105, 207, 154, 0.96)"
        )),
        hoverBackgroundColor: "rgba(105, 207, 154, 1.0)",
        borderWidth: 0,
        borderRadius: 0,
        borderSkipped: false,
        inflateAmount: 0,
        stack: "exposure",
        barThickness: 10,
      },
      {
        type: "line",
        label: "Net",
        data: netValues,
        showLine: false,
        pointRadius: 0,
        pointHoverRadius: 0,
        borderWidth: 0,
        pointBackgroundColor: "rgba(0, 0, 0, 0)",
        pointBorderColor: "rgba(0, 0, 0, 0)",
      },
    ],
  };

  const options: ChartOptions<"bar" | "line"> = {
    indexAxis: "y",
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", axis: "y", intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: "rgba(20, 22, 30, 0.92)",
        borderColor: "rgba(154, 171, 214, 0.18)",
        borderWidth: 1,
        cornerRadius: 4,
        padding: { top: 6, bottom: 6, left: 10, right: 10 },
        titleColor: "rgba(232, 237, 249, 0.6)",
        bodyColor: "#e8edf9",
        titleFont: { size: 10, weight: "normal" as const },
        bodyFont: { size: 11, weight: 500 as const },
        displayColors: true,
        boxWidth: 8,
        boxHeight: 8,
        boxPadding: 4,
        filter: (ctx) => String(ctx.dataset.label || "") === "Net" || Math.abs(Number(ctx.parsed.x ?? 0)) > 1e-12,
        callbacks: {
          beforeBody: (items) => {
            const row = rows[items[0]?.dataIndex ?? 0];
            if (!row) return [];
            const lines = [
              `${row.group === "sector" ? "Industry" : row.group === "style" ? "Style" : "Market"} factor`,
            ];
            if (typeof row.variance_share === "number" && Number.isFinite(row.variance_share)) {
              lines.push(`${formatCparPercent(row.variance_share, 1)} pre var`);
            }
            return lines;
          },
          label: (ctx) => {
            const value = Number(ctx.parsed.x ?? 0);
            const sign = value >= 0 ? "+" : "";
            return ` ${ctx.dataset.label}: ${sign}${formatCparNumber(value, 4)}${mode === "risk_contribution" ? "%" : ""}`;
          },
        },
      },
    },
    scales: {
      x: {
        stacked: true,
        border: { display: false },
        grid: { color: "rgba(154, 171, 214, 0.16)" },
        ticks: {
          color: "rgba(169, 182, 210, 0.5)",
          font: { size: 9 },
          callback: (value) => mode === "risk_contribution"
            ? `${Number(value).toFixed(1)}%`
            : formatCparNumber(Number(value), 3),
        },
      },
      y: {
        stacked: true,
        border: { display: false },
        grid: { display: false },
        ticks: {
          color: "rgba(232, 237, 249, 0.6)",
          font: {
            size: 10,
            weight: 500,
          },
        },
      },
    },
    onClick: (_event: ChartEvent, elements: ActiveElement[]) => {
      if (!elements.length) return;
      const index = elements[0].index;
      const row = rows[index];
      if (row) onSelectFactor(row.factor_id);
    },
  };

  return (
    <div className="cpar-factor-chart-shell" data-testid="cpar-risk-factor-chart">
      <div className="cpar-risk-factor-chart-canvas" style={{ height }}>
        <Chart
          type="bar"
          data={data}
          options={options}
          plugins={[zeroLinePlugin, tierSeparatorPlugin, netMarkerPlugin]}
          aria-label="cPAR factor loadings chart"
        />
      </div>

      <div className="exposure-axis-row">
        <span className="exposure-axis-hint left">← {leftLabel}</span>
        <span className="exposure-axis-label">{axisLabel}</span>
        <span className="exposure-axis-hint right">{rightLabel} →</span>
      </div>
    </div>
  );
}
