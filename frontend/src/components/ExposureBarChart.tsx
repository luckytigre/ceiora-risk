"use client";

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Tooltip,
  Legend,
  type ActiveElement,
  type ChartEvent,
  type ChartData,
  type ChartOptions,
  type Plugin,
} from "chart.js";
import { Chart } from "react-chartjs-2";
import type { FactorExposure } from "@/lib/types";
import { shortFactorLabel, factorTier } from "@/lib/factorLabels";

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Tooltip,
  Legend,
);

interface ExposureBarChartProps {
  factors: FactorExposure[];
  onBarClick?: (factor: string) => void;
  mode?: "raw" | "sensitivity" | "risk_contribution";
}

const zeroLinePlugin: Plugin<"bar" | "line"> = {
  id: "barZeroLine",
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
  id: "netMarkerPlugin",
  afterDatasetsDraw(chart) {
    const meta = chart.getDatasetMeta(2);
    if (!meta?.data?.length) return;
    const ctx = chart.ctx;
    const tickHalfLen = 7;
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
      ctx.moveTo(x, y - tickHalfLen);
      ctx.lineTo(x, y + tickHalfLen);
      ctx.stroke();
    }
    ctx.restore();
  },
};

export default function ExposureBarChart({ factors, onBarClick, mode = "raw" }: ExposureBarChartProps) {
  const axisLabel = mode === "risk_contribution"
    ? "% of total risk"
    : mode === "sensitivity"
      ? "vol-scaled loading"
      : "factor loading";
  const leftLabel = mode === "risk_contribution" ? "Hedging" : "Short";
  const rightLabel = mode === "risk_contribution" ? "Risk-adding" : "Long";
  const xTick = (n: number) => {
    if (mode === "risk_contribution") return `${n.toFixed(1)}%`;
    return n.toFixed(3);
  };

  // Visual orientation lock:
  // - long arm always right (+)
  // - short arm always left (-)
  // Decompose by contribution sign (not by position side), so the bars match
  // the true positive/negative contribution buckets seen in drilldowns.
  // Net marker always follows top-level signed value for correctness.
  const decomposed = factors.map((f) => {
    let posContrib = 0;
    let negContrib = 0;
    for (const item of f.drilldown) {
      const contrib = Number(item.contribution || 0);
      if (contrib >= 0) posContrib += contrib;
      else negContrib += contrib;
    }

    const value = Number(f.value || 0);
    const signedNet = posContrib + negContrib;
    const additive = f.drilldown.length > 0 && Math.abs(signedNet - value) <= 1e-4;

    const longArm = additive ? posContrib : Math.max(value, 0);
    const shortArm = additive ? negContrib : Math.min(value, 0);
    const net = value;
    return { ...f, longArm, shortArm, net };
  });

  // Sort by Toraniko regression hierarchy: industry → style (non-orth → orth).
  // Within each tier, sort by absolute net magnitude descending.
  const sorted = [...decomposed].sort((a, b) => {
    const tierDiff = factorTier(a.factor) - factorTier(b.factor);
    if (tierDiff !== 0) return tierDiff;
    const byMagnitude = Math.abs(b.net) - Math.abs(a.net);
    if (byMagnitude !== 0) return byMagnitude;
    return a.factor.localeCompare(b.factor);
  });
  // Find tier boundary indices (last index of each tier before the next tier starts)
  const tierBoundaries: number[] = [];
  for (let i = 0; i < sorted.length - 1; i++) {
    if (factorTier(sorted[i].factor) !== factorTier(sorted[i + 1].factor)) {
      tierBoundaries.push(i);
    }
  }

  const TIER_LABELS: Record<number, string> = { 1: "COUNTRY", 2: "INDUSTRY", 3: "STYLE" };

  const tierSeparatorPlugin: Plugin<"bar" | "line"> = {
    id: "tierSeparator",
    afterDraw(chart) {
      const yScale = chart.scales.y;
      if (!yScale) return;
      const ctx = chart.ctx;
      ctx.save();

      for (const boundaryIdx of tierBoundaries) {
        const y1 = yScale.getPixelForValue(boundaryIdx);
        const y2 = yScale.getPixelForValue(boundaryIdx + 1);
        const yMid = (y1 + y2) / 2;

        // Separator line
        ctx.beginPath();
        ctx.setLineDash([]);
        ctx.strokeStyle = "rgba(154, 171, 214, 0.16)";
        ctx.lineWidth = 1;
        ctx.moveTo(chart.chartArea.left, yMid);
        ctx.lineTo(chart.chartArea.right, yMid);
        ctx.stroke();

        // Tier label below the separator
        const nextTier = factorTier(sorted[boundaryIdx + 1].factor);
        const tierLabel = TIER_LABELS[nextTier];
        if (tierLabel) {
          ctx.font = "600 9px -apple-system, BlinkMacSystemFont, sans-serif";
          ctx.fillStyle = "rgba(169, 182, 210, 0.7)";
          ctx.textAlign = "right";
          ctx.textBaseline = "top";
          ctx.fillText(tierLabel, chart.chartArea.right - 1, yMid + 4);
        }
      }

      // Label for the first tier
      if (sorted.length > 0) {
        const firstTier = factorTier(sorted[0].factor);
        const firstLabel = TIER_LABELS[firstTier];
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
  const labels = sorted.map((f) => shortFactorLabel(f.factor));
  const longValues = sorted.map((f) => f.longArm);
  const shortValues = sorted.map((f) => f.shortArm);
  const netValues = sorted.map((f) => f.net);

  const data: ChartData<"bar" | "line", number[], string> = {
    labels,
    datasets: [
      {
        type: "bar",
        label: "Long Arm",
        data: longValues,
        backgroundColor: "rgba(107, 207, 154, 0.72)",
        hoverBackgroundColor: "rgba(107, 207, 154, 0.92)",
        borderWidth: 0,
        borderRadius: 3,
        borderSkipped: false,
        grouped: false,
        barThickness: 10,
      },
      {
        type: "bar",
        label: "Short Arm",
        data: shortValues,
        backgroundColor: "rgba(224, 87, 127, 0.72)",
        hoverBackgroundColor: "rgba(224, 87, 127, 0.92)",
        borderWidth: 0,
        borderRadius: 3,
        borderSkipped: false,
        grouped: false,
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
        bodyFont: { size: 11, weight: 500 },
        displayColors: true,
        boxWidth: 8,
        boxHeight: 8,
        boxPadding: 4,
        callbacks: {
          label: (ctx) => {
            const val = Number(ctx.parsed.x ?? 0);
            const sign = val >= 0 ? "+" : "";
            const suffix = mode === "risk_contribution" ? "%" : "";
            return ` ${ctx.dataset.label}: ${sign}${val.toFixed(4)}${suffix}`;
          },
        },
      },
    },
    scales: {
      x: {
        border: { display: false },
        grid: { color: "rgba(154, 171, 214, 0.16)" },
        ticks: {
          color: "rgba(169, 182, 210, 0.5)",
          callback: (value) => xTick(Number(value)),
          font: { size: 9 },
        },
      },
      y: {
        border: { display: false },
        grid: { display: false },
        ticks: {
          color: "rgba(232, 237, 249, 0.6)",
          font: { size: 10 },
        },
      },
    },
    onClick: (_: ChartEvent, elements: ActiveElement[]) => {
      if (elements.length > 0 && onBarClick) {
        const idx = elements[0].index;
        onBarClick(sorted[idx].factor);
      }
    },
  };

  const height = Math.max(400, sorted.length * 22);

  return (
    <div>
      <div style={{ height }}>
        <Chart
          type="bar"
          data={data}
          options={options}
          plugins={[zeroLinePlugin, tierSeparatorPlugin, netMarkerPlugin]}
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
