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
import { useAppSettings } from "./AppSettingsContext";
import {
  alphaColor,
  chartGridColor,
  chartLongColor,
  chartShortColor,
  chartTextColor,
  tooltipOptions,
} from "@/lib/charts/chartTheme";
import type { FactorCatalogEntry, FactorExposure } from "@/lib/types/cuse4";
import { exposureTier as exposureMethodTier } from "@/lib/exposureOrigin";
import { factorDisplayName, shortFactorLabel, factorTier } from "@/lib/factorLabels";

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
  orderByFactors?: string[];
  factorCatalog?: FactorCatalogEntry[];
  presentationThreshold?: number;
  visibleFactorIds?: string[];
}

export function chartPresentationThreshold(mode: ExposureBarChartProps["mode"]): number {
  if (mode === "risk_contribution") return 0.05;
  if (mode === "sensitivity") return 0.001;
  return 0.03;
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
    ctx.setLineDash([1, 6]);
    ctx.strokeStyle = chartTextColor("secondary", 0.09);
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
    const netIndex = chart.data.datasets.findIndex((dataset) => String(dataset.label || "") === "Net");
    if (netIndex < 0) return;
    const meta = chart.getDatasetMeta(netIndex);
    if (!meta?.data?.length) return;
    const ctx = chart.ctx;
    const tickHalfLen = 7;
    ctx.save();
    ctx.lineCap = "round";
    ctx.strokeStyle = chartTextColor("primary", 0.76);
    ctx.lineWidth = 1.75;
    ctx.shadowBlur = 0;

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

export default function ExposureBarChart({
  factors,
  onBarClick,
  mode = "raw",
  orderByFactors,
  factorCatalog,
  presentationThreshold: presentationThresholdOverride,
  visibleFactorIds,
}: ExposureBarChartProps) {
  const { themeMode } = useAppSettings();
  const axisLabel = mode === "risk_contribution"
    ? "% of total risk"
    : mode === "sensitivity"
      ? "vol-scaled loading"
      : "factor loading";
  const presentationThreshold = Math.max(
    presentationThresholdOverride ?? chartPresentationThreshold(mode),
    1e-12,
  );
  const leftLabel = mode === "risk_contribution" ? "Hedging" : "Short";
  const rightLabel = mode === "risk_contribution" ? "Risk-adding" : "Long";
  const xTick = (n: number) => {
    if (mode === "risk_contribution") return `${n.toFixed(1)}%`;
    return n.toFixed(3);
  };

  const visibleFactorSet = visibleFactorIds ? new Set(visibleFactorIds) : null;
  // Visual orientation lock:
  // - long arm always right (+)
  // - short arm always left (-)
  // Decompose by contribution sign (not by position side), so the bars match
  // the true positive/negative contribution buckets seen in drilldowns.
  // Net marker always follows top-level signed value for correctness.
  const decomposed = factors.map((f) => {
    let corePosContrib = 0;
    let coreNegContrib = 0;
    let fundamentalPosContrib = 0;
    let fundamentalNegContrib = 0;
    let returnsPosContrib = 0;
    let returnsNegContrib = 0;
    for (const item of f.drilldown) {
      const contrib = Number(item.contribution || 0);
      const tier = exposureMethodTier(item.exposure_origin, item.model_status);
      if (contrib >= 0) {
        if (tier === "returns") returnsPosContrib += contrib;
        else if (tier === "fundamental") fundamentalPosContrib += contrib;
        else corePosContrib += contrib;
      } else {
        if (tier === "returns") returnsNegContrib += contrib;
        else if (tier === "fundamental") fundamentalNegContrib += contrib;
        else coreNegContrib += contrib;
      }
    }

    const value = Number(f.value || 0);
    const signedNet = (
      corePosContrib
      + coreNegContrib
      + fundamentalPosContrib
      + fundamentalNegContrib
      + returnsPosContrib
      + returnsNegContrib
    );
    const additive = f.drilldown.length > 0 && Math.abs(signedNet - value) <= 1e-4;

    const coreLongArm = additive ? corePosContrib : Math.max(value, 0);
    const coreShortArm = additive ? coreNegContrib : Math.min(value, 0);
    const fundamentalLongArm = additive ? fundamentalPosContrib : 0;
    const fundamentalShortArm = additive ? fundamentalNegContrib : 0;
    const returnsLongArm = additive ? returnsPosContrib : 0;
    const returnsShortArm = additive ? returnsNegContrib : 0;
    const net = value;
    return {
      ...f,
      coreLongArm,
      coreShortArm,
      fundamentalLongArm,
      fundamentalShortArm,
      returnsLongArm,
      returnsShortArm,
      net,
    };
  }).filter((f) => {
    if (visibleFactorSet) return visibleFactorSet.has(f.factor_id);
    return (
      Math.abs(f.coreLongArm) >= presentationThreshold
      || Math.abs(f.coreShortArm) >= presentationThreshold
      || Math.abs(f.fundamentalLongArm) >= presentationThreshold
      || Math.abs(f.fundamentalShortArm) >= presentationThreshold
      || Math.abs(f.returnsLongArm) >= presentationThreshold
      || Math.abs(f.returnsShortArm) >= presentationThreshold
      || Math.abs(f.net) >= presentationThreshold
    );
  });

  // Sort by Toraniko regression hierarchy: industry → style (non-orth → orth).
  // Within each tier, sort by absolute net magnitude descending.
  const baseSorted = [...decomposed].sort((a, b) => {
    const tierDiff = factorTier(a.factor_id, factorCatalog) - factorTier(b.factor_id, factorCatalog);
    if (tierDiff !== 0) return tierDiff;
    const byMagnitude = Math.abs(b.net) - Math.abs(a.net);
    if (byMagnitude !== 0) return byMagnitude;
    return factorDisplayName(a.factor_id, factorCatalog).localeCompare(
      factorDisplayName(b.factor_id, factorCatalog),
    );
  });
  const orderMap = orderByFactors ? new Map(orderByFactors.map((factorId, index) => [factorId, index])) : null;
  const baseOrderMap = new Map(baseSorted.map((row, index) => [row.factor_id, index]));
  const sorted = orderMap
    ? [...baseSorted].sort((a, b) => {
        const aIndex = orderMap.has(a.factor_id) ? Number(orderMap.get(a.factor_id)) : Number.POSITIVE_INFINITY;
        const bIndex = orderMap.has(b.factor_id) ? Number(orderMap.get(b.factor_id)) : Number.POSITIVE_INFINITY;
        if (aIndex !== bIndex) return aIndex - bIndex;
        return Number(baseOrderMap.get(a.factor_id) ?? 0) - Number(baseOrderMap.get(b.factor_id) ?? 0);
      })
    : baseSorted;
  // Find tier boundary indices (last index of each tier before the next tier starts)
  const tierBoundaries: number[] = [];
  for (let i = 0; i < sorted.length - 1; i++) {
    if (factorTier(sorted[i].factor_id, factorCatalog) !== factorTier(sorted[i + 1].factor_id, factorCatalog)) {
      tierBoundaries.push(i);
    }
  }

  const TIER_LABELS: Record<number, string> = { 1: "MARKET", 2: "INDUSTRY", 3: "STYLE" };

  const tierSeparatorPlugin: Plugin<"bar" | "line"> = {
    id: "tierSeparator",
    afterDraw(chart) {
      const referenceMeta = chart.getDatasetMeta(0);
      if (!referenceMeta?.data?.length) return;
      const ctx = chart.ctx;
      const rowCenters = referenceMeta.data
        .map((point) => point.y)
        .filter((value): value is number => Number.isFinite(value));
      if (rowCenters.length !== sorted.length) return;
      ctx.save();

      for (const boundaryIdx of tierBoundaries) {
        const y1 = rowCenters[boundaryIdx];
        const y2 = rowCenters[boundaryIdx + 1];
        const yMid = (y1 + y2) / 2;

        // Separator line
        ctx.beginPath();
        ctx.setLineDash([]);
        ctx.strokeStyle = chartGridColor(0.07);
        ctx.lineWidth = 1;
        ctx.moveTo(chart.chartArea.left, yMid);
        ctx.lineTo(chart.chartArea.right, yMid);
        ctx.stroke();

        // Tier label below the separator
        const nextTier = factorTier(sorted[boundaryIdx + 1].factor_id, factorCatalog);
        const tierLabel = TIER_LABELS[nextTier];
        if (tierLabel) {
          ctx.font = "600 9px -apple-system, BlinkMacSystemFont, sans-serif";
          ctx.fillStyle = chartTextColor("secondary", 0.3);
          ctx.textAlign = "right";
          ctx.textBaseline = "top";
          ctx.fillText(tierLabel, chart.chartArea.right - 1, yMid + 4);
        }
      }

      // Label for the first tier
      if (sorted.length > 0) {
        const firstTier = factorTier(sorted[0].factor_id, factorCatalog);
        const firstLabel = TIER_LABELS[firstTier];
        if (firstLabel) {
          const rowGap = rowCenters.length > 1
            ? rowCenters[1] - rowCenters[0]
            : 18;
          const firstLabelY = Math.max(chart.chartArea.top + 2, rowCenters[0] - rowGap / 2 + 2);
          ctx.font = "600 9px -apple-system, BlinkMacSystemFont, sans-serif";
          ctx.fillStyle = chartTextColor("secondary", 0.3);
          ctx.textAlign = "right";
          ctx.textBaseline = "top";
          ctx.fillText(firstLabel, chart.chartArea.right - 1, firstLabelY);
        }
      }

      ctx.restore();
    },
  };
  const labels = sorted.map((f) => shortFactorLabel(f.factor_id, factorCatalog));
  const coreLongValues = sorted.map((f) => f.coreLongArm);
  const fundamentalLongValues = sorted.map((f) => f.fundamentalLongArm);
  const returnsLongValues = sorted.map((f) => f.returnsLongArm);
  const coreShortValues = sorted.map((f) => f.coreShortArm);
  const fundamentalShortValues = sorted.map((f) => f.fundamentalShortArm);
  const returnsShortValues = sorted.map((f) => f.returnsShortArm);
  const netValues = sorted.map((f) => f.net);

  const data: ChartData<"bar" | "line", number[], string> = {
    labels,
    datasets: [
      {
        type: "bar",
        label: "Core Long",
        data: coreLongValues,
        backgroundColor: chartLongColor(0.96),
        hoverBackgroundColor: chartLongColor(),
        borderWidth: 0,
        borderRadius: 0,
        borderSkipped: false,
        inflateAmount: 0,
        stack: "exposure",
        barThickness: 8,
      },
      {
        type: "bar",
        label: "Fundamental Projection Long",
        data: fundamentalLongValues,
        backgroundColor: chartTextColor("secondary", 0.32),
        hoverBackgroundColor: chartTextColor("secondary", 0.44),
        borderWidth: 0,
        borderRadius: 0,
        borderSkipped: false,
        inflateAmount: 0,
        stack: "exposure",
        barThickness: 8,
      },
      {
        type: "bar",
        label: "Returns Projection Long",
        data: returnsLongValues,
        backgroundColor: chartTextColor("muted", 0.24),
        hoverBackgroundColor: chartTextColor("muted", 0.34),
        borderWidth: 0,
        borderRadius: 0,
        borderSkipped: false,
        inflateAmount: 0,
        stack: "exposure",
        barThickness: 8,
      },
      {
        type: "bar",
        label: "Core Short",
        data: coreShortValues,
        backgroundColor: chartShortColor(0.96),
        hoverBackgroundColor: chartShortColor(),
        borderWidth: 0,
        borderRadius: 0,
        borderSkipped: false,
        inflateAmount: 0,
        stack: "exposure",
        barThickness: 8,
      },
      {
        type: "bar",
        label: "Fundamental Projection Short",
        data: fundamentalShortValues,
        backgroundColor: chartTextColor("secondary", 0.32),
        hoverBackgroundColor: chartTextColor("secondary", 0.44),
        borderWidth: 0,
        borderRadius: 0,
        borderSkipped: false,
        inflateAmount: 0,
        stack: "exposure",
        barThickness: 8,
      },
      {
        type: "bar",
        label: "Returns Projection Short",
        data: returnsShortValues,
        backgroundColor: chartTextColor("muted", 0.24),
        hoverBackgroundColor: chartTextColor("muted", 0.34),
        borderWidth: 0,
        borderRadius: 0,
        borderSkipped: false,
        inflateAmount: 0,
        stack: "exposure",
        barThickness: 8,
      },
      {
        type: "line",
        label: "Net",
        data: netValues,
        showLine: false,
        pointRadius: 0,
        pointHoverRadius: 0,
        borderWidth: 0,
        pointBackgroundColor: "transparent",
        pointBorderColor: "transparent",
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
        ...tooltipOptions(),
        padding: { top: 6, bottom: 6, left: 10, right: 10 },
        titleFont: { size: 10, weight: "normal" as const },
        bodyFont: { size: 11, weight: 500 },
        displayColors: true,
        boxWidth: 8,
        boxHeight: 8,
        boxPadding: 4,
        filter: (ctx) => {
          if (String(ctx.dataset.label || "") === "Net") return true;
          return Math.abs(Number(ctx.parsed.x ?? 0)) > 1e-12;
        },
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
        stacked: true,
        border: { display: false },
        grid: { color: chartGridColor(0.12) },
        ticks: {
          color: chartTextColor("secondary", 0.32),
          callback: (value) => xTick(Number(value)),
          font: { size: 9 },
        },
      },
      y: {
        stacked: true,
        border: { display: false },
        grid: { display: false },
        ticks: {
          color: chartTextColor("primary", 0.6),
          font: { size: 10 },
        },
      },
    },
    onClick: (_: ChartEvent, elements: ActiveElement[]) => {
      if (elements.length > 0 && onBarClick) {
        const idx = elements[0].index;
        onBarClick(sorted[idx].factor_id);
      }
    },
  };

  const height = Math.max(400, sorted.length * 22);

  return (
    <div>
      <div style={{ height }}>
      <Chart
        key={`exposure-chart-${themeMode}`}
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
