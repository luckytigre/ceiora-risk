"use client";

import {
  Chart as ChartJS,
  RadialLinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  type ChartData,
  type ChartOptions,
} from "chart.js";
import { Radar } from "react-chartjs-2";
import { shortFactorLabel, STYLE_FACTORS } from "@/lib/factorLabels";

ChartJS.register(RadialLinearScale, PointElement, LineElement, Filler, Tooltip);

/*
 * Axis ordering derived from empirical factor-return correlations
 * (~1,950 trading days, 2016–2026).  Adjacent factors on the radar
 * are the ones whose daily returns move together:
 *
 *   Value block (B/P ↔ ErnY r=+0.90, both vs Value r≈-0.95)
 *     → Growth (anti-corr w/ B/P & ErnY, r≈-0.40; corr w/ Value +0.39)
 *     → Profitability (near-zero corr w/ everything — neutral bridge)
 *     → Dividend Yield (mild anti-corr w/ ErnY -0.19)
 *   Size block (Size ↔ NLSize r=-0.34, Size ↔ Leverage r=-0.31)
 *     → Resid Vol (anti-corr w/ Size -0.31, corr w/ NLSize +0.25)
 *     → Liquidity (corr w/ NLSize -0.26, RVol +0.16)
 *   Momentum mini-block (Mom ↔ STRev r=+0.15, Mom ↔ Size +0.26)
 *     → Beta (anti-corr w/ STRev -0.13, Growth -0.13)
 *
 * Investment is excluded — all-NaN factor returns in the dataset.
 */
const RADAR_ORDER: string[] = [
  "Book-to-Price",
  "Earnings Yield",
  "Value",
  "Growth",
  "Profitability",
  "Dividend Yield",
  "Leverage",
  "Size",
  "Nonlinear Size",
  "Residual Volatility",
  "Liquidity",
  "Momentum",
  "Short-Term Reversal",
  "Beta",
];

interface FactorRadarChartProps {
  exposures: Record<string, number>;
}

export default function FactorRadarChart({ exposures }: FactorRadarChartProps) {
  // Use fixed ordering; only include factors present in the data
  const ordered = RADAR_ORDER.filter(
    (f) => STYLE_FACTORS.has(f) && f in exposures,
  );
  // Append any style factors not in the canonical list (future-proofing)
  for (const f of Object.keys(exposures)) {
    if (STYLE_FACTORS.has(f) && !ordered.includes(f)) ordered.push(f);
  }

  if (ordered.length === 0) return null;

  const labels = ordered.map((f) => shortFactorLabel(f));
  const values = ordered.map((f) => Number(exposures[f]) || 0);

  const data: ChartData<"radar"> = {
    labels,
    datasets: [
      {
        data: values,
        backgroundColor: "rgba(215, 87, 186, 0.18)",
        borderColor: "rgba(215, 87, 186, 0.7)",
        borderWidth: 1.5,
        pointBackgroundColor: "rgba(245, 186, 228, 0.9)",
        pointBorderColor: "rgba(215, 87, 186, 0.8)",
        pointRadius: 3,
        pointHoverRadius: 5,
        fill: true,
      },
    ],
  };

  const options: ChartOptions<"radar"> = {
    responsive: true,
    maintainAspectRatio: false,
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
        callbacks: {
          label: (ctx) => {
            const val = Number(ctx.parsed.r ?? 0);
            const sign = val >= 0 ? "+" : "";
            return ` ${sign}${val.toFixed(4)}`;
          },
        },
      },
    },
    scales: {
      r: {
        angleLines: {
          color: "rgba(154, 171, 214, 0.12)",
        },
        grid: {
          color: "rgba(154, 171, 214, 0.12)",
        },
        pointLabels: {
          color: "rgba(232, 237, 249, 0.6)",
          font: { size: 9 },
        },
        ticks: {
          display: false,
          stepSize: 0.5,
        },
      },
    },
  };

  return (
    <div style={{ height: 300 }}>
      <Radar data={data} options={options} />
    </div>
  );
}
