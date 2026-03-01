"use client";

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Tooltip,
  Legend,
  type ChartData,
  type ChartOptions,
  type TooltipItem,
} from "chart.js";
import { Bar } from "react-chartjs-2";
import { tooltipOptions } from "@/lib/charts/chartTheme";
import type { RiskShares } from "@/lib/types";

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend);

interface RiskDecompChartProps {
  shares: RiskShares;
}

export default function RiskDecompChart({ shares }: RiskDecompChartProps) {
  const labels = ["Risk Decomposition"];
  const data: ChartData<"bar", number[], string> = {
    labels,
    datasets: [
      {
        label: "Industry",
        data: [shares.industry || 0],
        backgroundColor: "#cc3558",
        barThickness: 18,
      },
      {
        label: "Style",
        data: [shares.style || 0],
        backgroundColor: "#f5bae4",
        barThickness: 18,
      },
      {
        label: "Idiosyncratic",
        data: [shares.idio || 0],
        backgroundColor: "#ff8f2a",
        barThickness: 18,
      },
    ],
  };

  const options: ChartOptions<"bar"> = {
    indexAxis: "y" as const,
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: true,
        position: "bottom" as const,
        labels: {
          color: "#a9b6d2",
          boxWidth: 10,
          padding: 16,
          font: { size: 11 },
        },
      },
      tooltip: {
        ...tooltipOptions(),
        callbacks: {
          label: (ctx: TooltipItem<"bar">) => {
            const raw = Number(ctx.raw ?? 0);
            return `${ctx.dataset.label}: ${raw.toFixed(1)}%`;
          },
        },
      },
    },
    scales: {
      x: {
        stacked: true,
        border: { display: false },
        grid: { display: false },
        ticks: {
          color: "#a9b6d2",
          callback: (v) => `${Number(v)}%`,
          font: { size: 11 },
        },
        max: 100,
      },
      y: {
        stacked: true,
        display: false,
      },
    },
  };

  return (
    <div style={{ height: 68 }}>
      <Bar data={data} options={options} />
    </div>
  );
}
