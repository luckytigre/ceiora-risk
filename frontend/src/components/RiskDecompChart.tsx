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
import type { RiskShares } from "@/lib/types/cuse4";

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend);

export interface RiskDecompRow {
  label: string;
  shares: RiskShares;
  showIdio?: boolean;
}

interface RiskDecompChartProps {
  rows: RiskDecompRow[];
}

export default function RiskDecompChart({ rows }: RiskDecompChartProps) {
  const labels = rows.map((row) => row.label);
  const normalizedRows = rows.map((row) => ({
    label: row.label,
    shares: row.shares,
    showIdio: row.showIdio ?? true,
  }));
  const datasets = [
    {
      label: "Market",
      data: normalizedRows.map((row) => row.shares.market || 0),
      backgroundColor: "#58b6c7",
      barThickness: 12,
      categoryPercentage: 0.52,
      barPercentage: 0.82,
    },
    {
      label: "Industry",
      data: normalizedRows.map((row) => row.shares.industry || 0),
      backgroundColor: "#cc3558",
      barThickness: 12,
      categoryPercentage: 0.52,
      barPercentage: 0.82,
    },
    {
      label: "Style",
      data: normalizedRows.map((row) => row.shares.style || 0),
      backgroundColor: "#f5bae4",
      barThickness: 12,
      categoryPercentage: 0.52,
      barPercentage: 0.82,
    },
    {
      label: "Idiosyncratic",
      data: normalizedRows.map((row) => (row.showIdio ? (row.shares.idio || 0) : 0)),
      backgroundColor: "#ff8f2a",
      barThickness: 12,
      categoryPercentage: 0.52,
      barPercentage: 0.82,
    },
  ];
  const data: ChartData<"bar", number[], string> = {
    labels,
    datasets,
  };

  const options: ChartOptions<"bar"> = {
    indexAxis: "y" as const,
    responsive: true,
    maintainAspectRatio: false,
    layout: {
      padding: { top: 8, bottom: 6 },
    },
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
          title: (items: TooltipItem<"bar">[]) => items[0]?.label ?? "",
          label: (ctx: TooltipItem<"bar">) => {
            const raw = Number(ctx.raw ?? 0);
            return `${ctx.dataset.label}: ${raw.toFixed(1)}%`;
          },
        },
        filter: (ctx) => {
          if (String(ctx.dataset.label || "") !== "Idiosyncratic") return true;
          return Boolean(normalizedRows[ctx.dataIndex]?.showIdio);
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
        border: { display: false },
        grid: { display: false },
        ticks: {
          color: "rgba(232, 237, 249, 0.6)",
          font: { size: 10, weight: 500 },
          padding: 10,
        },
      },
    },
  };

  const height = Math.max(132, normalizedRows.length * 56 + 28);

  return (
    <div style={{ height }}>
      <Bar data={data} options={options} />
    </div>
  );
}
