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

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend);

interface CparRiskShares {
  market: number;
  industry: number;
  style: number;
  idio: number;
}

export default function CparRiskDecompChart({
  shares,
  showIdio = true,
}: {
  shares: CparRiskShares;
  showIdio?: boolean;
}) {
  const labels = ["Risk Decomposition"];
  const datasets = [
    {
      label: "Market",
      data: [shares.market || 0],
      backgroundColor: "#58b6c7",
      barThickness: 18,
    },
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
  ];
  if (showIdio) {
    datasets.push({
      label: "Idiosyncratic",
      data: [shares.idio || 0],
      backgroundColor: "#ff8f2a",
      barThickness: 18,
    });
  }
  const data: ChartData<"bar", number[], string> = {
    labels,
    datasets,
  };

  const options: ChartOptions<"bar"> = {
    indexAxis: "y",
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: true,
        position: "bottom",
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
          callback: (value) => `${Number(value)}%`,
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
