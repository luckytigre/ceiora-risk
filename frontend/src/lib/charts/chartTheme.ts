import { Chart as ChartJS, type ChartOptions } from "chart.js";

const PALETTE = {
  category: ["#f5bae4", "#d757ba", "#cc3558", "#a64f79", "#ff8f2a", "#31f6ff", "#e98dd3", "#bf4ea0", "#ffb26f", "#7af8ff"],
  sector: "#cc3558",
  factor: "#f5bae4",
  idio: "#ff8f2a",
  positive: "#31f6ff",
  negative: "#cc3558",
  neutral: "#e4a7d7",
};

function hexToRgba(hex: string, alpha: number): string {
  const h = (hex || "").replace("#", "");
  if (h.length !== 6) return hex;
  const r = parseInt(h.slice(0, 2), 16);
  const g = parseInt(h.slice(2, 4), 16);
  const b = parseInt(h.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

export function chartColor(role: keyof typeof PALETTE, alpha = 1): string {
  const c = PALETTE[role] || PALETTE.category[0];
  if (typeof c === "string") return alpha < 1 ? hexToRgba(c, alpha) : c;
  return c[0];
}

export function chartPalette(count: number, offset = 0, alpha = 1): string[] {
  return Array.from({ length: Math.max(0, count) }, (_, i) => {
    const c = PALETTE.category[(i + offset) % PALETTE.category.length];
    return alpha < 1 ? hexToRgba(c, alpha) : c;
  });
}

export function applyChartDefaults(): void {
  try {
    const d = ChartJS.defaults as any;
    d.color = "#a9b6d2";
    d.borderColor = "rgba(154, 171, 214, 0.24)";
    d.font = d.font || {};
    d.font.family = "-apple-system, BlinkMacSystemFont, 'SF Pro Text', 'Segoe UI', system-ui, sans-serif";
    d.font.size = 11;

    d.plugins = d.plugins || {};
    d.plugins.legend = d.plugins.legend || {};
    d.plugins.legend.labels = d.plugins.legend.labels || {};
    d.plugins.legend.labels.color = "#a9b6d2";

    d.plugins.tooltip = d.plugins.tooltip || {};
    d.plugins.tooltip.backgroundColor = "rgba(30, 34, 44, 0.91)";
    d.plugins.tooltip.borderColor = "rgba(196, 203, 220, 0.36)";
    d.plugins.tooltip.borderWidth = 0;
    d.plugins.tooltip.cornerRadius = 0;
    d.plugins.tooltip.titleColor = "#b4bfd3";
    d.plugins.tooltip.bodyColor = "#b4bfd3";

    d.scale = d.scale || {};
    d.scale.grid = d.scale.grid || {};
    d.scale.grid.color = "rgba(215, 87, 186, 0.2)";
  } catch {}
}

export function tooltipOptions(): NonNullable<ChartOptions<"bar">["plugins"]>["tooltip"] {
  return {
    backgroundColor: "rgba(30, 34, 44, 0.91)",
    borderColor: "rgba(196, 203, 220, 0.36)",
    borderWidth: 0,
    cornerRadius: 0,
    padding: 8,
    titleColor: "#b4bfd3",
    bodyColor: "#b4bfd3",
    displayColors: true,
    boxPadding: 4,
    bodyFont: { size: 12 },
    titleFont: { size: 12, weight: "bold" as const },
  };
}

export function horizontalBarOptions(tickFormatter?: (v: number) => string): ChartOptions<"bar"> {
  return {
    indexAxis: "y",
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", axis: "y", intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: tooltipOptions(),
    },
    scales: {
      x: {
        border: { display: false },
        grid: { display: false },
        ticks: {
          color: "#a9b6d2",
          callback: tickFormatter ? (value) => tickFormatter(Number(value)) : undefined,
          font: { size: 11 },
        },
      },
      y: {
        border: { display: false },
        grid: { display: false },
        ticks: { color: "#a9b6d2", font: { size: 11 } },
      },
    },
  };
}
