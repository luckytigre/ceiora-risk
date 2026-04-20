import { Chart as ChartJS, type ChartOptions, type ChartType, type TooltipOptions as ChartTooltipOptions } from "chart.js";

const FALLBACK_PALETTE = {
  category: ["#cf70ad", "#63add8", "#dda15a", "#7fc1a1", "#c4cd72", "#b789d6"],
  market: "#5a9ecb",
  sector: "#e5a15f",
  factor: "#c063a4",
  idio: "#c4cd72",
  positive: "#79bc9c",
  negative: "#d27186",
  neutral: "#b6bcc8",
};

function readCssVar(name: string, fallback: string): string {
  if (typeof window === "undefined") return fallback;
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}

function colorStringToRgba(color: string, alpha: number): string {
  const trimmed = color.trim();
  if (trimmed.startsWith("#")) return hexToRgba(trimmed, alpha);
  const rgbaMatch = trimmed.match(/^rgba?\(([^)]+)\)$/i);
  if (!rgbaMatch) return color;
  const [r = "0", g = "0", b = "0"] = rgbaMatch[1].split(",").map((part) => part.trim());
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function currentPalette() {
  return {
    category: FALLBACK_PALETTE.category,
    market: readCssVar("--analytics-market", FALLBACK_PALETTE.market),
    sector: readCssVar("--analytics-industry", FALLBACK_PALETTE.sector),
    factor: readCssVar("--analytics-style", FALLBACK_PALETTE.factor),
    idio: readCssVar("--analytics-idio", FALLBACK_PALETTE.idio),
    positive: readCssVar("--positive", FALLBACK_PALETTE.positive),
    negative: readCssVar("--negative", FALLBACK_PALETTE.negative),
    neutral: readCssVar("--signal-neutral", FALLBACK_PALETTE.neutral),
  } as const;
}

function hexToRgba(hex: string, alpha: number): string {
  const h = (hex || "").replace("#", "");
  if (h.length !== 6) return hex;
  const r = parseInt(h.slice(0, 2), 16);
  const g = parseInt(h.slice(2, 4), 16);
  const b = parseInt(h.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

export function chartVar(name: string, fallback: string): string {
  return readCssVar(name, fallback);
}

export function alphaColor(color: string, alpha: number): string {
  return colorStringToRgba(color, alpha);
}

export function chartColor(role: keyof typeof FALLBACK_PALETTE, alpha = 1): string {
  const palette = currentPalette();
  const c = palette[role] || palette.category[0];
  if (typeof c === "string") return alpha < 1 ? colorStringToRgba(c, alpha) : c;
  return c[0];
}

export function chartPalette(count: number, offset = 0, alpha = 1): string[] {
  const palette = currentPalette();
  return Array.from({ length: Math.max(0, count) }, (_, i) => {
    const index = (i + offset) % palette.category.length;
    const fallback = palette.category[index];
    const themed = readCssVar(`--chart-${index + 1}`, fallback);
    return alpha < 1 ? colorStringToRgba(themed, alpha) : themed;
  });
}

export function chartTextColor(role: "primary" | "secondary" | "muted" = "secondary", alpha = 1): string {
  const fallback = role === "primary" ? "#e8edf9" : role === "muted" ? "#7f8cab" : "#a9b6d2";
  const color = readCssVar(`--text-${role}`, fallback);
  return alpha < 1 ? colorStringToRgba(color, alpha) : color;
}

export function chartGridColor(alpha = 1): string {
  const color = readCssVar("--chart-grid", "rgba(154, 171, 214, 0.18)");
  return alpha < 1 ? colorStringToRgba(color, alpha) : color;
}

export function chartBorderColor(alpha = 1): string {
  const color = readCssVar("--border", "rgba(154, 171, 214, 0.24)");
  return alpha < 1 ? colorStringToRgba(color, alpha) : color;
}

export function chartLongColor(alpha = 1): string {
  const color = readCssVar("--exposure-long-color", FALLBACK_PALETTE.positive);
  return alpha < 1 ? colorStringToRgba(color, alpha) : color;
}

export function chartShortColor(alpha = 1): string {
  const color = readCssVar("--exposure-short-color", FALLBACK_PALETTE.negative);
  return alpha < 1 ? colorStringToRgba(color, alpha) : color;
}

export function applyChartDefaults(): void {
  try {
    const d = ChartJS.defaults as any;
    d.color = readCssVar("--text-secondary", "#a9b6d2");
    d.borderColor = readCssVar("--border", "rgba(154, 171, 214, 0.24)");
    d.font = d.font || {};
    d.font.family = "-apple-system, BlinkMacSystemFont, 'SF Pro Text', 'Segoe UI', system-ui, sans-serif";
    d.font.size = 11;

    d.plugins = d.plugins || {};
    d.plugins.legend = d.plugins.legend || {};
    d.plugins.legend.labels = d.plugins.legend.labels || {};
    d.plugins.legend.labels.color = readCssVar("--text-secondary", "#a9b6d2");

    d.plugins.tooltip = d.plugins.tooltip || {};
    d.plugins.tooltip.backgroundColor = readCssVar("--chart-tooltip-bg", "rgba(30, 34, 44, 0.91)");
    d.plugins.tooltip.borderColor = readCssVar("--chart-tooltip-border", "rgba(154, 171, 214, 0.24)");
    d.plugins.tooltip.borderWidth = 0;
    d.plugins.tooltip.cornerRadius = 0;
    d.plugins.tooltip.titleColor = readCssVar("--chart-tooltip-text", "#a9b6d2");
    d.plugins.tooltip.bodyColor = readCssVar("--chart-tooltip-text", "#a9b6d2");

    d.scale = d.scale || {};
    d.scale.grid = d.scale.grid || {};
    d.scale.grid.color = readCssVar("--chart-grid", "rgba(154, 171, 214, 0.18)");
  } catch {}
}

export function tooltipOptions<TType extends ChartType = "bar">(): ChartTooltipOptions<TType> {
  return {
    backgroundColor: readCssVar("--chart-tooltip-bg", "rgba(30, 34, 44, 0.91)"),
    borderColor: readCssVar("--chart-tooltip-border", "rgba(154, 171, 214, 0.24)"),
    borderWidth: 0,
    cornerRadius: 0,
    padding: 8,
    titleColor: readCssVar("--chart-tooltip-text", "#a9b6d2"),
    bodyColor: readCssVar("--chart-tooltip-text", "#a9b6d2"),
    displayColors: true,
    boxPadding: 4,
    bodyFont: { size: 12 },
    titleFont: { size: 12, weight: "bold" as const },
  } as ChartTooltipOptions<TType>;
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
          color: readCssVar("--text-secondary", "#a9b6d2"),
          callback: tickFormatter ? (value) => tickFormatter(Number(value)) : undefined,
          font: { size: 11 },
        },
      },
      y: {
        border: { display: false },
        grid: { display: false },
        ticks: { color: readCssVar("--text-secondary", "#a9b6d2"), font: { size: 11 } },
      },
    },
  };
}
