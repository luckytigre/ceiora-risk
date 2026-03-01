"use client";

import { useRef, useEffect, useState, useMemo } from "react";
import type { CovMatrix } from "@/lib/types";
import { STYLE_FACTORS } from "@/lib/factorLabels";
import { shortFactorLabel } from "@/lib/factorLabels";

interface CovarianceHeatmapProps {
  data: CovMatrix;
}

/* Empirically-derived ordering that clusters correlated style factors adjacently.
   Based on ~1,950 days of daily factor return correlations from the Toraniko model. */
const STYLE_ORDER: string[] = [
  "Book-to-Price", "Earnings Yield", "Value",
  "Dividend Yield", "Profitability",
  "Growth", "Investment",
  "Leverage", "Size", "Nonlinear Size",
  "Liquidity", "Residual Volatility", "Beta",
  "Momentum", "Short-Term Reversal",
];

function corrColorStr(v: number): string {
  const clamped = Math.max(-1, Math.min(1, v));
  const t = Math.abs(clamped);
  const s = t * t;
  if (clamped >= 0) {
    const r = Math.round(14 + (59 - 14) * s);
    const g = Math.round(19 + (169 - 19) * s);
    const b = Math.round(33 + (225 - 33) * s);
    return `rgb(${r}, ${g}, ${b})`;
  } else {
    const r = Math.round(14 + (204 - 14) * s);
    const g = Math.round(19 + (53 - 19) * s);
    const b = Math.round(33 + (88 - 33) * s);
    return `rgb(${r}, ${g}, ${b})`;
  }
}

type Scope = "style" | "all";

function filterAndOrder(data: CovMatrix, scope: Scope): { factors: string[]; correlation: number[][] } {
  const { factors, correlation } = data;

  // Build index map
  const idxMap = new Map<string, number>();
  factors.forEach((f, i) => idxMap.set(f, i));

  // Determine ordered factor list
  let ordered: string[];
  if (scope === "style") {
    ordered = STYLE_ORDER.filter((f) => idxMap.has(f));
    // Add any style factors not in our predefined order
    for (const f of factors) {
      if (STYLE_FACTORS.has(f) && !ordered.includes(f)) ordered.push(f);
    }
  } else {
    // All: industry first (alphabetical), then style in cluster order
    const industry = factors.filter((f) => !STYLE_FACTORS.has(f)).sort();
    const style = STYLE_ORDER.filter((f) => idxMap.has(f));
    for (const f of factors) {
      if (STYLE_FACTORS.has(f) && !style.includes(f)) style.push(f);
    }
    ordered = [...industry, ...style];
  }

  // Build filtered correlation matrix
  const indices = ordered.map((f) => idxMap.get(f)!).filter((i) => i !== undefined);
  const filteredFactors = indices.map((i) => factors[i]);
  const filteredCorr = indices.map((i) => indices.map((j) => correlation[i]?.[j] ?? 0));

  return { factors: filteredFactors, correlation: filteredCorr };
}

export default function CovarianceHeatmap({ data }: CovarianceHeatmapProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [scope, setScope] = useState<Scope>("style");

  const filtered = useMemo(() => filterAndOrder(data, scope), [data, scope]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !filtered.factors.length) return;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const n = filtered.factors.length;
    const cellSize = Math.max(20, Math.min(36, Math.floor(560 / n)));
    const labelWidth = 90;
    const labelHeight = 72;
    const gridW = n * cellSize;
    const w = labelWidth + gridW;
    const h = labelHeight + n * cellSize;

    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    canvas.style.width = `${w}px`;
    canvas.style.height = `${h}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);

    const labels = filtered.factors.map(shortFactorLabel);

    // Draw cells with gap
    const gap = 1;
    for (let i = 0; i < n; i++) {
      for (let j = 0; j < n; j++) {
        const val = filtered.correlation[i]?.[j] ?? 0;
        const cx = labelWidth + j * cellSize;
        const cy = labelHeight + i * cellSize;

        // Cell background
        ctx.fillStyle = corrColorStr(val);
        ctx.fillRect(cx, cy, cellSize - gap, cellSize - gap);

        // Value text
        if (cellSize >= 26 && !(i === j)) {
          const absVal = Math.abs(val);
          ctx.fillStyle = absVal > 0.4 ? "rgba(232, 237, 249, 0.9)" : "rgba(169, 182, 210, 0.55)";
          ctx.font = `500 ${Math.max(8, cellSize * 0.28)}px -apple-system, BlinkMacSystemFont, sans-serif`;
          ctx.textAlign = "center";
          ctx.textBaseline = "middle";
          ctx.fillText(
            val.toFixed(2),
            cx + (cellSize - gap) / 2,
            cy + (cellSize - gap) / 2,
          );
        }

        // Diagonal: subtle marker
        if (i === j) {
          ctx.fillStyle = "rgba(232, 237, 249, 0.12)";
          ctx.fillRect(cx, cy, cellSize - gap, cellSize - gap);
        }
      }
    }

    // Row labels
    ctx.fillStyle = "rgba(232, 237, 249, 0.6)";
    ctx.font = "10px -apple-system, BlinkMacSystemFont, sans-serif";
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    for (let i = 0; i < n; i++) {
      ctx.fillText(labels[i], labelWidth - 6, labelHeight + i * cellSize + (cellSize - gap) / 2);
    }

    // Column labels (rotated)
    ctx.save();
    ctx.fillStyle = "rgba(232, 237, 249, 0.6)";
    ctx.font = "10px -apple-system, BlinkMacSystemFont, sans-serif";
    ctx.textAlign = "left";
    ctx.textBaseline = "middle";
    for (let j = 0; j < n; j++) {
      const x = labelWidth + j * cellSize + (cellSize - gap) / 2;
      const y = labelHeight - 6;
      ctx.save();
      ctx.translate(x, y);
      ctx.rotate(-Math.PI / 3);
      ctx.fillText(labels[j], 0, 0);
      ctx.restore();
    }
    ctx.restore();
  }, [filtered]);

  if (!data.factors?.length) {
    return <div style={{ color: "rgba(169, 182, 210, 0.6)", fontSize: 12 }}>No correlation data available</div>;
  }

  return (
    <div>
      <div className="heatmap-scope-toggle">
        <button className={scope === "style" ? "active" : ""} onClick={() => setScope("style")}>
          Style Factors
        </button>
        <button className={scope === "all" ? "active" : ""} onClick={() => setScope("all")}>
          All Factors
        </button>
      </div>
      <div style={{ overflowX: "auto" }}>
        <canvas ref={canvasRef} />
      </div>
      <div className="heatmap-legend">
        <span className="heatmap-legend-label">−1</span>
        <div className="heatmap-legend-bar" />
        <span className="heatmap-legend-label">0</span>
        <div className="heatmap-legend-bar pos" />
        <span className="heatmap-legend-label">+1</span>
      </div>
    </div>
  );
}
