"use client";

import { useRef, useEffect, useState, useMemo, useCallback } from "react";
import type { CovMatrix, FactorCatalogEntry } from "@/lib/types";
import { factorDisplayName, factorFamily, shortFactorLabel } from "@/lib/factorLabels";

interface CovarianceHeatmapProps {
  data: CovMatrix;
  factorCatalog?: FactorCatalogEntry[];
}

const STYLE_ORDER: string[] = [
  "Book-to-Price", "Earnings Yield",
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

function filterStyleFactors(
  data: CovMatrix,
  factorCatalog?: FactorCatalogEntry[],
): { factors: string[]; correlation: number[][] } {
  const factors = data.factors ?? [];
  const correlation = data.correlation ?? data.matrix ?? [];
  const idxMap = new Map<string, number>();
  factors.forEach((f, i) => idxMap.set(f, i));

  const styleFactors = factors.filter((factor) => factorFamily(factor, factorCatalog) === "style");
  const ordered = [...styleFactors].sort((a, b) => {
    const aName = factorDisplayName(a, factorCatalog);
    const bName = factorDisplayName(b, factorCatalog);
    const aIdx = STYLE_ORDER.indexOf(aName);
    const bIdx = STYLE_ORDER.indexOf(bName);
    if (aIdx >= 0 && bIdx >= 0) return aIdx - bIdx;
    if (aIdx >= 0) return -1;
    if (bIdx >= 0) return 1;
    return aName.localeCompare(bName);
  });

  const indices = ordered.map((f) => idxMap.get(f)!).filter((i) => i !== undefined);
  const filteredFactors = indices.map((i) => factors[i]);
  const filteredCorr = indices.map((i) => indices.map((j) => correlation[i]?.[j] ?? 0));
  return { factors: filteredFactors, correlation: filteredCorr };
}

interface Geometry {
  n: number;
  cellSize: number;
  gap: number;
  d: number;
  step: number;
  cx0: number;
  cy0: number;
  w: number;
  h: number;
  labelPad: number;
  topPad: number;
  labels: string[];
}

function computeGeometry(n: number, labels: string[], containerWidth: number): Geometry {
  const labelPad = Math.max(70, Math.min(100, Math.round(containerWidth * 0.08)));
  const topPad = 10;
  const gap = 1;

  // cellSize derived from container:
  // w = 2*labelPad + 2*(n-1)*step + 2*d + 4, step = cellSize*√2, d ≈ cellSize*√2
  // → cellSize ≈ (w - 2*labelPad - 4) / (2*√2*n)
  const available = containerWidth - 2 * labelPad - 4;
  const cellSize = Math.max(16, Math.floor(available / (2 * Math.SQRT1_2 * n)));

  const d = (cellSize - gap) * Math.SQRT1_2;
  const step = cellSize * Math.SQRT1_2;
  const halfSpanX = (n - 1) * step;
  const depthY = (n - 1) * step;
  const cx0 = labelPad + halfSpanX + d;
  const cy0 = topPad + d;
  const w = labelPad * 2 + halfSpanX * 2 + d * 2 + 4;
  const h = topPad + depthY + d * 2 + 4;
  return { n, cellSize, gap, d, step, cx0, cy0, w, h, labelPad, topPad, labels };
}

function drawHeatmap(
  ctx: CanvasRenderingContext2D,
  geo: Geometry,
  correlation: number[][],
  hover: { i: number; j: number } | null,
) {
  const { n, d, step, cx0, cy0, w, h, cellSize, labels } = geo;

  ctx.clearRect(0, 0, w, h);

  // Draw diamond cells (upper triangle, skip diagonal)
  for (let i = 0; i < n; i++) {
    for (let j = i + 1; j < n; j++) {
      const val = correlation[i]?.[j] ?? 0;
      const rx = cx0 + (i + j - (n - 1)) * step;
      const ry = cy0 + (j - i) * step;

      ctx.fillStyle = corrColorStr(val);
      ctx.beginPath();
      ctx.moveTo(rx, ry - d);
      ctx.lineTo(rx + d, ry);
      ctx.lineTo(rx, ry + d);
      ctx.lineTo(rx - d, ry);
      ctx.closePath();
      ctx.fill();

      // Hover highlight outline
      if (hover && hover.i === i && hover.j === j) {
        ctx.strokeStyle = "rgba(232, 237, 249, 0.8)";
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.moveTo(rx, ry - d);
        ctx.lineTo(rx + d, ry);
        ctx.lineTo(rx, ry + d);
        ctx.lineTo(rx - d, ry);
        ctx.closePath();
        ctx.stroke();
      }

      // Correlation text
      if (cellSize >= 22) {
        const absVal = Math.abs(val);
        ctx.fillStyle = absVal > 0.4 ? "rgba(232, 237, 249, 0.88)" : "rgba(169, 182, 210, 0.5)";
        ctx.font = `500 ${Math.max(7, cellSize * 0.26)}px -apple-system, BlinkMacSystemFont, sans-serif`;
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(val.toFixed(2), rx, ry);
      }
    }
  }

  // Label font size scales with cell size
  const labelFontSize = Math.max(9, Math.min(12, Math.round(cellSize * 0.42)));

  // Left-edge labels
  const leftHighlight = hover !== null ? hover.j : -1;
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let j = 0; j < n; j++) {
    const rx = cx0 + (0 + j - (n - 1)) * step;
    const ry = cy0 + j * step;
    const active = j === leftHighlight;
    ctx.fillStyle = active ? "rgba(232, 237, 249, 1)" : "rgba(232, 237, 249, 0.45)";
    ctx.font = active
      ? `600 ${labelFontSize}px -apple-system, BlinkMacSystemFont, sans-serif`
      : `${labelFontSize}px -apple-system, BlinkMacSystemFont, sans-serif`;
    ctx.fillText(labels[j], rx - d - 5, ry);
  }

  // Right-edge labels
  const rightHighlight = hover !== null ? hover.i : -1;
  ctx.textAlign = "left";
  for (let i = 0; i < n; i++) {
    const rx = cx0 + i * step;
    const ry = cy0 + ((n - 1) - i) * step;
    const active = i === rightHighlight;
    ctx.fillStyle = active ? "rgba(232, 237, 249, 1)" : "rgba(232, 237, 249, 0.45)";
    ctx.font = active
      ? `600 ${labelFontSize}px -apple-system, BlinkMacSystemFont, sans-serif`
      : `${labelFontSize}px -apple-system, BlinkMacSystemFont, sans-serif`;
    ctx.fillText(labels[i], rx + d + 5, ry);
  }
}

export default function CovarianceHeatmap({ data, factorCatalog }: CovarianceHeatmapProps) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const geoRef = useRef<Geometry | null>(null);
  const [hover, setHover] = useState<{ i: number; j: number } | null>(null);
  const [containerW, setContainerW] = useState(0);

  const filtered = useMemo(() => filterStyleFactors(data, factorCatalog), [data, factorCatalog]);

  // Track container width
  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width ?? 0;
      if (w > 0) setContainerW(w);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Hit-test: find which diamond cell the mouse is over (skip diagonal)
  const hitTest = useCallback((mouseX: number, mouseY: number): { i: number; j: number } | null => {
    const geo = geoRef.current;
    if (!geo) return null;
    const { n, step, cx0, cy0, d } = geo;

    for (let i = 0; i < n; i++) {
      for (let j = i + 1; j < n; j++) {
        const rx = cx0 + (i + j - (n - 1)) * step;
        const ry = cy0 + (j - i) * step;
        const dx = Math.abs(mouseX - rx);
        const dy = Math.abs(mouseY - ry);
        if (dx / d + dy / d <= 1) {
          return { i, j };
        }
      }
    }
    return null;
  }, []);

  // Setup canvas and draw
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !filtered.factors.length || !containerW) return;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const n = filtered.factors.length;
    const labels = filtered.factors.map((factor) => shortFactorLabel(factor, factorCatalog));
    const geo = computeGeometry(n, labels, containerW);
    geoRef.current = geo;

    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    canvas.width = Math.ceil(geo.w * dpr);
    canvas.height = Math.ceil(geo.h * dpr);
    canvas.style.width = `${Math.ceil(geo.w)}px`;
    canvas.style.height = `${Math.ceil(geo.h)}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    drawHeatmap(ctx, geo, filtered.correlation, null);
  }, [filtered, containerW, factorCatalog]);

  // Redraw on hover change
  useEffect(() => {
    const canvas = canvasRef.current;
    const geo = geoRef.current;
    if (!canvas || !geo) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    drawHeatmap(ctx, geo, filtered.correlation, hover);
  }, [hover, filtered]);

  const onMouseMove = useCallback((e: React.MouseEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    const scaleX = canvas.width / dpr / rect.width;
    const scaleY = canvas.height / dpr / rect.height;
    const x = (e.clientX - rect.left) * scaleX;
    const y = (e.clientY - rect.top) * scaleY;
    const hit = hitTest(x, y);
    setHover((prev) => {
      if (prev?.i === hit?.i && prev?.j === hit?.j) return prev;
      return hit;
    });
  }, [hitTest]);

  const onMouseLeave = useCallback(() => setHover(null), []);

  if (!data.factors?.length) {
    return <div style={{ color: "rgba(169, 182, 210, 0.6)", fontSize: 12 }}>No correlation data available</div>;
  }

  return (
    <div ref={wrapRef} style={{ margin: "0 -13px" }}>
      <div style={{ display: "flex", justifyContent: "center" }}>
        <canvas
          ref={canvasRef}
          onMouseMove={onMouseMove}
          onMouseLeave={onMouseLeave}
          style={{ cursor: hover ? "crosshair" : "default" }}
        />
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
