"use client";

import { useRef, useEffect } from "react";
import type { CovMatrix } from "@/lib/types";

interface CovarianceHeatmapProps {
  data: CovMatrix;
}

function corrColor(v: number): string {
  // -1 → red, 0 → dark, +1 → blue
  const clamped = Math.max(-1, Math.min(1, v));
  if (clamped >= 0) {
    const t = clamped;
    const r = Math.round(14 + (59 - 14) * (1 - t));
    const g = Math.round(19 + (169 - 19) * t * 0.5);
    const b = Math.round(33 + (225 - 33) * t);
    return `rgb(${r}, ${g}, ${b})`;
  } else {
    const t = -clamped;
    const r = Math.round(14 + (204 - 14) * t);
    const g = Math.round(19 + (53 - 19) * t);
    const b = Math.round(33 + (88 - 33) * t);
    return `rgb(${r}, ${g}, ${b})`;
  }
}

export default function CovarianceHeatmap({ data }: CovarianceHeatmapProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !data.factors?.length) return;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const n = data.factors.length;
    const cellSize = Math.max(16, Math.min(32, Math.floor(600 / n)));
    const labelWidth = 120;
    const labelHeight = 80;
    const w = labelWidth + n * cellSize;
    const h = labelHeight + n * cellSize;

    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    canvas.style.width = `${w}px`;
    canvas.style.height = `${h}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);

    // Draw cells
    for (let i = 0; i < n; i++) {
      for (let j = 0; j < n; j++) {
        const val = data.correlation[i]?.[j] ?? 0;
        ctx.fillStyle = corrColor(val);
        ctx.fillRect(labelWidth + j * cellSize, labelHeight + i * cellSize, cellSize - 1, cellSize - 1);

        // Value text for larger cells
        if (cellSize >= 24) {
          ctx.fillStyle = Math.abs(val) > 0.5 ? "#e8edf9" : "#7f8cab";
          ctx.font = `${Math.max(8, cellSize * 0.3)}px -apple-system, sans-serif`;
          ctx.textAlign = "center";
          ctx.textBaseline = "middle";
          ctx.fillText(
            val.toFixed(2),
            labelWidth + j * cellSize + cellSize / 2,
            labelHeight + i * cellSize + cellSize / 2
          );
        }
      }
    }

    // Row labels (left)
    ctx.fillStyle = "#a9b6d2";
    ctx.font = "10px -apple-system, sans-serif";
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    for (let i = 0; i < n; i++) {
      const label = data.factors[i].length > 15 ? data.factors[i].slice(0, 14) + "…" : data.factors[i];
      ctx.fillText(label, labelWidth - 4, labelHeight + i * cellSize + cellSize / 2);
    }

    // Column labels (top, rotated)
    ctx.save();
    ctx.textAlign = "left";
    ctx.textBaseline = "middle";
    for (let j = 0; j < n; j++) {
      const x = labelWidth + j * cellSize + cellSize / 2;
      const y = labelHeight - 4;
      ctx.save();
      ctx.translate(x, y);
      ctx.rotate(-Math.PI / 3);
      const label = data.factors[j].length > 12 ? data.factors[j].slice(0, 11) + "…" : data.factors[j];
      ctx.fillText(label, 0, 0);
      ctx.restore();
    }
    ctx.restore();
  }, [data]);

  if (!data.factors?.length) {
    return <div className="text-[var(--text-muted)] text-sm">No correlation data available</div>;
  }

  return (
    <div className="overflow-x-auto">
      <canvas ref={canvasRef} />
    </div>
  );
}
