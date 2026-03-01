"use client";

import { useEffect, useRef } from "react";

interface AnalyticsLoadingVizProps {
  message?: string | null;
  stepLabel?: string | null;
}

const PALETTE: readonly (readonly [number, number, number])[] = [
  [166, 79, 121],  // muted rose
  [204, 53, 88],   // crimson
  [215, 87, 186],  // magenta
  [245, 186, 228], // soft pink
  [215, 87, 186],  // magenta
  [210, 118, 28],  // darker amber
  [20, 155, 175],  // deeper teal
  [204, 53, 88],   // crimson
  [166, 79, 121],  // muted rose
];

const GLOW = [0.3, 0.4, 0.42, 0.54, 0.45, 0.72, 0.78, 0.44, 0.3];

function lerp(a: number, b: number, t: number) { return a + (b - a) * t; }

function lerpColor(
  a: readonly number[],
  b: readonly number[],
  t: number,
): [number, number, number] {
  return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t, a[2] + (b[2] - a[2]) * t];
}

function samplePalette(t: number) {
  const len = PALETTE.length - 1;
  const s = ((t % 1) + 1) % 1 * len;
  const idx = Math.floor(s);
  const frac = s - idx;
  const j = Math.min(idx + 1, len);
  return {
    rgb: lerpColor(PALETTE[idx], PALETTE[j], frac),
    glow: lerp(GLOW[idx], GLOW[j], frac),
  };
}

const GRID = 18;
const DOT_SPACING = 11;
const DOT_RADIUS = 1.6;
const CANVAS_PX = GRID * DOT_SPACING;

export default function AnalyticsLoadingViz({
  message = "Loading analytics...",
  stepLabel,
}: AnalyticsLoadingVizProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const resolvedMessage = message ?? "Loading analytics...";
  const ariaLabel = stepLabel
    ? `${resolvedMessage}. ${stepLabel}`
    : resolvedMessage;

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let raf: number;

    const draw = (now: number) => {
      const t = now / 1000;
      ctx.clearRect(0, 0, CANVAS_PX, CANVAS_PX);

      for (let row = 0; row < GRID; row++) {
        for (let col = 0; col < GRID; col++) {
          const cx = col * DOT_SPACING + DOT_SPACING / 2;
          const cy = row * DOT_SPACING + DOT_SPACING / 2;

          const phase = (row * 42 + col * 27) / 1000;

          // Gentle warp — bends the wavefront into slow curves
          const warp = Math.sin(row * 0.45) * 0.8 + Math.sin(col * 0.35) * 0.5;

          // Primary diagonal with warp
          const diag = row * 42 + col * 27 + warp * 40;
          const w1 = Math.sin((t - diag / 1000) * Math.PI * 2 / 0.7) * 0.5 + 0.5;

          // Secondary diagonal — slightly different angle, slower
          const diag2 = row * 35 + col * 45;
          const w2 = Math.sin((t - diag2 / 1000) * Math.PI * 2 / 1.3) * 0.5 + 0.5;

          const composite = w1 * 0.62 + w2 * 0.38;

          // Strobe: dim embers between peaks, bright flare on the wave
          const base = 0.08;
          const flare = Math.pow(composite, 2.2);
          const opacity = base + flare * (1.0 - base);

          // Colour drifts slowly across the grid
          const colorPhase = (t - phase) / 4.0;
          const { rgb: [r, g, b], glow: glowStr } = samplePalette(colorPhase);

          // Glow halo
          const glowR = DOT_RADIUS + composite * 3.0;
          if (opacity > 0.12) {
            const grad = ctx.createRadialGradient(cx, cy, 0, cx, cy, glowR + 2);
            grad.addColorStop(0, `rgba(${r | 0},${g | 0},${b | 0},${opacity * glowStr})`);
            grad.addColorStop(1, `rgba(${r | 0},${g | 0},${b | 0},0)`);
            ctx.fillStyle = grad;
            ctx.fillRect(cx - glowR - 2, cy - glowR - 2, (glowR + 2) * 2, (glowR + 2) * 2);
          }

          ctx.beginPath();
          ctx.arc(cx, cy, DOT_RADIUS, 0, Math.PI * 2);
          ctx.fillStyle = `rgba(${r | 0},${g | 0},${b | 0},${opacity})`;
          ctx.fill();
        }
      }

      raf = requestAnimationFrame(draw);
    };

    raf = requestAnimationFrame(draw);
    return () => cancelAnimationFrame(raf);
  }, []);

  return (
    <div
      className="analytics-stage"
      role="status"
      aria-live="polite"
      aria-label={ariaLabel}
    >
      <canvas
        ref={canvasRef}
        width={CANVAS_PX}
        height={CANVAS_PX}
        aria-hidden="true"
        style={{ width: 125, height: 125 }}
      />
    </div>
  );
}
