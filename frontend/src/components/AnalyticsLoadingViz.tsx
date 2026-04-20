"use client";

import { useEffect, useRef } from "react";
import { useAppSettings } from "./AppSettingsContext";

interface AnalyticsLoadingVizProps {
  message?: string | null;
  stepLabel?: string | null;
  animate?: boolean;
  className?: string;
}

const FALLBACK_PALETTE: readonly (readonly [number, number, number])[] = [
  [192, 99, 164],
  [229, 161, 95],
  [90, 158, 203],
  [229, 161, 95],
  [192, 99, 164],
  [121, 188, 156],
  [90, 158, 203],
  [229, 161, 95],
  [192, 99, 164],
];

const GLOW = [0.3, 0.4, 0.42, 0.54, 0.45, 0.72, 0.78, 0.44, 0.3] as const;

function lerp(a: number, b: number, t: number) { return a + (b - a) * t; }

function lerpColor(
  a: readonly number[],
  b: readonly number[],
  t: number,
): [number, number, number] {
  return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t, a[2] + (b[2] - a[2]) * t];
}

function readCssRgbTriplet(name: string, fallback: readonly [number, number, number]): [number, number, number] {
  if (typeof window === "undefined") return [...fallback] as [number, number, number];
  const raw = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  const parts = raw.split(",").map((part) => Number(part.trim()));
  if (parts.length !== 3 || parts.some((part) => Number.isNaN(part))) return [...fallback] as [number, number, number];
  return [parts[0], parts[1], parts[2]];
}

function buildPalette() {
  return [
    readCssRgbTriplet("--flow-ribbon-a1-rgb", FALLBACK_PALETTE[0]),
    readCssRgbTriplet("--analytics-cmac-rgb", FALLBACK_PALETTE[1]),
    readCssRgbTriplet("--analytics-cpar-rgb", FALLBACK_PALETTE[2]),
    readCssRgbTriplet("--analytics-cmac-rgb", FALLBACK_PALETTE[3]),
    readCssRgbTriplet("--analytics-cuse-rgb", FALLBACK_PALETTE[4]),
    readCssRgbTriplet("--positive-rgb", FALLBACK_PALETTE[5]),
    readCssRgbTriplet("--analytics-cpar-rgb", FALLBACK_PALETTE[6]),
    readCssRgbTriplet("--analytics-cmac-rgb", FALLBACK_PALETTE[7]),
    readCssRgbTriplet("--flow-ribbon-a1-rgb", FALLBACK_PALETTE[8]),
  ] as const;
}

function samplePalette(t: number, palette: readonly (readonly [number, number, number])[]) {
  const len = palette.length - 1;
  const s = ((t % 1) + 1) % 1 * len;
  const idx = Math.floor(s);
  const frac = s - idx;
  const j = Math.min(idx + 1, len);
  return {
    rgb: lerpColor(palette[idx], palette[j], frac),
    glow: lerp(GLOW[idx], GLOW[j], frac),
  };
}

const GRID = 18;
const DOT_SPACING = 11;
const DOT_RADIUS = 1.6;
const CANVAS_PX = GRID * DOT_SPACING;
const STATIC_GRID = 14;
const STATIC_SPACING = 9;
const STATIC_RADIUS = 1.7;
const STATIC_CANVAS_PX = STATIC_GRID * STATIC_SPACING;

function drawStaticGrid(ctx: CanvasRenderingContext2D) {
  ctx.clearRect(0, 0, STATIC_CANVAS_PX, STATIC_CANVAS_PX);
  const [r, g, b] = typeof window === "undefined"
    ? [198, 206, 220]
    : readCssRgbTriplet("--neo-dot-rgb", [198, 206, 220]);

  for (let row = 0; row < STATIC_GRID; row++) {
    for (let col = 0; col < STATIC_GRID; col++) {
      const cx = col * STATIC_SPACING + STATIC_SPACING / 2;
      const cy = row * STATIC_SPACING + STATIC_SPACING / 2;
      ctx.beginPath();
      ctx.arc(cx, cy, STATIC_RADIUS, 0, Math.PI * 2);
      ctx.fillStyle = `rgba(${r},${g},${b},0.62)`;
      ctx.fill();
    }
  }
}

export default function AnalyticsLoadingViz({
  message = "Loading analytics...",
  stepLabel,
  animate = true,
  className,
}: AnalyticsLoadingVizProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const { themeMode } = useAppSettings();
  const resolvedMessage = message ?? "Loading analytics...";
  const ariaLabel = stepLabel
    ? `${resolvedMessage}. ${stepLabel}`
    : resolvedMessage;
  const canvasPx = animate ? CANVAS_PX : STATIC_CANVAS_PX;
  const canvasCssPx = animate ? 125 : STATIC_CANVAS_PX;

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    const palette = buildPalette();

    let raf: number | null = null;

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
          const { rgb: [r, g, b], glow: glowStr } = samplePalette(colorPhase, palette);

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

      if (animate) {
        raf = requestAnimationFrame(draw);
      }
    };

    if (animate) {
      raf = requestAnimationFrame(draw);
    } else {
      drawStaticGrid(ctx);
    }

    return () => {
      if (raf !== null) cancelAnimationFrame(raf);
    };
  }, [animate, themeMode]);

  return (
    <div
      className={className ? `analytics-stage ${className}` : "analytics-stage"}
      role="status"
      aria-live="polite"
      aria-label={ariaLabel}
    >
      <canvas
        ref={canvasRef}
        width={canvasPx}
        height={canvasPx}
        aria-hidden="true"
        style={{ width: canvasCssPx, height: canvasCssPx }}
      />
    </div>
  );
}
