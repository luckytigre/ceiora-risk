"use client";

import { useEffect, useRef } from "react";
import { useBackground, type BgMode } from "./BackgroundContext";

/* ── Shared helpers ─────────────────────────────────────────── */

function clamp(v: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, v));
}

function hash01(a: number, b: number) {
  const n = Math.sin(a * 127.1 + b * 311.7) * 43758.5453123;
  return n - Math.floor(n);
}

function frac(v: number) {
  return v - Math.floor(v);
}

/* ── Mode: "topo" — topographic isoline dots ─────────────── */

const SCALE = 0.875;

function terrainHeight(x: number, z: number) {
  const ridge =
    0.62 * Math.sin(x * 0.0088 + z * 0.0042) +
    0.54 * Math.cos(x * 0.0056 - z * 0.0071) +
    0.38 * Math.sin((x + z) * 0.0042);
  const m1 = Math.exp(-((x + 180) ** 2) / 76000 - ((z + 60) ** 2) / 62000) * 2.2;
  const m2 = Math.exp(-((x - 210) ** 2) / 86000 - ((z - 30) ** 2) / 70000) * 2.0;
  const m3 = Math.exp(-((x - 40) ** 2) / 130000 - ((z - 190) ** 2) / 54000) * 1.7;
  return ridge + m1 + m2 + m3;
}

function drawTopo(ctx: CanvasRenderingContext2D, w: number, h: number) {
  const sceneZoom = 0.65;
  const scenePanX = -2000;
  const cxS = w * 0.5;
  const cyS = h * 0.52;
  const invZoom = 1 / sceneZoom;
  const step = 4.2 * SCALE;
  const isoStep = 0.32;
  const bandTol = 0.048;
  const e = 8 * SCALE;

  const sf = (sx: number, sy: number) => {
    const pX = cxS + (sx - cxS) * invZoom / SCALE + scenePanX;
    const pY = cyS + (sy - cyS) * invZoom / SCALE;
    const d = clamp((pY + 12) / (h / SCALE + 24), 0, 1);
    return terrainHeight((pX - cxS) * (0.95 + d * 0.28), (pY - cyS) * (1.05 + d * 0.35));
  };

  for (let yi = 0, sy = -12; sy <= h + 12; sy += step, yi++) {
    for (let xi = 0, sx = -12; sx <= w + 12; sx += step, xi++) {
      const val = sf(sx, sy);
      const hx = (sf(sx + e, sy) - sf(sx - e, sy)) / (2 * e);
      const hz = (sf(sx, sy + e) - sf(sx, sy - e)) / (2 * e);
      const grad = Math.hypot(hx, hz);
      const dense = clamp((grad - 0.008) / 0.02, 0, 1);
      if (dense > 0.58) { if (((xi + yi * 3) % 18) !== 0) continue; }
      else if (dense > 0.40) { if (((xi + yi * 2) % 8) !== 0) continue; }
      else if (dense > 0.28) { if (((xi + yi * 2) % 6) !== 0) continue; }

      const band = Math.abs(frac(val / isoStep) - 0.5);
      if (band > bandTol) continue;
      if (hash01(xi * 0.173, yi * 0.197) > 0.84) continue;
      if (sx < -24 || sx > w + 24 || sy < -24 || sy > h + 24) continue;

      const alpha = clamp(0.24 + (bandTol - band) * 0.35, 0.18, 0.52);
      const size = 1.3 * sceneZoom * 0.85 * SCALE;
      ctx.fillStyle = `rgba(176,176,176,${alpha.toFixed(3)})`;
      ctx.beginPath();
      ctx.arc(sx, sy, size, 0, Math.PI * 2);
      ctx.fill();
    }
  }
}

/* ── Mode: "flow" — organized ribbon sweeps ────────────────── */

/**
 * Each ribbon is a coherent bundle of parallel bezier curves.
 * All strands share the same control-point skeleton — they're
 * offset perpendicular to the curve at each of the 4 points,
 * with per-point spread control so ribbons can tighten and widen
 * gracefully along their path.
 */
interface Ribbon {
  /* Four bezier points as fractions of viewport */
  p0: [number, number];
  p1: [number, number];
  p2: [number, number];
  p3: [number, number];
  /* Perpendicular spread (px) at each of the 4 points */
  s0: number; s1: number; s2: number; s3: number;
  /* Number of strands in this ribbon */
  n: number;
  /* Alpha range [edge, center] */
  alpha: [number, number];
  lw: number;
  /* Gradient color stops — light, muted tints [r,g,b] */
  c0: [number, number, number];
  c1: [number, number, number];
}

function drawRibbon(ctx: CanvasRenderingContext2D, w: number, h: number, r: Ribbon) {
  for (let i = 0; i < r.n; i++) {
    const t = r.n <= 1 ? 0 : (i / (r.n - 1)) - 0.5; // -0.5 … +0.5

    // At each control point, offset perpendicular to the local tangent direction
    const perp = (ax: number, ay: number, bx: number, by: number): [number, number] => {
      const dx = bx - ax, dy = by - ay;
      const len = Math.hypot(dx, dy) || 1;
      return [-dy / len, dx / len];
    };

    const [p0, p1, p2, p3] = [r.p0, r.p1, r.p2, r.p3];
    const n0 = perp(p0[0], p0[1], p1[0], p1[1]);
    const n1 = perp(p0[0], p0[1], p2[0], p2[1]);
    const n2 = perp(p1[0], p1[1], p3[0], p3[1]);
    const n3 = perp(p2[0], p2[1], p3[0], p3[1]);

    const x0 = p0[0] * w + n0[0] * t * r.s0;
    const y0 = p0[1] * h + n0[1] * t * r.s0;
    const x1 = p1[0] * w + n1[0] * t * r.s1;
    const y1 = p1[1] * h + n1[1] * t * r.s1;
    const x2 = p2[0] * w + n2[0] * t * r.s2;
    const y2 = p2[1] * h + n2[1] * t * r.s2;
    const x3 = p3[0] * w + n3[0] * t * r.s3;
    const y3 = p3[1] * h + n3[1] * t * r.s3;

    const edgeFade = 1 - Math.abs(t) * 2;
    const a = r.alpha[0] + (r.alpha[1] - r.alpha[0]) * edgeFade;

    // Gradient along the curve from c0 → c1
    const grad = ctx.createLinearGradient(x0, y0, x3, y3);
    grad.addColorStop(0, `rgba(${r.c0[0]},${r.c0[1]},${r.c0[2]},${a.toFixed(3)})`);
    grad.addColorStop(1, `rgba(${r.c1[0]},${r.c1[1]},${r.c1[2]},${a.toFixed(3)})`);

    ctx.strokeStyle = grad;
    ctx.lineWidth = r.lw;
    ctx.beginPath();
    ctx.moveTo(x0, y0);
    ctx.bezierCurveTo(x1, y1, x2, y2, x3, y3);
    ctx.stroke();
  }
}

function drawFlow(ctx: CanvasRenderingContext2D, w: number, h: number) {
  ctx.lineCap = "round";

  /* Subtle tints derived from theme accents, lifted to light pastel range:
     accent-blue #3ba9e1  → [155,200,225]   accent-green #31f6ff → [150,220,225]
     accent-purple #d757ba → [210,165,200]   accent-yellow #ff8f2a → [220,190,155]
     neutral grey baseline: [176,176,176]                                        */

  const ribbons: Ribbon[] = [
    // Ribbon A: enters top-left, swoops down, pinches left-of-center,
    // then opens wide as it climbs to exit top-right.
    {
      p0: [-0.25, -0.30], p1: [0.05, 1.00], p2: [0.55, -0.05], p3: [1.30, -0.34],
      s0: 682, s1: 55, s2: 308, s3: 748,
      n: 25, alpha: [0.06, 0.20], lw: 0.7,
      c0: [155, 200, 225], c1: [210, 165, 200],
    },
    // Ribbon B: enters bottom-right, arcs high, pinches right-of-center,
    // then fans wide as it descends to exit bottom-left.
    {
      p0: [1.28, 1.11], p1: [0.90, -0.40], p2: [0.40, 0.68], p3: [-0.25, 1.13],
      s0: 638, s1: 60, s2: 286, s3: 715,
      n: 23, alpha: [0.05, 0.18], lw: 0.65,
      c0: [176, 180, 190], c1: [150, 220, 225],
    },
  ];

  for (const r of ribbons) {
    drawRibbon(ctx, w, h, r);
  }
}

/* ── Renderer ──────────────────────────────────────────────── */
const RENDERERS: Record<BgMode, (ctx: CanvasRenderingContext2D, w: number, h: number) => void> = {
  topo: drawTopo,
  flow: drawFlow,
  none: () => {},
};

/* Parallax: viewport-sized canvas (position:fixed), shifted via GPU transform */
const PARALLAX_RATE = 0.18;
const OVERSCAN = 0.7;

export default function Neo2DotBackground() {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const { mode } = useBackground();

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const draw = () => {
      const w = window.innerWidth;
      const h = Math.ceil(window.innerHeight * (1 + OVERSCAN));
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.floor(w * dpr);
      canvas.height = Math.floor(h * dpr);
      canvas.style.width = `${w}px`;
      canvas.style.height = `${h}px`;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, w, h);

      const renderer = RENDERERS[mode];
      if (renderer) renderer(ctx, w, h);
    };

    draw();

    const onResize = () => requestAnimationFrame(draw);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, [mode]);

  /* Scroll-driven parallax — pure GPU transform, no re-rendering */
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || mode === "none") return;

    let ticking = false;
    const onScroll = () => {
      if (ticking) return;
      ticking = true;
      requestAnimationFrame(() => {
        const y = window.scrollY * PARALLAX_RATE;
        canvas.style.transform = `translateY(${-y}px)`;
        ticking = false;
      });
    };

    window.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
    return () => window.removeEventListener("scroll", onScroll);
  }, [mode]);

  if (mode === "none") return null;

  return <canvas ref={canvasRef} className="neo2-dot-bg" aria-hidden />;
}
