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

/* ── Colour zones — shared by both modes ──────────────────── */

const ZONES: [number, number, number, number, number, number, number][] = [
  [166, 79, 121, 0.2, 0.3, 0.6, 0.08],   // muted rose
  [120, 80, 155, 0.8, 0.2, 0.5, 0.06],    // dusty violet
  [60, 120, 150, 0.5, 0.8, 0.55, 0.05],   // slate blue
  [204, 53, 88, 0.15, 0.85, 0.4, 0.04],   // faint crimson
];

function tintColor(x: number, y: number, w: number, h: number): [number, number, number] {
  let cr = 158, cg = 158, cb = 164;
  const nx = x / w, ny = y / h;
  for (const z of ZONES) {
    const dx = nx - z[3], dy = ny - z[4];
    const dist = Math.sqrt(dx * dx + dy * dy);
    const inf = Math.max(0, 1 - dist / z[5]);
    const s = inf * inf * z[6];
    cr = cr * (1 - s) + z[0] * s;
    cg = cg * (1 - s) + z[1] * s;
    cb = cb * (1 - s) + z[2] * s;
  }
  return [cr, cg, cb];
}

/* ── Mode: "field" — uniform grid, smooth noise brightness ─── */

function noiseField(x: number, y: number) {
  return (
    Math.sin(x * 0.012 + y * 0.009) * 0.5 +
    Math.sin(x * 0.007 - y * 0.013 + 2.0) * 0.4 +
    Math.sin((x + y) * 0.005 + 1.3) * 0.3 +
    Math.sin(x * 0.019 + y * 0.003 - 0.8) * 0.2
  );
}

function drawField(ctx: CanvasRenderingContext2D, w: number, h: number) {
  const spacing = 9;
  const baseR = 0.55;

  for (let y = spacing / 2; y < h; y += spacing) {
    for (let x = spacing / 2; x < w; x += spacing) {
      const n = noiseField(x, y);
      const norm = (n + 1.4) / 2.8;
      const alpha = 0.06 + norm * norm * 0.22;
      const r = baseR + norm * 0.25;
      const [cr, cg, cb] = tintColor(x, y, w, h);

      ctx.fillStyle = `rgba(${cr | 0},${cg | 0},${cb | 0},${alpha.toFixed(3)})`;
      ctx.beginPath();
      ctx.arc(x, y, r, 0, Math.PI * 2);
      ctx.fill();
    }
  }
}

/* ── Mode: "topo" — original topographic isoline dots ─────── */

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
  const step = 4.2;
  const isoStep = 0.32;
  const bandTol = 0.048;
  const e = 8;

  const sf = (sx: number, sy: number) => {
    const pX = cxS + (sx - cxS) * invZoom + scenePanX;
    const pY = cyS + (sy - cyS) * invZoom;
    const d = clamp((pY + 12) / (h + 24), 0, 1);
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
      const size = 1.3 * sceneZoom * 0.85;
      ctx.fillStyle = `rgba(176,176,176,${alpha.toFixed(3)})`;
      ctx.beginPath();
      ctx.arc(sx, sy, size, 0, Math.PI * 2);
      ctx.fill();
    }
  }
}

/* ── Renderer ──────────────────────────────────────────────── */

const RENDERERS: Record<string, (ctx: CanvasRenderingContext2D, w: number, h: number) => void> = {
  field: drawField,
  topo: drawTopo,
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
