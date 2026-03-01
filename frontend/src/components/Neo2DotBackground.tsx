"use client";

import { useEffect, useRef } from "react";

type Particle = {
  x: number;
  y: number;
  size: number;
  alpha: number;
  r: number;
  g: number;
  b: number;
};

function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

function hash01(a: number, b: number): number {
  const n = Math.sin(a * 127.1 + b * 311.7) * 43758.5453123;
  return n - Math.floor(n);
}

function frac(v: number): number {
  return v - Math.floor(v);
}

function toZoomedScenePoint(
  sx: number, sy: number, width: number, height: number,
  sceneZoom: number, scenePanX: number, scenePanY: number
): { x: number; y: number } {
  if (sceneZoom >= 0.999) return { x: sx + scenePanX, y: sy + scenePanY };
  const cx = width * 0.5;
  const cy = height * 0.52;
  return {
    x: cx + (sx - cx) / sceneZoom + scenePanX,
    y: cy + (sy - cy) / sceneZoom + scenePanY,
  };
}

function terrainHeight(x: number, z: number): number {
  const ridge =
    0.62 * Math.sin(x * 0.0088 + z * 0.0042) +
    0.54 * Math.cos(x * 0.0056 - z * 0.0071) +
    0.38 * Math.sin((x + z) * 0.0042);
  const m1 = Math.exp(-((x + 180) * (x + 180)) / 76000 - ((z + 60) * (z + 60)) / 62000) * 2.2;
  const m2 = Math.exp(-((x - 210) * (x - 210)) / 86000 - ((z - 30) * (z - 30)) / 70000) * 2.0;
  const m3 = Math.exp(-((x - 40) * (x - 40)) / 130000 - ((z - 190) * (z - 190)) / 54000) * 1.7;
  return ridge + m1 + m2 + m3;
}

function screenToField(
  sx: number, sy: number, width: number, height: number,
  sceneZoom: number, scenePanX: number, scenePanY: number
): number {
  const p = toZoomedScenePoint(sx, sy, width, height, sceneZoom, scenePanX, scenePanY);
  const cx = width * 0.5;
  const cy = height * 0.52;
  const depthNorm = clamp((p.y + 12) / (height + 24), 0, 1);
  const xw = (p.x - cx) * (0.95 + depthNorm * 0.28);
  const zw = (p.y - cy) * (1.05 + depthNorm * 0.35);
  return terrainHeight(xw, zw);
}

export default function Neo2DotBackground() {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const particlesRef = useRef<Particle[]>([]);
  const refinedTaskRef = useRef<number | null>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let width = window.innerWidth;
    let height = window.innerHeight;
    let dpr = Math.min(window.devicePixelRatio || 1, 2);
    const sceneZoom = 0.65;
    const scenePanX = -2000;
    const scenePanY = 0;

    const rebuildParticles = (quality: "coarse" | "full") => {
      const isCoarse = quality === "coarse";
      const step = isCoarse ? 9.6 : 4.2;
      const isoStep = isCoarse ? 0.35 : 0.32;
      const bandTol = isCoarse ? 0.07 : 0.048;
      const e = 8;
      const thinCutoff = isCoarse ? 0.93 : 0.84;
      const cx = width * 0.5;
      const cy = height * 0.52;
      const invZoom = 1 / sceneZoom;
      const sampleField = (sx: number, sy: number): number => {
        const pX = cx + (sx - cx) * invZoom + scenePanX;
        const pY = cy + (sy - cy) * invZoom + scenePanY;
        const depthNorm = clamp((pY + 12) / (height + 24), 0, 1);
        const xw = (pX - cx) * (0.95 + depthNorm * 0.28);
        const zw = (pY - cy) * (1.05 + depthNorm * 0.35);
        return terrainHeight(xw, zw);
      };
      const particles: Particle[] = [];

      for (let yi = 0, sy = -12; sy <= height + 12; sy += step, yi += 1) {
        const depthNorm = clamp((sy + 12) / (height + 24), 0, 1);
        for (let xi = 0, sx = -12; sx <= width + 12; sx += step, xi += 1) {
          const h = sampleField(sx, sy);
          if (!isCoarse) {
            const hx = (sampleField(sx + e, sy) - sampleField(sx - e, sy)) / (2 * e);
            const hz = (sampleField(sx, sy + e) - sampleField(sx, sy - e)) / (2 * e);
            const grad = Math.hypot(hx, hz);
            const dense = clamp((grad - 0.008) / 0.02, 0, 1);
            if (dense > 0.58) { if (((xi + yi * 3) % 18) !== 0) continue; }
            else if (dense > 0.40) { if (((xi + yi * 2) % 8) !== 0) continue; }
            else if (dense > 0.28) { if (((xi + yi * 2) % 6) !== 0) continue; }
          }

          const level = h / isoStep;
          const band = Math.abs(frac(level) - 0.5);
          if (band > bandTol) continue;

          const thinGlobal = hash01(xi * 0.173, yi * 0.197);
          if (thinGlobal > thinCutoff) continue;

          const px = sx;
          const py = sy;
          if (px < -24 || px > width + 24 || py < -24 || py > height + 24) continue;

          const alpha = clamp(0.16 + (bandTol - band) * 0.25, 0.12, 0.38);
          const lum = 176;
          const size = 1.3 * sceneZoom * 0.85;

          particles.push({
            x: px, y: py, size, alpha,
            r: Math.round(lum), g: Math.round(lum), b: Math.round(lum),
          });
        }
      }
      particlesRef.current = particles;
    };

    const drawStatic = () => {
      ctx.clearRect(0, 0, width, height);
      const particles = particlesRef.current;
      for (let i = 0; i < particles.length; i += 1) {
        const p = particles[i]!;
        ctx.fillStyle = `rgba(${p.r}, ${p.g}, ${p.b}, ${p.alpha.toFixed(3)})`;
        ctx.beginPath();
        ctx.arc(p.x, p.y, p.size, 0, Math.PI * 2);
        ctx.fill();
      }
    };

    const resize = () => {
      if (refinedTaskRef.current !== null) {
        window.clearTimeout(refinedTaskRef.current);
        refinedTaskRef.current = null;
      }

      width = window.innerWidth;
      height = window.innerHeight;
      dpr = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.floor(width * dpr);
      canvas.height = Math.floor(height * dpr);
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

      // Draw a fast coarse pass immediately, then refine next frame.
      rebuildParticles("coarse");
      drawStatic();

      // Run refinement after the browser has a chance to paint the coarse pass.
      refinedTaskRef.current = window.setTimeout(() => {
        rebuildParticles("full");
        drawStatic();
        refinedTaskRef.current = null;
      }, 24);
    };

    resize();
    window.addEventListener("resize", resize);
    return () => {
      window.removeEventListener("resize", resize);
      if (refinedTaskRef.current !== null) {
        window.clearTimeout(refinedTaskRef.current);
        refinedTaskRef.current = null;
      }
      ctx.clearRect(0, 0, width, height);
    };
  }, []);

  return <canvas ref={canvasRef} className="neo2-dot-bg" aria-hidden />;
}
