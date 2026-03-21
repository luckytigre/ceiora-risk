"use client";

import { useCallback, useState, type MouseEvent } from "react";
import { useRouter } from "next/navigation";
import LandingBackgroundLock from "./LandingBackgroundLock";

type FamilyKey = "cuse" | "cpar";

const FAMILY_TARGETS: Record<FamilyKey, string> = {
  cuse: "/cuse/exposures",
  cpar: "/cpar/risk",
};

const FAMILY_COLORS: Record<FamilyKey, { top: string; bottom: string }> = {
  cuse: { top: "rgba(184, 220, 226, 0.96)", bottom: "rgba(118, 164, 172, 0.94)" },
  cpar: { top: "rgba(228, 190, 220, 0.96)", bottom: "rgba(176, 132, 169, 0.94)" },
};
const LANDING_FAMILY_TRANSITION_EVENT = "landing-family-transition-start";

function prefersReducedMotion(): boolean {
  return typeof window !== "undefined" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

function lerp(start: number, end: number, progress: number): number {
  return start + (end - start) * progress;
}

function minimumJerk(progress: number): number {
  return 10 * progress ** 3 - 15 * progress ** 4 + 6 * progress ** 5;
}

function gaussianBump(progress: number, center: number, width: number, amplitude: number): number {
  const normalized = (progress - center) / width;
  return amplitude * Math.exp(-(normalized ** 2));
}

function buildFlightKeyframes(
  deltaX: number,
  deltaY: number,
  targetScaleX: number,
  targetScaleY: number,
): Keyframe[] {
  const samples = 16;
  const keyframes: Keyframe[] = [];
  const flightDistance = Math.sqrt(deltaX ** 2 + deltaY ** 2);
  const liftAmplitude = Math.min(-8, -flightDistance * 0.05);

  for (let index = 0; index <= samples; index += 1) {
    const phase = index / samples;
    const curve = minimumJerk(phase);
    const offset = phase;
    const growthBump = gaussianBump(phase, 0.24, 0.13, 0.19);
    const liftBump = gaussianBump(phase, 0.25, 0.16, liftAmplitude);
    const x = lerp(0, deltaX, curve);
    const y = lerp(0, deltaY, curve) + liftBump;
    const scaleX = lerp(1, targetScaleX, curve) + growthBump;
    const scaleY = lerp(1, targetScaleY, curve) + growthBump;
    const fadeProgress = phase <= 0.72 ? 0 : (phase - 0.72) / 0.28;
    const opacity = lerp(1, 0.02, minimumJerk(Math.min(1, Math.max(0, fadeProgress))));
    const blur = lerp(0, 3, minimumJerk(Math.min(1, Math.max(0, fadeProgress))));

    keyframes.push({
      transform: `translate3d(${x}px, ${y}px, 0) scale(${scaleX}, ${scaleY})`,
      opacity,
      filter: `blur(${blur}px)`,
      offset,
    });
  }

  return keyframes;
}

export default function LandingFamilyPicker() {
  const router = useRouter();
  const [animatingFamily, setAnimatingFamily] = useState<FamilyKey | null>(null);

  const navigateWithFlight = useCallback(
    async (family: FamilyKey, event: MouseEvent<HTMLAnchorElement>) => {
      event.preventDefault();
      if (animatingFamily) return;

      const href = FAMILY_TARGETS[family];
      const sourceEl = event.currentTarget;
      const sourceRect = sourceEl.getBoundingClientRect();
      if (prefersReducedMotion()) {
        router.push(href);
        return;
      }

      window.dispatchEvent(
        new CustomEvent(LANDING_FAMILY_TRANSITION_EVENT, {
          detail: { family },
        }),
      );
      await new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));

      const targetEl = document.querySelector(".dash-tabs-family-badge") as HTMLElement | null;
      if (!targetEl) {
        router.push(href);
        return;
      }
      const targetRect = targetEl.getBoundingClientRect();

      if (!targetRect.width || !targetRect.height) {
        router.push(href);
        return;
      }

      setAnimatingFamily(family);

      const ghost = sourceEl.cloneNode(true) as HTMLElement;
      ghost.setAttribute("aria-hidden", "true");
      ghost.classList.add("family-split-ghost");
      const colors = FAMILY_COLORS[family];
      Object.assign(ghost.style, {
        position: "fixed",
        left: `${sourceRect.left}px`,
        top: `${sourceRect.top}px`,
        width: `${sourceRect.width}px`,
        height: `${sourceRect.height}px`,
        margin: "0",
        zIndex: "12000",
        pointerEvents: "none",
        transformOrigin: "top left",
        "--family-split-top": colors.top,
        "--family-split-bottom": colors.bottom,
      });
      document.body.appendChild(ghost);

      const targetLeft = targetRect.left;
      const targetTop = targetRect.top;
      const deltaX = targetLeft - sourceRect.left;
      const deltaY = targetTop - sourceRect.top;
      const targetScaleX = targetRect.width / sourceRect.width;
      const targetScaleY = targetRect.height / sourceRect.height;

      const animation = ghost.animate(buildFlightKeyframes(deltaX, deltaY, targetScaleX, targetScaleY), {
        duration: 975,
        easing: "linear",
        fill: "forwards",
      });

      try {
        await animation.finished;
      } catch {
        // Ignore animation cancellation and continue routing.
      } finally {
        ghost.remove();
        router.push(href);
      }
    },
    [animatingFamily, router],
  );

  const cuseDimmed = animatingFamily !== null && animatingFamily !== "cuse";
  const cparDimmed = animatingFamily !== null && animatingFamily !== "cpar";
  const dividerDimmed = animatingFamily !== null;
  const driftClass = animatingFamily === "cuse" ? " is-drift-right" : animatingFamily === "cpar" ? " is-drift-left" : "";

  return (
    <div
      className={`family-split-landing${animatingFamily ? " is-transitioning" : ""}`}
      data-testid="family-split-landing"
    >
      <LandingBackgroundLock />
      <a
        href={FAMILY_TARGETS.cuse}
        className={`family-split-link family-split-link-cuse${cuseDimmed ? ` is-dimmed${driftClass}` : ""}${animatingFamily === "cuse" ? " is-selected" : ""}`}
        onClick={(event) => void navigateWithFlight("cuse", event)}
      >
        <span className="family-split-prefix">c</span>USE
      </a>
      <span
        className={`family-split-divider${dividerDimmed ? ` is-dimmed${driftClass}` : ""}`}
        aria-hidden="true"
      >
        |
      </span>
      <a
        href={FAMILY_TARGETS.cpar}
        className={`family-split-link family-split-link-cpar${cparDimmed ? ` is-dimmed${driftClass}` : ""}${animatingFamily === "cpar" ? " is-selected" : ""}`}
        onClick={(event) => void navigateWithFlight("cpar", event)}
      >
        <span className="family-split-prefix">c</span>PAR
      </a>
    </div>
  );
}
