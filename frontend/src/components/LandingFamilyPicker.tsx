"use client";

import { useCallback, useState, type MouseEvent } from "react";
import { useRouter } from "next/navigation";
import { preload } from "swr";
import LandingBackgroundLock from "./LandingBackgroundLock";
import { apiFetch } from "@/lib/apiTransport";
import { cparApiPath } from "@/lib/cparApi";
import { cuse4ApiPath } from "@/lib/cuse4Api";

type FamilyKey = "cuse" | "cpar";

const FAMILY_TARGETS: Record<FamilyKey, string> = {
  cuse: "/cuse/exposures",
  cpar: "/cpar/risk",
};

const FAMILY_API_KEYS: Record<FamilyKey, string> = {
  cuse: cuse4ApiPath.riskPageSnapshot(),
  cpar: cparApiPath.cparRisk(),
};
const LANDING_FAMILY_TRANSITION_EVENT = "landing-family-transition-start";

function prefersReducedMotion(): boolean {
  return typeof window !== "undefined" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

export default function LandingFamilyPicker() {
  const router = useRouter();
  const [animatingFamily, setAnimatingFamily] = useState<FamilyKey | null>(null);

  const prewarmFamily = useCallback(
    (family: FamilyKey) => {
      const href = FAMILY_TARGETS[family];
      void router.prefetch(href);
      void preload(FAMILY_API_KEYS[family], apiFetch);
    },
    [router],
  );

  const navigateWithFlight = useCallback(
    async (family: FamilyKey, event: MouseEvent<HTMLAnchorElement>) => {
      event.preventDefault();
      if (animatingFamily) return;

      const href = FAMILY_TARGETS[family];
      prewarmFamily(family);
      if (prefersReducedMotion()) {
        router.push(href);
        return;
      }

      setAnimatingFamily(family);
      window.dispatchEvent(
        new CustomEvent(LANDING_FAMILY_TRANSITION_EVENT, {
          detail: { family },
        }),
      );
      router.push(href);
    },
    [animatingFamily, prewarmFamily, router],
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
        onPointerEnter={() => prewarmFamily("cuse")}
        onFocus={() => prewarmFamily("cuse")}
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
        onPointerEnter={() => prewarmFamily("cpar")}
        onFocus={() => prewarmFamily("cpar")}
        onClick={(event) => void navigateWithFlight("cpar", event)}
      >
        <span className="family-split-prefix">c</span>PAR
      </a>
    </div>
  );
}
