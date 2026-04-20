"use client";

import { useEffect, useRef } from "react";
import { useBackground, type BgMode } from "./BackgroundContext";

export default function LandingBackgroundLock({ bodyClassName }: { bodyClassName?: string }) {
  const { mode, setMode } = useBackground();
  const initialModeRef = useRef<BgMode | null>(null);

  if (initialModeRef.current === null) {
    initialModeRef.current = mode;
  }

  useEffect(() => {
    const previousMode = initialModeRef.current ?? "topo";
    if (previousMode !== "topo") {
      setMode("topo");
    }

    return () => {
      if (previousMode !== "topo") {
        setMode(previousMode);
      }
    };
  }, [setMode]);

  useEffect(() => {
    if (!bodyClassName || typeof document === "undefined") return;
    document.body.classList.add(bodyClassName);
    return () => {
      document.body.classList.remove(bodyClassName);
    };
  }, [bodyClassName]);

  return null;
}
