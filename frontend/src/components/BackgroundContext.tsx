"use client";

import { createContext, useContext, useState, useCallback, type ReactNode } from "react";

export type BgMode = "topo" | "flow" | "none";

interface BackgroundContextValue {
  mode: BgMode;
  setMode: (m: BgMode) => void;
}

const BackgroundContext = createContext<BackgroundContextValue>({
  mode: "topo",
  setMode: () => {},
});

export function BackgroundProvider({ children }: { children: ReactNode }) {
  const [mode, setModeRaw] = useState<BgMode>(() => {
    if (typeof window !== "undefined") {
      const stored = String(localStorage.getItem("bg-mode") || "").trim().toLowerCase();
      if (stored === "none") return "none";
      if (stored === "flow") return "flow";
      return "topo";
    }
    return "topo";
  });

  const setMode = useCallback((m: BgMode) => {
    setModeRaw(m);
    if (typeof window !== "undefined") {
      localStorage.setItem("bg-mode", m);
    }
  }, []);

  return (
    <BackgroundContext.Provider value={{ mode, setMode }}>
      {children}
    </BackgroundContext.Provider>
  );
}

export function useBackground() {
  return useContext(BackgroundContext);
}
