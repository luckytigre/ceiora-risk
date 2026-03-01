"use client";

import { createContext, useContext, useState, useCallback, type ReactNode } from "react";

export type BgMode = "field" | "topo" | "none";

interface BackgroundContextValue {
  mode: BgMode;
  setMode: (m: BgMode) => void;
}

const BackgroundContext = createContext<BackgroundContextValue>({
  mode: "field",
  setMode: () => {},
});

export function BackgroundProvider({ children }: { children: ReactNode }) {
  const [mode, setModeRaw] = useState<BgMode>(() => {
    if (typeof window !== "undefined") {
      return (localStorage.getItem("bg-mode") as BgMode) || "field";
    }
    return "field";
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
