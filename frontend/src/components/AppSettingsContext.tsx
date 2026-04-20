"use client";

import { createContext, useCallback, useContext, useLayoutEffect, useState, type ReactNode } from "react";
import { applyChartDefaults } from "@/lib/charts/chartTheme";

export type CparFactorHistoryMode = "residual" | "market_adjusted";
export type ThemeMode = "dark" | "light";

interface AppSettingsContextValue {
  cparFactorHistoryMode: CparFactorHistoryMode;
  setCparFactorHistoryMode: (mode: CparFactorHistoryMode) => void;
  themeMode: ThemeMode;
  setThemeMode: (mode: ThemeMode) => void;
}

const DEFAULT_CPAR_FACTOR_HISTORY_MODE: CparFactorHistoryMode = "market_adjusted";
const CPAR_FACTOR_HISTORY_MODE_KEY = "cpar-factor-history-mode";
const DEFAULT_THEME_MODE: ThemeMode = "dark";
const THEME_MODE_KEY = "theme-mode";

export function resolveStoredThemeMode(): ThemeMode {
  if (typeof window === "undefined") return DEFAULT_THEME_MODE;
  const stored = String(localStorage.getItem(THEME_MODE_KEY) || "").trim().toLowerCase();
  return stored === "light" ? "light" : DEFAULT_THEME_MODE;
}

const AppSettingsContext = createContext<AppSettingsContextValue>({
  cparFactorHistoryMode: DEFAULT_CPAR_FACTOR_HISTORY_MODE,
  setCparFactorHistoryMode: () => {},
  themeMode: DEFAULT_THEME_MODE,
  setThemeMode: () => {},
});

export function AppSettingsProvider({ children }: { children: ReactNode }) {
  const [cparFactorHistoryMode, setCparFactorHistoryModeRaw] = useState<CparFactorHistoryMode>(() => {
    if (typeof window === "undefined") return DEFAULT_CPAR_FACTOR_HISTORY_MODE;
    const stored = String(localStorage.getItem(CPAR_FACTOR_HISTORY_MODE_KEY) || "").trim().toLowerCase();
    return stored === "residual" ? "residual" : DEFAULT_CPAR_FACTOR_HISTORY_MODE;
  });
  const [themeMode, setThemeModeRaw] = useState<ThemeMode>(() => {
    return resolveStoredThemeMode();
  });

  const setCparFactorHistoryMode = useCallback((mode: CparFactorHistoryMode) => {
    setCparFactorHistoryModeRaw(mode);
    if (typeof window !== "undefined") {
      localStorage.setItem(CPAR_FACTOR_HISTORY_MODE_KEY, mode);
    }
  }, []);

  const setThemeMode = useCallback((mode: ThemeMode) => {
    setThemeModeRaw(mode);
    if (typeof window !== "undefined") {
      localStorage.setItem(THEME_MODE_KEY, mode);
    }
  }, []);

  useLayoutEffect(() => {
    const root = document.documentElement;
    const body = document.body;
    root.dataset.theme = themeMode;
    body.dataset.theme = themeMode;
    root.style.colorScheme = themeMode;
    body.style.colorScheme = themeMode;
    applyChartDefaults();
  }, [themeMode]);

  return (
    <AppSettingsContext.Provider
      value={{
        cparFactorHistoryMode,
        setCparFactorHistoryMode,
        themeMode,
        setThemeMode,
      }}
    >
      {children}
    </AppSettingsContext.Provider>
  );
}

export function useAppSettings() {
  return useContext(AppSettingsContext);
}
