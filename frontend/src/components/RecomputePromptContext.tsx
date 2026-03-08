"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

const STORAGE_KEY = "recompute-pending";
const COUNT_KEY = "recompute-pending-count";
const DIRTY_SINCE_KEY = "recompute-pending-since";

interface RecomputePromptValue {
  pending: boolean;
  pendingCount: number;
  dirtySince: string | null;
  markPending: (delta?: number) => void;
  clearPending: () => void;
}

const RecomputePromptContext = createContext<RecomputePromptValue>({
  pending: false,
  pendingCount: 0,
  dirtySince: null,
  markPending: () => {},
  clearPending: () => {},
});

export function RecomputePromptProvider({ children }: { children: ReactNode }) {
  const [pending, setPending] = useState<boolean>(false);
  const [pendingCount, setPendingCount] = useState<number>(0);
  const [dirtySince, setDirtySince] = useState<string | null>(null);

  useEffect(() => {
    const nextPending = window.localStorage.getItem(STORAGE_KEY) === "1";
    const rawCount = window.localStorage.getItem(COUNT_KEY);
    const parsedCount = Number(rawCount);
    const nextCount = Number.isFinite(parsedCount) && parsedCount > 0 ? Math.floor(parsedCount) : 0;
    const rawDirtySince = window.localStorage.getItem(DIRTY_SINCE_KEY);
    const nextDirtySince = rawDirtySince && rawDirtySince.trim().length > 0 ? rawDirtySince : null;

    setPending(nextPending);
    setPendingCount(nextCount);
    setDirtySince(nextDirtySince);
  }, []);

  useEffect(() => {
    const onStorage = (event: StorageEvent) => {
      if (event.key === STORAGE_KEY) {
        setPending((event.newValue || "") === "1");
      }
      if (event.key === COUNT_KEY) {
        const parsed = Number(event.newValue || "0");
        setPendingCount(Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 0);
      }
      if (event.key === DIRTY_SINCE_KEY) {
        const next = (event.newValue || "").trim();
        setDirtySince(next.length > 0 ? next : null);
      }
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  const markPending = useCallback((delta = 1) => {
    const safeDelta = Number.isFinite(delta) && delta > 0 ? Math.floor(delta) : 1;
    setPending(true);
    setPendingCount((prev) => prev + safeDelta);
    setDirtySince((prev) => prev ?? new Date().toISOString());
    if (typeof window !== "undefined") {
      window.localStorage.setItem(STORAGE_KEY, "1");
      const prev = Number(window.localStorage.getItem(COUNT_KEY) || "0");
      const next = (Number.isFinite(prev) && prev > 0 ? Math.floor(prev) : 0) + safeDelta;
      window.localStorage.setItem(COUNT_KEY, String(next));
      if (!window.localStorage.getItem(DIRTY_SINCE_KEY)) {
        window.localStorage.setItem(DIRTY_SINCE_KEY, new Date().toISOString());
      }
    }
  }, []);

  const clearPending = useCallback(() => {
    setPending(false);
    setPendingCount(0);
    setDirtySince(null);
    if (typeof window !== "undefined") {
      window.localStorage.removeItem(STORAGE_KEY);
      window.localStorage.removeItem(COUNT_KEY);
      window.localStorage.removeItem(DIRTY_SINCE_KEY);
    }
  }, []);

  const value = useMemo(
    () => ({ pending, pendingCount, dirtySince, markPending, clearPending }),
    [pending, pendingCount, dirtySince, markPending, clearPending],
  );

  return (
    <RecomputePromptContext.Provider value={value}>
      {children}
    </RecomputePromptContext.Provider>
  );
}

export function useRecomputePrompt() {
  return useContext(RecomputePromptContext);
}
