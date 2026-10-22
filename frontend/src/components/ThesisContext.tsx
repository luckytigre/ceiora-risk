"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { thesisQuantityWire } from "@/lib/thesisQuantity";
import { createThesisRequest, deleteThesisRequest, getThesisState, renameThesisRequest, replacePositionAllocationsRequest, saveManualOrderRequest, type ThesisStateDto } from "@/lib/thesisApi";

export interface PortfolioThesis {
  id: string;
  name: string;
  color: string;
  createdAt: string;
}

export interface ThesisAllocation {
  thesisId: string;
  accountId: string;
  ric: string;
  ticker: string;
  quantity: number;
}

interface ThesisState {
  version: 1;
  revision: number;
  theses: PortfolioThesis[];
  allocations: ThesisAllocation[];
  manualOrders: Record<string, string[]>;
}

interface PositionAllocationDraft {
  thesisId: string;
  quantity: number;
}

interface ThesisContextValue extends ThesisState {
  available: boolean;
  ready: boolean;
  busy: boolean;
  loadError: string | null;
  refresh: () => Promise<void>;
  createThesis: (name: string) => Promise<PortfolioThesis | null>;
  renameThesis: (thesisId: string, name: string) => Promise<string | null>;
  deleteThesis: (thesisId: string) => Promise<string | null>;
  saveManualOrder: (scope: string, keys: string[]) => Promise<string | null>;
  replacePositionAllocations: (accountId: string, ric: string, drafts: PositionAllocationDraft[]) => Promise<string | null>;
}

const EMPTY_STATE: ThesisState = { version: 1, revision: 0, theses: [], allocations: [], manualOrders: {} };

function normalizeAccountId(value: string): string {
  return value.trim().toLowerCase();
}

function normalizeRic(value: string): string {
  return value.trim().toUpperCase();
}

function positionKey(accountId: string, ric: string): string {
  return `${normalizeAccountId(accountId)}:${normalizeRic(ric)}`;
}

function fromDto(dto: ThesisStateDto): ThesisState {
  for (const row of dto.allocations) thesisQuantityWire(Number(row.quantity));
  return { version: 1, revision: Number(dto.revision || 0), theses: dto.theses.map((row) => ({ id: row.id, name: row.name, color: row.color, createdAt: row.created_at })), allocations: dto.allocations.map((row) => ({ thesisId: row.thesis_id, accountId: row.account_id, ric: row.ric, ticker: row.ticker, quantity: Number(row.quantity) })), manualOrders: dto.manual_orders ?? {} };
}
function errorMessage(error: unknown): string { return error instanceof Error ? error.message : "Thesis request failed."; }

const ThesisContext = createContext<ThesisContextValue>({
  ...EMPTY_STATE,
  available: false,
  ready: false,
  busy: false,
  loadError: null,
  refresh: async () => {},
  createThesis: async () => null,
  renameThesis: async () => "Thesis service is not ready.",
  deleteThesis: async () => "Thesis service is not ready.",
  saveManualOrder: async () => "Thesis service is not ready.",
  replacePositionAllocations: async () => "Thesis service is not ready.",
});

export function ThesisProvider({ children, workspaceId }: { children: ReactNode; workspaceId: string }) {
  const [state, setState] = useState<ThesisState>(EMPTY_STATE);
  const [ready, setReady] = useState(false);
  const [busy, setBusy] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const stateRef = useRef(state);
  const busyRef = useRef(false);
  const mountedRef = useRef(false);
  const requestRef = useRef(0);
  const [saveStatus, setSaveStatus] = useState<"idle" | "saving" | "saved" | "failed">("idle");
  useEffect(() => {
    mountedRef.current = true;
    return () => { mountedRef.current = false; requestRef.current += 1; };
  }, []);
  const accept = useCallback((dto: ThesisStateDto) => {
    if (dto.workspace_id !== workspaceId || dto.version !== 1 || !Number.isSafeInteger(dto.revision) || dto.revision < 0) {
      throw new Error("The thesis workspace changed. Reload this page before editing.");
    }
    const next = fromDto(dto);
    if (next.revision < stateRef.current.revision) return null;
    stateRef.current = next;
    setState(next);
    setLoadError(null);
    return next;
  }, [workspaceId]);
  const refresh = useCallback(async () => {
    if (busyRef.current) return;
    const ticket = ++requestRef.current;
    try {
      const dto = await getThesisState();
      if (mountedRef.current && ticket === requestRef.current) accept(dto);
    } catch (error) {
      if (mountedRef.current && ticket === requestRef.current) setLoadError(errorMessage(error));
    } finally {
      if (mountedRef.current && ticket === requestRef.current) setReady(true);
    }
  }, [accept]);
  useEffect(() => { void refresh(); }, [refresh]);
  useEffect(() => {
    const onFocus = () => { void refresh(); };
    window.addEventListener("focus", onFocus);
    return () => window.removeEventListener("focus", onFocus);
  }, [refresh]);

  const runMutation = useCallback(async (request: (revision: number) => Promise<ThesisStateDto>) => {
    if (!mountedRef.current || busyRef.current) return { next: null, error: "Another thesis change is still saving." };
    busyRef.current = true;
    setBusy(true);
    setSaveStatus("saving");
    const ticket = ++requestRef.current;
    try {
      const dto = await request(stateRef.current.revision);
      if (!mountedRef.current || ticket !== requestRef.current) return { next: null, error: "The workspace session changed." };
      const next = accept(dto);
      setSaveStatus("saved");
      return { next, error: null };
    } catch (error) {
      const message = errorMessage(error);
      if (mountedRef.current && ticket === requestRef.current) {
        setSaveStatus("failed");
        // Invalidate optimistic order immediately; do not retain an unsaved view.
        setLoadError(message);
        try { const dto = await getThesisState(); if (mountedRef.current && ticket === requestRef.current) accept(dto); } catch { /* Retry remains available. */ }
      }
      return { next: null, error: message };
    } finally {
      busyRef.current = false;
      if (mountedRef.current) setBusy(false);
    }
  }, [accept]);

  const createThesis = useCallback(async (rawName: string): Promise<PortfolioThesis | null> => {
    const name = rawName.trim();
    if (!name) return null;
    const before = new Set(stateRef.current.theses.map((row) => row.id));
    const result = await runMutation((revision) => createThesisRequest(name, revision));
    return result.next?.theses.find((row) => !before.has(row.id)) ?? null;
  }, [runMutation]);

  const renameThesis = useCallback(async (thesisId: string, rawName: string) => {
    const name = rawName.trim();
    if (!name) return "A thesis name is required.";
    return (await runMutation((revision) => renameThesisRequest(thesisId, name, revision))).error;
  }, [runMutation]);

  const deleteThesis = useCallback(async (thesisId: string) => (await runMutation((revision) => deleteThesisRequest(thesisId, revision))).error, [runMutation]);

  const saveManualOrder = useCallback(async (scope: string, keys: string[]) => {
    if (busyRef.current) return "Another thesis change is still saving.";
    const current = stateRef.current;
    const optimistic = { ...current, manualOrders: { ...current.manualOrders, [scope]: keys } };
    stateRef.current = optimistic;
    setState(optimistic);
    return (await runMutation((revision) => saveManualOrderRequest(scope, keys, revision))).error;
  }, [runMutation]);

  const replacePositionAllocations = useCallback(async (
    rawAccountId: string,
    rawRic: string,
    drafts: PositionAllocationDraft[],
  ): Promise<string | null> => {
    const accountId = normalizeAccountId(rawAccountId);
    const ric = normalizeRic(rawRic);
    if (!accountId || !ric) return "A concrete account and RIC are required.";
    const normalized = drafts
      .map((draft) => ({ thesisId: draft.thesisId, quantity: Number(draft.quantity) }))
      .filter((draft) => draft.thesisId && Number.isFinite(draft.quantity) && Math.abs(draft.quantity) > 1e-9);
    return (await runMutation((revision) => replacePositionAllocationsRequest({ accountId, ric, expectedRevision: revision, allocations: normalized }))).error;
  }, [runMutation]);

  const value = useMemo<ThesisContextValue>(() => ({
    ...state,
    available: ready && !loadError,
    ready,
    busy,
    loadError,
    refresh,
    createThesis,
    renameThesis,
    deleteThesis,
    saveManualOrder,
    replacePositionAllocations,
  }), [busy, createThesis, deleteThesis, loadError, ready, refresh, renameThesis, replacePositionAllocations, saveManualOrder, state]);

  return <ThesisContext.Provider value={value}>
    {(loadError || saveStatus !== "idle") ? <div className="thesis-save-feedback" role="status">
      {loadError ? (loadError.startsWith("The thesis workspace changed") ? loadError : "Thesis workspace unavailable. Instrument view remains available.") : saveStatus === "saving" ? "Saving thesis changes…" : saveStatus === "saved" ? "Thesis changes saved." : "Thesis change failed. Review the table message and try again."}
      {loadError ? <button type="button" className="btn-action" onClick={() => { void refresh(); }}>Retry</button> : null}
    </div> : null}
    {children}
  </ThesisContext.Provider>;
}

export function useTheses(): ThesisContextValue {
  return useContext(ThesisContext);
}

export function thesisPositionKey(accountId: string, ric: string): string {
  return positionKey(accountId, ric);
}

export function manualPositionKey(accountId: string, ric: string): string {
  return positionKey(accountId, ric);
}
