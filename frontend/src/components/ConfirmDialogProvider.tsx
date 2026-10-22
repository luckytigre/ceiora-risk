"use client";

import { createContext, useCallback, useContext, useMemo, useRef, useState, type ReactNode } from "react";
import ConfirmActionModal from "@/components/ConfirmActionModal";

export interface ConfirmRequest {
  title: string;
  body: string;
  /** Label on the destructive action. Say what will happen, not "OK". */
  dangerText?: string;
  /** When set, the user must type this value before the action is enabled. */
  confirmValue?: string | null;
  confirmLabel?: string;
}

type ConfirmFn = (request: ConfirmRequest) => Promise<boolean>;

const ConfirmContext = createContext<ConfirmFn | null>(null);

/**
 * UX-02 — a promise-based replacement for window.confirm().
 *
 * Five call sites used the native dialog, three of them inside hooks where local
 * modal state is awkward. `const confirm = useConfirm()` then
 * `if (!(await confirm({...}))) return;` reads the same as the code it replaces,
 * so the call sites stay legible.
 *
 * Falls back to `true` only if the provider is missing, which would be a wiring
 * bug; it never silently degrades to window.confirm.
 */
export function ConfirmDialogProvider({ children }: { children: ReactNode }) {
  const [request, setRequest] = useState<ConfirmRequest | null>(null);
  const resolverRef = useRef<((value: boolean) => void) | null>(null);

  const settle = useCallback((value: boolean) => {
    const resolve = resolverRef.current;
    resolverRef.current = null;
    setRequest(null);
    resolve?.(value);
  }, []);

  const confirm = useCallback<ConfirmFn>((next) => {
    // A second request while one is open resolves the first as cancelled rather
    // than orphaning its promise.
    resolverRef.current?.(false);
    setRequest(next);
    return new Promise<boolean>((resolve) => {
      resolverRef.current = resolve;
    });
  }, []);

  const value = useMemo(() => confirm, [confirm]);

  return (
    <ConfirmContext.Provider value={value}>
      {children}
      <ConfirmActionModal
        open={Boolean(request)}
        title={request?.title ?? ""}
        body={request?.body ?? ""}
        dangerText={request?.dangerText ?? "Confirm"}
        confirmValue={request?.confirmValue ?? null}
        confirmLabel={request?.confirmLabel ?? "Type to confirm"}
        onCancel={() => settle(false)}
        onConfirm={() => settle(true)}
      />
    </ConfirmContext.Provider>
  );
}

export function useConfirm(): ConfirmFn {
  const ctx = useContext(ConfirmContext);
  if (!ctx) {
    throw new Error("useConfirm must be used inside <ConfirmDialogProvider>");
  }
  return ctx;
}

export default ConfirmDialogProvider;
