"use client";

import { createContext, useCallback, useContext, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useAuthSession } from "@/components/AuthSessionContext";
import { sessionPrincipalKey } from "@/lib/authSessionBootstrap";
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
 * Requests from an obsolete login are cancelled; missing provider wiring throws.
 */
export function ConfirmDialogProvider({ children }: { children: ReactNode }) {
  const { authenticated, session, context } = useAuthSession();
  const principal = authenticated ? JSON.stringify([sessionPrincipalKey(session), [...(context?.account_ids ?? [])].sort()]) : "";
  const principalRef = useRef(principal);
  const resolverScopeRef = useRef(principal);
  const [request, setRequest] = useState<(ConfirmRequest & { scope: string }) | null>(null);
  const resolverRef = useRef<((value: boolean) => void) | null>(null);

  const settle = useCallback((value: boolean) => {
    const resolve = resolverRef.current;
    resolverRef.current = null;
    setRequest(null);
    resolve?.(value && resolverScopeRef.current === principalRef.current);
  }, []);

  useLayoutEffect(() => {
    principalRef.current = principal;
    settle(false);
  }, [principal, settle]);
  useLayoutEffect(() => () => { resolverRef.current?.(false); resolverRef.current = null; }, []);

  const confirm = useCallback<ConfirmFn>((next) => {
    if (!principal || principalRef.current !== principal) return Promise.resolve(false);
    // A second request while one is open resolves the first as cancelled rather
    // than orphaning its promise.
    resolverRef.current?.(false);
    resolverScopeRef.current = principal;
    setRequest({ ...next, scope: principal });
    return new Promise<boolean>((resolve) => {
      resolverRef.current = resolve;
    });
  }, [principal]);

  const visible = request?.scope === principal ? request : null;
  const value = useMemo(() => confirm, [confirm]);

  return (
    <ConfirmContext.Provider value={value}>
      {children}
      <ConfirmActionModal
        open={Boolean(visible)}
        title={visible?.title ?? ""}
        body={visible?.body ?? ""}
        dangerText={visible?.dangerText ?? "Confirm"}
        confirmValue={visible?.confirmValue ?? null}
        confirmLabel={visible?.confirmLabel ?? "Type to confirm"}
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
