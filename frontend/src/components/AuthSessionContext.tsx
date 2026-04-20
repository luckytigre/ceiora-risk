"use client";

import { usePathname } from "next/navigation";
import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { isProtectedPagePath } from "@/lib/appAccess";
import type { AppAuthContextPayload } from "@/app/api/auth/_context";
import type { AppSessionPayload, AuthSessionBootstrapPayload } from "@/lib/authSessionBootstrap";

const BOOTSTRAP_REFRESH_DELAY_MS = 2000;
const PRESERVED_BACKGROUND_ERROR_CODES = new Set([
  "account_context_unavailable",
  "account_provisioning_required",
  "account_bootstrap_disabled",
]);

type AuthSessionState = {
  loading: boolean;
  authenticated: boolean;
  session: AppSessionPayload | null;
  context: AppAuthContextPayload | null;
  contextErrorCode: string | null;
  backgroundError: string | null;
  neonProjectUrl: string;
  error: string | null;
  refresh: () => Promise<void>;
};

const AuthSessionContext = createContext<AuthSessionState>({
  loading: true,
  authenticated: false,
  session: null,
  context: null,
  contextErrorCode: null,
  backgroundError: null,
  neonProjectUrl: "",
  error: null,
  refresh: async () => {},
});

class SessionStateError extends Error {
  status: number;
  code: string | null;

  constructor(status: number, message: string, code?: string | null) {
    super(message);
    this.name = "SessionStateError";
    this.status = status;
    this.code = code ?? null;
  }
}

async function loadSessionState() {
  const res = await fetch("/api/auth/session", {
    method: "GET",
    credentials: "same-origin",
    cache: "no-store",
  });
  if (!res.ok) {
    const payload = await res.json().catch(() => ({}));
    const detail =
      typeof payload?.detail === "string"
        ? payload.detail
        : typeof payload?.detail?.message === "string"
          ? payload.detail.message
          : "";
    const code =
      typeof payload?.detail?.code === "string"
        ? payload.detail.code
        : typeof payload?.detail?.error === "string"
          ? payload.detail.error
          : null;
    throw new SessionStateError(res.status, detail || `Session read failed (${res.status})`, code);
  }
  return (await res.json()) as {
    authenticated: boolean;
    session?: AppSessionPayload;
    context?: AppAuthContextPayload | null;
    contextError?: { message?: string; code?: string | null } | null;
  };
}

export function AuthSessionProvider({
  children,
  neonProjectUrl = "",
  initialState = null,
}: {
  children: ReactNode;
  neonProjectUrl?: string;
  initialState?: AuthSessionBootstrapPayload | null;
}) {
  const pathname = usePathname();
  const bootstrapped = Boolean(initialState?.authenticated);
  const [loading, setLoading] = useState(!bootstrapped);
  const [authenticated, setAuthenticated] = useState(Boolean(initialState?.authenticated));
  const [session, setSession] = useState<AppSessionPayload | null>(initialState?.session ?? null);
  const [context, setContext] = useState<AppAuthContextPayload | null>(initialState?.context ?? null);
  const [contextErrorCode, setContextErrorCode] = useState<string | null>(initialState?.contextError?.code ?? null);
  const [backgroundError, setBackgroundError] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(initialState?.contextError?.message ?? null);
  const [booted, setBooted] = useState(bootstrapped);

  const refresh = useCallback(async (options?: { background?: boolean; preserveStateOnError?: boolean }) => {
    const background = options?.background === true;
    const preserveStateOnError = options?.preserveStateOnError === true;
    const hasStableSession = authenticated || session !== null || context !== null;
    if (!booted && !background) setLoading(true);
    try {
      const payload = await loadSessionState();
      setBackgroundError(null);
      setAuthenticated(Boolean(payload.authenticated));
      setSession(payload.authenticated ? payload.session ?? null : null);
      setContext(payload.authenticated ? payload.context ?? null : null);
      setContextErrorCode(payload.authenticated ? payload.contextError?.code ?? null : null);
      setError(payload.authenticated ? payload.contextError?.message ?? null : null);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Session service unavailable.";
      const backgroundSafeToPreserve =
        preserveStateOnError
        && hasStableSession
        && err instanceof SessionStateError
        && (
          err.status === 409
          || PRESERVED_BACKGROUND_ERROR_CODES.has(String(err.code || ""))
        );
      if (backgroundSafeToPreserve) {
        setBackgroundError(message || "Session service unavailable.");
      } else {
        setAuthenticated(false);
        setSession(null);
        setContext(null);
        setContextErrorCode(null);
        setError(message || "Session service unavailable.");
      }
    } finally {
      setLoading(false);
      setBooted(true);
    }
  }, [authenticated, booted, context, session]);

  useEffect(() => {
    if (!pathname || !isProtectedPagePath(pathname)) {
      setLoading(false);
      return;
    }
    if (bootstrapped) return;
    const abort = new AbortController();
    void (async () => {
      if (abort.signal.aborted) return;
      await refresh();
    })();
    return () => {
      abort.abort();
    };
  }, [bootstrapped, pathname, refresh]);

  useEffect(() => {
    if (!pathname || !isProtectedPagePath(pathname) || !bootstrapped) {
      return;
    }
    const timer = window.setTimeout(() => {
      void refresh({ background: true, preserveStateOnError: true });
    }, BOOTSTRAP_REFRESH_DELAY_MS);
    return () => {
      window.clearTimeout(timer);
    };
  }, [bootstrapped, pathname, refresh]);

  useEffect(() => {
    if (!pathname || !isProtectedPagePath(pathname) || !bootstrapped) return;
    const refreshVisibleState = () => {
      if (document.visibilityState !== "visible") return;
      void refresh({ background: true, preserveStateOnError: true });
    };
    const onFocus = () => refreshVisibleState();
    const onVisibility = () => refreshVisibleState();
    window.addEventListener("focus", onFocus);
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      window.removeEventListener("focus", onFocus);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [bootstrapped, pathname, refresh]);

  const value = useMemo(
    () => ({ loading, authenticated, session, context, contextErrorCode, backgroundError, neonProjectUrl, error, refresh }),
    [authenticated, backgroundError, context, contextErrorCode, error, loading, neonProjectUrl, refresh, session],
  );

  return <AuthSessionContext.Provider value={value}>{children}</AuthSessionContext.Provider>;
}

export function useAuthSession() {
  return useContext(AuthSessionContext);
}
