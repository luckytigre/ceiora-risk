"use client";

import { createInternalNeonAuth } from "@neondatabase/auth";
import { useRouter, useSearchParams } from "next/navigation";
import { FormEvent, Suspense, useEffect, useMemo, useState } from "react";
import BrandLockup from "@/components/BrandLockup";
import LandingBackgroundLock from "@/components/LandingBackgroundLock";
import { DEFAULT_APP_HOME_PATH, normalizeReturnTo } from "@/lib/appAccess";
import type { AppAuthProvider } from "@/lib/appAuth";

type LoginClientProps = {
  provider: AppAuthProvider;
  authConfigured: boolean;
  neonProjectUrl: string;
  sharedLoginAllowed: boolean;
};

export default function LoginClient(props: LoginClientProps) {
  return (
    <Suspense
      fallback={
        <LoginShell
          provider={props.provider}
          authConfigured={props.authConfigured}
          neonProjectUrl={props.neonProjectUrl}
          sharedLoginAllowed={props.sharedLoginAllowed}
          status="idle"
          errorMessage=""
          configError={!props.authConfigured}
          returnTo={DEFAULT_APP_HOME_PATH}
        />
      }
    >
      <LoginPageInner {...props} />
    </Suspense>
  );
}

function formatSharedLoginError(error: unknown): string {
  const message =
    typeof error === "object" &&
    error !== null &&
    "message" in error &&
    typeof (error as { message?: unknown }).message === "string"
      ? String((error as { message: string }).message)
      : error instanceof Error
        ? error.message
        : "";
  return message || "Could not sign in.";
}

function parseApiDetail(payload: unknown): { message: string; code: string | null } {
  if (!payload || typeof payload !== "object") return { message: "", code: null };
  const detail = (payload as { detail?: unknown }).detail;
  if (typeof detail === "string") return { message: detail, code: null };
  if (!detail || typeof detail !== "object") return { message: "", code: null };
  return {
    message:
      typeof (detail as { message?: unknown }).message === "string"
        ? String((detail as { message: string }).message)
        : "",
    code:
      typeof (detail as { code?: unknown }).code === "string"
        ? String((detail as { code: string }).code)
        : null,
  };
}

function formatNeonLoginError(mode: "signin" | "signup", error: unknown, code?: string | null): string {
  const message =
    typeof error === "object" &&
    error !== null &&
    "message" in error &&
    typeof (error as { message?: unknown }).message === "string"
      ? String((error as { message: string }).message)
      : error instanceof Error
        ? error.message
        : "";
  if (mode === "signup" && /verify|inbox|confirmation|confirm/i.test(message)) {
    return "Account created. Check your inbox to continue.";
  }
  if (code === "account_provisioning_required") {
    return "Your Ceiora account is still being prepared. Try again in a moment.";
  }
  if (code === "account_context_unavailable") {
    return "Your Neon identity is valid, but Ceiora could not load your account context.";
  }
  if (code === "account_bootstrap_disabled") {
    return "Neon sign-in is working, but automatic personal workspace creation is disabled right now.";
  }
  if (message) return message;
  return mode === "signup" ? "Could not create account." : "Could not sign in.";
}

function shouldPreserveNeonIdentity(code: string | null | undefined): boolean {
  return (
    code === "account_provisioning_required" ||
    code === "account_context_unavailable" ||
    code === "account_bootstrap_disabled"
  );
}

function LoginPageInner({ provider, authConfigured, neonProjectUrl, sharedLoginAllowed }: LoginClientProps) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [mode, setMode] = useState<"signin" | "signup">("signin");
  const [status, setStatus] = useState<"idle" | "submitting" | "failed">("idle");
  const [errorMessage, setErrorMessage] = useState("");
  const [errorCode, setErrorCode] = useState<string | null>(null);
  const hasDefaultReturnTo =
    searchParams.has("returnTo") &&
    normalizeReturnTo(searchParams.get("returnTo")) === DEFAULT_APP_HOME_PATH &&
    searchParams.get("error") !== "misconfigured";
  const returnTo = useMemo(
    () => normalizeReturnTo(searchParams.get("returnTo") || DEFAULT_APP_HOME_PATH),
    [searchParams],
  );
  const configError = !authConfigured || searchParams.get("error") === "misconfigured";
  const routeError = searchParams.get("error");
  const neonAuth = useMemo(
    () => (provider === "neon" && neonProjectUrl ? createInternalNeonAuth(neonProjectUrl) : null),
    [provider, neonProjectUrl],
  );

  async function clearBrowserAuthState() {
    await fetch("/api/auth/logout", {
      method: "POST",
      credentials: "same-origin",
      cache: "no-store",
    }).catch(() => {});
    await neonAuth?.adapter.signOut().catch(() => {});
  }

  useEffect(() => {
    if (hasDefaultReturnTo) {
      router.replace("/login");
    }
  }, [hasDefaultReturnTo, router]);

  async function submitSharedLogin() {
    const res = await fetch("/api/auth/login", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ provider: "shared", username, password, returnTo }),
    });
    const payload = await res.json().catch(() => ({}));
    if (!res.ok) {
      const detail = parseApiDetail(payload);
      throw new Error(detail.message || formatSharedLoginError(null));
    }
    return payload;
  }

  async function submitNeonLogin() {
    if (!neonAuth) {
      throw new Error("Neon Auth is not configured.");
    }
    const email = username.trim();
    const cleanPassword = String(password || "");
    if (!email || !cleanPassword) {
      throw new Error(mode === "signup" ? "Email and password are required to create an account." : "Email and password are required.");
    }

    await neonAuth.adapter.signOut().catch(() => {});

    if (mode === "signup") {
      await neonAuth.adapter.signUp.email({
        email,
        password: cleanPassword,
        name: displayName.trim() || email.split("@")[0] || "Ceiora user",
      });
    }

    let idToken = await neonAuth.getJWTToken();
    if (!idToken) {
      await neonAuth.adapter.signIn.email({ email, password: cleanPassword });
      idToken = await neonAuth.getJWTToken();
    }
    if (!idToken) {
      throw new Error("Check your inbox to continue.");
    }

    const res = await fetch("/api/auth/login", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ provider: "neon", idToken, returnTo }),
    });
    const payload = await res.json().catch(() => ({}));
    if (!res.ok) {
      const detail = parseApiDetail(payload);
      const error = new Error(detail.message || formatNeonLoginError(mode, null, detail.code));
      (error as Error & { code?: string | null }).code = detail.code;
      throw error;
    }
    return payload;
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setStatus("submitting");
    setErrorMessage("");
    setErrorCode(null);
    try {
      const payload = provider === "neon" ? await submitNeonLogin() : await submitSharedLogin();
      const destination = typeof payload?.returnTo === "string" ? payload.returnTo : returnTo;
      window.location.replace(destination);
    } catch (error) {
      const code =
        typeof error === "object" &&
        error !== null &&
        "code" in error &&
        typeof (error as { code?: unknown }).code === "string"
          ? String((error as { code: string }).code)
          : null;
      if (provider === "neon" && !shouldPreserveNeonIdentity(code)) {
        await clearBrowserAuthState();
      }
      setStatus("failed");
      if (provider === "neon") {
        setErrorCode(code);
        setErrorMessage(formatNeonLoginError(mode, error, code));
      } else {
        setErrorMessage(formatSharedLoginError(error));
      }
    }
  }

  return (
      <LoginShell
      provider={provider}
      authConfigured={authConfigured}
      neonProjectUrl={neonProjectUrl}
      sharedLoginAllowed={sharedLoginAllowed}
      username={username}
      password={password}
      displayName={displayName}
      mode={mode}
      status={status}
      errorMessage={errorMessage}
      errorCode={errorCode}
      configError={configError}
      routeError={routeError}
      returnTo={returnTo}
      onUsernameChange={setUsername}
      onPasswordChange={setPassword}
      onDisplayNameChange={setDisplayName}
      onModeChange={setMode}
      onSubmit={handleSubmit}
    />
  );
}

type LoginShellProps = {
  provider: AppAuthProvider;
  authConfigured: boolean;
  neonProjectUrl: string;
  sharedLoginAllowed: boolean;
  username?: string;
  password?: string;
  displayName?: string;
  mode?: "signin" | "signup";
  status: "idle" | "submitting" | "failed";
  errorMessage: string;
  errorCode?: string | null;
  configError: boolean;
  routeError?: string | null;
  returnTo: string;
  onUsernameChange?: (value: string) => void;
  onPasswordChange?: (value: string) => void;
  onDisplayNameChange?: (value: string) => void;
  onModeChange?: (value: "signin" | "signup") => void;
  onSubmit?: (event: FormEvent<HTMLFormElement>) => void;
};

function LoginShell({
  provider,
  authConfigured,
  neonProjectUrl,
  sharedLoginAllowed,
  username = "",
  password = "",
  displayName = "",
  mode = "signin",
  status,
  errorMessage,
  errorCode = null,
  configError,
  routeError = null,
  returnTo,
  onUsernameChange,
  onPasswordChange,
  onDisplayNameChange,
  onModeChange,
  onSubmit,
}: LoginShellProps) {
  const neonConfigured = provider !== "neon" || Boolean(neonProjectUrl);
  const sharedDisabled = provider === "shared" && !sharedLoginAllowed;
  const providerLabel = provider === "neon" ? "Neon Auth" : "Shared access";
  const submitLabel =
    provider === "neon" ? (mode === "signup" ? "Create account" : "Sign in") : "Sign in";
  const routeErrorMessage =
    routeError === "account_context_unavailable"
      ? "Your last session no longer has a usable Ceiora account context. Sign in again to restore your workspace."
      : routeError === "account_provisioning_required"
        ? "Your Neon identity is valid, but Ceiora is still preparing your personal workspace."
      : routeError === "account_bootstrap_disabled"
        ? "Neon sign-in is working, but automatic personal workspace creation is disabled right now."
      : routeError === "session_expired"
        ? "Your previous session is no longer valid. Sign in again to continue."
      : "";
  const showCredentialFields = !(provider === "shared" && sharedDisabled);

  return (
    <>
      <LandingBackgroundLock bodyClassName="public-topo-boost" />
      <header className="dash-tabs">
        <div className="dash-tabs-brand-cluster">
          <BrandLockup
            href="/"
            className="dash-tabs-brand"
            markClassName="dash-tabs-brand-mark"
            wordmarkClassName="dash-tabs-brand-wordmark"
            markTitle="Ceiora"
          />
        </div>
        <div className="dash-tabs-center" aria-hidden="true" />
        <div className="dash-tabs-actions public-header-empty" aria-hidden="true" />
      </header>
      <div className="public-login-page">
        <div className="public-login-stage">
          <aside className="public-login-masthead">
            <span className="public-login-masthead-rule" aria-hidden="true" />
            <h1 className="public-login-headline">
              <em>{provider === "neon" ? "Pick up" : "Resume"}</em> where<br />
              you left off
            </h1>
            <p className="public-login-copy">
              {provider === "neon"
                ? "Personal accounts, personal portfolios, one private session at a time."
                : "Portfolio factor risk — covariance, exposures, decomposition. Opened in a single private session."}
            </p>
          </aside>

          <section className="public-login-shell">
            <form onSubmit={onSubmit} className="public-login-form">
              <div className="public-login-provider-row">
                <span className="public-login-provider-label">{providerLabel}</span>
                {provider === "neon" && showCredentialFields && (
                  <div className="public-login-mode-toggle" role="tablist" aria-label="Authentication mode">
                    <button
                      type="button"
                      className={mode === "signin" ? "is-active" : undefined}
                      onClick={() => onModeChange?.("signin")}
                    >
                      Sign in
                    </button>
                    <button
                      type="button"
                      className={mode === "signup" ? "is-active" : undefined}
                      onClick={() => onModeChange?.("signup")}
                    >
                      Create account
                    </button>
                  </div>
                )}
              </div>

              {provider === "neon" && mode === "signup" && showCredentialFields && (
                <label className="public-login-field">
                  <input
                    type="text"
                    autoComplete="name"
                    value={displayName}
                    onChange={(event) => onDisplayNameChange?.(event.target.value)}
                    placeholder=" "
                  />
                  <span className="public-login-field-label">Display name</span>
                </label>
              )}

              {showCredentialFields ? (
                <>
                  <label className="public-login-field">
                    <input
                      type={provider === "neon" ? "email" : "text"}
                      autoComplete={provider === "neon" ? "email" : "username"}
                      value={username}
                      onChange={(event) => onUsernameChange?.(event.target.value)}
                      placeholder=" "
                    />
                    <span className="public-login-field-label">
                      {provider === "neon" ? "Email" : "Username"}
                    </span>
                  </label>

                  <label className="public-login-field">
                    <input
                      type="password"
                      autoComplete={provider === "neon" && mode === "signup" ? "new-password" : "current-password"}
                      value={password}
                      onChange={(event) => onPasswordChange?.(event.target.value)}
                      placeholder=" "
                    />
                    <span className="public-login-field-label">Password</span>
                  </label>
                </>
              ) : null}

              {(configError || errorMessage || !neonConfigured || routeErrorMessage) && (
                <div className="public-login-message" aria-live="polite">
                  {configError
                    ? "App auth is not configured yet."
                    : !neonConfigured
                      ? "Neon Auth base URL is not configured."
                      : errorMessage || routeErrorMessage}
                  {provider === "neon" &&
                  (errorCode === "account_provisioning_required" ||
                    routeError === "account_provisioning_required" ||
                    errorCode === "account_bootstrap_disabled" ||
                    routeError === "account_bootstrap_disabled") ? (
                    <div className="public-login-error-note">
                      {errorCode === "account_bootstrap_disabled" || routeError === "account_bootstrap_disabled"
                        ? "Ask the operator to enable personal-account bootstrap before trying again."
                        : "Ceiora creates your personal workspace automatically on first sign-in."}
                    </div>
                  ) : null}
                </div>
              )}
              {sharedDisabled && (
                <div className="public-login-message" aria-live="polite">
                  Shared login is disabled while account-scoped Neon enforcement is enabled. Use Neon Auth to enter your personal workspace.
                </div>
              )}

              {showCredentialFields ? (
                <div className="public-login-actions">
                  <button
                    type="submit"
                    className="public-login-submit"
                    disabled={status === "submitting" || !authConfigured || sharedDisabled}
                  >
                    {status === "submitting" ? `${submitLabel}…` : submitLabel}
                    <span className="public-login-submit-arrow" aria-hidden="true">&rarr;</span>
                  </button>
                </div>
              ) : null}

              <input type="hidden" name="returnTo" value={returnTo} readOnly />
            </form>
          </section>
        </div>
      </div>
    </>
  );
}
