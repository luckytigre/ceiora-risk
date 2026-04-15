"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { FormEvent, Suspense, useMemo, useState } from "react";
import LandingBackgroundLock from "@/components/LandingBackgroundLock";
import { DEFAULT_APP_HOME_PATH, normalizeReturnTo } from "@/lib/appAccess";

const LOGIN_CAPABILITIES = [
  "cUSE exposure reading and factor decomposition",
  "cPAR portfolio risk, hedging, and what-if workflows",
  "Operator health, refresh state, and package publication controls",
] as const;

export default function LoginPage() {
  return (
    <Suspense fallback={<LoginShell status="idle" errorMessage="" configError={false} returnTo={DEFAULT_APP_HOME_PATH} />}>
      <LoginPageInner />
    </Suspense>
  );
}

function LoginPageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [status, setStatus] = useState<"idle" | "submitting" | "failed">("idle");
  const [errorMessage, setErrorMessage] = useState("");
  const returnTo = useMemo(
    () => normalizeReturnTo(searchParams.get("returnTo") || DEFAULT_APP_HOME_PATH),
    [searchParams],
  );
  const configError = searchParams.get("error") === "misconfigured";

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setStatus("submitting");
    setErrorMessage("");
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ username, password, returnTo }),
      });
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        setStatus("failed");
        setErrorMessage(typeof payload?.detail === "string" ? payload.detail : "Could not sign in.");
        return;
      }
      router.replace(typeof payload?.returnTo === "string" ? payload.returnTo : returnTo);
      router.refresh();
    } catch {
      setStatus("failed");
      setErrorMessage("Could not sign in.");
    }
  }

  return (
    <LoginShell
      username={username}
      password={password}
      status={status}
      errorMessage={errorMessage}
      configError={configError}
      returnTo={returnTo}
      onUsernameChange={setUsername}
      onPasswordChange={setPassword}
      onSubmit={handleSubmit}
    />
  );
}

type LoginShellProps = {
  username?: string;
  password?: string;
  status: "idle" | "submitting" | "failed";
  errorMessage: string;
  configError: boolean;
  returnTo: string;
  onUsernameChange?: (value: string) => void;
  onPasswordChange?: (value: string) => void;
  onSubmit?: (event: FormEvent<HTMLFormElement>) => void;
};

function LoginShell({
  username = "",
  password = "",
  status,
  errorMessage,
  configError,
  returnTo,
  onUsernameChange,
  onPasswordChange,
  onSubmit,
}: LoginShellProps) {
  return (
    <>
      <LandingBackgroundLock />
      <div className="public-auth-page">
        <div className="public-auth-shell">
          <section className="public-auth-aside chart-card" aria-label="Ceiora access overview">
            <div className="public-kicker">Shared access</div>
            <h1 className="public-auth-headline">Sign in to the Ceiora operator surface.</h1>
            <p className="public-auth-copy">
              The protected app combines the cUSE and cPAR families with portfolio workflows, diagnostics, and serving controls
              behind one shared login.
            </p>

            <div className="public-auth-capability-list">
              {LOGIN_CAPABILITIES.map((capability) => (
                <div key={capability} className="public-auth-capability">
                  {capability}
                </div>
              ))}
            </div>

            <div className="public-auth-return">
              <span className="public-panel-kicker">Return target</span>
              <code>{returnTo}</code>
            </div>
          </section>

          <form onSubmit={onSubmit} className="public-auth-card chart-card">
            <div className="public-panel-kicker">Authentication</div>
            <h2 className="public-auth-form-title">Use the shared app credentials.</h2>
            <p className="public-auth-form-copy">
              After sign-in you will be routed directly to the requested protected surface.
            </p>

            <div className="public-auth-form-grid">
              <label className="public-auth-field">
                <span>Username</span>
                <input
                  type="text"
                  autoComplete="username"
                  value={username}
                  onChange={(event) => onUsernameChange?.(event.target.value)}
                  placeholder="Shared account username"
                />
              </label>
              <label className="public-auth-field">
                <span>Password</span>
                <input
                  type="password"
                  autoComplete="current-password"
                  value={password}
                  onChange={(event) => onPasswordChange?.(event.target.value)}
                  placeholder="Shared account password"
                />
              </label>
            </div>

            {(configError || errorMessage) && (
              <div className="public-auth-message" aria-live="polite">
                {configError ? "App auth is not configured yet." : errorMessage}
              </div>
            )}

            <div className="public-auth-actions">
              <Link href="/" className="public-secondary-link">
                Back to overview
              </Link>
              <button type="submit" className="btn btn-secondary" disabled={status === "submitting"}>
                {status === "submitting" ? "Signing in..." : "Sign in"}
              </button>
            </div>
          </form>
        </div>
      </div>
    </>
  );
}
