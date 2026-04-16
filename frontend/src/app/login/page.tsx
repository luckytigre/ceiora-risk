"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { FormEvent, Suspense, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import LandingBackgroundLock from "@/components/LandingBackgroundLock";
import { DEFAULT_APP_HOME_PATH, normalizeReturnTo } from "@/lib/appAccess";

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
  const hasDefaultReturnTo =
    searchParams.has("returnTo") &&
    normalizeReturnTo(searchParams.get("returnTo")) === DEFAULT_APP_HOME_PATH &&
    searchParams.get("error") !== "misconfigured";
  const returnTo = useMemo(
    () => normalizeReturnTo(searchParams.get("returnTo") || DEFAULT_APP_HOME_PATH),
    [searchParams],
  );
  const configError = searchParams.get("error") === "misconfigured";

  useEffect(() => {
    if (hasDefaultReturnTo) {
      router.replace("/login");
    }
  }, [hasDefaultReturnTo, router]);

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
      <LandingBackgroundLock bodyClassName="public-topo-boost" />
      <header className="dash-tabs">
        <div className="dash-tabs-brand-cluster">
          <Link href="/" className="dash-tabs-brand">
            Ceiora
          </Link>
        </div>
        <div className="dash-tabs-center" aria-hidden="true" />
        <div className="dash-tabs-actions public-header-empty" aria-hidden="true" />
      </header>
      <div className="public-login-page">
        <div className="public-login-stage">
          <aside className="public-login-masthead">
            <span className="public-login-masthead-rule" aria-hidden="true" />
            <h1 className="public-login-headline">
              <em>Resume</em> where<br />
              you left off
            </h1>
            <p className="public-login-copy">
              Portfolio factor risk &mdash; covariance, exposures, decomposition.
              Opened in a single private session.
            </p>
          </aside>

          <section className="public-login-shell">
            <form onSubmit={onSubmit} className="public-login-form">
              <label className="public-login-field">
                <input
                  type="text"
                  autoComplete="username"
                  value={username}
                  onChange={(event) => onUsernameChange?.(event.target.value)}
                  placeholder="Username"
                />
              </label>

              <label className="public-login-field">
                <input
                  type="password"
                  autoComplete="current-password"
                  value={password}
                  onChange={(event) => onPasswordChange?.(event.target.value)}
                  placeholder="Password"
                />
              </label>

              {(configError || errorMessage) && (
                <div className="public-login-message" aria-live="polite">
                  {configError ? "App auth is not configured yet." : errorMessage}
                </div>
              )}

              <div className="public-login-actions">
                <button type="submit" className="public-login-submit" disabled={status === "submitting"}>
                  {status === "submitting" ? "Signing in" : "Sign in"}
                  <span className="public-login-submit-arrow" aria-hidden="true">&rarr;</span>
                </button>
              </div>
            </form>
          </section>
        </div>
      </div>
    </>
  );
}
