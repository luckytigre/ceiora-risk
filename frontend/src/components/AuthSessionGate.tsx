"use client";

import Link from "next/link";
import { Fragment, useEffect, useState } from "react";
import { sessionPrincipalKey } from "@/lib/authSessionBootstrap";
import { usePathname } from "next/navigation";
import { isProtectedPagePath } from "@/lib/appAccess";
import { useAuthSession } from "@/components/AuthSessionContext";
import { accountTypeFromSession, describeUiError } from "@/lib/uiErrors";

export default function AuthSessionGate({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const { loading, authenticated, session, context, error, contextErrorCode, refresh } = useAuthSession();
  const protectedPage = Boolean(pathname && isProtectedPagePath(pathname));
  const principal = sessionPrincipalKey(session);
  const [cachePrincipal, setCachePrincipal] = useState(principal);
  const changingPrincipal = Boolean(authenticated && principal && cachePrincipal && principal !== cachePrincipal);
  useEffect(() => {
    if (!authenticated || !principal) return;
    if (!cachePrincipal) { setCachePrincipal(principal); return; }
    // Global SWR mutators use the default cache. A complete document reload on
    // a different login clears both cached private reads and in-flight requests.
    if (principal !== cachePrincipal) window.location.reload();
  }, [authenticated, cachePrincipal, principal]);

  if (!protectedPage) return <>{children}</>;

  if (loading || changingPrincipal) {
    return (
      <div className="auth-session-gate">
        <div className="auth-session-gate-shell">
          <span className="auth-session-gate-folio">Checking session</span>
          <h2 className="auth-session-gate-title">Loading your workspace.</h2>
          <p className="auth-session-gate-copy">
            Verifying your session and active account before the dashboard renders.
          </p>
        </div>
      </div>
    );
  }

  if (error) {
    const description = describeUiError(
      { code: contextErrorCode || "account_context_unavailable", message: error },
      {
        surface: "your workspace",
        accountType: accountTypeFromSession(session, context),
        authProvider: session?.authProvider,
        isAdmin: Boolean(context?.is_admin || session?.isAdmin),
      },
    );
    return (
      <div className="auth-session-gate">
        <div className="auth-session-gate-shell">
          <span className="auth-session-gate-folio">{description.kind.replaceAll("_", " ")}</span>
          <h2 className="auth-session-gate-title">{description.title}.</h2>
          <p className="auth-session-gate-copy">{description.message}</p>
          {description.diagnostic ? (
            <details className="ui-error-diagnostic">
              <summary>Admin details</summary>
              <code>{description.diagnostic}</code>
            </details>
          ) : null}
          <div className="auth-session-gate-actions">
            {description.action === "retry" ? (
              <button
                type="button"
                className="public-inline-action auth-session-gate-button"
                onClick={() => {
                  void refresh();
                }}
              >
                Try again <span aria-hidden="true">↗</span>
              </button>
            ) : null}
            <Link href="/login" className="public-inline-action">
              Return to login <span aria-hidden="true">↗</span>
            </Link>
          </div>
        </div>
      </div>
    );
  }

  if (!authenticated) {
    return (
      <div className="auth-session-gate">
        <div className="auth-session-gate-shell">
          <span className="auth-session-gate-folio">Sign in required</span>
          <h2 className="auth-session-gate-title">Your session is no longer available.</h2>
          <p className="auth-session-gate-copy">
            Return to login and start a fresh session before opening protected pages.
          </p>
          <div className="auth-session-gate-actions">
            <Link href="/login" className="public-inline-action">
              Return to login <span aria-hidden="true">↗</span>
            </Link>
          </div>
        </div>
      </div>
    );
  }

  return <Fragment key={sessionPrincipalKey(session)}>{children}</Fragment>;
}
