"use client";

import Link from "next/link";
import { useAuthSession } from "@/components/AuthSessionContext";
import { accountTypeFromSession, describeUiError, type UiAccountType } from "@/lib/uiErrors";

/** Shared request failures offer session recovery and retry without model-owned actions. */
export default function RequestErrorState({
  title,
  surface = "data",
  error,
  operation = "read",
  accountType,
  accountName,
  onRetry,
}: {
  title?: string;
  surface?: string;
  error: unknown;
  operation?: "read" | "write" | "recalculate";
  accountType?: UiAccountType | null;
  accountName?: string | null;
  onRetry?: () => void;
}) {
  const { session, context } = useAuthSession();
  const description = describeUiError(error, {
    surface,
    operation,
    accountType: accountType ?? accountTypeFromSession(session, context),
    accountName,
    authProvider: session?.authProvider,
    isAdmin: Boolean(context?.is_admin || session?.isAdmin),
  });

  return (
    <div className="chart-card">
      <h3>{title || description.title}</h3>
      <div className="detail-history-empty" role="alert">
        {description.message}
        {description.diagnostic ? (
          <details className="ui-error-diagnostic">
            <summary>Admin details</summary>
            <code>{description.diagnostic}</code>
          </details>
        ) : null}
      </div>
      <div className="ui-error-actions">
        {description.action === "sign_in" ? (
          <Link href="/login" className="btn-action">Return to login</Link>
        ) : description.action === "retry" ? (
          <button className="btn-action" type="button" onClick={onRetry ?? (() => window.location.reload())}>
            Try again
          </button>
        ) : null}
      </div>
    </div>
  );
}
