"use client";

import { useEffect, useState } from "react";
import { useAuthSession } from "@/components/AuthSessionContext";
import {
  clearStoredAuthTokens,
  OPERATOR_TOKEN_STORAGE_KEY,
  readStoredAuthTokens,
  writeStoredAuthToken,
} from "@/lib/authTokens";

export default function AdminSettingsPage() {
  const { loading, context, error } = useAuthSession();
  const [tokens, setTokens] = useState(() => readStoredAuthTokens());

  function handleTokenChange(key: typeof OPERATOR_TOKEN_STORAGE_KEY, value: string) {
    writeStoredAuthToken(key, value);
    setTokens(readStoredAuthTokens());
  }

  function handleClearTokens() {
    clearStoredAuthTokens();
    setTokens(readStoredAuthTokens());
  }

  useEffect(() => {
    setTokens(readStoredAuthTokens());
  }, []);

  return (
    <div className="settings-page">
      <div className="settings-shell chart-card">
        <div className="settings-header">
          <div className="settings-kicker">Admin settings</div>
        </div>

        <section className="settings-section">
          <div className="settings-section-header settings-section-header-global">
            <h3>Admin session</h3>
            <div className="settings-section-desc">
              This page is only available to app-admin principals. Backend operator/editor tokens remain a separate transitional layer.
            </div>
          </div>
          {error ? (
            <div className="settings-empty-row">Session context unavailable: {error}</div>
          ) : null}
          <div className="settings-session-summary">
            <div className="settings-session-row">
              <span className="settings-option-label">Principal</span>
              <span className="settings-session-value">
                {loading ? "Loading…" : context?.display_name || context?.email || context?.subject || "Unknown"}
              </span>
            </div>
            <div className="settings-session-row">
              <span className="settings-option-label">Admin</span>
              <span className="settings-session-value">{loading ? "Loading…" : context?.is_admin ? "Yes" : "No"}</span>
            </div>
            <div className="settings-session-row">
              <span className="settings-option-label">Default account</span>
              <span className="settings-session-value">{loading ? "Loading…" : context?.default_account_id || "None"}</span>
            </div>
          </div>
        </section>

        {!loading && context?.is_admin && context?.admin_settings_enabled !== false ? (
        <section className="settings-section">
          <div className="settings-section-header settings-section-header-global">
            <h3>Privileged Backend Tokens</h3>
            <div className="settings-section-desc">
              Transitional maintenance surface for privileged backend tokens. These values remain stored only in this browser
              for operator/control actions while the app moves away from browser-held backend auth.
            </div>
          </div>
          <div className="settings-auth-grid">
            <label className="settings-auth-card">
              <span className="settings-option-label">Operator token</span>
              <span className="settings-option-help">Required for refresh, operator status, health diagnostics, and data diagnostics.</span>
              <input
                type="password"
                autoComplete="off"
                spellCheck={false}
                className="settings-auth-input"
                value={tokens.operatorToken}
                onChange={(event) => handleTokenChange(OPERATOR_TOKEN_STORAGE_KEY, event.target.value)}
                placeholder="Paste operator token"
              />
            </label>
          </div>
          <div className="settings-auth-footer">
            <div className="settings-option-help">
              {tokens.operatorToken
                ? "Only explicit maintenance/control routes forward the operator token stored in this browser."
                : "No browser auth tokens stored."}
            </div>
            <button type="button" className="settings-auth-clear" onClick={handleClearTokens}>
              Clear stored tokens
            </button>
          </div>
        </section>
        ) : null}
      </div>
    </div>
  );
}
