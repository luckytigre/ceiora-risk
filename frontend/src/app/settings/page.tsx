"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useState } from "react";
import { useAppSettings } from "@/components/AppSettingsContext";
import { useAuthSession } from "@/components/AuthSessionContext";
import { useBackground } from "@/components/BackgroundContext";

const BACKGROUND_OPTIONS = [
  {
    value: "topo",
    label: "Topographic",
    description: "Layered linework background.",
  },
  {
    value: "flow",
    label: "Flow",
    description: "Animated field background.",
  },
  {
    value: "none",
    label: "None",
    description: "No decorative background.",
  },
] as const;

const THEME_OPTIONS = [
  {
    value: "dark",
    label: "Dark",
    description: "Warm-neutral graphite field for the main analytical shell.",
  },
  {
    value: "light",
    label: "Light",
    description: "Pale mineral field for daytime work without losing Ceiora density.",
  },
] as const;

export default function SettingsPage() {
  const searchParams = useSearchParams();
  const { loading, session, context, error } = useAuthSession();
  const { cparFactorHistoryMode, setCparFactorHistoryMode, themeMode, setThemeMode } = useAppSettings();
  const { mode: backgroundMode, setMode: setBackgroundMode } = useBackground();
  const useMarketAdjustedHistory = cparFactorHistoryMode === "market_adjusted";
  const selectedBackground = BACKGROUND_OPTIONS.find((option) => option.value === backgroundMode) ?? BACKGROUND_OPTIONS[0];
  const selectedTheme = THEME_OPTIONS.find((option) => option.value === themeMode) ?? THEME_OPTIONS[0];
  const adminRedirected = searchParams.get("error") === "admin_required";

  return (
    <div className="settings-page">
      <div className="settings-shell chart-card">
        <div className="settings-header">
          <div className="settings-kicker">Settings</div>
        </div>
        {adminRedirected ? (
          <div className="settings-empty-row">
            Admin settings are only available to app-admin sessions. You have been returned to the standard settings surface.
          </div>
        ) : null}
        <section className="settings-section">
          <div className="settings-section-header settings-section-header-global">
            <h3>Session</h3>
          </div>
          {error ? (
            <div className="settings-empty-row">Session context unavailable: {error}</div>
          ) : null}
          <div className="settings-session-summary">
            <div className="settings-session-row">
              <span className="settings-option-label">Identity</span>
              <span className="settings-session-value">
                {loading ? "Loading…" : context?.display_name || context?.email || session?.username || "Unknown"}
              </span>
            </div>
            <div className="settings-session-row">
              <span className="settings-option-label">Provider</span>
              <span className="settings-session-value">
                {loading ? "Loading…" : session?.authProvider === "neon" ? "Neon Auth" : "Shared session"}
              </span>
            </div>
            {session?.authProvider === "neon" || context?.account_enforcement_enabled ? (
              <div className="settings-session-row">
                <span className="settings-option-label">Personal account</span>
                <span className="settings-session-value">
                  {loading ? "Loading…" : context?.default_account_id || "None provisioned"}
                </span>
              </div>
            ) : (
              <div className="settings-session-row">
                <span className="settings-option-label">Account scope</span>
                <span className="settings-session-value">{loading ? "Loading…" : "Shared access mode"}</span>
              </div>
            )}
          </div>
          {session?.isAdmin && context?.admin_settings_enabled !== false ? (
            <div className="settings-auth-footer">
              <div className="settings-option-help">
                Privileged maintenance controls now live on a separate admin-only settings surface.
              </div>
              <Link href="/settings/admin" className="settings-auth-clear">
                Open admin settings
              </Link>
            </div>
          ) : null}
        </section>
        <section className="settings-section">
          <div className="settings-section-header settings-section-header-global">
            <h3>Global</h3>
          </div>
          <div className="settings-inline-row">
            <div className="settings-inline-copy">
              <div className="settings-option-label">Theme</div>
              <div className="settings-option-help">{selectedTheme.description}</div>
            </div>
            <div className="settings-segmented-control" role="tablist" aria-label="Theme mode">
              {THEME_OPTIONS.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  role="tab"
                  aria-selected={themeMode === option.value}
                  className={`settings-segmented-option${themeMode === option.value ? " active" : ""}`}
                  onClick={() => setThemeMode(option.value)}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </div>
          <div className="settings-inline-row">
            <div className="settings-inline-copy">
              <div className="settings-option-label">Background</div>
              <div className="settings-option-help">{selectedBackground.description}</div>
            </div>
            <div className="settings-segmented-control" role="tablist" aria-label="Background mode">
              {BACKGROUND_OPTIONS.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  role="tab"
                  aria-selected={backgroundMode === option.value}
                  className={`settings-segmented-option${backgroundMode === option.value ? " active" : ""}`}
                  onClick={() => setBackgroundMode(option.value)}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </div>
        </section>

        <section className="settings-section">
          <div className="settings-section-header settings-section-header-cpar">
            <h3>cPAR</h3>
          </div>
          <div className="settings-inline-row">
            <div className="settings-inline-copy">
              <div className="settings-option-label">Include Regression Intercept In Residualized Factor Returns</div>
              <div className="settings-option-help">
                {useMarketAdjustedHistory
                  ? "On: non-market cPAR drilldowns show market-adjusted return, including the fitted intercept."
                  : "Off: non-market cPAR drilldowns show the pure zero-mean residual after intercept and market are removed."}
              </div>
            </div>
            <button
              type="button"
              className={`toggle-switch settings-toggle toggle-switch-positive${useMarketAdjustedHistory ? " active" : ""}`}
              onClick={() => setCparFactorHistoryMode(useMarketAdjustedHistory ? "residual" : "market_adjusted")}
              aria-pressed={useMarketAdjustedHistory}
              aria-label="Toggle regression intercept in residualized factor returns"
              title={
                useMarketAdjustedHistory
                  ? "Turn off regression intercept in residualized factor returns"
                  : "Turn on regression intercept in residualized factor returns"
              }
            >
              <span className="toggle-switch-track" />
            </button>
          </div>
        </section>

        <section className="settings-section">
          <div className="settings-section-header settings-section-header-cuse">
            <h3>cUSE</h3>
          </div>
          <div className="settings-empty-row">No cUSE-specific settings yet.</div>
        </section>
      </div>
    </div>
  );
}
