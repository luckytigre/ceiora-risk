"use client";

import type { HoldingsAccount, HoldingsImportMode } from "@/lib/types";

interface HoldingsImportPanelProps {
  selectedAccount: string;
  accountOptions: HoldingsAccount[];
  mode: HoldingsImportMode;
  csvSource: string;
  busy: boolean;
  modeOptions: HoldingsImportMode[];
  onAccountChange: (value: string) => void;
  onModeChange: (value: HoldingsImportMode) => void;
  onSourceChange: (value: string) => void;
  onFileChange: (file: File | null) => void;
  onRunImport: () => void;
  modeLabel: (mode: HoldingsImportMode) => string;
  modeHelp: (mode: HoldingsImportMode) => string;
}

export default function HoldingsImportPanel({
  selectedAccount,
  accountOptions,
  mode,
  csvSource,
  busy,
  modeOptions,
  onAccountChange,
  onModeChange,
  onSourceChange,
  onFileChange,
  onRunImport,
  modeLabel,
  modeHelp,
}: HoldingsImportPanelProps) {
  return (
    <div className="holdings-panel">
      <div className="holdings-panel-header">
        <div className="holdings-panel-icon">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
            <polyline points="14 2 14 8 20 8" />
            <line x1="12" y1="18" x2="12" y2="12" />
            <line x1="9" y1="15" x2="15" y2="15" />
          </svg>
        </div>
        <div>
          <div className="holdings-panel-title">CSV Import</div>
          <div className="holdings-panel-desc">
            Upload a CSV to write live holdings for the selected account. Dashboard analytics stay on the last published snapshot until you run `RECALC`.
          </div>
        </div>
      </div>

      <div className="holdings-grid-2col">
        <div className="holdings-form-block">
          <label htmlFor="account-id">Account</label>
          <input
            id="account-id"
            className="explore-input holdings-compact-input"
            list="account-id-options"
            placeholder="ACCT-CORE"
            value={selectedAccount}
            onChange={(e) => onAccountChange(e.target.value.toLowerCase())}
          />
          <datalist id="account-id-options">
            {accountOptions.map((a) => (
              <option key={a.account_id} value={a.account_id}>
                {a.account_name}
              </option>
            ))}
          </datalist>
        </div>

        <div className="holdings-form-block">
          <label htmlFor="import-mode">Mode</label>
          <select
            id="import-mode"
            className="health-select"
            value={mode}
            onChange={(e) => onModeChange(e.target.value as HoldingsImportMode)}
          >
            {modeOptions.map((m) => (
              <option key={m} value={m}>{modeLabel(m)}</option>
            ))}
          </select>
          <div className="holdings-mode-help">{modeHelp(mode)}</div>
        </div>

        <div className="holdings-form-block">
          <label htmlFor="csv-source">Source Tag</label>
          <input
            id="csv-source"
            className="explore-input holdings-compact-input"
            value={csvSource}
            onChange={(e) => onSourceChange(e.target.value)}
            placeholder="csv_upload"
          />
        </div>

        <div className="holdings-form-block">
          <label htmlFor="csv-file">File</label>
          <input
            id="csv-file"
            className="holdings-file-input"
            type="file"
            accept=".csv,text/csv"
            onChange={(e) => onFileChange(e.target.files?.[0] ?? null)}
          />
        </div>
      </div>

      <div className="holdings-form-actions" style={{ marginTop: 12 }}>
        <button
          className="btn-action"
          onClick={onRunImport}
          disabled={busy}
        >
          {busy ? "Running..." : "Run Live Import"}
        </button>
      </div>
    </div>
  );
}
