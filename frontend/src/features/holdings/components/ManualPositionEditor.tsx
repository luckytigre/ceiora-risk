"use client";

import type { HoldingsAccount, UniverseSearchItem } from "@/lib/types";

interface ManualPositionEditorProps {
  selectedAccount: string;
  accountOptions: HoldingsAccount[];
  busy: boolean;
  editTicker: string;
  editRic: string;
  editQty: string;
  editSource: string;
  ricTypeahead: UniverseSearchItem[];
  onTickerChange: (value: string) => void;
  onAccountChange: (value: string) => void;
  onRicChange: (value: string) => void;
  onQtyChange: (value: string) => void;
  onSourceChange: (value: string) => void;
  onUpsert: () => void;
  actionLabel?: string;
}

export default function ManualPositionEditor({
  selectedAccount,
  accountOptions,
  busy,
  editTicker,
  editRic,
  editQty,
  editSource,
  ricTypeahead,
  onTickerChange,
  onAccountChange,
  onRicChange,
  onQtyChange,
  onSourceChange,
  onUpsert,
  actionLabel = "Stage Position",
}: ManualPositionEditorProps) {
  return (
    <div className="holdings-panel">
      <div className="holdings-panel-header">
        <div className="holdings-panel-icon manual">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7" />
            <path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" />
          </svg>
        </div>
        <div>
          <div className="holdings-panel-title">Manual Entry</div>
          <div className="holdings-panel-desc">Stage a single position by ticker</div>
        </div>
      </div>

      <div className="holdings-grid-2col">
        <div className="holdings-form-block">
          <label htmlFor="manual-account-id">Account</label>
          <input
            id="manual-account-id"
            className="explore-input holdings-compact-input"
            list="manual-account-id-options"
            placeholder="ibkr_multistrat"
            value={selectedAccount}
            onChange={(e) => onAccountChange(e.target.value.toLowerCase())}
          />
          <datalist id="manual-account-id-options">
            {accountOptions.map((a) => (
              <option key={a.account_id} value={a.account_id}>
                {a.account_name}
              </option>
            ))}
          </datalist>
        </div>
        <div className="holdings-form-block">
          <label htmlFor="edit-ticker">Ticker</label>
          <input
            id="edit-ticker"
            className="explore-input holdings-compact-input"
            value={editTicker}
            onChange={(e) => onTickerChange(e.target.value.toUpperCase())}
            placeholder="AAPL"
          />
        </div>
        <div className="holdings-form-block">
          <label htmlFor="edit-ric">RIC</label>
          <input
            id="edit-ric"
            className="explore-input holdings-compact-input"
            list="edit-ric-options"
            value={editRic}
            onChange={(e) => onRicChange(e.target.value.toUpperCase())}
            placeholder="AAPL.OQ"
          />
          <datalist id="edit-ric-options">
            {ricTypeahead.map((row) => (
              <option key={`${row.ticker}:${row.ric}`} value={String(row.ric || "").toUpperCase()}>
                {row.ticker}{row.name ? ` — ${row.name}` : ""}
              </option>
            ))}
          </datalist>
        </div>
        <div className="holdings-form-block">
          <label htmlFor="edit-qty">Quantity</label>
          <input
            id="edit-qty"
            className="explore-input holdings-compact-input"
            inputMode="decimal"
            value={editQty}
            onChange={(e) => onQtyChange(e.target.value)}
            placeholder="125.5"
          />
        </div>
        <div className="holdings-form-block">
          <label htmlFor="edit-source">Source</label>
          <input
            id="edit-source"
            className="explore-input holdings-compact-input"
            value={editSource}
            onChange={(e) => onSourceChange(e.target.value)}
            placeholder="ui_edit"
          />
        </div>
      </div>

      <div className="holdings-form-actions" style={{ marginTop: 12 }}>
        <button
          className="btn-action"
          onClick={onUpsert}
          disabled={busy}
        >
          {busy ? "Applying..." : actionLabel}
        </button>
      </div>
    </div>
  );
}
