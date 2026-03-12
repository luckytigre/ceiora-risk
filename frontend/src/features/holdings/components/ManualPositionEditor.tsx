"use client";

import type { UniverseSearchItem } from "@/lib/types";

interface ManualPositionEditorProps {
  busy: boolean;
  editTicker: string;
  editRic: string;
  editQty: string;
  editSource: string;
  ricTypeahead: UniverseSearchItem[];
  onTickerChange: (value: string) => void;
  onRicChange: (value: string) => void;
  onQtyChange: (value: string) => void;
  onSourceChange: (value: string) => void;
  onUpsert: () => void;
  actionLabel?: string;
}

export default function ManualPositionEditor({
  busy,
  editTicker,
  editRic,
  editQty,
  editSource,
  ricTypeahead,
  onTickerChange,
  onRicChange,
  onQtyChange,
  onSourceChange,
  onUpsert,
  actionLabel = "Stage Position",
}: ManualPositionEditorProps) {
  return (
    <div className="holdings-grid" style={{ marginTop: 14 }}>
      <div className="holdings-form-block">
        <label htmlFor="edit-ticker">Ticker</label>
        <input
          id="edit-ticker"
          className="explore-input"
          value={editTicker}
          onChange={(e) => onTickerChange(e.target.value.toUpperCase())}
          placeholder="AAPL"
        />
      </div>
      <div className="holdings-form-block">
        <label htmlFor="edit-ric">RIC</label>
        <input
          id="edit-ric"
          className="explore-input"
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
          className="explore-input"
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
          className="explore-input"
          value={editSource}
          onChange={(e) => onSourceChange(e.target.value)}
          placeholder="ui_edit"
        />
      </div>
      <div className="holdings-form-block">
        <button
          className="explore-search-btn"
          onClick={onUpsert}
          disabled={busy}
          style={{ width: "fit-content", paddingLeft: 0 }}
        >
          {busy ? "Applying..." : actionLabel}
        </button>
      </div>
    </div>
  );
}
