"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { UniverseSearchItem } from "@/lib/types/analytics";
import type { HoldingsAccount } from "@/lib/types/holdings";
import { useUniverseSearch } from "@/hooks/useCuse4Api";
import { useDebouncedValue } from "@/hooks/useDebouncedValue";

function highlightMatch(text: string, query: string) {
  if (!query) return text;
  const idx = text.toLowerCase().indexOf(query.toLowerCase());
  if (idx === -1) return text;
  return (
    <>
      {text.slice(0, idx)}
      <mark className="explore-highlight">{text.slice(idx, idx + query.length)}</mark>
      {text.slice(idx + query.length)}
    </>
  );
}

interface ManualPositionEditorProps {
  selectedAccount: string;
  accountOptions: HoldingsAccount[];
  busy: boolean;
  editTicker: string;
  editRic: string;
  editQty: string;
  editSource: string;
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
  onTickerChange,
  onAccountChange,
  onRicChange,
  onQtyChange,
  onSourceChange,
  onUpsert,
  actionLabel = "Stage Position",
}: ManualPositionEditorProps) {
  // --- Ticker typeahead state ---
  const [tickerFocused, setTickerFocused] = useState(false);
  const [tickerDropdownOpen, setTickerDropdownOpen] = useState(false);
  const [tickerActiveIndex, setTickerActiveIndex] = useState(-1);
  const tickerWrapRef = useRef<HTMLDivElement>(null);

  const debouncedTicker = useDebouncedValue(editTicker.trim().toUpperCase(), 220);
  const { data: tickerSearch } = useUniverseSearch(debouncedTicker, 8);
  const tickerResults = useMemo(
    () => (tickerSearch?.results ?? []).filter((r) => typeof r.ric === "string" && r.ric.trim().length > 0),
    [tickerSearch?.results],
  );

  // --- RIC typeahead state ---
  const [ricFocused, setRicFocused] = useState(false);
  const [ricDropdownOpen, setRicDropdownOpen] = useState(false);
  const [ricActiveIndex, setRicActiveIndex] = useState(-1);
  const ricWrapRef = useRef<HTMLDivElement>(null);

  const debouncedRic = useDebouncedValue(editRic.trim().toUpperCase(), 220);
  const { data: ricSearch } = useUniverseSearch(debouncedRic, 8);
  const ricResults = useMemo(
    () => (ricSearch?.results ?? []).filter((r) => typeof r.ric === "string" && r.ric.trim().length > 0),
    [ricSearch?.results],
  );
  const tickerSuggestions = useMemo(
    () => (tickerResults.length > 0 ? tickerResults : ricResults),
    [tickerResults, ricResults],
  );
  const ricSuggestions = useMemo(
    () => (ricResults.length > 0 ? ricResults : tickerResults),
    [ricResults, tickerResults],
  );

  // --- Ticker dropdown open/close ---
  useEffect(() => {
    if (
      tickerFocused
      && (editTicker.trim().length > 0 || (editTicker.trim().length === 0 && editRic.trim().length > 0))
      && tickerSuggestions.length > 0
    ) {
      setTickerDropdownOpen(true);
      setTickerActiveIndex(-1);
    } else {
      setTickerDropdownOpen(false);
    }
  }, [tickerFocused, editTicker, editRic, tickerSuggestions.length]);

  // --- RIC dropdown open/close ---
  useEffect(() => {
    if (
      ricFocused
      && (editRic.trim().length > 0 || (editRic.trim().length === 0 && editTicker.trim().length > 0))
      && ricSuggestions.length > 0
    ) {
      setRicDropdownOpen(true);
      setRicActiveIndex(-1);
    } else {
      setRicDropdownOpen(false);
    }
  }, [ricFocused, editRic, editTicker, ricSuggestions.length]);

  // --- Click-outside handlers ---
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (tickerWrapRef.current && !tickerWrapRef.current.contains(e.target as Node)) {
        setTickerDropdownOpen(false);
        setTickerFocused(false);
      }
      if (ricWrapRef.current && !ricWrapRef.current.contains(e.target as Node)) {
        setRicDropdownOpen(false);
        setRicFocused(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  // --- Auto-fill: ticker → RIC ---
  useEffect(() => {
    if (!editTicker.trim() || editRic.trim().length > 0) return;
    const exact = tickerResults.find((r) => r.ticker.toUpperCase() === editTicker.trim().toUpperCase());
    if (exact?.ric) onRicChange(String(exact.ric).toUpperCase());
  }, [editTicker, editRic, tickerResults, onRicChange]);

  // --- Auto-fill: RIC → ticker ---
  useEffect(() => {
    if (!editRic.trim() || editTicker.trim().length > 0) return;
    const exact = ricResults.find((r) => String(r.ric || "").toUpperCase() === editRic.trim().toUpperCase());
    if (exact?.ticker) onTickerChange(exact.ticker.toUpperCase());
  }, [editRic, editTicker, ricResults, onTickerChange]);

  // --- Ticker selection ---
  const selectTicker = useCallback(
    (row: UniverseSearchItem) => {
      onTickerChange(row.ticker.toUpperCase());
      if (row.ric) onRicChange(String(row.ric).toUpperCase());
      setTickerDropdownOpen(false);
      setTickerActiveIndex(-1);
    },
    [onTickerChange, onRicChange],
  );

  // --- RIC selection ---
  const selectRic = useCallback(
    (row: UniverseSearchItem) => {
      onRicChange(String(row.ric || "").toUpperCase());
      if (row.ticker) onTickerChange(row.ticker.toUpperCase());
      setRicDropdownOpen(false);
      setRicActiveIndex(-1);
    },
    [onTickerChange, onRicChange],
  );

  const handleTickerKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (!tickerDropdownOpen || tickerSuggestions.length === 0) {
        if (e.key === "Enter") e.preventDefault();
        return;
      }
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setTickerActiveIndex((prev) => (prev < tickerSuggestions.length - 1 ? prev + 1 : 0));
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setTickerActiveIndex((prev) => (prev > 0 ? prev - 1 : tickerSuggestions.length - 1));
      } else if (e.key === "Enter") {
        e.preventDefault();
        if (tickerActiveIndex >= 0 && tickerActiveIndex < tickerSuggestions.length) {
          selectTicker(tickerSuggestions[tickerActiveIndex]);
        }
      } else if (e.key === "Escape") {
        setTickerDropdownOpen(false);
      }
    },
    [tickerActiveIndex, tickerDropdownOpen, tickerSuggestions, selectTicker],
  );

  const handleRicKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (!ricDropdownOpen || ricSuggestions.length === 0) {
        if (e.key === "Enter") e.preventDefault();
        return;
      }
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setRicActiveIndex((prev) => (prev < ricSuggestions.length - 1 ? prev + 1 : 0));
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setRicActiveIndex((prev) => (prev > 0 ? prev - 1 : ricSuggestions.length - 1));
      } else if (e.key === "Enter") {
        e.preventDefault();
        if (ricActiveIndex >= 0 && ricActiveIndex < ricSuggestions.length) {
          selectRic(ricSuggestions[ricActiveIndex]);
        }
      } else if (e.key === "Escape") {
        setRicDropdownOpen(false);
      }
    },
    [ricActiveIndex, ricDropdownOpen, ricSuggestions, selectRic],
  );

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

      {/* Ticker + RIC side by side with typeahead */}
      <div className="holdings-ticker-ric-row">
        <div className="holdings-form-block" ref={tickerWrapRef} style={{ position: "relative" }}>
          <label htmlFor="edit-ticker">Ticker</label>
          <input
            id="edit-ticker"
            className="explore-input holdings-compact-input"
            value={editTicker}
            onChange={(e) => {
              onTickerChange(e.target.value.toUpperCase());
              if (editRic) onRicChange("");
            }}
            onKeyDown={handleTickerKeyDown}
            onFocus={() => setTickerFocused(true)}
            onBlur={(e) => {
              if (tickerWrapRef.current?.contains(e.relatedTarget as Node)) return;
              setTickerFocused(false);
              setTickerDropdownOpen(false);
            }}
            placeholder="AAPL"
            autoComplete="off"
          />
          {tickerDropdownOpen && tickerSuggestions.length > 0 && (
            <div className="explore-typeahead holdings-typeahead">
              {tickerSuggestions.map((row, idx) => (
                <button
                  key={`${row.ticker}:${row.ric}`}
                  className={`explore-typeahead-item${idx === tickerActiveIndex ? " active" : ""}`}
                  onMouseEnter={() => setTickerActiveIndex(idx)}
                  onMouseDown={(e) => e.preventDefault()}
                  onClick={() => selectTicker(row)}
                >
                  <span className="ticker">{highlightMatch(row.ticker, editTicker || editRic)}</span>
                  <span className="name">{highlightMatch(row.name, editTicker || editRic)}</span>
                  <span className="explore-typeahead-classifications">
                    <span>{row.trbc_economic_sector_short_abbr || row.trbc_economic_sector_short || ""}</span>
                  </span>
                  <span className="ric-hint">{row.ric || ""}</span>
                </button>
              ))}
            </div>
          )}
        </div>

        <div className="holdings-form-block" ref={ricWrapRef} style={{ position: "relative" }}>
          <label htmlFor="edit-ric">RIC</label>
          <input
            id="edit-ric"
            className="explore-input holdings-compact-input"
            value={editRic}
            onChange={(e) => {
              onRicChange(e.target.value.toUpperCase());
              if (editTicker) onTickerChange("");
            }}
            onKeyDown={handleRicKeyDown}
            onFocus={() => setRicFocused(true)}
            onBlur={(e) => {
              if (ricWrapRef.current?.contains(e.relatedTarget as Node)) return;
              setRicFocused(false);
              setRicDropdownOpen(false);
            }}
            placeholder="AAPL.OQ"
            autoComplete="off"
          />
          {ricDropdownOpen && ricSuggestions.length > 0 && (
            <div className="explore-typeahead holdings-typeahead">
              {ricSuggestions.map((row, idx) => (
                <button
                  key={`${row.ticker}:${row.ric}`}
                  className={`explore-typeahead-item${idx === ricActiveIndex ? " active" : ""}`}
                  onMouseEnter={() => setRicActiveIndex(idx)}
                  onMouseDown={(e) => e.preventDefault()}
                  onClick={() => selectRic(row)}
                >
                  <span className="ticker">{highlightMatch(String(row.ric || ""), editRic || editTicker)}</span>
                  <span className="name">{row.ticker}{row.name ? ` — ${row.name}` : ""}</span>
                  <span className="explore-typeahead-classifications">
                    <span>{row.trbc_economic_sector_short_abbr || row.trbc_economic_sector_short || ""}</span>
                  </span>
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      <div className="holdings-grid-2col" style={{ marginTop: 10 }}>
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
