"use client";

import { useEffect, useMemo, useState } from "react";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ApiErrorState from "@/components/ApiErrorState";
import ConfirmActionModal from "@/components/ConfirmActionModal";
import {
  useHoldingsAccounts,
  useHoldingsModes,
  useHoldingsPositions,
  usePortfolio,
  useUniverseSearch,
} from "@/hooks/useApi";
import type { HoldingsImportMode } from "@/lib/types";
import { useDebouncedValue } from "@/hooks/useDebouncedValue";
import HoldingsImportPanel from "@/features/holdings/components/HoldingsImportPanel";
import HoldingsLedgerSection from "@/features/holdings/components/HoldingsLedgerSection";
import HoldingsMutationFeedback from "@/features/holdings/components/HoldingsMutationFeedback";
import ManualPositionEditor from "@/features/holdings/components/ManualPositionEditor";
import { useHoldingsManager } from "@/features/holdings/hooks/useHoldingsManager";

function modeLabel(m: HoldingsImportMode): string {
  if (m === "replace_account") return "Full Replace Account";
  if (m === "upsert_absolute") return "Overwrite Listed Positions";
  return "Increment Delta";
}

function modeHelp(m: HoldingsImportMode): string {
  if (m === "replace_account") return "Deletes positions not present in the CSV for this account.";
  if (m === "upsert_absolute") return "Only listed positions are overwritten; others stay untouched.";
  return "Adds CSV quantities to existing quantities for listed positions.";
}

function normalizeAccountId(value: string | null | undefined): string {
  return String(value || "").trim().toLowerCase();
}

function normalizeTicker(value: string | null | undefined): string {
  return String(value || "").trim().toUpperCase();
}

function fmtQty(n: number): string {
  return n.toLocaleString(undefined, {
    minimumFractionDigits: Math.abs(n) >= 1000 ? 0 : 0,
    maximumFractionDigits: 6,
  });
}

export default function PositionsPage() {
  const { data: portfolio, isLoading: pLoading, error: pError } = usePortfolio();
  const { data: modesData } = useHoldingsModes();
  const { data: accountsData, error: accountError } = useHoldingsAccounts();
  const [selectedAccount, setSelectedAccount] = useState("");
  const { data: holdingsData, error: holdingsError } = useHoldingsPositions(null);

  const [mode, setMode] = useState<HoldingsImportMode>("upsert_absolute");
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvSource, setCsvSource] = useState("csv_upload");
  const [editRic, setEditRic] = useState("");
  const [editTicker, setEditTicker] = useState("");
  const [editQty, setEditQty] = useState("");
  const [editSource, setEditSource] = useState("ui_edit");
  const [holdingsManagerExpanded, setHoldingsManagerExpanded] = useState(true);

  const tickerSearchQuery = editTicker.trim().toUpperCase();
  const debouncedTickerSearchQuery = useDebouncedValue(tickerSearchQuery, 220);
  const { data: tickerRicSearch } = useUniverseSearch(debouncedTickerSearchQuery, 12);

  useEffect(() => {
    if (!modesData?.default) return;
    setMode(modesData.default);
  }, [modesData?.default]);

  useEffect(() => {
    const accounts = accountsData?.accounts ?? [];
    if (!selectedAccount && accounts.length > 0) {
      setSelectedAccount(accounts[0].account_id);
    }
  }, [accountsData?.accounts, selectedAccount]);

  const ricTypeahead = useMemo(
    () =>
      (tickerRicSearch?.results ?? []).filter(
        (row) => typeof row.ric === "string" && row.ric.trim().length > 0,
      ),
    [tickerRicSearch?.results],
  );

  useEffect(() => {
    if (!tickerSearchQuery || editRic.trim().length > 0) return;
    const exact = ricTypeahead.find((row) => String(row.ticker || "").toUpperCase() === tickerSearchQuery);
    if (!exact?.ric) return;
    setEditRic(String(exact.ric).toUpperCase());
  }, [tickerSearchQuery, editRic, ricTypeahead]);
  const positions = portfolio?.positions ?? [];
  const accountOptions = accountsData?.accounts ?? [];
  const liveHoldingsRows = holdingsData?.positions ?? [];

  const {
    busy,
    confirmConfig,
    draftCount,
    draftDeleteCount,
    draftEntries,
    errorMessage,
    getDraftQuantityText,
    hasDraftForTarget,
    isDraftInvalid,
    rejectionPreview,
    resultMessage,
    handleAdjust,
    handleApplyDrafts,
    handleCsvImport,
    handleDraftQuantityChange,
    handleManualUpsert,
    discardDrafts,
    setConfirmConfig,
  } = useHoldingsManager(selectedAccount, liveHoldingsRows);

  const holdingsRows = useMemo(() => {
    const liveKeys = new Set(
      liveHoldingsRows.map((row) => `${row.account_id}:${row.ric || row.ticker}`),
    );
    const stagedOnlyRows = draftEntries
      .filter((entry) => !liveKeys.has(`${entry.account_id}:${entry.ric || entry.ticker}`))
      .map((entry) => ({
        account_id: entry.account_id,
        ric: entry.ric || "",
        ticker: entry.ticker || "",
        quantity: 0,
        instrument_type: null,
        source: entry.source,
        updated_at: "staged",
      }));
    return [...liveHoldingsRows, ...stagedOnlyRows].sort(
      (a, b) => Math.abs(b.quantity) - Math.abs(a.quantity),
    );
  }, [draftEntries, liveHoldingsRows]);
  const modeledPositions = positions;

  const modelVsLiveDiffs = useMemo(() => {
    const liveMap = new Map<string, { account: string; ticker: string; quantity: number }>();
    const modelMap = new Map<string, { account: string; ticker: string; quantity: number }>();

    for (const row of liveHoldingsRows) {
      const ticker = normalizeTicker(row.ticker || row.ric);
      if (!ticker) continue;
      const account = normalizeAccountId(row.account_id);
      liveMap.set(`${account}::${ticker}`, { account, ticker, quantity: Number(row.quantity) || 0 });
    }
    for (const pos of modeledPositions) {
      const ticker = normalizeTicker(pos.ticker);
      if (!ticker) continue;
      const account = normalizeAccountId(pos.account);
      modelMap.set(`${account}::${ticker}`, { account, ticker, quantity: Number(pos.shares) || 0 });
    }

    const keys = new Set([...liveMap.keys(), ...modelMap.keys()]);
    const diffs = [...keys].map((key) => {
      const liveRow = liveMap.get(key);
      const modeledRow = modelMap.get(key);
      const live = liveRow?.quantity ?? null;
      const modeled = modeledRow?.quantity ?? null;
      const delta = live !== null && modeled !== null ? live - modeled : null;
      const status =
        live === null ? "modeled-only" : modeled === null ? "live-only" : Math.abs(delta || 0) <= 1e-6 ? "aligned" : "changed";
      return {
        account: liveRow?.account || modeledRow?.account || "",
        ticker: liveRow?.ticker || modeledRow?.ticker || "",
        live,
        modeled,
        delta,
        status,
      };
    });

    return diffs
      .filter((row) => row.status !== "aligned")
      .sort((a, b) => {
        const rank = (status: string) => (status === "changed" ? 0 : 1);
        const deltaA = Math.abs(a.delta ?? a.live ?? a.modeled ?? 0);
        const deltaB = Math.abs(b.delta ?? b.live ?? b.modeled ?? 0);
        return rank(a.status) - rank(b.status) || deltaB - deltaA || a.account.localeCompare(b.account) || a.ticker.localeCompare(b.ticker);
      });
  }, [liveHoldingsRows, modeledPositions]);

  const getLedgerDraftQuantityText = (row: (typeof holdingsRows)[number]) =>
    getDraftQuantityText({
      account_id: row.account_id,
      ric: row.ric,
      ticker: row.ticker,
      current_quantity: row.quantity,
    });

  const hasLedgerDraft = (row: (typeof holdingsRows)[number]) =>
    hasDraftForTarget({
      account_id: row.account_id,
      ric: row.ric,
      ticker: row.ticker,
    });

  const isLedgerDraftInvalid = (row: (typeof holdingsRows)[number]) =>
    isDraftInvalid({
      account_id: row.account_id,
      ric: row.ric,
      ticker: row.ticker,
    });

  if (pLoading) {
    return <AnalyticsLoadingViz message="Loading positions..." />;
  }
  if (pError || accountError) {
    return <ApiErrorState title="Positions Not Ready" error={pError || accountError} />;
  }

  return (
    <div>
      <div className="chart-card mb-4">
        <div className="holdings-section-header">
          <h3>Holdings Manager</h3>
          <button
            type="button"
            className="holdings-panel-toggle"
            aria-expanded={holdingsManagerExpanded}
            onClick={() => setHoldingsManagerExpanded((prev) => !prev)}
          >
            {holdingsManagerExpanded ? "Collapse" : "Expand"}
            <span className={`kpi-toggle-glyph ${holdingsManagerExpanded ? "open" : ""}`}>+</span>
          </button>
        </div>

        {holdingsManagerExpanded && (
          <>
            <div className="holdings-manager-grid">
              <ManualPositionEditor
                selectedAccount={selectedAccount}
                accountOptions={accountOptions}
                busy={busy}
                editTicker={editTicker}
                editRic={editRic}
                editQty={editQty}
                editSource={editSource}
                ricTypeahead={ricTypeahead}
                onAccountChange={setSelectedAccount}
                onTickerChange={setEditTicker}
                onRicChange={setEditRic}
                onQtyChange={setEditQty}
                onSourceChange={setEditSource}
                onUpsert={() => {
                  const staged = handleManualUpsert({
                    editRic,
                    editTicker,
                    editQty,
                    editSource,
                  });
                  if (staged) {
                    setEditTicker("");
                    setEditRic("");
                    setEditQty("");
                  }
                }}
                actionLabel="Stage Position"
              />

              <HoldingsImportPanel
                selectedAccount={selectedAccount}
                accountOptions={accountOptions}
                mode={mode}
                csvSource={csvSource}
                busy={busy}
                modeOptions={modesData?.modes ?? ["replace_account", "upsert_absolute", "increment_delta"]}
                onAccountChange={setSelectedAccount}
                onModeChange={setMode}
                onSourceChange={setCsvSource}
                onFileChange={setCsvFile}
                onRunImport={() => void handleCsvImport({ csvFile, csvSource, mode })}
                modeLabel={modeLabel}
                modeHelp={modeHelp}
              />
            </div>

            {draftCount > 0 && (
              <div className="draft-banner">
                <div className="draft-banner-text">
                  {draftCount} staged edit{draftCount === 1 ? "" : "s"} pending
                  {draftDeleteCount > 0 ? `, including ${draftDeleteCount} staged remove${draftDeleteCount === 1 ? "" : "s"}` : ""}.
                  Nothing is written to Neon until you hit `RECALC`.
                </div>
                <div className="draft-banner-actions">
                  <button className="btn-action" onClick={() => void handleApplyDrafts()} disabled={busy}>
                    {busy ? "Applying..." : `RECALC ${draftCount > 0 ? `(${draftCount})` : ""}`}
                  </button>
                  <button className="btn-action" onClick={discardDrafts} disabled={busy}>
                    Discard Drafts
                  </button>
                </div>
              </div>
            )}
            <HoldingsMutationFeedback
              resultMessage={resultMessage}
              errorMessage={errorMessage}
              rejectionPreview={rejectionPreview}
              draftCount={draftCount}
              draftDeleteCount={draftDeleteCount}
            />
          </>
        )}
      </div>

      <HoldingsLedgerSection
        holdingsRows={holdingsRows}
        modeledPositions={modeledPositions}
        holdingsError={holdingsError}
        busy={busy}
        getDraftQuantityText={getLedgerDraftQuantityText}
        hasDraftForRow={hasLedgerDraft}
        isDraftInvalidForRow={isLedgerDraftInvalid}
        onAdjust={handleAdjust}
        onDraftQuantityChange={handleDraftQuantityChange}
      />

      <div className="chart-card mb-4">
        <h3>Modeled Snapshot</h3>
        <div className="section-subtitle">
          This is the last modeled view across all accounts. It updates only after `RECALC`, so it is shown here as a compact check instead of a second full positions table.
        </div>
        <div className="data-metric-grid" style={{ marginTop: 12 }}>
          <div className="data-metric-card">
            <h4>Live Holdings Rows</h4>
            <div className="data-metric-value">{liveHoldingsRows.length}</div>
            <div className="data-metric-desc">Rows currently stored in Neon across all accounts.</div>
          </div>
          <div className="data-metric-card">
            <h4>Modeled Rows</h4>
            <div className="data-metric-value">{modeledPositions.length}</div>
            <div className="data-metric-desc">Rows in the last cached portfolio snapshot across all accounts.</div>
          </div>
          <div className="data-metric-card">
            <h4>Unmodeled Differences</h4>
            <div className="data-metric-value">{modelVsLiveDiffs.length}</div>
            <div className="data-metric-desc">Names where live holdings and modeled snapshot still differ.</div>
          </div>
          <div className="data-metric-card">
            <h4>Source Dates</h4>
            <div className="data-metric-row">
              <span className="data-metric-label">Prices</span>
              <span className="data-metric-value">{portfolio?.source_dates?.prices_asof || "—"}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Fundamentals</span>
              <span className="data-metric-value">{portfolio?.source_dates?.fundamentals_asof || "—"}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Classification</span>
              <span className="data-metric-value">{portfolio?.source_dates?.classification_asof || "—"}</span>
            </div>
          </div>
        </div>

        {modelVsLiveDiffs.length > 0 ? (
          <div className="dash-table" style={{ marginTop: 14 }}>
            <table>
              <thead>
                <tr>
                  <th>Account</th>
                  <th>Ticker</th>
                  <th>Status</th>
                  <th className="text-right">Live Qty</th>
                  <th className="text-right">Modeled Qty</th>
                  <th className="text-right">Delta</th>
                </tr>
              </thead>
              <tbody>
                {modelVsLiveDiffs.slice(0, 10).map((row) => (
                  <tr key={`${row.account}:${row.ticker}`}>
                    <td>{row.account || "—"}</td>
                    <td>{row.ticker}</td>
                    <td>{row.status}</td>
                    <td className="text-right">{row.live === null ? "—" : fmtQty(row.live)}</td>
                    <td className="text-right">{row.modeled === null ? "—" : fmtQty(row.modeled)}</td>
                    <td className="text-right">{row.delta === null ? "—" : fmtQty(row.delta)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="section-subtitle" style={{ marginTop: 14, marginBottom: 0 }}>
            Live holdings and the modeled snapshot are aligned across all accounts.
          </div>
        )}
      </div>

      <ConfirmActionModal
        open={!!confirmConfig}
        title={confirmConfig?.title || ""}
        body={confirmConfig?.body || ""}
        confirmValue={confirmConfig?.confirmValue ?? null}
        confirmLabel={confirmConfig?.confirmLabel}
        dangerText={confirmConfig?.dangerText || "Confirm"}
        onCancel={() => setConfirmConfig(null)}
        onConfirm={async () => {
          if (confirmConfig) {
            await confirmConfig.onConfirm();
          }
        }}
      />
    </div>
  );
}
