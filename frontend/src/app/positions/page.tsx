"use client";

import { useEffect, useMemo, useState } from "react";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ApiErrorState from "@/components/ApiErrorState";
import ConfirmActionModal from "@/components/ConfirmActionModal";
import PositionTable from "@/components/PositionTable";
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

export default function PositionsPage() {
  const { data: portfolio, isLoading: pLoading, error: pError } = usePortfolio();
  const { data: modesData } = useHoldingsModes();
  const { data: accountsData, error: accountError } = useHoldingsAccounts();
  const [selectedAccount, setSelectedAccount] = useState("");
  const { data: holdingsData, error: holdingsError } = useHoldingsPositions(selectedAccount || undefined);

  const [mode, setMode] = useState<HoldingsImportMode>("upsert_absolute");
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvSource, setCsvSource] = useState("csv_upload");
  const [editRic, setEditRic] = useState("");
  const [editTicker, setEditTicker] = useState("");
  const [editQty, setEditQty] = useState("");
  const [editSource, setEditSource] = useState("ui_edit");

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
    errorMessage,
    getDraftQuantityText,
    hasDraftForTarget,
    isDraftInvalid,
    rejectionPreview,
    resultMessage,
    selectedAccountDrafts,
    handleAdjust,
    handleApplyDrafts,
    handleCsvImport,
    handleDraftQuantityChange,
    handleManualUpsert,
    handleRemove,
    discardDrafts,
    setConfirmConfig,
  } = useHoldingsManager(selectedAccount, liveHoldingsRows);

  const holdingsRows = useMemo(() => {
    const liveKeys = new Set(
      liveHoldingsRows.map((row) => `${row.account_id}:${row.ric || row.ticker}`),
    );
    const stagedOnlyRows = selectedAccountDrafts
      .filter((entry) => !liveKeys.has(`${entry.account_id}:${entry.ric || entry.ticker}`))
      .map((entry) => ({
        account_id: entry.account_id,
        ric: entry.ric || "",
        ticker: entry.ticker || "",
        quantity: 0,
        source: entry.source,
        updated_at: "staged",
      }));
    return [...liveHoldingsRows, ...stagedOnlyRows].sort(
      (a, b) => Math.abs(b.quantity) - Math.abs(a.quantity),
    );
  }, [liveHoldingsRows, selectedAccountDrafts]);

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

  const getPositionDraftQuantityText = (pos: (typeof positions)[number]) =>
    getDraftQuantityText({
      account_id: pos.account,
      ticker: pos.ticker,
      current_quantity: pos.shares,
    });

  const hasPositionDraft = (pos: (typeof positions)[number]) =>
    hasDraftForTarget({
      account_id: pos.account,
      ticker: pos.ticker,
    });

  const isPositionDraftInvalid = (pos: (typeof positions)[number]) =>
    isDraftInvalid({
      account_id: pos.account,
      ticker: pos.ticker,
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
        <h3>Holdings Manager</h3>
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
        <ManualPositionEditor
          busy={busy}
          editTicker={editTicker}
          editRic={editRic}
          editQty={editQty}
          editSource={editSource}
          ricTypeahead={ricTypeahead}
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
        {draftCount > 0 && (
          <div
            style={{
              marginTop: 14,
              padding: "12px 14px",
              border: "1px solid rgba(224, 190, 92, 0.28)",
              background: "rgba(224, 190, 92, 0.06)",
              display: "grid",
              gap: 10,
            }}
          >
            <div style={{ color: "rgba(232, 237, 249, 0.9)", fontSize: 13, lineHeight: 1.5 }}>
              {draftCount} staged edit{draftCount === 1 ? "" : "s"} pending
              {draftDeleteCount > 0 ? `, including ${draftDeleteCount} staged remove${draftDeleteCount === 1 ? "" : "s"}` : ""}.
              Nothing is written to Neon until you hit `RECALC`.
            </div>
            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
              <button className="btn btn-secondary" onClick={() => void handleApplyDrafts()} disabled={busy}>
                {busy ? "Applying..." : `RECALC ${draftCount > 0 ? `(${draftCount})` : ""}`}
              </button>
              <button className="btn btn-secondary" onClick={discardDrafts} disabled={busy}>
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
      </div>

      <HoldingsLedgerSection
        selectedAccount={selectedAccount}
        holdingsRows={holdingsRows}
        holdingsError={holdingsError}
        busy={busy}
        getDraftQuantityText={getLedgerDraftQuantityText}
        hasDraftForRow={hasLedgerDraft}
        isDraftInvalidForRow={isLedgerDraftInvalid}
        onAdjust={handleAdjust}
        onDraftQuantityChange={handleDraftQuantityChange}
        onRemove={handleRemove}
      />

      <div className="chart-card mb-4">
        <h3>Model Portfolio Positions ({positions.length})</h3>
        <PositionTable
          positions={positions}
          getDraftQuantityText={getPositionDraftQuantityText}
          hasDraftForPosition={hasPositionDraft}
          isDraftInvalidForPosition={isPositionDraftInvalid}
          onDraftQuantityChange={(position, value) =>
            handleDraftQuantityChange(
              {
                account_id: position.account,
                ric: "",
                ticker: position.ticker,
                quantity: position.shares,
                source: position.source,
                updated_at: null,
              },
              value,
            )
          }
          onAdjust={(position, delta) =>
            handleAdjust(
              {
                account_id: position.account,
                ric: "",
                ticker: position.ticker,
                quantity: position.shares,
                source: position.source,
                updated_at: null,
              },
              delta,
            )
          }
        />
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
