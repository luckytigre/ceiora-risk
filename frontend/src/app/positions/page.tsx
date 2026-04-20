"use client";

import { useEffect, useMemo, useState } from "react";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ApiErrorState from "@/features/cuse4/components/ApiErrorState";
import ConfirmActionModal from "@/components/ConfirmActionModal";
import MethodLabel, { type MethodLabelTone } from "@/components/MethodLabel";
import { compareNumber, compareText, useSortableRows } from "@/hooks/useSortableRows";
import { useCuseRiskPageSnapshot } from "@/hooks/useCuse4Api";
import { useCparRisk } from "@/hooks/useCparApi";
import { useHoldingsAccounts, useHoldingsModes, useHoldingsPositions } from "@/hooks/useHoldingsApi";
import type { HoldingsImportMode } from "@/lib/types/holdings";
import HoldingsImportPanel from "@/features/holdings/components/HoldingsImportPanel";
import HoldingsLedgerSection from "@/features/holdings/components/HoldingsLedgerSection";
import HoldingsMutationFeedback from "@/features/holdings/components/HoldingsMutationFeedback";
import ManualPositionEditor from "@/features/holdings/components/ManualPositionEditor";
import { useHoldingsManager } from "@/features/holdings/hooks/useHoldingsManager";
import { buildAnalyticsTruthCompactSummary, summarizeAnalyticsTruth } from "@/lib/cuse4Truth";
import { exposureMethodDisplayLabel, exposureMethodTone } from "@/lib/exposureOrigin";

type ModelDiffSortKey = "account" | "ticker" | "method" | "status" | "live" | "modeled" | "delta";
const SNAPSHOT_WARNING_STYLE = {
  marginTop: 12,
  padding: "12px 14px",
  border: "1px solid color-mix(in srgb, var(--negative) 32%, transparent)",
  background: "color-mix(in srgb, var(--negative) 10%, transparent)",
  color: "var(--text-primary)",
  fontSize: 13,
  lineHeight: 1.5,
} as const;

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
  const { data: cuseSnapshot, isLoading: cuseLoading, error: cuseError } = useCuseRiskPageSnapshot();
  const { data: cparRiskData } = useCparRisk();
  const { data: modesData } = useHoldingsModes();
  const { data: accountsData, error: accountError } = useHoldingsAccounts();
  const [selectedAccount, setSelectedAccount] = useState("");
  const { data: holdingsData, error: holdingsError } = useHoldingsPositions(null);
  const portfolio = cuseSnapshot?.portfolio;
  const riskData = cuseSnapshot?.risk;

  const [mode, setMode] = useState<HoldingsImportMode>("upsert_absolute");
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvSource, setCsvSource] = useState("csv_upload");
  const [editRic, setEditRic] = useState("");
  const [editTicker, setEditTicker] = useState("");
  const [editQty, setEditQty] = useState("");
  const [editSource, setEditSource] = useState("ui_edit");

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
        source: entry.source,
        updated_at: "staged",
      }));
    return [...liveHoldingsRows, ...stagedOnlyRows].sort(
      (a, b) => Math.abs(b.quantity) - Math.abs(a.quantity),
    );
  }, [draftEntries, liveHoldingsRows]);
  const modeledPositions = positions;
  const truth = useMemo(
    () => summarizeAnalyticsTruth({ portfolio, risk: riskData }),
    [portfolio, riskData],
  );
  const compactTruthSummary = useMemo(() => buildAnalyticsTruthCompactSummary(truth), [truth]);
  const snapshotMismatch = !truth.snapshotsCoherent && truth.snapshotIds.length > 1;

  const modelVsLiveDiffs = useMemo(() => {
    const liveMap = new Map<string, { accountScope: string; ticker: string; quantity: number }>();
    const accountsByTicker = new Map<string, Set<string>>();
    const modelMap = new Map<string, { ticker: string; quantity: number; method: string; methodTone: MethodLabelTone }>();

    for (const row of liveHoldingsRows) {
      const ticker = normalizeTicker(row.ticker || row.ric);
      if (!ticker) continue;
      const existing = liveMap.get(ticker);
      const quantity = Number(row.quantity) || 0;
      if (existing) {
        existing.quantity += quantity;
      } else {
        liveMap.set(ticker, { accountScope: "", ticker, quantity });
      }
      const account = normalizeAccountId(row.account_id);
      if (account) {
        const accounts = accountsByTicker.get(ticker) ?? new Set<string>();
        accounts.add(account);
        accountsByTicker.set(ticker, accounts);
      }
    }
    for (const pos of modeledPositions) {
      const ticker = normalizeTicker(pos.ticker);
      if (!ticker) continue;
      modelMap.set(ticker, {
        ticker,
        quantity: Number(pos.shares) || 0,
        method: exposureMethodDisplayLabel(pos.exposure_origin, pos.model_status),
        methodTone: exposureMethodTone(pos.exposure_origin, pos.model_status),
      });
    }

    for (const [ticker, liveRow] of liveMap.entries()) {
      const accounts = [...(accountsByTicker.get(ticker) ?? new Set<string>())].sort();
      liveRow.accountScope = accounts.length > 1 ? "MULTI" : (accounts[0]?.toUpperCase() ?? "—");
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
        accountScope: liveRow?.accountScope || (live === null ? "MODELED" : "—"),
        ticker: liveRow?.ticker || modeledRow?.ticker || "",
        method: modeledRow?.method || "\u2014",
        methodTone: modeledRow?.methodTone || "neutral",
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
        return rank(a.status) - rank(b.status) || deltaB - deltaA || a.accountScope.localeCompare(b.accountScope) || a.ticker.localeCompare(b.ticker);
      });
  }, [liveHoldingsRows, modeledPositions]);
  const diffComparators = useMemo<Record<ModelDiffSortKey, (left: (typeof modelVsLiveDiffs)[number], right: (typeof modelVsLiveDiffs)[number]) => number>>(
    () => ({
      account: (left, right) => compareText(left.accountScope, right.accountScope),
      ticker: (left, right) => compareText(left.ticker, right.ticker),
      method: (left, right) => compareText(left.method, right.method),
      status: (left, right) => compareText(left.status, right.status),
      live: (left, right) => compareNumber(left.live, right.live),
      modeled: (left, right) => compareNumber(left.modeled, right.modeled),
      delta: (left, right) => compareNumber(left.delta, right.delta),
    }),
    [modelVsLiveDiffs],
  );
  const { sortedRows: sortedModelVsLiveDiffs, handleSort: handleModelDiffSort, arrow: modelDiffArrow } = useSortableRows<
    (typeof modelVsLiveDiffs)[number],
    ModelDiffSortKey
  >({
    rows: modelVsLiveDiffs,
    comparators: diffComparators,
  });

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

  if (cuseLoading) {
    return <AnalyticsLoadingViz message="Loading positions..." />;
  }
  if (cuseError || accountError) {
    return <ApiErrorState title="Positions Not Ready" error={cuseError || accountError} />;
  }

  return (
    <div data-testid="positions-surface">
      <div className="chart-card mb-4">
        <div className="holdings-section-header">
          <h3>Holdings Manager</h3>
        </div>

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
      </div>

      <HoldingsLedgerSection
        holdingsRows={holdingsRows}
        modeledPositions={modeledPositions}
        cparModeledPositions={cparRiskData?.positions ?? []}
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
        {snapshotMismatch ? (
          <div style={SNAPSHOT_WARNING_STYLE}>
            The modeled snapshot and risk metadata are spanning multiple published snapshots ({truth.snapshotIds.join(" / ")}).
            Live holdings above remain authoritative, but this modeled analytics section is withheld until RECALC finishes or the page reloads into one coherent publish.
          </div>
        ) : (
          <>
            {compactTruthSummary && (
              <div className="section-subtitle" style={{ marginTop: 12, marginBottom: 0 }}>
                {compactTruthSummary}
              </div>
            )}
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
                <div className="data-metric-desc">Tickers where aggregated live holdings and the modeled snapshot still differ.</div>
              </div>
              <div className="data-metric-card">
                <h4>Source Dates</h4>
                <div className="data-metric-row">
                  <span className="data-metric-label">Prices</span>
                  <span className="data-metric-value">{truth.sourceDates.prices_asof || "—"}</span>
                </div>
                <div className="data-metric-row">
                  <span className="data-metric-label">Fundamentals</span>
                  <span className="data-metric-value">{truth.sourceDates.fundamentals_asof || "—"}</span>
                </div>
                <div className="data-metric-row">
                  <span className="data-metric-label">Classification</span>
                  <span className="data-metric-value">{truth.sourceDates.classification_asof || "—"}</span>
                </div>
                <div className="data-metric-row">
                  <span className="data-metric-label">Served Exposures</span>
                  <span className="data-metric-value">{truth.exposuresServedAsOf || "—"}</span>
                </div>
              </div>
            </div>

            {modelVsLiveDiffs.length > 0 ? (
              <div className="dash-table" style={{ marginTop: 14 }}>
                <table>
                  <thead>
                    <tr>
                      <th onClick={() => handleModelDiffSort("account")}>Account Scope{modelDiffArrow("account")}</th>
                      <th onClick={() => handleModelDiffSort("ticker")}>Ticker{modelDiffArrow("ticker")}</th>
                      <th onClick={() => handleModelDiffSort("method")}>Method{modelDiffArrow("method")}</th>
                      <th onClick={() => handleModelDiffSort("status")}>Status{modelDiffArrow("status")}</th>
                      <th className="text-right" onClick={() => handleModelDiffSort("live")}>Live Qty{modelDiffArrow("live")}</th>
                      <th className="text-right" onClick={() => handleModelDiffSort("modeled")}>Modeled Qty{modelDiffArrow("modeled")}</th>
                      <th className="text-right" onClick={() => handleModelDiffSort("delta")}>Delta{modelDiffArrow("delta")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedModelVsLiveDiffs.slice(0, 10).map((row) => (
                      <tr key={`${row.accountScope}:${row.ticker}`}>
                        <td>{row.accountScope || "—"}</td>
                        <td>{row.ticker}</td>
                        <td><MethodLabel label={row.method} tone={(row.methodTone || "neutral") as MethodLabelTone} /></td>
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
          </>
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
