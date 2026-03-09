"use client";

import { useEffect, useMemo, useState } from "react";
import { mutate } from "swr";
import { apiPath, ApiError } from "@/lib/api";
import {
  removeHoldingPosition,
  triggerHoldingsImport,
  upsertHoldingPosition,
  useHoldingsAccounts,
  useHoldingsModes,
  useHoldingsPositions,
  usePortfolio,
  useUniverseSearch,
} from "@/hooks/useApi";
import { useDebouncedValue } from "@/hooks/useDebouncedValue";
import ConfirmActionModal from "@/components/ConfirmActionModal";
import PositionTable from "@/components/PositionTable";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ApiErrorState from "@/components/ApiErrorState";
import type { HoldingsImportMode, HoldingsPosition } from "@/lib/types";

function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === "\"") {
      if (inQuotes && i + 1 < line.length && line[i + 1] === "\"") {
        current += "\"";
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(current.trim());
      current = "";
      continue;
    }
    current += ch;
  }
  out.push(current.trim());
  return out;
}

function normalizeHeader(raw: string): string {
  return raw.trim().toLowerCase().replace(/[\s\-]+/g, "_");
}

function pickField(cells: string[], idx: Record<string, number>, keys: string[]): string {
  for (const key of keys) {
    const i = idx[key];
    if (typeof i === "number" && i >= 0 && i < cells.length) {
      return String(cells[i] ?? "").trim();
    }
  }
  return "";
}

function parseHoldingsCsv(text: string, defaultSource: string) {
  const lines = text
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  if (lines.length < 2) {
    throw new Error("CSV must include a header row and at least one data row.");
  }
  const headers = parseCsvLine(lines[0]).map(normalizeHeader);
  const idx: Record<string, number> = {};
  headers.forEach((h, i) => {
    idx[h] = i;
  });

  const rows: Array<{ account_id?: string; ric?: string; ticker?: string; quantity: number; source?: string }> = [];
  const rejected: string[] = [];

  for (let r = 1; r < lines.length; r += 1) {
    const cells = parseCsvLine(lines[r]);
    const ric = pickField(cells, idx, ["ric", "ric_code", "lseg_ric"]).toUpperCase();
    const ticker = pickField(cells, idx, ["ticker", "symbol", "security"]).toUpperCase();
    const qtyRaw = pickField(cells, idx, ["quantity", "qty", "shares", "position", "position_qty"]).replaceAll(",", "");
    const source = pickField(cells, idx, ["source", "origin"]) || defaultSource;
    const accountId = pickField(cells, idx, ["account_id", "account", "accountid", "acct"]);

    const qty = Number.parseFloat(qtyRaw);
    if (!Number.isFinite(qty)) {
      rejected.push(`line ${r + 1}: invalid quantity "${qtyRaw}"`);
      continue;
    }
    if (!ric && !ticker) {
      rejected.push(`line ${r + 1}: missing ticker/ric`);
      continue;
    }
    rows.push({
      account_id: accountId || undefined,
      ric: ric || undefined,
      ticker: ticker || undefined,
      quantity: qty,
      source,
    });
  }

  if (rows.length === 0) {
    throw new Error("CSV rows were parsed, but none were usable.");
  }
  return { rows, rejected };
}

function fmtQty(n: number): string {
  const abs = Math.abs(n);
  const digits = abs >= 1000 ? 0 : abs >= 100 ? 1 : abs >= 10 ? 2 : 4;
  return n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: digits });
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
  const [busy, setBusy] = useState(false);
  const [confirmConfig, setConfirmConfig] = useState<null | {
    title: string;
    body: string;
    confirmValue?: string | null;
    confirmLabel?: string;
    dangerText: string;
    onConfirm: () => Promise<void>;
  }>(null);
  const [resultMessage, setResultMessage] = useState("");
  const [errorMessage, setErrorMessage] = useState("");
  const [rejectionPreview, setRejectionPreview] = useState<Array<Record<string, unknown>>>([]);
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

  if (pLoading) {
    return <AnalyticsLoadingViz message="Loading positions..." />;
  }
  if (pError || accountError) {
    return <ApiErrorState title="Positions Not Ready" error={pError || accountError} />;
  }

  const positions = portfolio?.positions ?? [];
  const accountOptions = accountsData?.accounts ?? [];
  const holdingsRows = [...(holdingsData?.positions ?? [])].sort((a, b) => Math.abs(b.quantity) - Math.abs(a.quantity));

  async function revalidateHoldingsViews(accountId: string) {
    await Promise.all([
      mutate(apiPath.holdingsAccounts()),
      mutate(apiPath.holdingsPositions(accountId)),
      mutate(apiPath.portfolio()),
      mutate(apiPath.operatorStatus()),
    ]);
  }

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

  async function runCsvImport() {
    setErrorMessage("");
    setResultMessage("");
    setRejectionPreview([]);
    if (!selectedAccount) {
      setErrorMessage("Select or enter an account ID first.");
      return;
    }
    if (!csvFile) {
      setErrorMessage("Select a CSV file first.");
      return;
    }

    try {
      setBusy(true);
      const raw = await csvFile.text();
      const parsed = parseHoldingsCsv(raw, csvSource || "csv_upload");
      const out = await triggerHoldingsImport({
        account_id: selectedAccount,
        mode,
        rows: parsed.rows,
        filename: csvFile.name,
        default_source: csvSource || "csv_upload",
        trigger_refresh: false,
      });
      const extras = parsed.rejected.length > 0 ? ` | CSV parse rejects: ${parsed.rejected.length}` : "";
      setResultMessage(
        `${out.status}: ${out.applied_upserts} upserts, ${out.applied_deletes} deletes, ${out.rejected_rows} backend rejects${extras}.`,
      );
      setRejectionPreview((out.preview_rejections ?? []).slice(0, 15));
      await revalidateHoldingsViews(selectedAccount);
    } catch (err) {
      if (err instanceof ApiError) {
        setErrorMessage(typeof err.detail === "string" ? err.detail : err.message);
      } else if (err instanceof Error) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage("Import failed.");
      }
    } finally {
      setBusy(false);
    }
  }

  async function handleCsvImport() {
    try {
      if (!csvFile) {
        void runCsvImport();
        return;
      }
      const raw = await csvFile.text();
      const parsed = parseHoldingsCsv(raw, csvSource || "csv_upload");
      const destructiveRows = parsed.rows.filter((row) => {
        const qty = Number(row.quantity || 0);
        if (mode === "replace_account") return true;
        if (mode === "upsert_absolute") return qty === 0;
        const existing = holdingsRows.find(
          (h) =>
            (row.ric && h.ric === String(row.ric).toUpperCase()) ||
            (row.ticker && h.ticker === String(row.ticker).toUpperCase()),
        );
        return !!existing && Number(existing.quantity) + qty === 0;
      });
      if (destructiveRows.length === 0) {
        void runCsvImport();
        return;
      }
      setConfirmConfig({
        title: "Confirm destructive CSV import",
        body:
          mode === "replace_account"
            ? `This will replace all current positions in ${selectedAccount || "the selected account"} with only the rows present in the CSV. Positions omitted from the file will be deleted.`
            : `This CSV will delete ${destructiveRows.length} position${destructiveRows.length === 1 ? "" : "s"} in ${selectedAccount || "the selected account"} because the imported quantity resolves to zero.`,
        confirmValue: selectedAccount || null,
        confirmLabel: "Type account ID",
        dangerText: "Run import",
        onConfirm: async () => {
          setConfirmConfig(null);
          await runCsvImport();
        },
      });
    } catch (err) {
      if (err instanceof Error) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage("Unable to inspect CSV import.");
      }
    }
  }

  async function runManualUpsert() {
    setErrorMessage("");
    setResultMessage("");
    if (!selectedAccount) {
      setErrorMessage("Select or enter an account ID first.");
      return;
    }
    const qty = Number.parseFloat(editQty);
    if (!Number.isFinite(qty)) {
      setErrorMessage("Quantity must be numeric.");
      return;
    }
    if (!editRic.trim() && !editTicker.trim()) {
      setErrorMessage("Provide ticker or RIC.");
      return;
    }

    try {
      setBusy(true);
      const out = await upsertHoldingPosition({
        account_id: selectedAccount,
        ric: editRic.trim() ? editRic.trim().toUpperCase() : undefined,
        ticker: editTicker.trim() ? editTicker.trim().toUpperCase() : undefined,
        quantity: qty,
        source: editSource || "ui_edit",
        trigger_refresh: false,
      });
      setResultMessage(`${out.status}: ${out.action} ${out.ticker || out.ric} @ ${fmtQty(out.quantity)}`);
      await revalidateHoldingsViews(selectedAccount);
    } catch (err) {
      if (err instanceof ApiError) {
        setErrorMessage(typeof err.detail === "string" ? err.detail : err.message);
      } else if (err instanceof Error) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage("Position update failed.");
      }
    } finally {
      setBusy(false);
    }
  }

  function handleManualUpsert() {
    const qty = Number.parseFloat(editQty);
    if (Number.isFinite(qty) && qty === 0) {
      setConfirmConfig({
        title: "Confirm zero-quantity upsert",
        body: "A zero quantity is treated as a delete for this position in the selected account.",
        confirmValue: "REMOVE",
        confirmLabel: "Type to confirm",
        dangerText: "Remove position",
        onConfirm: async () => {
          setConfirmConfig(null);
          await runManualUpsert();
        },
      });
      return;
    }
    void runManualUpsert();
  }

  async function runRemove(row: HoldingsPosition) {
    setErrorMessage("");
    setResultMessage("");
    try {
      setBusy(true);
      const out = await removeHoldingPosition({
        account_id: row.account_id,
        ric: row.ric,
        trigger_refresh: false,
      });
      setResultMessage(`${out.status}: removed ${row.ticker || row.ric}`);
      await revalidateHoldingsViews(row.account_id);
    } catch (err) {
      if (err instanceof ApiError) {
        setErrorMessage(typeof err.detail === "string" ? err.detail : err.message);
      } else if (err instanceof Error) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage("Position remove failed.");
      }
    } finally {
      setBusy(false);
    }
  }

  function handleRemove(row: HoldingsPosition) {
    setConfirmConfig({
      title: "Confirm position removal",
      body: `This will delete ${row.ticker || row.ric} from account ${row.account_id}.`,
      confirmValue: "REMOVE",
      confirmLabel: "Type to confirm",
      dangerText: "Remove",
      onConfirm: async () => {
        setConfirmConfig(null);
        await runRemove(row);
      },
    });
  }

  async function handleAdjust(row: HoldingsPosition, delta: number) {
    const targetQty = Number(row.quantity) + Number(delta);
    if (targetQty === 0) {
      setConfirmConfig({
        title: "Confirm zero-share adjustment",
        body: `This adjustment will remove ${row.ticker || row.ric} from account ${row.account_id}.`,
        confirmValue: "REMOVE",
        confirmLabel: "Type to confirm",
        dangerText: "Remove position",
        onConfirm: async () => {
          setConfirmConfig(null);
          await handleAdjustConfirmed(row, delta);
        },
      });
      return;
    }
    await handleAdjustConfirmed(row, delta);
  }

  async function handleAdjustConfirmed(row: HoldingsPosition, delta: number) {
    setErrorMessage("");
    setResultMessage("");
    try {
      setBusy(true);
      const targetQty = Number(row.quantity) + Number(delta);
      const out = await upsertHoldingPosition({
        account_id: row.account_id,
        ric: row.ric,
        quantity: targetQty,
        source: "ui_stepper",
        trigger_refresh: false,
      });
      setResultMessage(`${out.status}: ${row.ticker || row.ric} -> ${fmtQty(targetQty)} shares`);
      await revalidateHoldingsViews(row.account_id);
    } catch (err) {
      if (err instanceof ApiError) {
        setErrorMessage(typeof err.detail === "string" ? err.detail : err.message);
      } else if (err instanceof Error) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage("Position adjust failed.");
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <div className="chart-card mb-4">
        <h3>Holdings Manager</h3>
        <div className="holdings-grid">
          <div className="holdings-form-block">
            <label htmlFor="account-id">Account ID</label>
            <input
              id="account-id"
              className="explore-input holdings-compact-input"
              list="account-id-options"
              placeholder="ACCT-CORE"
              value={selectedAccount}
              onChange={(e) => setSelectedAccount(e.target.value.toUpperCase())}
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
            <label htmlFor="import-mode">CSV Mode</label>
            <select
              id="import-mode"
              className="health-select"
              value={mode}
              onChange={(e) => setMode(e.target.value as HoldingsImportMode)}
            >
              {(modesData?.modes ?? ["replace_account", "upsert_absolute", "increment_delta"]).map((m) => (
                <option key={m} value={m}>{modeLabel(m)}</option>
              ))}
            </select>
            <div style={{ color: "rgba(169,182,210,0.8)", fontSize: 11 }}>{modeHelp(mode)}</div>
          </div>

          <div className="holdings-form-block">
            <label htmlFor="csv-source">Source Tag</label>
            <input
              id="csv-source"
              className="explore-input holdings-compact-input"
              value={csvSource}
              onChange={(e) => setCsvSource(e.target.value)}
              placeholder="csv_upload"
            />
          </div>

          <div className="holdings-form-block">
            <label htmlFor="csv-file">Import CSV</label>
            <input
              id="csv-file"
              className="holdings-file-input"
              type="file"
              accept=".csv,text/csv"
              onChange={(e) => setCsvFile(e.target.files?.[0] ?? null)}
            />
          </div>

          <div className="holdings-form-block">
            <button
              className="explore-search-btn"
              onClick={handleCsvImport}
              disabled={busy}
              style={{ width: "fit-content", paddingLeft: 0 }}
            >
              {busy ? "Running..." : "Run CSV Import"}
            </button>
          </div>
        </div>

        <div className="holdings-grid" style={{ marginTop: 14 }}>
          <div className="holdings-form-block">
            <label htmlFor="edit-ticker">Ticker</label>
            <input
              id="edit-ticker"
              className="explore-input"
              value={editTicker}
              onChange={(e) => setEditTicker(e.target.value.toUpperCase())}
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
              onChange={(e) => setEditRic(e.target.value.toUpperCase())}
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
              onChange={(e) => setEditQty(e.target.value)}
              placeholder="125.5"
            />
          </div>
          <div className="holdings-form-block">
            <label htmlFor="edit-source">Source</label>
            <input
              id="edit-source"
              className="explore-input"
              value={editSource}
              onChange={(e) => setEditSource(e.target.value)}
              placeholder="ui_edit"
            />
          </div>
          <div className="holdings-form-block">
            <button
              className="explore-search-btn"
              onClick={handleManualUpsert}
              disabled={busy}
              style={{ width: "fit-content", paddingLeft: 0 }}
            >
              {busy ? "Saving..." : "Upsert Position"}
            </button>
          </div>
        </div>

        {resultMessage && (
          <div style={{ marginTop: 10, color: "rgba(107, 207, 154, 0.88)", fontSize: 12 }}>
            {resultMessage}
          </div>
        )}
        {errorMessage && (
          <div style={{ marginTop: 10, color: "rgba(224, 87, 127, 0.92)", fontSize: 12 }}>
            {errorMessage}
          </div>
        )}
        {rejectionPreview.length > 0 && (
          <div style={{ marginTop: 10, fontSize: 11, color: "rgba(232, 237, 249, 0.75)" }}>
            Preview rejections:
            <pre style={{ marginTop: 6, whiteSpace: "pre-wrap" }}>
              {JSON.stringify(rejectionPreview, null, 2)}
            </pre>
          </div>
        )}
      </div>

      <div className="chart-card mb-4">
        <h3>
          Current Holdings
          {selectedAccount ? ` (${selectedAccount})` : ""}
          {" "}
          [{holdingsRows.length}]
        </h3>
        <div className="detail-history-empty" style={{ marginBottom: 10 }}>
          This table is the live holdings ledger. The model portfolio table below refreshes after a serving update, so temporary differences are expected until `RECALC` runs.
        </div>
        {holdingsError ? (
          <ApiErrorState title="Holdings Not Ready" error={holdingsError} />
        ) : (
          <div className="dash-table" style={{ overflowX: "auto" }}>
            <table>
              <thead>
                <tr>
                  <th>Account</th>
                  <th>Ticker</th>
                  <th>RIC</th>
                  <th className="text-right">Quantity</th>
                  <th>Source</th>
                  <th>Updated</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {holdingsRows.map((row) => (
                  <tr key={`${row.account_id}:${row.ric}`}>
                    <td>{row.account_id}</td>
                    <td>{row.ticker || "—"}</td>
                    <td>{row.ric}</td>
                    <td className="text-right">{fmtQty(row.quantity)}</td>
                    <td>{row.source || "—"}</td>
                    <td>{row.updated_at || "—"}</td>
                    <td>
                      <span style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                        <button
                          className="explore-search-btn"
                          onClick={() => handleAdjust(row, 5)}
                          disabled={busy}
                          style={{ padding: 0 }}
                          title={`Increase ${row.ticker} by 5 shares`}
                        >
                          ↑5
                        </button>
                        <button
                          className="explore-search-btn"
                          onClick={() => handleAdjust(row, -5)}
                          disabled={busy}
                          style={{ padding: 0 }}
                          title={`Decrease ${row.ticker} by 5 shares`}
                        >
                          ↓5
                        </button>
                        <button
                          className="explore-search-btn"
                          onClick={() => handleRemove(row)}
                          disabled={busy}
                          style={{ padding: 0 }}
                        >
                          Remove
                        </button>
                      </span>
                    </td>
                  </tr>
                ))}
                {holdingsRows.length === 0 && (
                  <tr>
                    <td colSpan={7} style={{ color: "rgba(169,182,210,0.75)" }}>
                      No positions for this account yet.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="chart-card mb-4">
        <h3>Model Portfolio Positions ({positions.length})</h3>
        <PositionTable positions={positions} />
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
