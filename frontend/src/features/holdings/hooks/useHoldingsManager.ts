"use client";

import { useMemo, useState } from "react";
import { mutate } from "swr";
import { ApiError, apiPath } from "@/lib/api";
import {
  triggerHoldingsImport,
  triggerServeRefresh,
} from "@/hooks/useApi";
import type { HoldingsImportMode, HoldingsPosition } from "@/lib/types";
import { fmtQty, parseHoldingsCsv } from "../lib/csv";

export interface HoldingsConfirmConfig {
  title: string;
  body: string;
  confirmValue?: string | null;
  confirmLabel?: string;
  dangerText: string;
  onConfirm: () => Promise<void>;
}

export interface HoldingsDraftEntry {
  key: string;
  account_id: string;
  ric?: string;
  ticker?: string;
  quantity_text: string;
  source: string;
}

function normalizeAccountId(raw: string | null | undefined): string {
  return String(raw || "").trim().toLowerCase();
}

function normalizeRic(raw: string | null | undefined): string {
  return String(raw || "").trim().toUpperCase();
}

function normalizeTicker(raw: string | null | undefined): string {
  return String(raw || "").trim().toUpperCase();
}

function draftKey(accountId: string, ric?: string | null, ticker?: string | null): string {
  const account = normalizeAccountId(accountId);
  const id = normalizeRic(ric) || normalizeTicker(ticker);
  return `${account}::${id}`;
}

function formatDraftQuantity(n: number): string {
  if (!Number.isFinite(n)) return "";
  const rounded = Number(n.toFixed(6));
  if (Number.isInteger(rounded)) return String(rounded);
  return String(rounded);
}

function parseDraftQuantity(raw: string): number | null {
  const clean = String(raw || "").trim().replaceAll(",", "");
  if (!clean) return null;
  const qty = Number.parseFloat(clean);
  return Number.isFinite(qty) ? qty : null;
}

export function useHoldingsManager(selectedAccount: string, holdingsRows: HoldingsPosition[]) {
  const [busy, setBusy] = useState(false);
  const [confirmConfig, setConfirmConfig] = useState<HoldingsConfirmConfig | null>(null);
  const [resultMessage, setResultMessage] = useState("");
  const [errorMessage, setErrorMessage] = useState("");
  const [rejectionPreview, setRejectionPreview] = useState<Array<Record<string, unknown>>>([]);
  const [drafts, setDrafts] = useState<Record<string, HoldingsDraftEntry>>({});

  const normalizedSelectedAccount = normalizeAccountId(selectedAccount);

  const draftEntries = useMemo(() => Object.values(drafts), [drafts]);
  const draftCount = draftEntries.length;
  const draftDeleteCount = draftEntries.filter((entry) => {
    const qty = parseDraftQuantity(entry.quantity_text);
    return qty !== null && qty === 0;
  }).length;
  const selectedAccountDrafts = useMemo(
    () => draftEntries.filter((entry) => normalizeAccountId(entry.account_id) === normalizedSelectedAccount),
    [draftEntries, normalizedSelectedAccount],
  );

  async function revalidateHoldingsViews(accountIds: string[]) {
    const uniqueAccounts = [...new Set(accountIds.map(normalizeAccountId).filter(Boolean))];
    await Promise.all([
      mutate(apiPath.holdingsAccounts()),
      ...uniqueAccounts.map((accountId) => mutate(apiPath.holdingsPositions(accountId))),
      mutate(apiPath.operatorStatus()),
    ]);
  }

  function clearMessages() {
    setErrorMessage("");
    setResultMessage("");
    setRejectionPreview([]);
  }

  function stageDraft(
    {
      account_id,
      ric,
      ticker,
      quantity_text,
      source,
    }: {
      account_id: string;
      ric?: string | null;
      ticker?: string | null;
      quantity_text: string;
      source?: string | null;
    },
    options?: { clearMessages?: boolean },
  ) {
    const accountId = normalizeAccountId(account_id);
    const ricNorm = normalizeRic(ric);
    const tickerNorm = normalizeTicker(ticker);
    if (!accountId) {
      setErrorMessage("Select or enter an account ID first.");
      return;
    }
    if (!ricNorm && !tickerNorm) {
      setErrorMessage("Provide ticker or RIC.");
      return;
    }
    if (options?.clearMessages ?? true) {
      clearMessages();
    } else {
      setErrorMessage("");
    }
    const key = draftKey(accountId, ricNorm, tickerNorm);
    setDrafts((prev) => ({
      ...prev,
      [key]: {
        key,
        account_id: accountId,
        ric: ricNorm || undefined,
        ticker: tickerNorm || undefined,
        quantity_text,
        source: String(source || "ui_edit").trim() || "ui_edit",
      },
    }));
  }

  function discardDrafts() {
    setDrafts({});
    clearMessages();
    setResultMessage("Discarded staged holdings edits.");
  }

  function getDraftForTarget(args: {
    account_id: string;
    ric?: string | null;
    ticker?: string | null;
  }): HoldingsDraftEntry | null {
    const key = draftKey(args.account_id, args.ric, args.ticker);
    return drafts[key] ?? null;
  }

  function getDraftQuantityText(args: {
    account_id: string;
    ric?: string | null;
    ticker?: string | null;
    current_quantity: number;
  }): string {
    const draft = getDraftForTarget(args);
    return draft ? draft.quantity_text : formatDraftQuantity(args.current_quantity);
  }

  function hasDraftForTarget(args: {
    account_id: string;
    ric?: string | null;
    ticker?: string | null;
  }): boolean {
    return getDraftForTarget(args) !== null;
  }

  function isDraftInvalid(args: {
    account_id: string;
    ric?: string | null;
    ticker?: string | null;
  }): boolean {
    const draft = getDraftForTarget(args);
    return !!draft && parseDraftQuantity(draft.quantity_text) === null;
  }

  async function runCsvImport({
    csvFile,
    csvSource,
    mode,
  }: {
    csvFile: File | null;
    csvSource: string;
    mode: HoldingsImportMode;
  }) {
    clearMessages();
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
      await revalidateHoldingsViews([selectedAccount]);
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

  async function handleCsvImport({
    csvFile,
    csvSource,
    mode,
  }: {
    csvFile: File | null;
    csvSource: string;
    mode: HoldingsImportMode;
  }) {
    try {
      if (!csvFile) {
        void runCsvImport({ csvFile, csvSource, mode });
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
        void runCsvImport({ csvFile, csvSource, mode });
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
          await runCsvImport({ csvFile, csvSource, mode });
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

  function handleManualUpsert(args: {
    editRic: string;
    editTicker: string;
    editQty: string;
    editSource: string;
  }) {
    clearMessages();
    if (!selectedAccount) {
      setErrorMessage("Select or enter an account ID first.");
      return false;
    }
    const qty = parseDraftQuantity(args.editQty);
    if (qty === null) {
      setErrorMessage("Quantity must be numeric.");
      return false;
    }
    if (!args.editRic.trim() && !args.editTicker.trim()) {
      setErrorMessage("Provide ticker or RIC.");
      return false;
    }
    stageDraft(
      {
        account_id: selectedAccount,
        ric: args.editRic,
        ticker: args.editTicker,
        quantity_text: formatDraftQuantity(qty),
        source: args.editSource || "ui_edit",
      },
      { clearMessages: false },
    );
    setResultMessage(
      `Staged ${args.editTicker.trim().toUpperCase() || args.editRic.trim().toUpperCase()} @ ${fmtQty(qty)} shares. Changes are local until RECALC.`,
    );
    return true;
  }

  function handleDraftQuantityChange(row: HoldingsPosition, quantityText: string) {
    stageDraft(
      {
        account_id: row.account_id,
        ric: row.ric,
        ticker: row.ticker,
        quantity_text: quantityText,
        source: row.source || "ui_edit",
      },
      { clearMessages: false },
    );
  }

  function handleAdjust(row: HoldingsPosition, delta: number) {
    const currentText = getDraftQuantityText({
      account_id: row.account_id,
      ric: row.ric,
      ticker: row.ticker,
      current_quantity: row.quantity,
    });
    const currentQty = parseDraftQuantity(currentText);
    if (currentQty === null) {
      setErrorMessage(`Quantity for ${row.ticker || row.ric} is not numeric yet.`);
      return;
    }
    const targetQty = currentQty + Number(delta);
    stageDraft(
      {
        account_id: row.account_id,
        ric: row.ric,
        ticker: row.ticker,
        quantity_text: formatDraftQuantity(targetQty),
        source: row.source || "ui_stepper",
      },
      { clearMessages: false },
    );
    setResultMessage(`Staged ${row.ticker || row.ric} -> ${fmtQty(targetQty)} shares. Apply when ready.`);
  }

  function handleRemove(row: HoldingsPosition) {
    stageDraft(
      {
        account_id: row.account_id,
        ric: row.ric,
        ticker: row.ticker,
        quantity_text: "0",
        source: row.source || "ui_edit",
      },
      { clearMessages: false },
    );
    setResultMessage(`Staged removal for ${row.ticker || row.ric}. Apply when ready.`);
  }

  async function handleApplyDrafts() {
    clearMessages();
    if (draftEntries.length === 0) {
      setErrorMessage("No staged edits to apply.");
      return;
    }
    const invalidDraft = draftEntries.find((entry) => parseDraftQuantity(entry.quantity_text) === null);
    if (invalidDraft) {
      setErrorMessage(`Fix the quantity for ${invalidDraft.ticker || invalidDraft.ric} before applying.`);
      return;
    }

    try {
      setBusy(true);
      const byAccount = new Map<
        string,
        Array<{ ric?: string; ticker?: string; quantity: number; source?: string }>
      >();
      for (const entry of draftEntries) {
        const quantity = parseDraftQuantity(entry.quantity_text);
        if (quantity === null) continue;
        const accountId = normalizeAccountId(entry.account_id);
        const rows = byAccount.get(accountId) ?? [];
        rows.push({
          ric: entry.ric,
          ticker: entry.ticker,
          quantity,
          source: entry.source,
        });
        byAccount.set(accountId, rows);
      }

      for (const [accountId, rows] of byAccount.entries()) {
        await triggerHoldingsImport({
          account_id: accountId,
          mode: "upsert_absolute",
          rows,
          default_source: "ui_staged",
          trigger_refresh: false,
        });
      }
      await triggerServeRefresh();
      await revalidateHoldingsViews([...byAccount.keys()]);
      setDrafts({});
      setResultMessage(
        `Applied ${draftEntries.length} staged edit${draftEntries.length === 1 ? "" : "s"} across ${byAccount.size} account${byAccount.size === 1 ? "" : "s"} and started RECALC.`,
      );
    } catch (err) {
      if (err instanceof ApiError) {
        setErrorMessage(typeof err.detail === "string" ? err.detail : err.message);
      } else if (err instanceof Error) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage("Could not apply staged edits.");
      }
    } finally {
      setBusy(false);
    }
  }

  return {
    busy,
    confirmConfig,
    draftCount,
    draftDeleteCount,
    draftEntries,
    selectedAccountDrafts,
    errorMessage,
    rejectionPreview,
    resultMessage,
    handleAdjust,
    handleApplyDrafts,
    handleCsvImport,
    handleDraftQuantityChange,
    handleManualUpsert,
    handleRemove,
    hasDraftForTarget,
    getDraftQuantityText,
    isDraftInvalid,
    discardDrafts,
    setConfirmConfig,
  };
}
