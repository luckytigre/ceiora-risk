"use client";

import { useState } from "react";
import { mutate } from "swr";
import { ApiError, apiPath } from "@/lib/api";
import {
  removeHoldingPosition,
  triggerHoldingsImport,
  upsertHoldingPosition,
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

export function useHoldingsManager(selectedAccount: string, holdingsRows: HoldingsPosition[]) {
  const [busy, setBusy] = useState(false);
  const [confirmConfig, setConfirmConfig] = useState<HoldingsConfirmConfig | null>(null);
  const [resultMessage, setResultMessage] = useState("");
  const [errorMessage, setErrorMessage] = useState("");
  const [rejectionPreview, setRejectionPreview] = useState<Array<Record<string, unknown>>>([]);

  async function revalidateHoldingsViews(accountId: string) {
    await Promise.all([
      mutate(apiPath.holdingsAccounts()),
      mutate(apiPath.holdingsPositions(accountId)),
      mutate(apiPath.operatorStatus()),
    ]);
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

  async function runManualUpsert({
    editRic,
    editTicker,
    editQty,
    editSource,
  }: {
    editRic: string;
    editTicker: string;
    editQty: string;
    editSource: string;
  }) {
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

  function handleManualUpsert(args: {
    editRic: string;
    editTicker: string;
    editQty: string;
    editSource: string;
  }) {
    const qty = Number.parseFloat(args.editQty);
    if (Number.isFinite(qty) && qty === 0) {
      setConfirmConfig({
        title: "Confirm zero-quantity upsert",
        body: "A zero quantity is treated as a delete for this position in the selected account.",
        confirmValue: "REMOVE",
        confirmLabel: "Type to confirm",
        dangerText: "Remove position",
        onConfirm: async () => {
          setConfirmConfig(null);
          await runManualUpsert(args);
        },
      });
      return;
    }
    void runManualUpsert(args);
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

  return {
    busy,
    confirmConfig,
    errorMessage,
    rejectionPreview,
    resultMessage,
    handleAdjust,
    handleCsvImport,
    handleManualUpsert,
    handleRemove,
    setConfirmConfig,
  };
}
