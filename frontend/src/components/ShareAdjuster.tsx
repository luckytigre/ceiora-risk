"use client";

import { useMemo, useState } from "react";
import { mutate } from "swr";
import { apiPath, ApiError } from "@/lib/api";
import { triggerHoldingsImport } from "@/hooks/useApi";
import ConfirmActionModal from "@/components/ConfirmActionModal";
import type { PortfolioData, Position } from "@/lib/types";

interface ShareAdjusterProps {
  ticker: string;
  currentShares: number;
  accountId: string;
  step?: number;
}

function normalizeAccountId(raw: string | null | undefined): string | null {
  const clean = String(raw || "").trim().toLowerCase();
  if (!clean) return null;
  if (clean === "multi") return null;
  if (!/^[a-z0-9_-]{2,64}$/.test(clean)) return null;
  return clean;
}

export default function ShareAdjuster({ ticker, currentShares, accountId, step = 5 }: ShareAdjusterProps) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastAction, setLastAction] = useState<"up" | "down" | null>(null);
  const [confirmDelta, setConfirmDelta] = useState<number | null>(null);

  const accountIdNorm = useMemo(
    () => normalizeAccountId(accountId),
    [accountId],
  );

  function optimisticPortfolioUpdate(delta: number) {
    mutate(
      apiPath.portfolio(),
      (current?: PortfolioData) => {
        if (!current) return current;
        const rows: Position[] = (current.positions || []).map((p) => ({ ...p }));
        const idx = rows.findIndex((p) => String(p.ticker || "").toUpperCase() === ticker.toUpperCase());
        if (idx >= 0) {
          const startingShares = Number(rows[idx].shares ?? currentShares ?? 0);
          const targetShares = startingShares + Number(delta);
          rows[idx].shares = targetShares;
          rows[idx].long_short = targetShares < 0 ? "SHORT" : "LONG";
          rows[idx].market_value = Number(rows[idx].price || 0) * targetShares;
        }
        const total = rows.reduce((acc, p) => acc + Number(p.market_value || 0), 0);
        for (const p of rows) {
          p.weight = total !== 0 ? Number(p.market_value || 0) / total : 0;
        }
        return {
          ...current,
          positions: rows,
          total_value: total,
          position_count: rows.length,
        };
      },
      false,
    );
  }

  async function adjust(delta: number, direction: "up" | "down") {
    setError(null);
    setLastAction(direction);
    if (!accountIdNorm) {
      setError("No account ID");
      return;
    }
    if (Number(currentShares) + Number(delta) === 0) {
      setConfirmDelta(delta);
      return;
    }
    await adjustConfirmed(delta);
  }

  async function adjustConfirmed(delta: number) {
    if (!accountIdNorm) {
      setError("No account ID");
      return;
    }
    try {
      setBusy(true);
      await triggerHoldingsImport({
        account_id: accountIdNorm,
        mode: "increment_delta",
        rows: [{ ticker, quantity: delta, source: "ui_stepper" }],
        trigger_refresh: false,
      });
      optimisticPortfolioUpdate(delta);
      await Promise.all([
        mutate(apiPath.holdingsPositions(accountIdNorm)),
        mutate(apiPath.holdingsAccounts()),
        mutate(apiPath.operatorStatus()),
      ]);
    } catch (err) {
      if (err instanceof ApiError) {
        setError(typeof err.detail === "string" ? err.detail : err.message);
      } else if (err instanceof Error) {
        setError(err.message);
      } else {
        setError("Adjust failed");
      }
    } finally {
      setBusy(false);
      window.setTimeout(() => setLastAction(null), 900);
    }
  }

  return (
    <span className="share-adjuster-wrap">
      <button
        className={`share-adjuster-btn ${lastAction === "up" ? "active" : ""}`}
        onClick={() => adjust(step, "up")}
        disabled={busy}
        title={`Increase ${ticker} by ${step} shares`}
        aria-label={`Increase ${ticker} by ${step} shares`}
      >
        ↑
      </button>
      <button
        className={`share-adjuster-btn ${lastAction === "down" ? "active" : ""}`}
        onClick={() => adjust(-step, "down")}
        disabled={busy}
        title={`Decrease ${ticker} by ${step} shares`}
        aria-label={`Decrease ${ticker} by ${step} shares`}
      >
        ↓
      </button>
      {error && <span className="share-adjuster-error" title={error}>!</span>}
      <ConfirmActionModal
        open={confirmDelta !== null}
        title="Confirm zero-share adjustment"
        body={`This stepper action will remove ${ticker} from account ${accountIdNorm?.toUpperCase() || accountId}.`}
        confirmValue="REMOVE"
        confirmLabel="Type to confirm"
        dangerText="Remove position"
        onCancel={() => setConfirmDelta(null)}
        onConfirm={async () => {
          const delta = confirmDelta;
          setConfirmDelta(null);
          if (delta !== null) {
            await adjustConfirmed(delta);
          }
        }}
      />
    </span>
  );
}
