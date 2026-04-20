"use client";

import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import { CparInlineLoadingState } from "@/features/cpar/components/CparLoadingState";
import { useCparPositionHedge } from "@/hooks/useCparApi";
import {
  describeCparHedgeStatus,
  formatCparPercent,
  readCparDependencyErrorMessage,
  readCparError,
} from "@/lib/cparTruth";
import type { CparPackageMeta, CparPortfolioPositionRow, CparPositionHedgePackage } from "@/lib/types/cpar";

function formatMoney(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "—";
  return value.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

function formatQty(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "—";
  const decimals = Math.abs(value) < 10 ? 1 : 0;
  return value.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function signTone(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value) || value === 0) return "";
  return value > 0 ? "positive" : "negative";
}

function PackageBlock({
  title,
  pkg,
}: {
  title: string;
  pkg: CparPositionHedgePackage;
}) {
  const status = describeCparHedgeStatus(pkg.hedge_status);

  return (
    <section className="cpar-position-hedge-package">
      <div className="cpar-position-hedge-package-head">
        <div>
          <strong>{title}</strong>
          <div className="cpar-table-sub">{pkg.hedge_reason || status.detail}</div>
        </div>
        <span className={`cpar-position-hedge-status ${status.tone}`}>{status.label}</span>
      </div>
      <div className="cpar-badge-row compact">
        <span className="cpar-detail-chip">Reduction {formatCparPercent(pkg.non_market_reduction_ratio, 1)}</span>
      </div>
      {pkg.trade_rows.length === 0 ? (
        <div className="detail-history-empty compact">No hedge legs were required for this package.</div>
      ) : (
        <div className="dash-table cpar-position-hedge-table">
          <table>
            <thead>
              <tr>
                <th>ETF</th>
                <th className="text-right">Quantity</th>
                <th className="text-right">Value</th>
              </tr>
            </thead>
            <tbody>
              {pkg.trade_rows.map((row) => {
                const tone = signTone(row.quantity);
                return (
                  <tr key={`${pkg.mode}-${row.factor_id}`}>
                    <td>
                      <strong>{row.proxy_ticker}</strong>
                      <span className="cpar-table-sub">{row.label || row.factor_id}</span>
                    </td>
                    <td className={`text-right cpar-number-cell ${tone}`.trim()}>{formatQty(row.quantity)}</td>
                    <td className="text-right cpar-number-cell">{formatMoney(row.dollar_notional)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

export default function CparPositionHedgePopover({
  row,
  anchorEl,
  onClose,
  packageIdentity,
  scope,
  accountId,
}: {
  row: CparPortfolioPositionRow;
  anchorEl: HTMLElement;
  onClose: () => void;
  packageIdentity: Pick<CparPackageMeta, "package_run_id" | "package_date">;
  scope: "all_permitted_accounts" | "account";
  accountId?: string | null;
}) {
  const [mounted, setMounted] = useState(false);
  const [coords, setCoords] = useState<{ top: number; left: number } | null>(null);
  const { data, error, isLoading } = useCparPositionHedge(row.ric, scope, accountId, true);
  const driftedPackage = Boolean(
    data
      && (data.package_run_id !== packageIdentity.package_run_id
        || data.package_date !== packageIdentity.package_date),
  );
  const errorState = error ? readCparError(error) : null;

  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    function updateCoords() {
      const rect = anchorEl.getBoundingClientRect();
      const width = Math.min(440, window.innerWidth - 24);
      setCoords({
        top: rect.bottom + 8,
        left: Math.min(Math.max(12, rect.left), window.innerWidth - width - 12),
      });
    }

    function handlePointerDown(event: MouseEvent) {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (anchorEl.contains(target)) return;
      if (target instanceof Element && target.closest("[data-cpar-position-hedge-popover]")) return;
      onClose();
    }

    function handleEscape(event: KeyboardEvent) {
      if (event.key === "Escape") onClose();
    }

    updateCoords();
    window.addEventListener("resize", updateCoords);
    window.addEventListener("scroll", updateCoords, true);
    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("keydown", handleEscape);
    return () => {
      window.removeEventListener("resize", updateCoords);
      window.removeEventListener("scroll", updateCoords, true);
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("keydown", handleEscape);
    };
  }, [anchorEl, onClose]);

  const title = useMemo(() => `${row.ticker || row.ric} Hedge Package`, [row.ric, row.ticker]);
  const baseNotional = typeof data?.position?.base_notional === "number"
    ? data.position.base_notional
    : Math.abs(Number(row.market_value || 0));

  if (!mounted || !coords) return null;

  return createPortal(
    <div
      className="cpar-position-hedge-popover"
      data-cpar-position-hedge-popover
      style={{ top: coords.top, left: coords.left }}
    >
      <div className="cpar-position-hedge-popover-head">
        <div>
          <strong>{title}</strong>
          <div className="cpar-table-sub">Base notional {formatMoney(baseNotional)}</div>
        </div>
        <button type="button" className="cpar-position-hedge-close" onClick={onClose} aria-label="Close hedge popover">
          ×
        </button>
      </div>

      {isLoading && !data ? <CparInlineLoadingState message="Loading row hedge packages..." /> : null}

      {errorState ? (
        <div className={`cpar-inline-message ${errorState.kind === "missing" ? "warning" : "error"}`}>
          <strong>Hedge surface unavailable.</strong>
          <span>{readCparDependencyErrorMessage(error)}</span>
        </div>
      ) : null}

      {driftedPackage ? (
        <div className="cpar-inline-message warning">
          <strong>Package drift detected.</strong>
          <span>
            The row hedge response was built from a different cPAR package than the parent risk table, so the popup was
            withheld.
          </span>
        </div>
      ) : null}

      {data && !driftedPackage ? (
        <div className="cpar-position-hedge-package-grid">
          <PackageBlock title="Market Neutral" pkg={data.packages.market_neutral} />
          <PackageBlock title="Factor Neutral" pkg={data.packages.factor_neutral} />
        </div>
      ) : null}
    </div>,
    document.body,
  );
}
