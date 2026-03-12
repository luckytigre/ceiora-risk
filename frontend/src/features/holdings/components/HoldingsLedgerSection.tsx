"use client";

import ApiErrorState from "@/components/ApiErrorState";
import type { HoldingsPosition } from "@/lib/types";
import { fmtQty } from "../lib/csv";
import InlineShareDraftEditor from "./InlineShareDraftEditor";

interface HoldingsLedgerSectionProps {
  selectedAccount: string;
  holdingsRows: HoldingsPosition[];
  holdingsError?: unknown;
  busy: boolean;
  getDraftQuantityText: (row: HoldingsPosition) => string;
  hasDraftForRow: (row: HoldingsPosition) => boolean;
  isDraftInvalidForRow: (row: HoldingsPosition) => boolean;
  onAdjust: (row: HoldingsPosition, delta: number) => void;
  onDraftQuantityChange: (row: HoldingsPosition, value: string) => void;
  onRemove: (row: HoldingsPosition) => void;
}

export default function HoldingsLedgerSection({
  selectedAccount,
  holdingsRows,
  holdingsError,
  busy,
  getDraftQuantityText,
  hasDraftForRow,
  isDraftInvalidForRow,
  onAdjust,
  onDraftQuantityChange,
  onRemove,
}: HoldingsLedgerSectionProps) {
  return (
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
                  <td className="text-right">
                    <InlineShareDraftEditor
                      quantityText={getDraftQuantityText(row)}
                      disabled={busy}
                      draftActive={hasDraftForRow(row)}
                      invalid={isDraftInvalidForRow(row)}
                      titleBase={row.ticker || row.ric}
                      onQuantityTextChange={(value) => onDraftQuantityChange(row, value)}
                      onStep={(delta) => onAdjust(row, delta)}
                    />
                    {hasDraftForRow(row) && !isDraftInvalidForRow(row) && (
                      <div style={{ marginTop: 4, fontSize: 11, color: "rgba(224, 190, 92, 0.9)" }}>
                        Draft: {fmtQty(Number.parseFloat(getDraftQuantityText(row)) || 0)}
                      </div>
                    )}
                  </td>
                  <td>{row.source || "—"}</td>
                  <td>{row.updated_at || "—"}</td>
                  <td>
                    <span style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                      <button
                        className="explore-search-btn"
                        onClick={() => onRemove(row)}
                        disabled={busy}
                        style={{ padding: 0 }}
                      >
                        Stage Remove
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
  );
}
