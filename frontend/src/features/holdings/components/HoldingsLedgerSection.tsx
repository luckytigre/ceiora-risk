"use client";

import { useMemo, useState } from "react";
import ApiErrorState from "@/components/ApiErrorState";
import TableRowToggle from "@/components/TableRowToggle";
import type { HoldingsPosition, Position } from "@/lib/types";
import InlineShareDraftEditor from "./InlineShareDraftEditor";

interface HoldingsLedgerSectionProps {
  holdingsRows: HoldingsPosition[];
  modeledPositions: Position[];
  holdingsError?: unknown;
  busy: boolean;
  getDraftQuantityText: (row: HoldingsPosition) => string;
  hasDraftForRow: (row: HoldingsPosition) => boolean;
  isDraftInvalidForRow: (row: HoldingsPosition) => boolean;
  onAdjust: (row: HoldingsPosition, delta: number) => void;
  onDraftQuantityChange: (row: HoldingsPosition, value: string) => void;
}

type SortKey =
  | "ticker"
  | "quantity"
  | "price"
  | "market_value"
  | "source"
  | "account_id";

type MarketValueSortMode = "abs" | "signed";

const COLLAPSED_ROWS = 18;

function normalizeTicker(value: string | null | undefined): string {
  return String(value || "").trim().toUpperCase();
}

function fmtQty(n: number): string {
  return n.toLocaleString(undefined, { maximumFractionDigits: 6 });
}

function fmtCurrency(n: number | null): string {
  if (n === null || !Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

function fmtMarketValue(n: number | null): string {
  if (n === null || !Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return n.toFixed(2);
}

function marketValueTone(n: number | null): string {
  if (n === null || !Number.isFinite(n)) return "";
  if (n > 0) return "positive";
  if (n < 0) return "negative";
  return "";
}

export default function HoldingsLedgerSection({
  holdingsRows,
  modeledPositions,
  holdingsError,
  busy,
  getDraftQuantityText,
  hasDraftForRow,
  isDraftInvalidForRow,
  onAdjust,
  onDraftQuantityChange,
}: HoldingsLedgerSectionProps) {
  const [sortKey, setSortKey] = useState<SortKey>("market_value");
  const [sortAsc, setSortAsc] = useState(false);
  const [marketValueSortMode, setMarketValueSortMode] = useState<MarketValueSortMode>("abs");
  const [showAllRows, setShowAllRows] = useState(false);

  const modeledMap = useMemo(() => {
    const out = new Map<string, Position>();
    for (const pos of modeledPositions) {
      out.set(normalizeTicker(pos.ticker), pos);
    }
    return out;
  }, [modeledPositions]);

  const enrichedRows = useMemo(() => {
    return holdingsRows.map((row) => {
      const modeled = modeledMap.get(normalizeTicker(row.ticker));
      return {
        row,
        modeled,
        price: modeled?.price ?? null,
        marketValue: modeled?.price != null ? Number(row.quantity || 0) * Number(modeled.price || 0) : (modeled?.market_value ?? null),
      };
    });
  }, [holdingsRows, modeledMap]);

  const sortedRows = useMemo(() => {
    const rows = [...enrichedRows];
    rows.sort((a, b) => {
      const valueFor = (item: (typeof rows)[number]) => {
        switch (sortKey) {
          case "ticker":
            return item.row.ticker || item.row.ric;
          case "quantity":
            return Number(item.row.quantity || 0);
          case "price":
            return item.price ?? Number.NEGATIVE_INFINITY;
          case "market_value":
            return marketValueSortMode === "abs"
              ? Math.abs(item.marketValue ?? Number.NEGATIVE_INFINITY)
              : item.marketValue ?? Number.NEGATIVE_INFINITY;
          case "source":
            return item.row.source || "";
          case "account_id":
            return item.row.account_id || "";
        }
      };
      const av = valueFor(a);
      const bv = valueFor(b);
      if (typeof av === "number" && typeof bv === "number") {
        return sortAsc ? av - bv : bv - av;
      }
      return sortAsc
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
    });
    return rows;
  }, [enrichedRows, marketValueSortMode, sortAsc, sortKey]);

  const visibleRows = showAllRows ? sortedRows : sortedRows.slice(0, COLLAPSED_ROWS);

  function handleSort(nextKey: SortKey) {
    if (nextKey === "market_value") {
      if (sortKey !== "market_value") {
        setSortKey("market_value");
        setMarketValueSortMode("abs");
        setSortAsc(false);
        return;
      }
      if (marketValueSortMode === "abs" && !sortAsc) {
        setSortAsc(true);
        return;
      }
      if (marketValueSortMode === "abs" && sortAsc) {
        setMarketValueSortMode("signed");
        setSortAsc(false);
        return;
      }
      if (marketValueSortMode === "signed" && !sortAsc) {
        setSortAsc(true);
        return;
      }
      setMarketValueSortMode("abs");
      setSortAsc(false);
      return;
    }
    if (nextKey === sortKey) {
      setSortAsc((prev) => !prev);
      return;
    }
    setSortKey(nextKey);
    setSortAsc(false);
  }

  function arrow(key: SortKey): string {
    if (key === "market_value" && sortKey === "market_value") {
      const mode = marketValueSortMode === "abs" ? "abs" : "sgn";
      return sortAsc ? ` (${mode}) ↑` : ` (${mode}) ↓`;
    }
    return sortKey === key ? (sortAsc ? " ↑" : " ↓") : "";
  }

  if (holdingsError) {
    return (
      <div className="chart-card mb-4">
        <h3>Portfolio Holdings</h3>
        <ApiErrorState title="Holdings Not Ready" error={holdingsError} />
      </div>
    );
  }

  return (
    <div className="chart-card mb-4">
      <h3>Portfolio Holdings [{holdingsRows.length}]</h3>
      <div className="section-subtitle">
        Live Neon-backed holdings across all accounts. Inline changes stay local until `RECALC`, then the batch is written once and the modeled snapshot refreshes afterward.
      </div>
      <div className="dash-table">
        <table>
          <thead>
            <tr>
              <th onClick={() => handleSort("ticker")}>Ticker{arrow("ticker")}</th>
              <th className="text-right" onClick={() => handleSort("quantity")}>Quantity{arrow("quantity")}</th>
              <th className="text-right" onClick={() => handleSort("price")}>Price{arrow("price")}</th>
              <th className="text-right" onClick={() => handleSort("market_value")}>Mkt Val{arrow("market_value")}</th>
              <th className="text-right" onClick={() => handleSort("source")}>Source{arrow("source")}</th>
              <th className="text-right" onClick={() => handleSort("account_id")}>Account{arrow("account_id")}</th>
            </tr>
          </thead>
          <tbody>
            {visibleRows.map(({ row, price, marketValue }) => (
              <tr key={`${row.account_id}:${row.ric || row.ticker}`}>
                <td>{row.ticker || "—"}</td>
                <td className="text-right">
                  <InlineShareDraftEditor
                    quantityText={getDraftQuantityText(row)}
                    disabled={busy}
                    draftActive={hasDraftForRow(row)}
                    invalid={isDraftInvalidForRow(row)}
                    titleBase={row.ticker || row.ric}
                    onQuantityTextChange={(value) => onDraftQuantityChange(row, value)}
                    onStep={(step) => onAdjust(row, step)}
                  />
                </td>
                <td className="text-right">{fmtCurrency(price)}</td>
                <td className={`text-right ${marketValueTone(marketValue)}`.trim()}>{fmtMarketValue(marketValue)}</td>
                <td className="text-right">{row.source || "—"}</td>
                <td className="text-right">{row.account_id}</td>
              </tr>
            ))}
            {visibleRows.length === 0 && (
              <tr>
                <td colSpan={6} className="holdings-empty-row">
                  No holdings are loaded yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
        <TableRowToggle
          totalRows={sortedRows.length}
          collapsedRows={COLLAPSED_ROWS}
          expanded={showAllRows}
          onToggle={() => setShowAllRows((prev) => !prev)}
          label="holdings"
        />
      </div>
    </div>
  );
}
