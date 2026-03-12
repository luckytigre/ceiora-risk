"use client";

import { useState } from "react";
import type { Position } from "@/lib/types";
import TableRowToggle from "@/components/TableRowToggle";
import InlineShareDraftEditor from "@/features/holdings/components/InlineShareDraftEditor";

interface PositionTableProps {
  positions: Position[];
  getDraftQuantityText?: (position: Position) => string;
  hasDraftForPosition?: (position: Position) => boolean;
  isDraftInvalidForPosition?: (position: Position) => boolean;
  onDraftQuantityChange?: (position: Position, value: string) => void;
  onAdjust?: (position: Position, delta: number) => void;
}

type SortKey =
  | "ticker"
  | "name"
  | "long_short"
  | "trbc_economic_sector_short"
  | "shares"
  | "price"
  | "market_value"
  | "account"
  | "sleeve"
  | "source";
type MarketValueSortMode = "abs" | "signed";
const COLLAPSED_ROWS = 14;

function fmt(n: number): string {
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

function fmtMarketValue(n: number): string {
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return n.toFixed(2);
}

function fmtShares(n: number): string {
  return Math.abs(n).toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function marketValueTone(n: number): string {
  if (n > 0) return "positive";
  if (n < 0) return "negative";
  return "";
}

export default function PositionTable({
  positions,
  getDraftQuantityText,
  hasDraftForPosition,
  isDraftInvalidForPosition,
  onDraftQuantityChange,
  onAdjust,
}: PositionTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>("market_value");
  const [sortAsc, setSortAsc] = useState(false);
  const [marketValueSortMode, setMarketValueSortMode] = useState<MarketValueSortMode>("abs");
  const [showAllRows, setShowAllRows] = useState(false);

  const sorted = [...positions].sort((a, b) => {
    const av = a[sortKey];
    const bv = b[sortKey];
    if (typeof av === "number" && typeof bv === "number") {
      if (sortKey === "market_value" && marketValueSortMode === "abs") {
        return sortAsc ? Math.abs(av) - Math.abs(bv) : Math.abs(bv) - Math.abs(av);
      }
      return sortAsc ? av - bv : bv - av;
    }
    return sortAsc
      ? String(av).localeCompare(String(bv))
      : String(bv).localeCompare(String(av));
  });

  const handleSort = (key: SortKey) => {
    if (key === "market_value") {
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
    if (key === sortKey) setSortAsc(!sortAsc);
    else { setSortKey(key); setSortAsc(false); }
  };

  const arrow = (key: SortKey) => {
    if (key === "market_value" && sortKey === "market_value") {
      const mode = marketValueSortMode === "abs" ? "abs" : "sgn";
      return sortAsc ? ` (${mode}) ↑` : ` (${mode}) ↓`;
    }
    return sortKey === key ? (sortAsc ? " ↑" : " ↓") : "";
  };
  const visibleRows = showAllRows ? sorted : sorted.slice(0, COLLAPSED_ROWS);

  return (
    <div className="dash-table" style={{ overflowX: "auto" }}>
      <table>
        <thead>
          <tr>
            <th onClick={() => handleSort("ticker")}>Ticker{arrow("ticker")}</th>
            <th onClick={() => handleSort("name")}>Name{arrow("name")}</th>
            <th onClick={() => handleSort("long_short")}>Long/Short{arrow("long_short")}</th>
            <th onClick={() => handleSort("trbc_economic_sector_short")}>TRBC Sector{arrow("trbc_economic_sector_short")}</th>
            <th className="text-right" onClick={() => handleSort("shares")}>Share Count{arrow("shares")}</th>
            <th className="text-right" onClick={() => handleSort("price")}>Share Price{arrow("price")}</th>
            <th className="text-right" onClick={() => handleSort("market_value")}>Value{arrow("market_value")}</th>
            <th onClick={() => handleSort("account")}>Account{arrow("account")}</th>
            <th onClick={() => handleSort("sleeve")}>Sleeve{arrow("sleeve")}</th>
            <th onClick={() => handleSort("source")}>Source{arrow("source")}</th>
          </tr>
        </thead>
        <tbody>
          {visibleRows.map((pos) => (
            <tr key={pos.ticker}>
              <td>{pos.ticker}</td>
              <td>{pos.name && pos.name.trim().length > 0 ? pos.name : "—"}</td>
              <td>
                <span className={pos.long_short === "SHORT" ? "negative" : "positive"}>
                  {pos.long_short}
                </span>
              </td>
              <td>{pos.trbc_economic_sector_short || "—"}</td>
              <td className="text-right">
                {getDraftQuantityText && onDraftQuantityChange && onAdjust ? (
                  <InlineShareDraftEditor
                    quantityText={getDraftQuantityText(pos)}
                    disabled={!pos.account || String(pos.account).trim().toLowerCase() === "multi"}
                    draftActive={Boolean(hasDraftForPosition?.(pos))}
                    invalid={Boolean(isDraftInvalidForPosition?.(pos))}
                    titleBase={pos.ticker}
                    onQuantityTextChange={(value) => onDraftQuantityChange(pos, value)}
                    onStep={(delta) => onAdjust(pos, delta)}
                  />
                ) : (
                  fmtShares(pos.shares)
                )}
              </td>
              <td className="text-right">{fmt(pos.price)}</td>
              <td className={`text-right ${marketValueTone(pos.market_value)}`.trim()}>{fmtMarketValue(pos.market_value)}</td>
              <td>{pos.account || "—"}</td>
              <td>{pos.sleeve || "—"}</td>
              <td>{pos.source || "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <TableRowToggle
        totalRows={sorted.length}
        collapsedRows={COLLAPSED_ROWS}
        expanded={showAllRows}
        onToggle={() => setShowAllRows((prev) => !prev)}
        label="positions"
      />
    </div>
  );
}
