"use client";

import { useState } from "react";
import type { Position } from "@/lib/types";
import TableRowToggle from "@/components/TableRowToggle";

interface PositionTableProps {
  positions: Position[];
}

type SortKey =
  | "ticker"
  | "name"
  | "long_short"
  | "trbc_sector"
  | "shares"
  | "price"
  | "market_value"
  | "account"
  | "sleeve"
  | "source";
const COLLAPSED_ROWS = 14;

function fmt(n: number): string {
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

function fmtShares(n: number): string {
  return Math.abs(n).toLocaleString(undefined, { maximumFractionDigits: 2 });
}

export default function PositionTable({ positions }: PositionTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>("market_value");
  const [sortAsc, setSortAsc] = useState(false);
  const [showAllRows, setShowAllRows] = useState(false);

  const sorted = [...positions].sort((a, b) => {
    const av = a[sortKey];
    const bv = b[sortKey];
    if (typeof av === "number" && typeof bv === "number") {
      return sortAsc ? av - bv : bv - av;
    }
    return sortAsc
      ? String(av).localeCompare(String(bv))
      : String(bv).localeCompare(String(av));
  });

  const handleSort = (key: SortKey) => {
    if (key === sortKey) setSortAsc(!sortAsc);
    else { setSortKey(key); setSortAsc(false); }
  };

  const arrow = (key: SortKey) =>
    sortKey === key ? (sortAsc ? " ↑" : " ↓") : "";
  const visibleRows = showAllRows ? sorted : sorted.slice(0, COLLAPSED_ROWS);

  return (
    <div className="dash-table" style={{ overflowX: "auto" }}>
      <table>
        <thead>
          <tr>
            <th onClick={() => handleSort("ticker")}>Ticker{arrow("ticker")}</th>
            <th onClick={() => handleSort("name")}>Name{arrow("name")}</th>
            <th onClick={() => handleSort("long_short")}>Long/Short{arrow("long_short")}</th>
            <th onClick={() => handleSort("trbc_sector")}>TRBC Sector{arrow("trbc_sector")}</th>
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
              <td>{pos.name || pos.ticker}</td>
              <td>
                <span className={pos.long_short === "SHORT" ? "negative" : "positive"}>
                  {pos.long_short}
                </span>
              </td>
              <td>{pos.trbc_sector || "—"}</td>
              <td className="text-right">{fmtShares(pos.shares)}</td>
              <td className="text-right">{fmt(pos.price)}</td>
              <td className="text-right">{fmt(pos.market_value)}</td>
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
