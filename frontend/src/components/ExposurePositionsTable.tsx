"use client";

import { useState } from "react";
import type { Position } from "@/lib/types";
import TableRowToggle from "@/components/TableRowToggle";
import ShareAdjuster from "@/components/ShareAdjuster";

interface ExposurePositionsTableProps {
  positions: Position[];
}

type SortKey = "ticker" | "trbc_industry_group" | "shares" | "market_value" | "risk_mix";
const COLLAPSED_ROWS = 14;

function fmtCurrency(n: number): string {
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

function fmtShares(n: number): string {
  return n.toLocaleString(undefined, { maximumFractionDigits: 3 });
}

function riskMixLabel(pos: Position): string {
  const mix = pos.risk_mix ?? { country: 0, industry: 0, style: 0, idio: 0 };
  return `Ctry ${mix.country.toFixed(1)}% / Ind ${mix.industry.toFixed(1)}% / Sty ${mix.style.toFixed(1)}% / Idio ${mix.idio.toFixed(1)}%`;
}

function riskMixSortValue(pos: Position): number {
  const mix = pos.risk_mix ?? { country: 0, industry: 0, style: 0, idio: 0 };
  return Number(mix.idio || 0);
}

export default function ExposurePositionsTable({ positions }: ExposurePositionsTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>("market_value");
  const [sortAsc, setSortAsc] = useState(false);
  const [showAllRows, setShowAllRows] = useState(false);

  const sorted = [...positions].sort((a, b) => {
    if (sortKey === "risk_mix") {
      const av = riskMixSortValue(a);
      const bv = riskMixSortValue(b);
      return sortAsc ? av - bv : bv - av;
    }
    const av = a[sortKey];
    const bv = b[sortKey];
    if (typeof av === "number" && typeof bv === "number") {
      return sortAsc ? av - bv : bv - av;
    }
    return sortAsc
      ? String(av ?? "").localeCompare(String(bv ?? ""))
      : String(bv ?? "").localeCompare(String(av ?? ""));
  });

  const handleSort = (key: SortKey) => {
    if (key === sortKey) setSortAsc((prev) => !prev);
    else {
      setSortKey(key);
      setSortAsc(false);
    }
  };
  const arrow = (key: SortKey) => (sortKey === key ? (sortAsc ? " ↑" : " ↓") : "");
  const visibleRows = showAllRows ? sorted : sorted.slice(0, COLLAPSED_ROWS);

  return (
    <div className="dash-table">
      <table>
        <thead>
          <tr>
            <th onClick={() => handleSort("ticker")}>Ticker{arrow("ticker")}</th>
            <th onClick={() => handleSort("trbc_industry_group")}>TRBC Industry{arrow("trbc_industry_group")}</th>
            <th className="text-right" onClick={() => handleSort("shares")}>Share Count{arrow("shares")}</th>
            <th className="text-right" onClick={() => handleSort("market_value")}>Market Value{arrow("market_value")}</th>
            <th className="text-right" onClick={() => handleSort("risk_mix")}>
              Risk Mix (Ctry/Ind/Sty/Idio){arrow("risk_mix")}
            </th>
          </tr>
        </thead>
        <tbody>
          {visibleRows.map((pos) => (
            <tr key={pos.ticker}>
              <td>{pos.ticker}</td>
              <td>{pos.trbc_industry_group || "Unmapped"}</td>
              <td className="text-right">
                <span className="share-cell">
                  <span>{fmtShares(pos.shares)}</span>
                  <ShareAdjuster ticker={pos.ticker} currentShares={pos.shares} accountId={pos.account} />
                </span>
              </td>
              <td className="text-right">{fmtCurrency(pos.market_value)}</td>
              <td className="text-right">{riskMixLabel(pos)}</td>
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
