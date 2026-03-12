"use client";

import { useState } from "react";
import type { Position } from "@/lib/types";
import TableRowToggle from "@/components/TableRowToggle";
import InlineShareDraftEditor from "@/features/holdings/components/InlineShareDraftEditor";

interface ExposurePositionsTableProps {
  positions: Position[];
  getDraftQuantityText?: (position: Position) => string;
  hasDraftForPosition?: (position: Position) => boolean;
  isDraftInvalidForPosition?: (position: Position) => boolean;
  onDraftQuantityChange?: (position: Position, value: string) => void;
  onAdjust?: (position: Position, delta: number) => void;
}

type SortKey = "ticker" | "trbc_industry_group" | "shares" | "market_value" | "risk_mix";
type MarketValueSortMode = "abs" | "signed";
const COLLAPSED_ROWS = 14;

function fmtMarketValue(n: number): string {
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return n.toFixed(2);
}

function fmtShares(n: number): string {
  return n.toLocaleString(undefined, { maximumFractionDigits: 3 });
}

function marketValueTone(n: number): string {
  if (n > 0) return "positive";
  if (n < 0) return "negative";
  return "";
}

function normalizeRiskMix(pos: Position) {
  const raw = (pos.risk_mix ?? {}) as Partial<NonNullable<Position["risk_mix"]>>;
  return {
    country: Number(raw.country ?? 0) || 0,
    industry: Number(raw.industry ?? 0) || 0,
    style: Number(raw.style ?? 0) || 0,
    idio: Number(raw.idio ?? 0) || 0,
  };
}

function riskMixLabel(pos: Position): string {
  const mix = normalizeRiskMix(pos);
  return `Ctry ${mix.country.toFixed(1)}% / Ind ${mix.industry.toFixed(1)}% / Sty ${mix.style.toFixed(1)}% / Idio ${mix.idio.toFixed(1)}%`;
}

function riskMixSortValue(pos: Position): number {
  const mix = normalizeRiskMix(pos);
  return Number(mix.idio || 0);
}

export default function ExposurePositionsTable({
  positions,
  getDraftQuantityText,
  hasDraftForPosition,
  isDraftInvalidForPosition,
  onDraftQuantityChange,
  onAdjust,
}: ExposurePositionsTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>("market_value");
  const [sortAsc, setSortAsc] = useState(false);
  const [marketValueSortMode, setMarketValueSortMode] = useState<MarketValueSortMode>("abs");
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
      if (sortKey === "market_value" && marketValueSortMode === "abs") {
        return sortAsc ? Math.abs(av) - Math.abs(bv) : Math.abs(bv) - Math.abs(av);
      }
      return sortAsc ? av - bv : bv - av;
    }
    return sortAsc
      ? String(av ?? "").localeCompare(String(bv ?? ""))
      : String(bv ?? "").localeCompare(String(av ?? ""));
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
    if (key === sortKey) setSortAsc((prev) => !prev);
    else {
      setSortKey(key);
      setSortAsc(false);
    }
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
              <td className={`text-right ${marketValueTone(pos.market_value)}`.trim()}>{fmtMarketValue(pos.market_value)}</td>
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
