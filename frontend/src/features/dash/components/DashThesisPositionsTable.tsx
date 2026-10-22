"use client";

import { useCallback, useState, type ReactNode, type KeyboardEvent } from "react";
import HelpLabel from "@/components/HelpLabel";
import InlineThesisRows from "@/components/InlineThesisRows";
import ThesisAllocationCell from "@/components/ThesisAllocationCell";
import type { DashPosition as Position } from "@/features/dash/dashPositions";
import { useTheses } from "@/components/ThesisContext";

type SortKey = "ticker" | "trbc_industry_group" | "shares" | "market_value" | "risk_mix";
type MarketValueSortMode = "abs" | "signed";

function fmtMarketValue(value: number | null): string {
  if (value === null) return "—";
  const sign = value < 0 ? "−" : "";
  const absolute = Math.abs(value);
  if (absolute >= 1e6) return `${sign}$${(absolute / 1e6).toFixed(2)}M`;
  if (absolute >= 1e3) return `${sign}$${(absolute / 1e3).toFixed(1)}K`;
  return `${sign}$${absolute.toFixed(0)}`;
}

function fmtShares(value: number): string {
  return value.toLocaleString(undefined, { maximumFractionDigits: 3 });
}

function riskMix(pos: Position) {
  return {
    market: Number(pos.risk_mix?.market ?? 0) || 0,
    industry: Number(pos.risk_mix?.industry ?? 0) || 0,
    style: Number(pos.risk_mix?.style ?? 0) || 0,
    idio: Number(pos.risk_mix?.idio ?? 0) || 0,
  };
}

function riskMixLabel(pos: Position): string {
  if (!pos.risk_mix) return "—";
  const mix = riskMix(pos);
  return `${mix.market.toFixed(0)} / ${mix.industry.toFixed(0)} / ${mix.style.toFixed(0)} / ${mix.idio.toFixed(0)}`;
}

export default function DashThesisPositionsTable({ positions }: { positions: Position[] }) {
  const { available: thesesAvailable } = useTheses();
  const [sortKey, setSortKey] = useState<SortKey>("market_value");
  const [sortAsc, setSortAsc] = useState(false);
  const [tickerManual, setTickerManual] = useState(false);
  const [marketValueSortMode, setMarketValueSortMode] = useState<MarketValueSortMode>("abs");

  const sorted = [...positions].sort((left, right) => {
    const leftValue = sortKey === "risk_mix" ? left.risk_mix?.idio : left[sortKey];
    const rightValue = sortKey === "risk_mix" ? right.risk_mix?.idio : right[sortKey];
    if (leftValue == null) return rightValue == null ? 0 : 1;
    if (rightValue == null) return -1;
    if (typeof leftValue === "number" && typeof rightValue === "number") {
      if (sortKey === "market_value" && marketValueSortMode === "abs") {
        return sortAsc ? Math.abs(leftValue) - Math.abs(rightValue) : Math.abs(rightValue) - Math.abs(leftValue);
      }
      return sortAsc ? leftValue - rightValue : rightValue - leftValue;
    }
    return sortAsc
      ? String(leftValue ?? "").localeCompare(String(rightValue ?? ""))
      : String(rightValue ?? "").localeCompare(String(leftValue ?? ""));
  });

  const handleSort = (key: SortKey) => {
    if (key === "ticker") {
      if (sortKey !== "ticker") { setSortKey("ticker"); setSortAsc(false); setTickerManual(false); return; }
      if (tickerManual) { setTickerManual(false); setSortAsc(false); return; }
      if (!sortAsc) { setSortAsc(true); return; }
      setTickerManual(thesesAvailable); if (!thesesAvailable) setSortAsc(false);
      return;
    }
    setTickerManual(false);
    if (key === "market_value") {
      if (sortKey !== "market_value") { setSortKey("market_value"); setMarketValueSortMode("abs"); setSortAsc(false); return; }
      if (marketValueSortMode === "abs" && !sortAsc) { setSortAsc(true); return; }
      if (marketValueSortMode === "abs") { setMarketValueSortMode("signed"); setSortAsc(false); return; }
      if (!sortAsc) { setSortAsc(true); return; }
      setMarketValueSortMode("abs"); setSortAsc(false);
      return;
    }
    if (sortKey === key) setSortAsc((current) => !current);
    else { setSortKey(key); setSortAsc(false); }
  };

  const arrow = (key: SortKey): ReactNode => {
    if (key === "market_value" && sortKey === key) {
      return <> <span className="th-sort-mode">{marketValueSortMode === "abs" ? "abs" : "sgn"}</span>{sortAsc ? " ↑" : " ↓"}</>;
    }
    if (key === "ticker" && sortKey === key && tickerManual) return <> <span className="th-sort-mode">manual</span> ↕</>;
    return sortKey === key ? (sortAsc ? " ↑" : " ↓") : "";
  };

  const sortHeader = (key: SortKey) => ({
    tabIndex: 0,
    "aria-sort": sortKey === key ? (key === "ticker" && tickerManual ? "other" as const : sortAsc ? "ascending" as const : "descending" as const) : "none" as const,
    onKeyDown: (event: KeyboardEvent<HTMLTableCellElement>) => {
      if (event.key === "Enter" || event.key === " ") { event.preventDefault(); handleSort(key); }
    },
  });

  const getPosition = useCallback((pos: Position) => ({
    accountId: pos.account,
    ric: pos.ric || "",
    ticker: pos.ticker,
    quantity: pos.shares,
    price: pos.price ?? 0,
    marketValue: pos.market_value ?? 0,
  }), []);

  const renderCells = useCallback((pos: Position, displayQuantity: number, grouped: boolean, sourceGroupId?: string) => {
    const marketValue = grouped && pos.price !== null ? pos.price * displayQuantity : pos.market_value;
    return <>
      <td>{pos.ticker}</td>
      <td>{pos.trbc_industry_group || "Unmapped"}</td>
      <td className="text-right">{fmtShares(displayQuantity)}</td>
      <td className={`text-right ${marketValue !== null && marketValue > 0 ? "positive" : marketValue !== null && marketValue < 0 ? "negative" : ""}`.trim()}>{fmtMarketValue(marketValue)}</td>
      <td className="text-right">{riskMixLabel(pos)}</td>
      <td>
        <ThesisAllocationCell
          accountId={pos.account}
          ric={pos.ric || ""}
          ticker={pos.ticker}
          positionQuantity={pos.shares}
          disabled={!pos.account || !pos.ric || String(pos.account).trim().toLowerCase() === "multi"}
          summaryMode={grouped && sourceGroupId === "unallocated" ? "unallocated" : "allocations"}
        />
      </td>
    </>;
  }, []);

  return (
    <div className="dash-overview-table dash-table-module">
      <div className="dash-table">
        <div className="dash-table-sticky-chrome">
          <div className="thesis-section-heading">
            <div className="module-title-with-help">
              <h3>{thesesAvailable ? "Positions by Thesis" : "Positions"}</h3>
              <HelpLabel label="Positions by thesis" plain="Holdings retain account identity and signed quantities across models. Risk mix uses the selected model’s existing position basis; aggregate-only or unavailable mixes show a dash." iconOnly />
            </div>
            <div id="dash-thesis-controls" className="thesis-header-controls" />
          </div>
        </div>
        <table>
          <thead>
            <tr>
              <th className="sortable" {...sortHeader("ticker")} onClick={() => handleSort("ticker")}>Ticker{arrow("ticker")}</th>
              <th className="sortable" {...sortHeader("trbc_industry_group")} onClick={() => handleSort("trbc_industry_group")}>Industry{arrow("trbc_industry_group")}</th>
              <th className="text-right sortable" {...sortHeader("shares")} onClick={() => handleSort("shares")}>Qty{arrow("shares")}</th>
              <th className="text-right sortable" {...sortHeader("market_value")} onClick={() => handleSort("market_value")}>Mkt Value{arrow("market_value")}</th>
              <th className="text-right sortable" {...sortHeader("risk_mix")} onClick={() => handleSort("risk_mix")} title="Market / Industry / Style / Idiosyncratic">Risk Mix{arrow("risk_mix")}</th>
              <th>Thesis</th>
            </tr>
          </thead>
          <tbody>
            <InlineThesisRows
              rows={sorted}
              colSpan={6}
              grouped={thesesAvailable}
              marketValueColumnIndex={3}
              headerControlsId="dash-thesis-controls"
              manualOrder={sortKey === "ticker" && tickerManual}
              groupedTickerDirection={sortKey === "ticker" && !tickerManual ? (sortAsc ? "asc" : "desc") : undefined}
              orderScope="dash"
              groupedSortMetric={sortKey === "market_value" ? "market_value" : sortKey === "shares" ? "quantity" : undefined}
              groupedSortAscending={sortAsc}
              groupedMarketValueAbsolute={marketValueSortMode === "abs"}
              getPosition={getPosition}
              renderCells={renderCells}
            />
          </tbody>
        </table>
      </div>
    </div>
  );
}
