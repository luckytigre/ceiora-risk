"use client";

import { useState, type ReactNode } from "react";
import type { Position } from "@/lib/types/cuse4";
import TableRowToggle from "@/components/TableRowToggle";
import MethodLabel from "@/components/MethodLabel";
import InlineShareDraftEditor from "@/features/holdings/components/InlineShareDraftEditor";
import { exposureMethodDisplayLabel, exposureMethodRank, exposureMethodTone } from "@/lib/exposureOrigin";
import ThesisAllocationCell from "@/components/ThesisAllocationCell";
import { useTheses } from "@/components/ThesisContext";
import InlineThesisRows from "@/components/InlineThesisRows";
import MethodHelpLabel from "@/components/MethodHelpLabel";
import HelpLabel from "@/components/HelpLabel";

interface ExposurePositionsTableProps {
  positions: Position[];
  getDraftQuantityText?: (position: Position) => string;
  hasDraftForPosition?: (position: Position) => boolean;
  isDraftInvalidForPosition?: (position: Position) => boolean;
  onDraftQuantityChange?: (position: Position, value: string) => void;
  onAdjust?: (position: Position, delta: number) => void;
}

type SortKey = "ticker" | "method" | "trbc_industry_group" | "shares" | "market_value" | "risk_mix";
type MarketValueSortMode = "abs" | "signed";
const COLLAPSED_ROWS = 10;

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
    market: Number(raw.market ?? 0) || 0,
    industry: Number(raw.industry ?? 0) || 0,
    style: Number(raw.style ?? 0) || 0,
    idio: Number(raw.idio ?? 0) || 0,
  };
}

function riskMixLabel(pos: Position): string {
  const mix = normalizeRiskMix(pos);
  return `${mix.market.toFixed(1)}% / ${mix.industry.toFixed(1)}% / ${mix.style.toFixed(1)}% / ${mix.idio.toFixed(1)}%`;
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
  const [tickerManual, setTickerManual] = useState(false);
  const [marketValueSortMode, setMarketValueSortMode] = useState<MarketValueSortMode>("abs");
  const [showAllRows, setShowAllRows] = useState(false);
  const { available: thesesAvailable } = useTheses();
  const [viewMode, setViewMode] = useState<"positions" | "theses">("positions");

  const sorted = [...positions].sort((a, b) => {
    if (sortKey === "method") {
      const rankA = exposureMethodRank(a.exposure_origin, a.model_status);
      const rankB = exposureMethodRank(b.exposure_origin, b.model_status);
      if (rankA !== rankB) return sortAsc ? rankA - rankB : rankB - rankA;
      const labelA = exposureMethodDisplayLabel(a.exposure_origin, a.model_status);
      const labelB = exposureMethodDisplayLabel(b.exposure_origin, b.model_status);
      return sortAsc ? labelA.localeCompare(labelB) : labelB.localeCompare(labelA);
    }
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
    if (key === "ticker") {
      if (sortKey !== "ticker") { setSortKey("ticker"); setSortAsc(false); setTickerManual(false); return; }
      if (tickerManual) { setTickerManual(false); setSortAsc(false); return; }
      if (!sortAsc) { setSortAsc(true); return; }
      setTickerManual(thesesAvailable); if (!thesesAvailable) setSortAsc(false);
      return;
    }
    setTickerManual(false);
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
  const arrow = (key: SortKey): ReactNode => {
    if (key === "market_value" && sortKey === "market_value") {
      const mode = marketValueSortMode === "abs" ? "abs" : "sgn";
      return (
        <>
          {" "}
          <span className="th-sort-mode">{mode}</span>
          {sortAsc ? " ↑" : " ↓"}
        </>
      );
    }
    if (key === "ticker" && sortKey === "ticker" && tickerManual) return <> <span className="th-sort-mode">manual</span> ↕</>;
    return sortKey === key ? (sortAsc ? " ↑" : " ↓") : "";
  };
  return (
    <div className="dash-table-module">
      <div className="dash-table">
        <div className="dash-table-sticky-chrome">
          <div className="thesis-section-heading">
            <div className="module-title-with-help"><h3>Positions</h3><HelpLabel label="Positions organization" plain="In thesis view, drag a row, right-click it, or press Enter to move all or part of a position." iconOnly /></div>
            <div className="thesis-heading-actions">
              {viewMode === "theses" ? <div id="cuse-risk-thesis-controls" className="thesis-header-controls" /> : null}
              <div style={thesesAvailable ? undefined : { display: "none" }} className="thesis-view-toggle" role="group" aria-label="Risk positions view">
                <button type="button" className={viewMode === "positions" ? "active" : ""} onClick={() => setViewMode("positions")}>By instrument</button>
                <button type="button" className={viewMode === "theses" ? "active" : ""} onClick={() => setViewMode("theses")}>By thesis</button>
              </div>
            </div>
          </div>
        </div>
        <table>
        <thead>
          <tr>
            <th className="sortable" tabIndex={0} aria-sort={sortKey === "ticker" ? (tickerManual ? "other" : sortAsc ? "ascending" : "descending") : "none"} title={tickerManual ? "Manual ticker order; click for descending" : "Ticker sort rotates through descending, ascending, and manual"} onClick={() => handleSort("ticker")} onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); handleSort("ticker"); } }}>Ticker{arrow("ticker")}</th>
            <th className="sortable" onClick={() => handleSort("method")}><span className="col-help-wrap"><MethodHelpLabel family="cuse" />{arrow("method")}</span></th>
            <th className="sortable" onClick={() => handleSort("trbc_industry_group")}>TRBC Industry{arrow("trbc_industry_group")}</th>
            <th className="text-right sortable" onClick={() => handleSort("shares")}>Share Count{arrow("shares")}</th>
            <th className="text-right sortable" onClick={() => handleSort("market_value")}>Market Value{arrow("market_value")}</th>
            <th className="text-right sortable" onClick={() => handleSort("risk_mix")}>
              Risk Mix (Mkt / Ind / Sty / Idio){arrow("risk_mix")}
            </th>
            <th>Thesis</th>
          </tr>
        </thead>
        <tbody>
          <InlineThesisRows
            rows={sorted}
            colSpan={7}
            grouped={viewMode === "theses"}
            marketValueColumnIndex={4}
            headerControlsId="cuse-risk-thesis-controls"
            manualOrder={sortKey === "ticker" && tickerManual}
            groupedTickerDirection={sortKey === "ticker" && !tickerManual ? (sortAsc ? "asc" : "desc") : undefined}
            orderScope="cuse-risk"
            ungroupedLimit={viewMode === "positions" && !showAllRows ? COLLAPSED_ROWS : undefined}
            groupedSortMetric={sortKey === "market_value" ? "market_value" : sortKey === "shares" ? "quantity" : undefined}
            groupedSortAscending={sortAsc}
            groupedMarketValueAbsolute={marketValueSortMode === "abs"}
            getPosition={(pos) => ({ accountId: pos.account, ric: pos.ric || "", ticker: pos.ticker, quantity: pos.shares, price: pos.price, marketValue: pos.market_value })}
            renderCells={(pos, displayQuantity, grouped, sourceGroupId) => <>
              <td>{pos.ticker}</td>
              <td>
                <MethodLabel
                  label={exposureMethodDisplayLabel(pos.exposure_origin, pos.model_status, {
                    projectionOutputStatus: pos.projection_output_status,
                    servedExposureAvailable: pos.served_exposure_available,
                  })}
                  tone={exposureMethodTone(pos.exposure_origin, pos.model_status, {
                    projectionOutputStatus: pos.projection_output_status,
                    servedExposureAvailable: pos.served_exposure_available,
                  })}
                />
              </td>
              <td>{pos.trbc_industry_group || "Unmapped"}</td>
              <td className="text-right">
                {grouped ? fmtShares(displayQuantity) : getDraftQuantityText && onDraftQuantityChange && onAdjust ? (
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
              <td className={`text-right ${marketValueTone(grouped ? pos.price * displayQuantity : pos.market_value)}`.trim()}>{fmtMarketValue(grouped ? pos.price * displayQuantity : pos.market_value)}</td>
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
            </>}
          />
        </tbody>
        </table>
      </div>
      {viewMode === "positions" ? <TableRowToggle
        totalRows={sorted.length}
        collapsedRows={COLLAPSED_ROWS}
        expanded={showAllRows}
        onToggle={() => setShowAllRows((prev) => !prev)}
        label="positions"
      /> : null}
    </div>
  );
}
