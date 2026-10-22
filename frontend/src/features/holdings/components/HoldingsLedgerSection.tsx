"use client";

import { useMemo, useState, type ReactNode } from "react";
import ApiErrorState from "@/features/cuse4/components/ApiErrorState";
import MethodLabel from "@/components/MethodLabel";
import TableRowToggle from "@/components/TableRowToggle";
import type { Position } from "@/lib/types/analytics";
import type { CparPortfolioPositionRow } from "@/lib/types/cpar";
import type { HoldingsPosition } from "@/lib/types/holdings";
import { exposureMethodDisplayLabel, exposureMethodRank, exposureMethodTone } from "@/lib/exposureOrigin";
import { describeCparPositionMethod } from "@/lib/cparTruth";
import InlineShareDraftEditor from "./InlineShareDraftEditor";
import ThesisAllocationCell from "@/components/ThesisAllocationCell";
import { useTheses } from "@/components/ThesisContext";
import InlineThesisRows from "@/components/InlineThesisRows";
import MethodHelpLabel from "@/components/MethodHelpLabel";
import HelpLabel from "@/components/HelpLabel";

interface HoldingsLedgerSectionProps {
  holdingsRows: HoldingsPosition[];
  modeledPositions: Position[];
  cparModeledPositions: CparPortfolioPositionRow[];
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
  | "cuse_method"
  | "cpar_method"
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

function normalizeAccountId(value: string | null | undefined): string {
  return String(value || "").trim().toLowerCase();
}

function cparMethodLabel(row: CparPortfolioPositionRow | null | undefined): string {
  if (!row) return "—";
  return describeCparPositionMethod(row.coverage, row.fit_status).label;
}

function fmtCurrency(n: number | null): string {
  if (n === null || !Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}
function fmtQty(n: number): string { return n.toLocaleString(undefined, { maximumFractionDigits: 6 }); }

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
  cparModeledPositions,
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
  const [tickerManual, setTickerManual] = useState(false);
  const [marketValueSortMode, setMarketValueSortMode] = useState<MarketValueSortMode>("abs");
  const [showAllRows, setShowAllRows] = useState(false);
  const { available: thesesAvailable } = useTheses();
  const [viewMode, setViewMode] = useState<"positions" | "theses">("positions");

  const modeledMap = useMemo(() => {
    const out = new Map<string, Position>();
    for (const pos of modeledPositions) {
      out.set(normalizeTicker(pos.ticker), pos);
    }
    return out;
  }, [modeledPositions]);

  const cparModeledMap = useMemo(() => {
    const byAccountRic = new Map<string, CparPortfolioPositionRow>();
    const byAccountTicker = new Map<string, CparPortfolioPositionRow>();
    const byRic = new Map<string, CparPortfolioPositionRow>();
    const byTicker = new Map<string, CparPortfolioPositionRow>();
    for (const pos of cparModeledPositions) {
      const accountId = normalizeAccountId(pos.account_id);
      const ticker = normalizeTicker(pos.ticker);
      const ric = String(pos.ric || "").trim().toUpperCase();
      if (accountId && ric && !byAccountRic.has(`${accountId}:${ric}`)) {
        byAccountRic.set(`${accountId}:${ric}`, pos);
      }
      if (accountId && ticker && !byAccountTicker.has(`${accountId}:${ticker}`)) {
        byAccountTicker.set(`${accountId}:${ticker}`, pos);
      }
      if (ric && !byRic.has(ric)) {
        byRic.set(ric, pos);
      }
      if (ticker && !byTicker.has(ticker)) {
        byTicker.set(ticker, pos);
      }
    }
    return { byAccountRic, byAccountTicker, byRic, byTicker };
  }, [cparModeledPositions]);

  const enrichedRows = useMemo(() => {
    return holdingsRows.map((row) => {
      const modeled = modeledMap.get(normalizeTicker(row.ticker));
      const accountId = normalizeAccountId(row.account_id);
      const ticker = normalizeTicker(row.ticker);
      const ric = String(row.ric || "").trim().toUpperCase();
      const cparModeled = (
        (accountId && ric ? cparModeledMap.byAccountRic.get(`${accountId}:${ric}`) : null)
        || (accountId && ticker ? cparModeledMap.byAccountTicker.get(`${accountId}:${ticker}`) : null)
        || (ric ? cparModeledMap.byRic.get(ric) : null)
        || (ticker ? cparModeledMap.byTicker.get(ticker) : null)
        || null
      );
      return {
        row,
        modeled,
        cparModeled,
        price: modeled?.price ?? null,
        marketValue: modeled?.price != null ? Number(row.quantity || 0) * Number(modeled.price || 0) : (modeled?.market_value ?? null),
      };
    });
  }, [cparModeledMap, holdingsRows, modeledMap]);

  const sortedRows = useMemo(() => {
    const rows = [...enrichedRows];
    rows.sort((a, b) => {
      const valueFor = (item: (typeof rows)[number]) => {
        switch (sortKey) {
          case "ticker":
            return item.row.ticker || item.row.ric;
          case "cuse_method":
            return exposureMethodDisplayLabel(item.modeled?.exposure_origin, item.modeled?.model_status);
          case "cpar_method":
            return cparMethodLabel(item.cparModeled);
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
      if (sortKey === "cuse_method") {
        const rankA = exposureMethodRank(a.modeled?.exposure_origin, a.modeled?.model_status);
        const rankB = exposureMethodRank(b.modeled?.exposure_origin, b.modeled?.model_status);
        if (rankA !== rankB) return sortAsc ? rankA - rankB : rankB - rankA;
      }
      if (typeof av === "number" && typeof bv === "number") {
        return sortAsc ? av - bv : bv - av;
      }
      return sortAsc
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
    });
    return rows;
  }, [enrichedRows, marketValueSortMode, sortAsc, sortKey]);

  function handleSort(nextKey: SortKey) {
    if (nextKey === "ticker") {
      if (sortKey !== "ticker") { setSortKey("ticker"); setSortAsc(false); setTickerManual(false); return; }
      if (tickerManual) { setTickerManual(false); setSortAsc(false); return; }
      if (!sortAsc) { setSortAsc(true); return; }
      setTickerManual(thesesAvailable); if (!thesesAvailable) setSortAsc(false);
      return;
    }
    setTickerManual(false);
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

  function arrow(key: SortKey): ReactNode {
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
  }

  if (holdingsError) {
    return (
      <div className="chart-card mb-4">
        <h3>Portfolio Holdings</h3>
        <ApiErrorState surface="portfolio holdings" error={holdingsError} />
      </div>
    );
  }

  return (
    <div className="chart-card mb-4" data-testid="positions-ledger">
      <div className="thesis-section-heading">
        <div className="module-title-with-help">
          <h3>Portfolio Holdings [{holdingsRows.length}]</h3>
          <HelpLabel
            label="Portfolio Holdings"
            plain="Live holdings across all accounts. In thesis view, drag a row, right-click it, or press Enter to move all or part of a position without changing the underlying holding."
            iconOnly
          />
        </div>
        <div className="thesis-heading-actions">
          {viewMode === "theses" ? <div id="holdings-thesis-controls" className="thesis-header-controls" /> : null}
          <div style={thesesAvailable ? undefined : { display: "none" }} className="thesis-view-toggle" role="group" aria-label="Portfolio holdings view">
            <button type="button" className={viewMode === "positions" ? "active" : ""} onClick={() => setViewMode("positions")}>By instrument</button>
            <button type="button" className={viewMode === "theses" ? "active" : ""} onClick={() => setViewMode("theses")}>By thesis</button>
          </div>
        </div>
      </div>
      <div className="dash-table">
        <table>
          <thead>
            <tr>
              <th className="sortable" tabIndex={0} aria-sort={sortKey === "ticker" ? (tickerManual ? "other" : sortAsc ? "ascending" : "descending") : "none"} title={tickerManual ? "Manual ticker order; click for descending" : "Ticker sort rotates through descending, ascending, and manual"} onClick={() => handleSort("ticker")} onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); handleSort("ticker"); } }}>Ticker{arrow("ticker")}</th>
              <th className="sortable" onClick={() => handleSort("cuse_method")}><span className="col-help-wrap"><MethodHelpLabel family="cuse" label="cUSE Method" />{arrow("cuse_method")}</span></th>
              <th className="sortable" onClick={() => handleSort("cpar_method")}><span className="col-help-wrap"><MethodHelpLabel family="cpar" label="cPAR Method" />{arrow("cpar_method")}</span></th>
              <th className="text-right sortable" onClick={() => handleSort("quantity")}>Quantity{arrow("quantity")}</th>
              <th className="text-right sortable" onClick={() => handleSort("price")}>Price{arrow("price")}</th>
              <th className="text-right sortable" onClick={() => handleSort("market_value")}>Mkt Val{arrow("market_value")}</th>
              <th className="text-right sortable" onClick={() => handleSort("source")}>Source{arrow("source")}</th>
              <th className="text-right sortable" onClick={() => handleSort("account_id")}>Account{arrow("account_id")}</th>
              <th>Thesis</th>
            </tr>
          </thead>
          <tbody>
            <InlineThesisRows
              rows={sortedRows}
              colSpan={9}
              grouped={viewMode === "theses"}
              marketValueColumnIndex={5}
              headerControlsId="holdings-thesis-controls"
              manualOrder={sortKey === "ticker" && tickerManual}
              groupedTickerDirection={sortKey === "ticker" && !tickerManual ? (sortAsc ? "asc" : "desc") : undefined}
              orderScope="holdings"
              ungroupedLimit={viewMode === "positions" && !showAllRows ? COLLAPSED_ROWS : undefined}
              groupedSortMetric={sortKey === "market_value" ? "market_value" : sortKey === "quantity" ? "quantity" : undefined}
              groupedSortAscending={sortAsc}
              groupedMarketValueAbsolute={marketValueSortMode === "abs"}
              emptyLabel="No holdings are loaded yet."
              getPosition={({ row, price, marketValue }) => ({ accountId: row.account_id, ric: row.ric, ticker: row.ticker, quantity: Number(row.quantity || 0), price, marketValue })}
              renderCells={({ row, modeled, cparModeled, price, marketValue }, displayQuantity, grouped, sourceGroupId) => <>
                <td>{row.ticker || "—"}</td>
                <td>
                  <MethodLabel
                    label={exposureMethodDisplayLabel(modeled?.exposure_origin, modeled?.model_status)}
                    tone={exposureMethodTone(modeled?.exposure_origin, modeled?.model_status)}
                  />
                </td>
                <td>
                  <MethodLabel
                    label={cparMethodLabel(cparModeled)}
                    tone={cparModeled ? describeCparPositionMethod(cparModeled.coverage, cparModeled.fit_status).tone : "neutral"}
                  />
                </td>
                <td className="text-right">
                  {grouped ? fmtQty(displayQuantity) : <InlineShareDraftEditor
                    quantityText={getDraftQuantityText(row)}
                    disabled={busy}
                    draftActive={hasDraftForRow(row)}
                    invalid={isDraftInvalidForRow(row)}
                    titleBase={row.ticker || row.ric}
                    onQuantityTextChange={(value) => onDraftQuantityChange(row, value)}
                    onStep={(step) => onAdjust(row, step)}
                  />}
                </td>
                <td className="text-right">{fmtCurrency(price)}</td>
                <td className={`text-right ${marketValueTone(grouped && price != null ? price * displayQuantity : marketValue)}`.trim()}>{fmtMarketValue(grouped && price != null ? price * displayQuantity : marketValue)}</td>
                <td className="text-right">{row.source || "—"}</td>
                <td className="text-right">{row.account_id}</td>
                <td>
                  <ThesisAllocationCell
                    accountId={row.account_id}
                    ric={row.ric}
                    ticker={row.ticker}
                    positionQuantity={row.quantity}
                    summaryMode={grouped && sourceGroupId === "unallocated" ? "unallocated" : "allocations"}
                  />
                </td>
              </>}
            />
          </tbody>
        </table>
      </div>
      {viewMode === "positions" ? <TableRowToggle
          totalRows={sortedRows.length}
          collapsedRows={COLLAPSED_ROWS}
          expanded={showAllRows}
          onToggle={() => setShowAllRows((prev) => !prev)}
          label="holdings"
        /> : null}
    </div>
  );
}
