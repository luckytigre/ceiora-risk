"use client";

import { useEffect, useMemo, useState, type ReactNode } from "react";
import TableRowToggle from "@/components/TableRowToggle";
import MethodLabel from "@/components/MethodLabel";
import CparPositionHedgePopover from "@/features/cpar/components/CparPositionHedgePopover";
import HelpLabel from "@/components/HelpLabel";
import { describeCparPositionMethod, formatCparMarketValueThousands } from "@/lib/cparTruth";
import type { CparPackageMeta, CparPortfolioPositionRow } from "@/lib/types/cpar";
import ThesisAllocationCell from "@/components/ThesisAllocationCell";
import AccountThesisWorkspace from "@/features/holdings/components/AccountThesisWorkspace";
import { isConcreteThesisPosition } from "@/lib/thesisIdentity";
import { useTheses } from "@/components/ThesisContext";
import InlineThesisRows from "@/components/InlineThesisRows";
import MethodHelpLabel from "@/components/MethodHelpLabel";

const COLLAPSED_ROWS = 8;
type SortKey = "ticker" | "method" | "trbc_industry_group" | "quantity" | "market_value" | "risk_mix";
type MarketValueSortMode = "abs" | "signed";

function fmtMarketValue(value: number | null | undefined): string {
  return formatCparMarketValueThousands(value);
}

function fmtShares(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return value.toLocaleString(undefined, { maximumFractionDigits: 3 });
}

function methodLabel(row: CparPortfolioPositionRow): string {
  return describeCparPositionMethod(row.coverage, row.fit_status).label;
}

function normalizeRiskMix(row: CparPortfolioPositionRow) {
  const raw = row.risk_mix;
  if (!raw) return null;
  return {
    market: Number(raw.market ?? 0) || 0,
    industry: Number(raw.industry ?? 0) || 0,
    style: Number(raw.style ?? 0) || 0,
    idio: Number(raw.idio ?? 0) || 0,
  };
}

function riskMixLabel(row: CparPortfolioPositionRow): string {
  const mix = normalizeRiskMix(row);
  if (!mix) return "—";
  return `${mix.market.toFixed(1)}% / ${mix.industry.toFixed(1)}% / ${mix.style.toFixed(1)}% / ${mix.idio.toFixed(1)}%`;
}

function riskMixSortValue(row: CparPortfolioPositionRow): number {
  return normalizeRiskMix(row)?.idio ?? -1;
}

function rowKey(row: CparPortfolioPositionRow): string {
  return `${row.account_id || "scope"}:${row.ric}`;
}

export default function CparRiskPositionsContributionTable({
  rows,
  packageIdentity,
}: {
  rows: CparPortfolioPositionRow[];
  packageIdentity: Pick<CparPackageMeta, "package_run_id" | "package_date">;
}) {
  const [expanded, setExpanded] = useState(false);
  const [sortKey, setSortKey] = useState<SortKey>("market_value");
  const [sortAsc, setSortAsc] = useState(false);
  const [tickerManual, setTickerManual] = useState(false);
  const [marketValueSortMode, setMarketValueSortMode] = useState<MarketValueSortMode>("abs");
  const [activeRowKey, setActiveRowKey] = useState<string | null>(null);
  const [activeAnchorEl, setActiveAnchorEl] = useState<HTMLElement | null>(null);
  const { available: thesisCapability } = useTheses();
  const concretePositions = rows.every(row => isConcreteThesisPosition(row.account_id, row.ric));
  const thesesAvailable = thesisCapability && concretePositions;
  const [viewMode, setViewMode] = useState<"positions" | "theses">("positions");
  const coveredCount = useMemo(
    () => rows.filter((row) => row.coverage === "covered").length,
    [rows],
  );
  const sortedRows = useMemo(() => {
    const nextRows = [...rows];
    nextRows.sort((left, right) => {
      if (sortKey === "method") {
        const leftLabel = methodLabel(left);
        const rightLabel = methodLabel(right);
        return sortAsc ? leftLabel.localeCompare(rightLabel) : rightLabel.localeCompare(leftLabel);
      }
      if (sortKey === "risk_mix") {
        const leftValue = riskMixSortValue(left);
        const rightValue = riskMixSortValue(right);
        return sortAsc ? leftValue - rightValue : rightValue - leftValue;
      }
      if (sortKey === "quantity") {
        return sortAsc
          ? (left.quantity || 0) - (right.quantity || 0)
          : (right.quantity || 0) - (left.quantity || 0);
      }
      if (sortKey === "market_value") {
        const leftValue = Number(left.market_value || 0);
        const rightValue = Number(right.market_value || 0);
        if (marketValueSortMode === "abs") {
          return sortAsc ? Math.abs(leftValue) - Math.abs(rightValue) : Math.abs(rightValue) - Math.abs(leftValue);
        }
        return sortAsc ? leftValue - rightValue : rightValue - leftValue;
      }
      const leftValue = String(left[sortKey] || "");
      const rightValue = String(right[sortKey] || "");
      return sortAsc ? leftValue.localeCompare(rightValue) : rightValue.localeCompare(leftValue);
    });
    return nextRows;
  }, [marketValueSortMode, rows, sortAsc, sortKey]);
  useEffect(() => {
    if (!activeRowKey) return;
    if (rows.some((row) => rowKey(row) === activeRowKey)) return;
    setActiveRowKey(null);
    setActiveAnchorEl(null);
  }, [activeRowKey, rows]);

  const handleSort = (nextKey: SortKey) => {
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
    if (sortKey === nextKey) {
      setSortAsc((current) => !current);
      return;
    }
    setSortKey(nextKey);
    setSortAsc(false);
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

  if (viewMode === "theses" && thesisCapability) return <section className="chart-card">
    <div className="thesis-section-heading">
      <h3>Positions</h3>
      <div className="thesis-view-toggle" role="group" aria-label="cPAR risk positions view">
        <button type="button" onClick={() => setViewMode("positions")}>By instrument</button>
        <button type="button" className="active" onClick={() => setViewMode("theses")}>By thesis</button>
      </div>
    </div>
    <AccountThesisWorkspace analytics={rows} family="cpar" />
  </section>;


  return (
    <section className="chart-card" data-testid="cpar-risk-positions">
      <div className="thesis-section-heading">
        <div className="module-title-with-help">
          <h3>Positions</h3>
          <HelpLabel
            label="Positions"
            plain="This is the cPAR aggregate-book analogue of the cUSE risk-page positions table. In thesis view, drag a row, right-click it, or press Enter to move all or part of a position."
            iconOnly
          />
        </div>
        <div className="thesis-heading-actions">
          {viewMode === "theses" ? <div id="cpar-risk-thesis-controls" className="thesis-header-controls" /> : null}
          <div style={thesisCapability ? undefined : { display: "none" }} className="thesis-view-toggle" role="group" aria-label="cPAR risk positions view">
            <button type="button" className={viewMode === "positions" ? "active" : ""} onClick={() => setViewMode("positions")}>By instrument</button>
            <button type="button" className={viewMode === "theses" ? "active" : ""} onClick={() => setViewMode("theses")}>By thesis</button>
          </div>
        </div>
      </div>
      <div className="cpar-badge-row compact">
        <span className="cpar-detail-chip">{coveredCount} covered rows</span>
        <span className="cpar-detail-chip">{rows.length - coveredCount} excluded rows</span>
      </div>

      {thesisCapability && !concretePositions ? <p className="section-subtitle">Instrument view summarizes model rows. Choose By thesis to organize your account positions.</p> : null}
      <div className="dash-table">
        <table>
          <thead>
            <tr>
              <th className="sortable" tabIndex={0} aria-sort={sortKey === "ticker" ? (tickerManual ? "other" : sortAsc ? "ascending" : "descending") : "none"} title={tickerManual ? "Manual ticker order; click for descending" : "Ticker sort rotates through descending, ascending, and manual"} onClick={() => handleSort("ticker")} onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); handleSort("ticker"); } }}>Ticker{arrow("ticker")}</th>
              <th className="sortable" onClick={() => handleSort("method")}><span className="col-help-wrap"><MethodHelpLabel family="cpar" />{arrow("method")}</span></th>
              <th className="sortable" onClick={() => handleSort("trbc_industry_group")}>TRBC Industry{arrow("trbc_industry_group")}</th>
              <th className="text-right sortable" onClick={() => handleSort("quantity")}>Share Count{arrow("quantity")}</th>
              <th className="text-right sortable" onClick={() => handleSort("market_value")}>Market Value{arrow("market_value")}</th>
              <th className="text-right sortable" onClick={() => handleSort("risk_mix")}>Risk Mix (Mkt / Ind / Sty / Idio){arrow("risk_mix")}</th>
              <th>Thesis</th>
            </tr>
          </thead>
          <tbody>
            <InlineThesisRows
              rows={sortedRows}
              colSpan={7}
              grouped={viewMode === "theses"}
              marketValueColumnIndex={4}
              headerControlsId="cpar-risk-thesis-controls"
              manualOrder={sortKey === "ticker" && tickerManual}
              groupedTickerDirection={sortKey === "ticker" && !tickerManual ? (sortAsc ? "asc" : "desc") : undefined}
              orderScope="cpar-risk"
              ungroupedLimit={viewMode === "positions" && !expanded ? COLLAPSED_ROWS : undefined}
              emptyLabel="No holdings rows are available across the active book."
              groupedSortMetric={sortKey === "market_value" ? "market_value" : sortKey === "quantity" ? "quantity" : undefined}
              groupedSortAscending={sortAsc}
              groupedMarketValueAbsolute={marketValueSortMode === "abs"}
              getPosition={(row) => ({ accountId: row.account_id, ric: row.ric, ticker: row.ticker || row.ric, quantity: row.quantity, price: row.price, marketValue: row.market_value })}
              renderCells={(row, displayQuantity, grouped, sourceGroupId) => <>
                  <td>
                    {row.coverage === "covered" && typeof row.market_value === "number" ? (
                      <>
                        <button
                          type="button"
                          className="cpar-clickable-ticker"
                          onClick={(event) => {
                            const nextRowKey = activeRowKey === rowKey(row) ? null : rowKey(row);
                            setActiveRowKey(nextRowKey);
                            setActiveAnchorEl(nextRowKey ? event.currentTarget : null);
                          }}
                        >
                          {row.ticker || row.ric}
                        </button>
                        {activeRowKey === rowKey(row) && activeAnchorEl ? (
                          <CparPositionHedgePopover
                            row={row}
                            anchorEl={activeAnchorEl}
                            onClose={() => {
                              setActiveRowKey(null);
                              setActiveAnchorEl(null);
                            }}
                            packageIdentity={packageIdentity}
                            scope={row.account_id === "all_accounts" ? "all_permitted_accounts" : "account"}
                            accountId={row.account_id === "all_accounts" ? null : row.account_id}
                          />
                        ) : null}
                      </>
                    ) : (
                      row.ticker || "—"
                    )}
                  </td>
                  <td>
                    <MethodLabel
                      label={methodLabel(row)}
                      tone={describeCparPositionMethod(row.coverage, row.fit_status).tone}
                    />
                  </td>
                  <td>{row.trbc_industry_group || "Unmapped"}</td>
                  <td className="text-right">{fmtShares(grouped ? displayQuantity : row.quantity)}</td>
                  <td className="text-right">{fmtMarketValue(grouped && typeof row.price === "number" ? row.price * displayQuantity : row.market_value)}</td>
                  <td className="text-right">{riskMixLabel(row)}</td>
                  <td>
                    <ThesisAllocationCell
                      accountId={row.account_id}
                      ric={row.ric}
                      ticker={row.ticker || row.ric}
                      positionQuantity={row.quantity}
                      disabled={!row.account_id || row.account_id === "all_accounts"}
                      summaryMode={grouped && sourceGroupId === "unallocated" ? "unallocated" : "allocations"}
                    />
                  </td>
              </>}
            />
          </tbody>
        </table>
      </div>

      {viewMode === "positions" ? <TableRowToggle
        totalRows={rows.length}
        collapsedRows={COLLAPSED_ROWS}
        expanded={expanded}
        onToggle={() => setExpanded((current) => !current)}
        label="positions"
      /> : null}
    </section>
  );
}
