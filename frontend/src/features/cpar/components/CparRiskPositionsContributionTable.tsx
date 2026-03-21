"use client";

import { useMemo, useState } from "react";
import TableRowToggle from "@/components/TableRowToggle";
import { describeCparFitStatus } from "@/lib/cparTruth";
import type { CparPortfolioPositionRow } from "@/lib/types/cpar";

const COLLAPSED_ROWS = 8;
type SortKey = "ticker" | "method" | "trbc_industry_group" | "quantity" | "market_value" | "risk_mix";
type MarketValueSortMode = "abs" | "signed";

function fmtMarketValue(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  const abs = Math.abs(value);
  if (abs >= 1e6) return `${(abs / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${(abs / 1e3).toFixed(1)}K`;
  return abs.toFixed(2);
}

function fmtShares(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return value.toLocaleString(undefined, { maximumFractionDigits: 3 });
}

function methodLabel(row: CparPortfolioPositionRow): string {
  if (row.coverage === "missing_price") return "Missing Price";
  if (row.coverage === "missing_cpar_fit") return "Missing cPAR Fit";
  if (row.coverage === "insufficient_history") return "Insufficient History";
  if (!row.fit_status) return "Package Fit";
  if (row.fit_status === "limited_history") return "Limited Fit";
  const fit = describeCparFitStatus(row.fit_status);
  return fit.label;
}

function normalizeRiskMix(row: CparPortfolioPositionRow) {
  let market = 0;
  let industry = 0;
  let style = 0;
  for (const contribution of row.thresholded_contributions) {
    const value = Math.abs(Number(contribution.beta || 0));
    if (!value) continue;
    if (contribution.group === "market") market += value;
    else if (contribution.group === "sector") industry += value;
    else if (contribution.group === "style") style += value;
  }
  const total = market + industry + style;
  if (total <= 0) {
    return {
      market: 0,
      industry: 0,
      style: 0,
      idio: row.coverage === "covered" ? 0 : 100,
    };
  }
  const scale = 100 / total;
  return {
    market: market * scale,
    industry: industry * scale,
    style: style * scale,
    idio: 0,
  };
}

function riskMixLabel(row: CparPortfolioPositionRow): string {
  const mix = normalizeRiskMix(row);
  return `Mkt ${mix.market.toFixed(1)}% / Ind ${mix.industry.toFixed(1)}% / Sty ${mix.style.toFixed(1)}% / Idio ${mix.idio.toFixed(1)}%`;
}

function riskMixSortValue(row: CparPortfolioPositionRow): number {
  return normalizeRiskMix(row).idio;
}

export default function CparRiskPositionsContributionTable({
  rows,
}: {
  rows: CparPortfolioPositionRow[];
}) {
  const [expanded, setExpanded] = useState(false);
  const [sortKey, setSortKey] = useState<SortKey>("market_value");
  const [sortAsc, setSortAsc] = useState(false);
  const [marketValueSortMode, setMarketValueSortMode] = useState<MarketValueSortMode>("abs");
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
  const visibleRows = expanded ? sortedRows : sortedRows.slice(0, COLLAPSED_ROWS);

  const handleSort = (nextKey: SortKey) => {
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

  const arrow = (key: SortKey) => {
    if (key === "market_value" && sortKey === "market_value") {
      const mode = marketValueSortMode === "abs" ? "abs" : "sgn";
      return sortAsc ? ` (${mode}) ↑` : ` (${mode}) ↓`;
    }
    return sortKey === key ? (sortAsc ? " ↑" : " ↓") : "";
  };

  return (
    <section className="chart-card" data-testid="cpar-risk-positions">
      <h3>Positions (Factor Risk Mix)</h3>
      <div className="section-subtitle">
        This is the cPAR aggregate-book analogue of the cUSE risk-page positions table: covered rows show the weighted
        factor mix driving the aggregate portfolio vector, while excluded rows stay explicit.
      </div>
      <div className="cpar-badge-row compact">
        <span className="cpar-detail-chip">{coveredCount} covered rows</span>
        <span className="cpar-detail-chip">{rows.length - coveredCount} excluded rows</span>
      </div>

      <div className="dash-table">
        <table>
          <thead>
            <tr>
              <th onClick={() => handleSort("ticker")}>Ticker{arrow("ticker")}</th>
              <th onClick={() => handleSort("method")}>Method{arrow("method")}</th>
              <th onClick={() => handleSort("trbc_industry_group")}>TRBC Industry{arrow("trbc_industry_group")}</th>
              <th className="text-right" onClick={() => handleSort("quantity")}>Share Count{arrow("quantity")}</th>
              <th className="text-right" onClick={() => handleSort("market_value")}>Market Value{arrow("market_value")}</th>
              <th className="text-right" onClick={() => handleSort("risk_mix")}>Risk Mix (Mkt/Ind/Sty/Idio){arrow("risk_mix")}</th>
            </tr>
          </thead>
          <tbody>
            {visibleRows.length === 0 ? (
              <tr>
                <td colSpan={6} className="cpar-empty-row">No holdings rows are available across the active book.</td>
              </tr>
            ) : (
              visibleRows.map((row) => (
                <tr key={row.ric}>
                  <td>{row.ticker || "—"}</td>
                  <td>{methodLabel(row)}</td>
                  <td>{row.trbc_industry_group || "Unmapped"}</td>
                  <td className="text-right">{fmtShares(row.quantity)}</td>
                  <td className="text-right">{fmtMarketValue(row.market_value)}</td>
                  <td className="text-right">{riskMixLabel(row)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <TableRowToggle
        totalRows={rows.length}
        collapsedRows={COLLAPSED_ROWS}
        expanded={expanded}
        onToggle={() => setExpanded((current) => !current)}
        label="positions"
      />
    </section>
  );
}
