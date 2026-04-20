"use client";

import { useEffect, useMemo, useState } from "react";
import TableRowToggle from "@/components/TableRowToggle";
import MethodLabel from "@/components/MethodLabel";
import CparPositionHedgePopover from "@/features/cpar/components/CparPositionHedgePopover";
import { describeCparPositionMethod, formatCparMarketValueThousands } from "@/lib/cparTruth";
import type { CparPackageMeta, CparPortfolioPositionRow } from "@/lib/types/cpar";

const COLLAPSED_ROWS = 8;
type SortKey = "ticker" | "method" | "trbc_industry_group" | "quantity" | "market_value" | "risk_mix";
type MarketValueSortMode = "abs" | "signed";

function fmtMarketValue(value: number | null | undefined): string {
  return formatCparMarketValueThousands(value, { absolute: true });
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
  return `Mkt ${mix.market.toFixed(1)}% / Ind ${mix.industry.toFixed(1)}% / Sty ${mix.style.toFixed(1)}% / Idio ${mix.idio.toFixed(1)}%`;
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
  const [marketValueSortMode, setMarketValueSortMode] = useState<MarketValueSortMode>("abs");
  const [activeRowKey, setActiveRowKey] = useState<string | null>(null);
  const [activeAnchorEl, setActiveAnchorEl] = useState<HTMLElement | null>(null);
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

  useEffect(() => {
    if (!activeRowKey) return;
    if (rows.some((row) => rowKey(row) === activeRowKey)) return;
    setActiveRowKey(null);
    setActiveAnchorEl(null);
  }, [activeRowKey, rows]);

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
        display-factor mix driving the aggregate portfolio view, while excluded rows stay explicit.
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
                <tr key={rowKey(row)}>
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
