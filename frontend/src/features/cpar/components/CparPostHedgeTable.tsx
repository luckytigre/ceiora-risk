"use client";

import { useMemo, useState } from "react";
import TableRowToggle from "@/components/TableRowToggle";
import { compareNumber, compareText, useSortableRows } from "@/hooks/useSortableRows";
import { formatCparNumber } from "@/lib/cparTruth";
import type { CparPostHedgeExposure } from "@/lib/types/cpar";

type SortKey = "factor" | "group" | "pre" | "hedge" | "post";
const COLLAPSED_ROWS = 10;

function signTone(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value) || value === 0) return "";
  return value > 0 ? "positive" : "negative";
}

export default function CparPostHedgeTable({
  rows,
}: {
  rows: CparPostHedgeExposure[];
}) {
  const [expanded, setExpanded] = useState(false);
  const comparators = useMemo<Record<SortKey, (left: CparPostHedgeExposure, right: CparPostHedgeExposure) => number>>(
    () => ({
      factor: (left, right) => compareText(left.label || left.factor_id, right.label || right.factor_id),
      group: (left, right) => compareText(left.group, right.group),
      pre: (left, right) => compareNumber(left.pre_beta, right.pre_beta),
      hedge: (left, right) => compareNumber(left.hedge_leg, right.hedge_leg),
      post: (left, right) => compareNumber(left.post_beta, right.post_beta),
    }),
    [],
  );
  const { sortedRows, handleSort, arrow } = useSortableRows<CparPostHedgeExposure, SortKey>({
    rows,
    comparators,
  });
  const visibleRows = expanded ? sortedRows : sortedRows.slice(0, COLLAPSED_ROWS);

  return (
    <section className="chart-card" data-testid="cpar-post-hedge-table">
      <h3>Post-Hedge Exposures</h3>
      {rows.length === 0 ? (
        <div className="detail-history-empty compact">
          No post-hedge exposure rows were returned for this package.
        </div>
      ) : (
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th onClick={() => handleSort("factor")}>Factor{arrow("factor")}</th>
                <th onClick={() => handleSort("group")}>Group{arrow("group")}</th>
                <th className="text-right" onClick={() => handleSort("pre")}>Pre{arrow("pre")}</th>
                <th className="text-right" onClick={() => handleSort("hedge")}>Hedge{arrow("hedge")}</th>
                <th className="text-right" onClick={() => handleSort("post")}>Post{arrow("post")}</th>
              </tr>
            </thead>
            <tbody>
              {visibleRows.map((row) => (
                <tr key={row.factor_id}>
                  <td>
                    <strong>{row.label || row.factor_id}</strong>
                    <span className="cpar-table-sub">{row.factor_id}</span>
                  </td>
                  <td>{row.group || "—"}</td>
                  <td className={`text-right cpar-number-cell ${signTone(row.pre_beta)}`.trim()}>{formatCparNumber(row.pre_beta, 3)}</td>
                  <td className={`text-right cpar-number-cell ${signTone(row.hedge_leg)}`.trim()}>{formatCparNumber(row.hedge_leg, 3)}</td>
                  <td className={`text-right cpar-number-cell ${signTone(row.post_beta)}`.trim()}>{formatCparNumber(row.post_beta, 3)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <TableRowToggle
        totalRows={sortedRows.length}
        collapsedRows={COLLAPSED_ROWS}
        expanded={expanded}
        onToggle={() => setExpanded((current) => !current)}
        label="factors"
      />
    </section>
  );
}
