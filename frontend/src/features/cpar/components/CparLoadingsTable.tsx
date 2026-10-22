"use client";

import { useMemo } from "react";
import { compareNumber, compareText, useSortableRows } from "@/hooks/useSortableRows";
import { formatCparNumber } from "@/lib/cparTruth";
import type { CparLoading } from "@/lib/types/cpar";

type SortKey = "factor" | "group" | "beta";

export default function CparLoadingsTable({
  title,
  rows,
  emptyText,
}: {
  title: string;
  rows: CparLoading[];
  emptyText?: string;
}) {
  const comparators = useMemo<Record<SortKey, (left: CparLoading, right: CparLoading) => number>>(
    () => ({
      factor: (left, right) => compareText(left.label || left.factor_id, right.label || right.factor_id),
      group: (left, right) => compareText(left.group, right.group),
      beta: (left, right) => compareNumber(left.beta, right.beta),
    }),
    [],
  );
  const { sortedRows, handleSort, arrow } = useSortableRows<CparLoading, SortKey>({
    rows,
    comparators,
  });

  return (
    <section className="chart-card">
      <h3>{title}</h3>
      {rows.length === 0 ? (
        <div className="detail-history-empty compact">
          {emptyText || "No persisted loadings were available for this cPAR fit."}
        </div>
      ) : (
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th className="sortable" onClick={() => handleSort("factor")}>Factor{arrow("factor")}</th>
                <th className="sortable" onClick={() => handleSort("group")}>Group{arrow("group")}</th>
                <th className="text-right sortable" onClick={() => handleSort("beta")}>Beta{arrow("beta")}</th>
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((row) => (
                <tr key={row.factor_id}>
                  <td>
                    <strong>{row.label}</strong>
                    <span className="cpar-table-sub">{row.factor_id}</span>
                  </td>
                  <td>{row.group}</td>
                  <td className="text-right cpar-number-cell">{formatCparNumber(row.beta, 3)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
