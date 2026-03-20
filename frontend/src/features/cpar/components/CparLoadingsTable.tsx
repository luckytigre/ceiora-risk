"use client";

import { formatCparNumber } from "@/lib/cparTruth";
import type { CparLoading } from "@/lib/types/cpar";

export default function CparLoadingsTable({
  title,
  rows,
  emptyText,
}: {
  title: string;
  rows: CparLoading[];
  emptyText?: string;
}) {
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
                <th>Factor</th>
                <th>Group</th>
                <th className="text-right">Beta</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
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
