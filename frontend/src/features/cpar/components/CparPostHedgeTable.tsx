"use client";

import { formatCparNumber } from "@/lib/cparTruth";
import type { CparPostHedgeExposure } from "@/lib/types/cpar";

export default function CparPostHedgeTable({
  rows,
}: {
  rows: CparPostHedgeExposure[];
}) {
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
                <th>Factor</th>
                <th>Group</th>
                <th className="text-right">Pre</th>
                <th className="text-right">Hedge</th>
                <th className="text-right">Post</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.factor_id}>
                  <td>
                    <strong>{row.label || row.factor_id}</strong>
                    <span className="cpar-table-sub">{row.factor_id}</span>
                  </td>
                  <td>{row.group || "—"}</td>
                  <td className="text-right cpar-number-cell">{formatCparNumber(row.pre_beta, 3)}</td>
                  <td className="text-right cpar-number-cell">{formatCparNumber(row.hedge_leg, 3)}</td>
                  <td className="text-right cpar-number-cell">{formatCparNumber(row.post_beta, 3)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
