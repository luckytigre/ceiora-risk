"use client";

import { useMemo } from "react";
import { compareNumber, compareText, useSortableRows } from "@/hooks/useSortableRows";
import { formatCparMarketValueThousands, formatCparNumber } from "@/lib/cparTruth";
import type { CparPortfolioWhatIfData } from "@/lib/types/cpar";

type SortKey = "instrument" | "current" | "delta" | "hypothetical" | "market_value_delta" | "coverage";

export default function CparRiskWhatIfPreviewSection({
  whatIf,
}: {
  whatIf: CparPortfolioWhatIfData;
}) {
  const comparators = useMemo<Record<SortKey, (left: CparPortfolioWhatIfData["scenario_rows"][number], right: CparPortfolioWhatIfData["scenario_rows"][number]) => number>>(
    () => ({
      instrument: (left, right) => compareText(left.ticker || left.ric, right.ticker || right.ric),
      current: (left, right) => compareNumber(left.current_quantity, right.current_quantity),
      delta: (left, right) => compareNumber(left.quantity_delta, right.quantity_delta),
      hypothetical: (left, right) => compareNumber(left.hypothetical_quantity, right.hypothetical_quantity),
      market_value_delta: (left, right) => compareNumber(left.market_value_delta, right.market_value_delta),
      coverage: (left, right) => compareText(left.coverage_reason || left.coverage, right.coverage_reason || right.coverage),
    }),
    [],
  );
  const { sortedRows, handleSort, arrow } = useSortableRows<
    CparPortfolioWhatIfData["scenario_rows"][number],
    SortKey
  >({
    rows: whatIf.scenario_rows,
    comparators,
  });

  return (
    <section className="chart-card" data-testid="cpar-portfolio-whatif-scenarios">
      <h3>Scenario Preview Rows</h3>
      <div className="section-subtitle">
        Each row is previewed against the active package only. Coverage and fit warnings remain explicit, and no
        holdings mutation occurs.
      </div>
      <div className="dash-table">
        <table>
          <thead>
            <tr>
              <th className="sortable" onClick={() => handleSort("instrument")}>Instrument{arrow("instrument")}</th>
              <th className="text-right sortable" onClick={() => handleSort("current")}>Current Qty{arrow("current")}</th>
              <th className="text-right sortable" onClick={() => handleSort("delta")}>Delta{arrow("delta")}</th>
              <th className="text-right sortable" onClick={() => handleSort("hypothetical")}>Hyp Qty{arrow("hypothetical")}</th>
              <th className="text-right sortable" onClick={() => handleSort("market_value_delta")}>MV Delta{arrow("market_value_delta")}</th>
              <th className="sortable" onClick={() => handleSort("coverage")}>Coverage{arrow("coverage")}</th>
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row) => (
              <tr key={row.ric}>
                <td>
                  <strong>{row.ticker || row.ric}</strong>
                  <span className="cpar-table-sub">{row.display_name || row.ric}</span>
                </td>
                <td className="text-right cpar-number-cell">{formatCparNumber(row.current_quantity, 2)}</td>
                <td className="text-right cpar-number-cell">{formatCparNumber(row.quantity_delta, 2)}</td>
                <td className="text-right cpar-number-cell">{formatCparNumber(row.hypothetical_quantity, 2)}</td>
                <td className="text-right cpar-number-cell">{formatCparMarketValueThousands(row.market_value_delta)}</td>
                <td>{row.coverage_reason || row.coverage}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
