"use client";

import { formatCparNumber } from "@/lib/cparTruth";
import type { CparHedgeMode, CparPortfolioWhatIfData } from "@/lib/types/cpar";
import CparPortfolioHedgePanel from "./CparPortfolioHedgePanel";

export default function CparRiskWhatIfPreviewSection({
  whatIf,
  mode,
  onModeChange,
}: {
  whatIf: CparPortfolioWhatIfData;
  mode: CparHedgeMode;
  onModeChange: (mode: CparHedgeMode) => void;
}) {
  return (
    <>
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
                <th>Instrument</th>
                <th className="text-right">Current Qty</th>
                <th className="text-right">Delta</th>
                <th className="text-right">Hyp Qty</th>
                <th className="text-right">MV Delta</th>
                <th>Coverage</th>
              </tr>
            </thead>
            <tbody>
              {whatIf.scenario_rows.map((row) => (
                <tr key={row.ric}>
                  <td>
                    <strong>{row.ticker || row.ric}</strong>
                    <span className="cpar-table-sub">{row.display_name || row.ric}</span>
                  </td>
                  <td className="text-right cpar-number-cell">{formatCparNumber(row.current_quantity, 2)}</td>
                  <td className="text-right cpar-number-cell">{formatCparNumber(row.quantity_delta, 2)}</td>
                  <td className="text-right cpar-number-cell">{formatCparNumber(row.hypothetical_quantity, 2)}</td>
                  <td className="text-right cpar-number-cell">{formatCparNumber(row.market_value_delta, 2)}</td>
                  <td>{row.coverage_reason || row.coverage}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <div className="cpar-two-column">
        <CparPortfolioHedgePanel
          data={whatIf.current}
          mode={mode}
          onModeChange={onModeChange}
          title="Current Account Hedge"
          subtitle="This is the live covered account vector under the active cPAR package, before staged share deltas are applied."
          testId="cpar-portfolio-current-hedge-panel"
        />
        <CparPortfolioHedgePanel
          data={whatIf.hypothetical}
          mode={mode}
          onModeChange={onModeChange}
          title="Hypothetical Account Hedge"
          subtitle="This is the same account after applying the staged cPAR what-if deltas, still using the same active package and persisted covariance surface."
          testId="cpar-portfolio-hypothetical-hedge-panel"
        />
      </div>
    </>
  );
}
