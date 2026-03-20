"use client";

import Link from "next/link";
import { formatCparNumber } from "@/lib/cparTruth";
import type { CparTickerDetailData } from "@/lib/types/cpar";
import CparLoadingsTable from "./CparLoadingsTable";

export default function CparExploreLoadingsSection({
  detail,
  hedgeHref,
}: {
  detail: CparTickerDetailData;
  hedgeHref: string;
}) {
  return (
    <>
      <section className="chart-card cpar-explore-loadings-module" data-testid="cpar-loadings-panel">
        <div className="cpar-explore-module-header">
          <div>
            <div className="cpar-explore-kicker">Loadings</div>
            <h3 className="cpar-explore-module-title">Persisted Factor Interpretation</h3>
            <div className="cpar-explore-module-subtitle">
              `/cpar/explore` stays on the persisted fit row. Raw and thresholded loadings remain visible here while
              hedge mode switching stays on `/cpar/hedge`.
            </div>
          </div>
          <div className="cpar-explore-module-status">{detail.thresholded_loadings.length} thresholded</div>
        </div>

        <div className="explore-hero-stats cpar-explore-hero-stats cpar-explore-loadings-stats">
          <div className="explore-hero-stat">
            <span className="label">Raw Factors</span>
            <span className="value">{detail.raw_loadings.length}</span>
          </div>
          <div className="explore-hero-stat">
            <span className="label">Thresholded</span>
            <span className="value">{detail.thresholded_loadings.length}</span>
          </div>
          <div className="explore-hero-stat">
            <span className="label">Market Step</span>
            <span className="value">{formatCparNumber(detail.beta_market_step1, 3)}</span>
          </div>
          <div className="explore-hero-stat">
            <span className="label">Trade Beta</span>
            <span className="value">{formatCparNumber(detail.beta_spy_trade, 3)}</span>
          </div>
        </div>

        <div className="cpar-two-column cpar-explore-loadings-grid">
          <CparLoadingsTable title="Raw ETF Loadings" rows={detail.raw_loadings} />
          <CparLoadingsTable
            title="Thresholded ETF Loadings"
            rows={detail.thresholded_loadings}
            emptyText="Thresholding zeroed every non-market leg in the persisted trade-space payload."
          />
        </div>
      </section>

      <section className="chart-card cpar-explore-handoff-card" data-testid="cpar-hedge-workspace-card">
        <div className="cpar-explore-module-header">
          <div>
            <div className="cpar-explore-kicker">Hedge Workflow</div>
            <h3 className="cpar-explore-module-title">Continue In `/cpar/hedge`</h3>
            <div className="cpar-explore-module-subtitle">
              Hedge preview, mode switching, and post-hedge interpretation remain isolated on the dedicated hedge page.
            </div>
          </div>
          <div className="cpar-explore-module-status">No refit</div>
        </div>

        <div className="cpar-inline-message neutral">
          <strong>Open the same instrument in the hedge workspace.</strong>
          <span>
            The hedge page reuses the same active-package ticker and RIC selection, then applies the persisted hedge
            route without any request-time refit or build behavior.
          </span>
          <div className="cpar-badge-row compact">
            <Link href={hedgeHref} className="cpar-detail-chip" prefetch={false}>
              Continue To /cpar/hedge
            </Link>
          </div>
        </div>
      </section>

      {detail.fit_status === "limited_history" ? (
        <section className="chart-card cpar-explore-limited-note">
          <div className="cpar-explore-module-header">
            <div>
              <div className="cpar-explore-kicker">Interpretation Note</div>
              <h3 className="cpar-explore-module-title">Limited History Requires More Caution</h3>
              <div className="cpar-explore-module-subtitle">
                `limited_history` still renders persisted loadings and keeps the hedge workspace available, but
                adjacent-package comparisons deserve more caution than a full-history `ok` row.
              </div>
            </div>
          </div>
          <div className="detail-history-empty compact">
            If you continue to `/cpar/hedge`, compare the stability and non-market reduction metrics before using the
            persisted hedge output operationally.
          </div>
          {detail.pre_hedge_factor_variance_proxy !== null ? (
            <div className="cpar-badge-row compact">
              <span className="cpar-detail-chip">
                Current pre-hedge variance proxy: {detail.pre_hedge_factor_variance_proxy?.toFixed(3)}
              </span>
            </div>
          ) : null}
        </section>
      ) : null}
    </>
  );
}
