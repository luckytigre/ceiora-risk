"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import { useCparMeta } from "@/hooks/useApi";
import { canNavigateCparSearchResult, formatCparPackageDate, readCparError, summarizeFactorRegistry } from "@/lib/cparTruth";
import type { CparSearchItem } from "@/lib/types";
import CparPackageBanner from "@/features/cpar/components/CparPackageBanner";
import CparSearchPanel from "@/features/cpar/components/CparSearchPanel";
import CparWarningsBar from "@/features/cpar/components/CparWarningsBar";

function buildExploreHref(item: CparSearchItem): string {
  const params = new URLSearchParams();
  if (item.ticker) params.set("ticker", item.ticker);
  params.set("ric", item.ric);
  return `/cpar/explore?${params.toString()}`;
}

export default function CparLandingPage() {
  const router = useRouter();
  const { data, error, isLoading } = useCparMeta();

  if (isLoading && !data) {
    return <AnalyticsLoadingViz message="Loading cPAR package..." />;
  }

  const metaError = error ? readCparError(error) : null;
  const factorCounts = data ? summarizeFactorRegistry(data.factors) : null;

  return (
    <div className="cpar-page">
      {data ? (
        <CparPackageBanner meta={data} factors={data.factors} />
      ) : metaError ? (
        <section className="chart-card cpar-alert-card" data-testid="cpar-not-ready">
          <h3>{metaError.kind === "not_ready" ? "cPAR Package Not Ready" : "cPAR Read Surface Unavailable"}</h3>
          <div className="section-subtitle">
            {metaError.message}
            {metaError.buildProfile ? ` Build profile: ${metaError.buildProfile}.` : ""}
          </div>
          <div className="detail-history-empty compact">
            No request-time fitting exists on this page. Publish a durable cPAR package first, then reload.
          </div>
        </section>
      ) : null}

      <div className="cpar-two-column">
        <section className="chart-card">
          <h3>Route Map</h3>
          <div className="section-subtitle">
            cPAR uses family-local routes under the shared shell. Health stays diagnostics-first, while risk, explore,
            and hedge each own a narrower package-based workflow.
          </div>
          <div className="cpar-badge-row">
            <Link href="/cpar/risk" className="cpar-detail-chip" prefetch={false}>Open /cpar/risk</Link>
            <Link href="/cpar/explore" className="cpar-detail-chip" prefetch={false}>Open /cpar/explore</Link>
            <Link href="/cpar/hedge" className="cpar-detail-chip" prefetch={false}>Open /cpar/hedge</Link>
          </div>
          <div className="cpar-inline-message neutral">
            <strong>Package-first routing.</strong>
            <span>
              `/cpar/health` stays the package diagnostics surface, `/cpar/risk` stays the account-scoped hedge and
              what-if workspace, and `/cpar/explore` plus `/cpar/hedge` stay instrument-scoped.
            </span>
          </div>
        </section>

        <section className="chart-card">
          <h3>Warning Legend</h3>
          <div className="section-subtitle">
            cPAR fit status and warning badges are package-level interpretation aids, not live rebuild signals.
          </div>
          <div className="cpar-legend-stack">
            <div>
              <CparWarningsBar fitStatus="ok" compact />
              <div className="cpar-legend-copy">Full weekly history with no material continuity caution.</div>
            </div>
            <div>
              <CparWarningsBar fitStatus="limited_history" compact />
              <div className="cpar-legend-copy">Rendered normally, but history depth or continuity is weaker.</div>
            </div>
            <div>
              <CparWarningsBar fitStatus="insufficient_history" compact />
              <div className="cpar-legend-copy">Identity and warnings stay visible, but loadings and hedge output are blocked.</div>
            </div>
            <div>
              <CparWarningsBar warnings={["continuity_gap", "ex_us_caution"]} compact />
              <div className="cpar-legend-copy">Non-blocking cautions layered on top of the fit status.</div>
            </div>
          </div>
        </section>
      </div>

      <section className="chart-card">
        <h3>Explore Entry Point</h3>
        <div className="section-subtitle">
          Health stays diagnostics-first, but you can still jump straight into the active package detail workflow from here.
        </div>
        <CparSearchPanel
          onSelectResult={(item) => {
            if (!canNavigateCparSearchResult(item)) return;
            router.push(buildExploreHref(item));
          }}
          helperText="Jump into the active package and open one persisted ticker detail view."
        />
      </section>

      {data ? (
        <section className="chart-card" data-testid="cpar-factor-registry">
          <h3>Factor Registry</h3>
          <div className="section-subtitle">
            Active cPAR1 proxy set for the current package dated {formatCparPackageDate(data.package_date)}.
          </div>
          <div className="cpar-registry-summary">
            <span className="cpar-detail-chip">Market {factorCounts?.market ?? 0}</span>
            <span className="cpar-detail-chip">Sectors {factorCounts?.sector ?? 0}</span>
            <span className="cpar-detail-chip">Styles {factorCounts?.style ?? 0}</span>
          </div>
          <div className="dash-table">
            <table>
              <thead>
                <tr>
                  <th>Factor</th>
                  <th>Ticker</th>
                  <th>Group</th>
                  <th className="text-right">Order</th>
                </tr>
              </thead>
              <tbody>
                {data.factors.map((factor) => (
                  <tr key={factor.factor_id}>
                    <td>
                      <strong>{factor.label}</strong>
                      <span className="cpar-table-sub">{factor.factor_id}</span>
                    </td>
                    <td>{factor.ticker}</td>
                    <td>{factor.group}</td>
                    <td className="text-right cpar-number-cell">{factor.display_order}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}
    </div>
  );
}
