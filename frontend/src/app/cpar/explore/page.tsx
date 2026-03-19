"use client";

import { Suspense } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import { useCparMeta, useCparTicker } from "@/hooks/useApi";
import { canNavigateCparSearchResult, readCparError, sameCparPackageIdentity } from "@/lib/cparTruth";
import type { CparSearchItem } from "@/lib/types";
import CparInstrumentSummaryCard from "@/features/cpar/components/CparInstrumentSummaryCard";
import CparLoadingsTable from "@/features/cpar/components/CparLoadingsTable";
import CparSearchPanel from "@/features/cpar/components/CparSearchPanel";

function buildExploreHref(item: CparSearchItem): string {
  const params = new URLSearchParams();
  if (item.ticker) params.set("ticker", item.ticker);
  params.set("ric", item.ric);
  return `/cpar/explore?${params.toString()}`;
}

function buildHedgeHref(ticker: string | null | undefined, ric: string): string {
  const params = new URLSearchParams();
  if (ticker) params.set("ticker", ticker);
  params.set("ric", ric);
  return `/cpar/hedge?${params.toString()}`;
}

function CparExplorePageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const ticker = searchParams?.get("ticker")?.trim().toUpperCase() || null;
  const ric = searchParams?.get("ric")?.trim() || null;
  const querySeed = ric || ticker || "";

  const { data: meta, error: metaError, isLoading: metaLoading } = useCparMeta();
  const metaState = metaError ? readCparError(metaError) : null;
  const metaReady = Boolean(meta) && !metaState;
  const {
    data: detail,
    error: detailError,
    isLoading: detailLoading,
  } = useCparTicker(metaReady ? ticker : null, ric);

  if (metaLoading && !meta) {
    return <AnalyticsLoadingViz message="Loading cPAR explore..." />;
  }

  const detailState = detailError ? readCparError(detailError) : null;
  const detailPackageMismatch = Boolean(meta && detail && !sameCparPackageIdentity(meta, detail));
  const detailBlocked = detail?.fit_status === "insufficient_history" || detailPackageMismatch;

  return (
    <div className="cpar-page">
      {metaState ? (
        <section className="chart-card cpar-alert-card" data-testid="cpar-explore-not-ready">
          <h3>{metaState.kind === "not_ready" ? "cPAR Explore Not Ready" : "cPAR Explore Unavailable"}</h3>
          <div className="section-subtitle">{metaState.message}</div>
        </section>
      ) : null}

      <div className="cpar-two-column">
        <CparSearchPanel
          initialQuery={querySeed}
          selectedRic={ric}
          title="Search The Active Package"
          helperText="The explore page resolves to one active-package row. Use RIC selection when a ticker is ambiguous."
          onSelectResult={(item) => {
            if (!canNavigateCparSearchResult(item)) return;
            router.push(buildExploreHref(item));
          }}
        />

        <section className="chart-card" data-testid="cpar-detail-panel">
          <h3>Selected Instrument</h3>
          {!ticker && ric ? (
            <div className="cpar-inline-message warning">
              <strong>RIC result cannot open detail directly.</strong>
              <span>
                This active-package search hit has no ticker symbol, and the current cPAR detail route is ticker-keyed.
              </span>
              <span>Use another search result with a ticker, or extend the backend route contract in a later slice.</span>
            </div>
          ) : !ticker ? (
            <div className="detail-history-empty">
              Select a search result to load one cPAR package row and review its persisted fit detail.
            </div>
          ) : metaState ? (
            <div className="cpar-inline-message warning">
              <strong>Current package metadata is unavailable.</strong>
              <span>Reload after the active cPAR package is readable again before opening detail or the hedge workspace.</span>
            </div>
          ) : detailLoading && !detail ? (
            <AnalyticsLoadingViz message={`Loading cPAR detail for ${ric || ticker}...`} />
          ) : detailState ? (
            <div className={`cpar-inline-message ${detailState.kind === "ambiguous" ? "warning" : "error"}`}>
              <strong>
                {detailState.kind === "ambiguous"
                  ? "Ticker is ambiguous."
                  : detailState.kind === "missing"
                    ? "Ticker not found."
                    : "Detail unavailable."}
              </strong>
              <span>{detailState.message}</span>
              {detailState.kind === "ambiguous" ? (
                <span>Choose a specific RIC from the search results on the left.</span>
              ) : null}
            </div>
          ) : detailPackageMismatch ? (
            <div className="cpar-inline-message error" data-testid="cpar-package-mismatch">
              <strong>Active package changed during read.</strong>
              <span>The banner package no longer matches the persisted detail row.</span>
              <span>Reload the page to pin one cPAR package before reading loadings or opening the hedge workspace.</span>
            </div>
          ) : detail ? (
            <CparInstrumentSummaryCard
              detail={detail}
              footer={
                detailBlocked ? (
                  <div className="cpar-inline-message warning" data-testid="cpar-insufficient-history">
                    <strong>Loadings and the hedge workflow are blocked.</strong>
                    <span>
                      This row is persisted as `insufficient_history`, so the frontend only renders identity, package
                      metadata, and warnings.
                    </span>
                  </div>
                ) : (
                  <div className="cpar-inline-message neutral">
                    <strong>Explore stays on persisted fit interpretation.</strong>
                    <span>
                      Use the dedicated hedge workspace for mode switching, hedge legs, and post-hedge inspection. This
                      page stays focused on the selected package row and its loadings.
                    </span>
                    <div className="cpar-badge-row compact">
                      <Link
                        href={buildHedgeHref(detail.ticker, detail.ric)}
                        className="cpar-detail-chip"
                        prefetch={false}
                      >
                        Open Hedge Workspace
                      </Link>
                    </div>
                  </div>
                )
              }
            />
          ) : null}
        </section>
      </div>

      {detail && !detailBlocked && !metaState ? (
        <>
          <div className="cpar-two-column">
            <CparLoadingsTable title="Raw ETF Loadings" rows={detail.raw_loadings} />
            <CparLoadingsTable
              title="Thresholded ETF Loadings"
              rows={detail.thresholded_loadings}
              emptyText="Thresholding zeroed every non-market leg in the persisted trade-space payload."
            />
          </div>
          <section className="chart-card" data-testid="cpar-hedge-workspace-card">
            <h3>Dedicated Hedge Workflow</h3>
            <div className="section-subtitle">
              Hedge preview, mode switching, and post-hedge inspection now live on `/cpar/hedge` so explore can stay
              focused on persisted fit detail and loadings.
            </div>
            <div className="cpar-inline-message neutral">
              <strong>Open the same instrument in the hedge workspace.</strong>
              <span>
                The hedge page reuses the same active-package ticker and RIC selection, then applies the persisted
                hedge route without any request-time refit.
              </span>
              <div className="cpar-badge-row compact">
                <Link
                  href={buildHedgeHref(detail.ticker, detail.ric)}
                  className="cpar-detail-chip"
                  prefetch={false}
                >
                  Continue To /cpar/hedge
                </Link>
              </div>
            </div>
          </section>
        </>
      ) : null}

      {detail && detail.fit_status === "limited_history" && !detailPackageMismatch && !metaState ? (
        <section className="chart-card">
          <h3>Interpretation Note</h3>
          <div className="section-subtitle">
            `limited_history` still renders persisted loadings and keeps the hedge workspace available, but adjacent package comparisons deserve more
            caution than a full-history `ok` row.
          </div>
          <div className="detail-history-empty compact">
            If you open `/cpar/hedge`, compare the stability and non-market reduction metrics before using the persisted
            hedge output.
          </div>
          {detail.pre_hedge_factor_variance_proxy !== null ? (
            <div className="cpar-detail-chip">
              Current pre-hedge variance proxy: {detail.pre_hedge_factor_variance_proxy?.toFixed(3)}
            </div>
          ) : null}
        </section>
      ) : null}
    </div>
  );
}

export default function CparExplorePage() {
  return (
    <Suspense fallback={<AnalyticsLoadingViz message="Loading cPAR explore..." />}>
      <CparExplorePageInner />
    </Suspense>
  );
}
