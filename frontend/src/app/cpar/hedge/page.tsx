"use client";

import { Suspense } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import CparHedgePanel from "@/features/cpar/components/CparHedgePanel";
import CparInstrumentSummaryCard from "@/features/cpar/components/CparInstrumentSummaryCard";
import CparSearchPanel from "@/features/cpar/components/CparSearchPanel";
import { useCparMeta, useCparTicker } from "@/hooks/useApi";
import { canNavigateCparSearchResult, readCparError, sameCparPackageIdentity } from "@/lib/cparTruth";
import type { CparSearchItem } from "@/lib/types";

function buildHedgeHref(item: CparSearchItem): string {
  const params = new URLSearchParams();
  if (item.ticker) params.set("ticker", item.ticker);
  params.set("ric", item.ric);
  return `/cpar/hedge?${params.toString()}`;
}

function buildExploreHref(ticker: string | null | undefined, ric: string): string {
  const params = new URLSearchParams();
  if (ticker) params.set("ticker", ticker);
  params.set("ric", ric);
  return `/cpar/explore?${params.toString()}`;
}

function CparHedgePageInner() {
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
    return <AnalyticsLoadingViz message="Loading cPAR hedge workspace..." />;
  }

  const detailState = detailError ? readCparError(detailError) : null;
  const detailPackageMismatch = Boolean(meta && detail && !sameCparPackageIdentity(meta, detail));

  return (
    <div className="cpar-page">
      {metaState ? (
        <section className="chart-card cpar-alert-card" data-testid="cpar-hedge-not-ready">
          <h3>{metaState.kind === "not_ready" ? "cPAR Hedge Not Ready" : "cPAR Hedge Unavailable"}</h3>
          <div className="section-subtitle">{metaState.message}</div>
          <div className="detail-history-empty compact">
            Hedge output is read-only and package-based. Publish a durable cPAR package first, then reload.
          </div>
        </section>
      ) : null}

      <div className="cpar-two-column">
        <CparSearchPanel
          initialQuery={querySeed}
          selectedRic={ric}
          title="Find A Hedge Subject"
          helperText="Resolve one active-package ticker or RIC, then open its persisted hedge workflow."
          onSelectResult={(item) => {
            if (!canNavigateCparSearchResult(item)) return;
            router.push(buildHedgeHref(item));
          }}
        />

        {!ticker && ric ? (
          <section className="chart-card" data-testid="cpar-hedge-subject-panel">
            <h3>Selected Hedge Subject</h3>
            <div className="cpar-inline-message warning">
              <strong>RIC result cannot open hedge directly.</strong>
              <span>
                This active-package search hit has no ticker symbol, and the current cPAR hedge route is ticker-keyed.
              </span>
              <span>Use another search result with a ticker, or extend the backend contract in a later slice.</span>
            </div>
          </section>
        ) : !ticker ? (
          <section className="chart-card" data-testid="cpar-hedge-subject-panel">
            <h3>Selected Hedge Subject</h3>
            <div className="detail-history-empty">
              Select a search result to load one persisted cPAR package row and its hedge workflow.
            </div>
          </section>
        ) : metaState ? (
          <section className="chart-card" data-testid="cpar-hedge-subject-panel">
            <h3>Selected Hedge Subject</h3>
            <div className="cpar-inline-message warning">
              <strong>Current package metadata is unavailable.</strong>
              <span>Reload after the active cPAR package is readable again before opening hedge output.</span>
            </div>
          </section>
        ) : detailLoading && !detail ? (
          <section className="chart-card" data-testid="cpar-hedge-subject-panel">
            <h3>Selected Hedge Subject</h3>
            <AnalyticsLoadingViz message={`Loading cPAR hedge subject for ${ric || ticker}...`} />
          </section>
        ) : detailState ? (
          <section className="chart-card" data-testid="cpar-hedge-subject-panel">
            <h3>Selected Hedge Subject</h3>
            <div className={`cpar-inline-message ${detailState.kind === "ambiguous" ? "warning" : "error"}`}>
              <strong>
                {detailState.kind === "ambiguous"
                  ? "Ticker is ambiguous."
                  : detailState.kind === "missing"
                    ? "Ticker not found."
                    : "Hedge subject unavailable."}
              </strong>
              <span>{detailState.message}</span>
              {detailState.kind === "ambiguous" ? (
                <span>Choose a specific RIC from the search results on the left.</span>
              ) : null}
            </div>
          </section>
        ) : detailPackageMismatch ? (
          <section className="chart-card" data-testid="cpar-hedge-subject-panel">
            <h3>Selected Hedge Subject</h3>
            <div className="cpar-inline-message error" data-testid="cpar-hedge-package-mismatch">
              <strong>Active package changed during read.</strong>
              <span>The banner package no longer matches the persisted hedge subject row.</span>
              <span>Reload the page to pin one cPAR package before interpreting hedge output.</span>
            </div>
          </section>
        ) : detail ? (
          <CparInstrumentSummaryCard
            detail={detail}
            title="Selected Hedge Subject"
            testId="cpar-hedge-subject-panel"
            footer={(
              <div className="cpar-inline-message neutral">
                <strong>Persisted-only hedge semantics.</strong>
                <span>
                  This workspace uses the selected row&apos;s persisted thresholded loadings plus the persisted
                  covariance surface for the same active package. No request-time refit occurs here.
                </span>
                <div className="cpar-badge-row compact">
                  <Link
                    href={buildExploreHref(detail.ticker, detail.ric)}
                    className="cpar-detail-chip"
                    prefetch={false}
                  >
                    Review Loadings In /cpar/explore
                  </Link>
                </div>
              </div>
            )}
          />
        ) : null}
      </div>

      {detail && !metaState && !detailState && !detailPackageMismatch ? (
        <>
          <section className="chart-card">
            <h3>Workflow Notes</h3>
            <div className="section-subtitle">
              `factor_neutral` uses the thresholded raw ETF package across market, sector, and style legs. `market_neutral`
              uses only the SPY leg when the persisted trade-space beta is material.
            </div>
            <div className="cpar-inline-message neutral">
              <strong>Fail-closed package truth.</strong>
              <span>
                If package identity drifts between the selected subject and the hedge preview, the workspace blocks the
                result and requires a reload instead of mixing surfaces across packages.
              </span>
            </div>
          </section>
          <CparHedgePanel
            ticker={detail.ticker || ticker || detail.ric}
            ric={detail.ric}
            fitStatus={detail.fit_status}
            expectedPackageRunId={detail.package_run_id}
            expectedPackageDate={detail.package_date}
          />
        </>
      ) : null}
    </div>
  );
}

export default function CparHedgePage() {
  return (
    <Suspense fallback={<AnalyticsLoadingViz message="Loading cPAR hedge workspace..." />}>
      <CparHedgePageInner />
    </Suspense>
  );
}
