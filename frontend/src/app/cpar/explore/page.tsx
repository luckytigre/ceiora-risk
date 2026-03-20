"use client";

import { Suspense } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { useCparMeta, useCparTicker } from "@/hooks/useCparApi";
import { canNavigateCparSearchResult, readCparError, sameCparPackageIdentity } from "@/lib/cparTruth";
import type { CparSearchItem } from "@/lib/types/cpar";
import { CparInlineLoadingState, CparPageLoadingState } from "@/features/cpar/components/CparLoadingState";
import CparExploreDetailModule from "@/features/cpar/components/CparExploreDetailModule";
import CparExploreLoadingsSection from "@/features/cpar/components/CparExploreLoadingsSection";
import CparExploreSearchModule from "@/features/cpar/components/CparExploreSearchModule";

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

function ExploreSelectionState({
  title,
  message,
  tone = "neutral",
  testId = "cpar-detail-panel",
}: {
  title: string;
  message: string;
  tone?: "neutral" | "warning" | "error";
  testId?: string;
}) {
  return (
    <section className="chart-card cpar-explore-selection-state" data-testid={testId}>
      <div className="cpar-explore-module-header">
        <div>
          <div className="cpar-explore-kicker">Selected Instrument</div>
          <h3 className="cpar-explore-module-title">{title}</h3>
        </div>
      </div>
      <div className={`cpar-inline-message ${tone}`}>
        <strong>{title}</strong>
        <span>{message}</span>
      </div>
    </section>
  );
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
    return <CparPageLoadingState message="Loading cPAR explore..." />;
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

      <div className="cpar-explore-top-grid">
        <CparExploreSearchModule
          initialQuery={querySeed}
          selectedRic={ric}
          onSelectResult={(item) => {
            if (!canNavigateCparSearchResult(item)) return;
            router.push(buildExploreHref(item));
          }}
        />

        <div className="cpar-explore-detail-stack">
          {!ticker && ric ? (
            <ExploreSelectionState
              title="Ticker Required For Detail"
              message="This active-package search hit has no ticker symbol, and the current cPAR detail route is ticker-keyed. Use another search result with a ticker, or extend the backend route contract in a later slice."
              tone="warning"
            />
          ) : !ticker ? (
            <section className="chart-card cpar-explore-selection-state" data-testid="cpar-detail-panel">
              <div className="cpar-explore-module-header">
                <div>
                  <div className="cpar-explore-kicker">Selected Instrument</div>
                  <h3 className="cpar-explore-module-title">Pick One Persisted Fit Row</h3>
                  <div className="cpar-explore-module-subtitle">
                    `/cpar/explore` stays on one active-package row, its persisted loadings, and supplemental
                    package-date source context.
                  </div>
                </div>
              </div>
              <div className="detail-history-empty">
                Select a search result to load one cPAR package row and review its persisted fit detail.
              </div>
            </section>
          ) : metaState ? (
            <ExploreSelectionState
              title="Active Package Metadata Unavailable"
              message="Reload after the active cPAR package is readable again before opening detail or the hedge workspace."
              tone="warning"
            />
          ) : detailLoading && !detail ? (
            <section className="chart-card cpar-explore-selection-state" data-testid="cpar-detail-panel">
              <div className="cpar-explore-module-header">
                <div>
                  <div className="cpar-explore-kicker">Selected Instrument</div>
                  <h3 className="cpar-explore-module-title">Loading Persisted Fit Detail</h3>
                </div>
              </div>
              <CparInlineLoadingState message={`Loading cPAR detail for ${ric || ticker}...`} />
            </section>
          ) : detailState ? (
            <section className="chart-card cpar-explore-selection-state" data-testid="cpar-detail-panel">
              <div className="cpar-explore-module-header">
                <div>
                  <div className="cpar-explore-kicker">Selected Instrument</div>
                  <h3 className="cpar-explore-module-title">
                    {detailState.kind === "ambiguous"
                      ? "Resolve Ticker Ambiguity"
                      : detailState.kind === "missing"
                        ? "Ticker Not Found"
                        : "Detail Read Unavailable"}
                  </h3>
                </div>
              </div>
              <div className={`cpar-inline-message ${detailState.kind === "ambiguous" ? "warning" : "error"}`}>
                <strong>
                  {detailState.kind === "ambiguous"
                    ? "Ticker is ambiguous."
                    : detailState.kind === "missing"
                      ? "Ticker not found."
                      : "Detail unavailable."}
                </strong>
                <span>{detailState.message}</span>
                {detailState.kind === "ambiguous" ? <span>Choose a specific RIC from the search results on the left.</span> : null}
              </div>
            </section>
          ) : detailPackageMismatch ? (
            <section className="chart-card cpar-explore-selection-state" data-testid="cpar-detail-panel">
              <div className="cpar-explore-module-header">
                <div>
                  <div className="cpar-explore-kicker">Selected Instrument</div>
                  <h3 className="cpar-explore-module-title">Reload To Pin One Package</h3>
                </div>
              </div>
              <div className="cpar-inline-message error" data-testid="cpar-package-mismatch">
                <strong>Active package changed during read.</strong>
                <span>The banner package no longer matches the persisted detail row.</span>
                <span>Reload the page to pin one cPAR package before reading loadings or opening the hedge workspace.</span>
              </div>
            </section>
          ) : detail ? (
            <CparExploreDetailModule
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
                      This page stays focused on the selected package row, its source-context augmentation, and its
                      persisted loadings. Hedge-specific interaction stays on `/cpar/hedge`.
                    </span>
                  </div>
                )
              }
            />
          ) : null}
        </div>
      </div>

      {detail && !detailBlocked && !metaState ? (
        <CparExploreLoadingsSection detail={detail} hedgeHref={buildHedgeHref(detail.ticker, detail.ric)} />
      ) : null}
    </div>
  );
}

export default function CparExplorePage() {
  return (
    <Suspense fallback={<CparPageLoadingState message="Loading cPAR explore..." />}>
      <CparExplorePageInner />
    </Suspense>
  );
}
