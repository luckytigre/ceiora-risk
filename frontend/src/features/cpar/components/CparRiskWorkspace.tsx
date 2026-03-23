"use client";

import { Suspense, useMemo } from "react";
import { CparPageLoadingState } from "@/features/cpar/components/CparLoadingState";
import CparRiskCovarianceSection from "@/features/cpar/components/CparRiskCovarianceSection";
import CparRiskDecompChart from "@/features/cpar/components/CparRiskDecompChart";
import CparRiskFactorSummaryCard from "@/features/cpar/components/CparRiskFactorSummaryCard";
import CparRiskPositionsContributionTable from "@/features/cpar/components/CparRiskPositionsContributionTable";
import { useCparMeta, useCparRisk } from "@/hooks/useCparApi";
import {
  normalizeCparRiskData,
  readCparDependencyErrorMessage,
  readCparError,
  sameCparPackageIdentity,
} from "@/lib/cparTruth";
import { deriveRawLoadingSharesFromCparLoadings } from "@/lib/riskDecompBars";

function CparRiskWorkspaceInner() {
  const { data: meta, error: metaError, isLoading: metaLoading } = useCparMeta();
  const metaState = metaError ? readCparError(metaError) : null;
  const {
    data: risk,
    error: riskError,
    isLoading: riskLoading,
  } = useCparRisk(Boolean(meta) && !metaState);
  const normalizedRisk = useMemo(() => normalizeCparRiskData(risk), [risk]);
  const rawLoadingShares = useMemo(
    () => deriveRawLoadingSharesFromCparLoadings(normalizedRisk?.aggregate_display_loadings),
    [normalizedRisk?.aggregate_display_loadings],
  );
  const volScaledShares = normalizedRisk?.vol_scaled_shares ?? normalizedRisk?.risk_shares ?? { market: 0, industry: 0, style: 0, idio: 100 };
  const riskState = riskError ? readCparError(riskError) : null;
  const packageMismatch = Boolean(meta && normalizedRisk && !sameCparPackageIdentity(meta, normalizedRisk));

  if ((metaLoading && !meta) || (!metaState && riskLoading && !risk)) {
    return <CparPageLoadingState message="Loading cPAR risk..." />;
  }

  return (
    <div className="cpar-page">
      {metaState ? (
        <section className="chart-card cpar-alert-card" data-testid="cpar-portfolio-not-ready">
          <h3>{metaState.kind === "not_ready" ? "cPAR Risk Not Ready" : "cPAR Risk Unavailable"}</h3>
          <div className="section-subtitle">{metaState.message}</div>
          <div className="detail-history-empty compact">
            This page is package-based and read-only. Publish a durable cPAR package first, then reload.
          </div>
        </section>
      ) : null}

      {metaState ? (
        null
      ) : riskState ? (
        <section className="chart-card" data-testid="cpar-portfolio-error">
          <h3>Risk Surface</h3>
          <div className={`cpar-inline-message ${riskState.kind === "missing" ? "warning" : "error"}`}>
            <strong>
              {riskState.kind === "missing"
                ? "Aggregate risk surface missing."
                : riskState.kind === "not_ready"
                  ? "Risk package not ready."
                  : "Risk surface unavailable."}
            </strong>
            <span>{readCparDependencyErrorMessage(riskError)}</span>
          </div>
        </section>
      ) : packageMismatch ? (
        <section className="chart-card" data-testid="cpar-portfolio-package-mismatch">
          <h3>Risk Surface</h3>
          <div className="cpar-inline-message error">
            <strong>Active package changed during read.</strong>
            <span>The risk workflow no longer matches the active package metadata.</span>
            <span>Reload the page to pin one cPAR package before reading the aggregate risk surface.</span>
          </div>
        </section>
      ) : normalizedRisk ? (
        <>
          <div className="chart-card" style={{ marginBottom: 12 }}>
            <h3>Raw Loadings</h3>
            <div className="section-subtitle">
              Absolute raw loading footprint split across market, industry, and style factors.
            </div>
            <CparRiskDecompChart shares={rawLoadingShares} showIdio={false} />
          </div>
          <div className="chart-card" style={{ marginBottom: 12 }}>
            <h3>Vol-Scaled Decomposition</h3>
            <div className="section-subtitle">
              Vol-scaled footprint split across market, industry, style, and idiosyncratic components.
            </div>
            <CparRiskDecompChart shares={volScaledShares} />
          </div>
          <CparRiskFactorSummaryCard portfolio={normalizedRisk} />
          <CparRiskPositionsContributionTable rows={normalizedRisk.positions} />
          <CparRiskCovarianceSection covMatrix={normalizedRisk.cov_matrix} factors={meta?.factors ?? []} />
        </>
      ) : null}
    </div>
  );
}

export default function CparRiskWorkspace() {
  return (
    <Suspense fallback={<CparPageLoadingState message="Loading cPAR risk..." />}>
      <CparRiskWorkspaceInner />
    </Suspense>
  );
}
