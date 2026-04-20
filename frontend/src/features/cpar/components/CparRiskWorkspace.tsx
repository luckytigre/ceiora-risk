"use client";

import { Suspense, useMemo } from "react";
import { CparPageLoadingState } from "@/features/cpar/components/CparLoadingState";
import CparRiskCovarianceSection from "@/features/cpar/components/CparRiskCovarianceSection";
import CparRiskDecompChart from "@/features/cpar/components/CparRiskDecompChart";
import CparRiskFactorSummaryCard from "@/features/cpar/components/CparRiskFactorSummaryCard";
import CparRiskPositionsContributionTable from "@/features/cpar/components/CparRiskPositionsContributionTable";
import { useCparRisk } from "@/hooks/useCparApi";
import {
  normalizeCparRiskData,
  readCparDependencyErrorMessage,
  readCparError,
} from "@/lib/cparTruth";
import {
  COMBINED_DECOMP_SUBTITLE,
  deriveRawLoadingSharesFromCparLoadings,
  RISK_DECOMP_SECTION_TITLE,
} from "@/lib/riskDecompBars";

function CparRiskLoadingCards() {
  return (
    <div className="cpar-page">
      <section className="chart-card" data-testid="cpar-risk-loading">
        <h3>{RISK_DECOMP_SECTION_TITLE}</h3>
        <div className="section-subtitle">{COMBINED_DECOMP_SUBTITLE}</div>
        <div className="detail-history-empty loading-pulse">Loading first cPAR risk snapshot...</div>
      </section>
      <section className="chart-card">
        <h3>Risk Surface</h3>
        <div className="detail-history-empty loading-pulse">Loading factor summary and aggregate positions...</div>
      </section>
    </div>
  );
}

function CparRiskWorkspaceInner() {
  const {
    data: risk,
    error: riskError,
    isLoading: riskLoading,
  } = useCparRisk(true);
  const normalizedRisk = useMemo(() => normalizeCparRiskData(risk), [risk]);
  const rawLoadingShares = useMemo(
    () => deriveRawLoadingSharesFromCparLoadings(
      normalizedRisk?.aggregate_display_loadings,
      normalizedRisk?.display_factor_chart,
      normalizedRisk?.positions,
    ),
    [normalizedRisk?.aggregate_display_loadings, normalizedRisk?.display_factor_chart, normalizedRisk?.positions],
  );
  const volScaledShares = normalizedRisk?.vol_scaled_shares ?? normalizedRisk?.risk_shares ?? { market: 0, industry: 0, style: 0, idio: 100 };
  const riskState = riskError ? readCparError(riskError) : null;

  if (riskLoading && !risk) {
    return <CparRiskLoadingCards />;
  }

  return (
    <div className="cpar-page">
      {riskState ? (
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
      ) : normalizedRisk ? (
        <>
          <div className="chart-card" style={{ marginBottom: 12 }}>
            <h3>{RISK_DECOMP_SECTION_TITLE}</h3>
            <div className="section-subtitle">
              {COMBINED_DECOMP_SUBTITLE}
            </div>
            <CparRiskDecompChart
              rows={[
                { label: "Raw Loadings", shares: rawLoadingShares },
                { label: "Vol-Scaled", shares: volScaledShares },
              ]}
            />
          </div>
          <CparRiskFactorSummaryCard portfolio={normalizedRisk} />
          <CparRiskPositionsContributionTable
            rows={normalizedRisk.positions}
            packageIdentity={{
              package_run_id: normalizedRisk.package_run_id,
              package_date: normalizedRisk.package_date,
            }}
          />
          <CparRiskCovarianceSection covMatrix={normalizedRisk.cov_matrix} factors={normalizedRisk.factors ?? []} />
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
