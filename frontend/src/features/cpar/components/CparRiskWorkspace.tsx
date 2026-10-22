"use client";

import { Suspense, useMemo } from "react";
import { CparPageLoadingState } from "@/features/cpar/components/CparLoadingState";
import InlineApiError from "@/components/InlineApiError";
import HelpLabel from "@/components/HelpLabel";
import CparRiskCovarianceSection from "@/features/cpar/components/CparRiskCovarianceSection";
import CparRiskDecompChart from "@/features/cpar/components/CparRiskDecompChart";
import CparRiskFactorSummaryCard from "@/features/cpar/components/CparRiskFactorSummaryCard";
import CparRiskPositionsContributionTable from "@/features/cpar/components/CparRiskPositionsContributionTable";
import { useCparRisk } from "@/hooks/useCparApi";
import {
  normalizeCparRiskData,
} from "@/lib/cparTruth";
import { describeUiError } from "@/lib/uiErrors";
import {
  COMBINED_DECOMP_SUBTITLE,
  deriveRawLoadingSharesFromCparLoadings,
  RISK_DECOMP_SECTION_TITLE,
} from "@/lib/riskDecompBars";

function CparRiskLoadingCards() {
  return (
    <div className="cpar-page">
      <section className="chart-card" data-testid="cpar-risk-loading">
        <div className="module-title-with-help">
          <h3>{RISK_DECOMP_SECTION_TITLE}</h3>
          <HelpLabel label={RISK_DECOMP_SECTION_TITLE} plain={COMBINED_DECOMP_SUBTITLE} iconOnly />
        </div>
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
  const riskErrorKind = riskError ? describeUiError(riskError).kind : null;

  if (riskLoading && !risk) {
    return <CparRiskLoadingCards />;
  }

  return (
    <div className="cpar-page">
      {riskError ? (
        <section className="chart-card" data-testid="cpar-portfolio-error">
          <h3>Risk Surface</h3>
          <InlineApiError
            error={riskError}
            surface="cPAR risk"
            className={`cpar-inline-message ${riskErrorKind === "not_found" ? "warning" : "error"}`}
          />
        </section>
      ) : normalizedRisk ? (
        <>
          <div className="chart-card" style={{ marginBottom: 12 }}>
            <div className="module-title-with-help">
              <h3>{RISK_DECOMP_SECTION_TITLE}</h3>
              <HelpLabel label={RISK_DECOMP_SECTION_TITLE} plain={COMBINED_DECOMP_SUBTITLE} iconOnly />
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
    <Suspense fallback={<CparPageLoadingState message="Loading cPAR risk…" />}>
      <CparRiskWorkspaceInner />
    </Suspense>
  );
}
