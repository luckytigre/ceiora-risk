"use client";

import LazyMountOnVisible from "@/components/LazyMountOnVisible";
import CparCovarianceHeatmap from "@/features/cpar/components/CparCovarianceHeatmap";
import { shortFactorLabel } from "@/lib/factorLabels";
import type { FactorCatalogEntry } from "@/lib/types/analytics";
import type { CparCovMatrix, CparFactorSpec } from "@/lib/types/cpar";

function buildFactorCatalog(factors: CparFactorSpec[]): FactorCatalogEntry[] {
  return factors.map((factor) => ({
    factor_id: factor.factor_id,
    factor_name: factor.label,
    short_label: shortFactorLabel(factor.label),
    family: factor.group === "sector" ? "industry" : factor.group,
    block: factor.group,
    display_order: factor.display_order,
    covariance_display: true,
    exposure_publish: true,
    active: true,
    method_version: factor.method_version,
  }));
}

export default function CparRiskCovarianceSection({
  covMatrix,
  factors,
}: {
  covMatrix: CparCovMatrix;
  factors: CparFactorSpec[];
}) {
  const factorCatalog = buildFactorCatalog(factors);

  return (
    <section className="chart-card">
      <h3>Factor Correlation Heatmap</h3>
      <div className="section-subtitle">
        cPAR uses the active package covariance surface. This heatmap stays package-pinned and renders the full cPAR
        registry across market, industry, and style factors.
      </div>
      <LazyMountOnVisible
        minHeight={320}
        fallback={<div className="detail-history-empty">Scroll to load the factor correlation heatmap.</div>}
      >
        <div className="heatmap-centered-70">
          <CparCovarianceHeatmap data={covMatrix} factorCatalog={factorCatalog} factorScope="all" />
        </div>
      </LazyMountOnVisible>
    </section>
  );
}
