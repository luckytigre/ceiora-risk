"use client";

import {
  describeCparPackageFreshness,
  formatCparPackageDate,
  formatCparTimestamp,
  summarizeFactorRegistry,
} from "@/lib/cparTruth";
import type { CparFactorSpec, CparPackageMeta } from "@/lib/types/cpar";

function metric(label: string, value: string, detail?: string, testId?: string) {
  return (
    <div className="cpar-package-metric" data-testid={testId}>
      <div className="cpar-package-label">{label}</div>
      <div className="cpar-package-value">{value}</div>
      {detail ? <div className="cpar-package-detail">{detail}</div> : null}
    </div>
  );
}

export default function CparPackageBanner({
  meta,
  factors = [],
  title = "Active cPAR Package",
  subtitle,
}: {
  meta: CparPackageMeta;
  factors?: CparFactorSpec[];
  title?: string;
  subtitle?: string;
}) {
  const counts = summarizeFactorRegistry(factors);
  const freshness = describeCparPackageFreshness(meta);
  return (
    <section className="chart-card cpar-package-banner" data-testid="cpar-package-banner">
      <div className="cpar-section-kicker">Package-Based Read Surface</div>
      <h3>{title}</h3>
      <div className="section-subtitle">
        {subtitle
          || "cPAR reads are anchored to one complete persisted package. The frontend does not refit or blend live cUSE4 surfaces into this view."}
      </div>
      <div className="cpar-package-grid">
        {metric("Package Date", formatCparPackageDate(meta.package_date), meta.package_run_id)}
        {metric("Freshness", freshness.label, freshness.detail, "cpar-package-freshness")}
        {metric(
          "Built",
          formatCparTimestamp(meta.completed_at),
          meta.started_at ? `Started ${formatCparTimestamp(meta.started_at)}` : undefined,
        )}
        {metric("Method", meta.method_version, meta.factor_registry_version)}
        {metric("Authority", meta.data_authority.toUpperCase(), meta.profile)}
        {metric("History", `${meta.lookback_weeks}w`, `Half-life ${meta.half_life_weeks}w`)}
        {metric("Min Obs", String(meta.min_observations), `Universe ${meta.universe_count}`)}
        {metric(
          "Registry",
          `${factors.length || "—"} factors`,
          `M ${counts.market} · S ${counts.sector} · St ${counts.style}`,
        )}
      </div>
    </section>
  );
}
