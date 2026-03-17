"use client";

import dynamic from "next/dynamic";
import LazyMountOnVisible from "@/components/LazyMountOnVisible";
import type { HealthDiagnosticsData } from "@/lib/types";

const SectionRegression = dynamic(() => import("./SectionRegression"), {
  ssr: false,
  loading: () => <div className="chart-card"><div className="detail-history-empty loading-pulse">Loading regression diagnostics…</div></div>,
});
const SectionExposure = dynamic(() => import("./SectionExposure"), {
  ssr: false,
  loading: () => <div className="chart-card"><div className="detail-history-empty loading-pulse">Loading exposure diagnostics…</div></div>,
});
const SectionFactorReturns = dynamic(() => import("./SectionFactorReturns"), {
  ssr: false,
  loading: () => <div className="chart-card"><div className="detail-history-empty loading-pulse">Loading factor return diagnostics…</div></div>,
});
const SectionCovarianceQuality = dynamic(() => import("./SectionCovarianceQuality"), {
  ssr: false,
  loading: () => <div className="chart-card"><div className="detail-history-empty loading-pulse">Loading covariance diagnostics…</div></div>,
});
const SectionCoverage = dynamic(() => import("./SectionCoverage"), {
  ssr: false,
  loading: () => <div className="chart-card"><div className="detail-history-empty loading-pulse">Loading coverage diagnostics…</div></div>,
});

export default function HealthDiagnosticsRoot({ data }: { data: HealthDiagnosticsData }) {
  const refreshState = String(data.diagnostics_refresh_state || "").trim().toLowerCase();
  const freshnessLabel = refreshState === "recomputed"
    ? "Recomputed on core lane"
    : refreshState === "carried_forward"
      ? "Carried forward from last core diagnostics"
      : data._cached
        ? "Cached"
        : "Freshly Computed";
  return (
    <>
      <div className="chart-card">
        <h3 style={{ marginBottom: 6 }}>Model Health Diagnostics</h3>
        <div className="health-meta-row">
          <span>As of {data.as_of ?? "—"}</span>
          <span>{freshnessLabel}</span>
        </div>
        {refreshState === "carried_forward" && (
          <div className="section-subtitle" style={{ marginBottom: 0 }}>
            Deep diagnostics were reused from the last core rebuild and may lag the newest quick-refresh snapshot.
          </div>
        )}
        {data.notes?.length > 0 && (
          <ul className="health-notes">
            {data.notes.map((n) => <li key={n}>{n}</li>)}
          </ul>
        )}
      </div>

      <SectionRegression data={data} />
      <LazyMountOnVisible minHeight={720} fallback={<div className="chart-card"><div className="detail-history-empty">Scroll to load exposure diagnostics.</div></div>}>
        <SectionExposure data={data} />
      </LazyMountOnVisible>
      <LazyMountOnVisible minHeight={620} fallback={<div className="chart-card"><div className="detail-history-empty">Scroll to load factor return diagnostics.</div></div>}>
        <SectionFactorReturns data={data} />
      </LazyMountOnVisible>
      <LazyMountOnVisible minHeight={560} fallback={<div className="chart-card"><div className="detail-history-empty">Scroll to load covariance diagnostics.</div></div>}>
        <SectionCovarianceQuality data={data} />
      </LazyMountOnVisible>
      <LazyMountOnVisible minHeight={720} fallback={<div className="chart-card"><div className="detail-history-empty">Scroll to load source coverage diagnostics.</div></div>}>
        <SectionCoverage data={data} />
      </LazyMountOnVisible>
    </>
  );
}
