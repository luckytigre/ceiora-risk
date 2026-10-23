"use client";

import { Suspense, useMemo, type ReactNode } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import HelpLabel from "@/components/HelpLabel";
import ExposureBarChart from "@/features/cuse4/components/ExposureBarChart";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import RequestErrorState from "@/components/shared/RequestErrorState";
import DashThesisPositionsTable from "@/features/dash/components/DashThesisPositionsTable";
import CparRiskFactorLoadingsChart from "@/features/cpar/components/CparRiskFactorLoadingsChart";
import { useCuseRiskPageSnapshot } from "@/hooks/useCuse4Api";
import { useCparRisk } from "@/hooks/useCparApi";
import { useHoldingsPositions } from "@/hooks/useHoldingsApi";
import { buildDashPositions, type DashPosition } from "@/features/dash/dashPositions";

function DashPanels({ positions, chart, context }: { positions: DashPosition[]; chart: ReactNode; context: string }) {
  return <>
    <p className="dash-model-context" role="status">{context}</p>
    <p className="dash-model-context">Holdings reflect the current ledger; model analytics reflect the last served snapshot. Risk mix is shown only for matching account-level quantities; — means unavailable or awaiting refresh.</p>
    <div className="dash-overview-grid">
      <section className="chart-card dash-overview-panel dash-overview-positions" data-testid="dash-thesis-panel">
        <DashThesisPositionsTable positions={positions} />
      </section>
      <section className="chart-card dash-overview-panel dash-overview-exposure" data-testid="dash-exposure-panel">
        <div className="module-title-with-help">
          <h3>Risk Exposure — Long / Short</h3>
          <HelpLabel label="Long and short factor exposure" plain="Signed factor exposure for the selected model. cPAR uses its explanatory display-factor basis; table risk mixes retain the model’s separate position-risk basis." iconOnly />
        </div>
        {chart}
      </section>
    </div>
  </>;
}

function CuseDash() {
  const { data: snapshot, isLoading, error } = useCuseRiskPageSnapshot();
  const holdings = useHoldingsPositions(null);
  const factors = snapshot?.exposures?.raw?.factors ?? [];
  const visibleFactorIds = useMemo(() => [...factors]
    .sort((left, right) => {
      const gross = (factor: (typeof factors)[number]) => factor.drilldown.reduce((sum, row) => sum + Math.abs(Number(row.contribution || 0)), 0);
      return Math.max(Math.abs(right.value), gross(right)) - Math.max(Math.abs(left.value), gross(left));
    }).slice(0, 15).map((factor) => factor.factor_id), [factors]);
  if (error || holdings.error) return <RequestErrorState surface="cUSE DASH" error={error || holdings.error} />;
  if ((isLoading && !snapshot) || !holdings.data) return <AnalyticsLoadingViz message="Loading cUSE DASH…" />;
  if (!snapshot) return <p className="detail-history-empty">No cUSE snapshot is available.</p>;
  return <DashPanels
    positions={buildDashPositions(holdings.data.positions, snapshot.portfolio?.positions ?? [])}
    context={`cUSE · Exposures as of ${snapshot.portfolio?.source_dates?.exposures_served_asof ?? snapshot.portfolio?.source_dates?.exposures_asof ?? "unavailable"}`}
    chart={factors.length ? <ExposureBarChart factors={factors} factorCatalog={snapshot.risk?.factor_catalog ?? []} visibleFactorIds={visibleFactorIds} mode="raw" viewportHeight="var(--dash-module-body-height)" minimumRowHeight={18} /> : <p className="detail-history-empty">No cUSE factor exposures are available.</p>}
  />;
}

function CparDash() {
  const { data: risk, isLoading, error } = useCparRisk();
  const holdings = useHoldingsPositions(null);
  if (error || holdings.error) return <RequestErrorState surface="cPAR DASH" error={error || holdings.error} />;
  if ((isLoading && !risk) || !holdings.data) return <AnalyticsLoadingViz message="Loading cPAR DASH…" />;
  if (!risk) return <p className="detail-history-empty">No cPAR snapshot is available.</p>;
  const rows = [...risk.display_factor_chart].sort((a, b) => a.display_order - b.display_order);
  return <DashPanels
    positions={buildDashPositions(holdings.data.positions, risk.positions)}
    context={`cPAR · Package ${risk.package_date ?? "unavailable"} · ${risk.covered_positions_count}/${risk.positions_count} covered · ${risk.portfolio_status}${risk.portfolio_reason ? ` · ${risk.portfolio_reason}` : ""}`}
    chart={rows.length ? <div className="dash-cpar-exposure-scroll"><CparRiskFactorLoadingsChart rows={rows} mode="raw" selectedFactorId={null} onSelectFactor={() => {}} /></div> : <p className="detail-history-empty">No covered holdings contributed cPAR display exposures.</p>}
  />;
}

function DashWorkspace() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const pathname = usePathname();
  const model = searchParams.get("model") === "cpar" ? "cpar" : "cuse";
  return <div className="dash-overview" data-testid="dash-workspace" data-model={model}>
    <div className="dash-model-selector settings-segmented-control" role="group" aria-label="DASH model">
      {(["cuse", "cpar"] as const).map((family) => <button key={family} type="button" className={`settings-segmented-option${model === family ? " active" : ""}`} aria-pressed={model === family} onClick={() => {
        const next = new URLSearchParams(searchParams.toString());
        next.set("model", family);
        router.push(`${pathname}?${next.toString()}`, { scroll: false });
      }}>{family === "cuse" ? "cUSE" : "cPAR"}</button>)}
    </div>
    {model === "cpar" ? <CparDash /> : <CuseDash />}
  </div>;
}

export default function DashPage() {
  return <Suspense fallback={<AnalyticsLoadingViz message="Loading DASH…" />}><DashWorkspace /></Suspense>;
}
