"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { useExposures, usePortfolio, useRisk } from "@/hooks/useApi";
import ExposureBarChart from "@/components/ExposureBarChart";
import FactorDrilldown from "@/components/FactorDrilldown";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ExposurePositionsTable from "@/components/ExposurePositionsTable";
import CovarianceHeatmap from "@/components/CovarianceHeatmap";
import RiskDecompChart from "@/components/RiskDecompChart";
import TableRowToggle from "@/components/TableRowToggle";
import HelpLabel from "@/components/HelpLabel";
import ApiErrorState from "@/components/ApiErrorState";
import LazyMountOnVisible from "@/components/LazyMountOnVisible";
import type { FactorDetail } from "@/lib/types";
import { factorDisplayName } from "@/lib/factorLabels";
import { buildAnalyticsTruthCompactSummary, summarizeAnalyticsTruth } from "@/lib/analyticsTruth";

const MODES = [
  { key: "raw", label: "Exposure" },
  { key: "sensitivity", label: "Sensitivity" },
  { key: "risk_contribution", label: "Risk Contrib" },
] as const;
type SortKey = keyof FactorDetail;
const COLLAPSED_ROWS = 10;

export default function ExposuresPage() {
  const [mode, setMode] = useState<string>("raw");
  const [selectedFactor, setSelectedFactor] = useState<string | null>(null);
  const [riskSortKey, setRiskSortKey] = useState<SortKey>("pct_of_total");
  const [riskSortAsc, setRiskSortAsc] = useState(false);
  const [showAllRiskRows, setShowAllRiskRows] = useState(false);
  const { data, isLoading, error } = useExposures(mode);
  const { data: portfolioData, isLoading: portfolioLoading, error: portfolioError } = usePortfolio();
  const { data: riskData, isLoading: riskLoading, error: riskError } = useRisk();
  const factors = data?.factors ?? [];
  const positions = portfolioData?.positions ?? [];
  const riskDetails = riskData?.factor_details ?? [];
  const factorCatalog = riskData?.factor_catalog ?? [];
  const riskShares = riskData?.risk_shares ?? { market: 0, industry: 0, style: 0, idio: 100 };
  const cov = riskData?.cov_matrix
    ? {
        factors: riskData.cov_matrix.factors ?? [],
        correlation: riskData.cov_matrix.correlation ?? riskData.cov_matrix.matrix ?? [],
      }
    : { factors: [], correlation: [] };

  // Extract cross-section summary from the factor data
  const crossSection = useMemo(() => {
    const ns = factors
      .map((f) => Number(f.cross_section_n || 0))
      .filter((n) => n > 0);
    if (ns.length === 0) return null;
    const min = Math.min(...ns);
    const max = Math.max(...ns);
    const date = factors.find((f) => f.coverage_date)?.coverage_date ?? null;
    return { min, max, date };
  }, [factors]);
  const truth = useMemo(
    () => summarizeAnalyticsTruth({ portfolio: portfolioData, risk: riskData, exposures: data }),
    [data, portfolioData, riskData],
  );
  const compactTruthSummary = useMemo(() => {
    const prefix = crossSection
      ? (
          crossSection.min === crossSection.max
            ? `N = ${crossSection.min.toLocaleString()}`
            : `N = ${crossSection.min.toLocaleString()}–${crossSection.max.toLocaleString()}`
        )
      : null;
    return buildAnalyticsTruthCompactSummary(truth, { prefix });
  }, [crossSection, truth]);
  const snapshotMismatch = !truth.snapshotsCoherent && truth.snapshotIds.length > 1;

  if (isLoading) {
    return <AnalyticsLoadingViz message="Loading exposures..." />;
  }
  if (error || portfolioError || riskError) {
    return <ApiErrorState title="Risk Data Not Ready" error={error || portfolioError || riskError} />;
  }
  if (snapshotMismatch) {
    return (
      <div>
        <div className="chart-card">
          <h3 style={{ marginTop: 0 }}>Risk Snapshot In Flight</h3>
          <div
            style={{
              marginTop: 12,
              padding: "12px 14px",
              border: "1px solid rgba(204, 53, 88, 0.25)",
              background: "rgba(204, 53, 88, 0.08)",
              color: "rgba(248, 221, 228, 0.92)",
              fontSize: 13,
              lineHeight: 1.5,
            }}
          >
            Portfolio, risk, and exposures are spanning multiple published snapshots right now ({truth.snapshotIds.join(" / ")}).
            This page withholds analytics until RECALC finishes or the page reloads into one coherent publish.
          </div>
        </div>
      </div>
    );
  }

  const selected = selectedFactor
    ? factors.find((f) => f.factor_id === selectedFactor)
    : null;
  const sortedRiskRows = [...riskDetails].sort((a, b) => {
    const av = a[riskSortKey];
    const bv = b[riskSortKey];
    if (riskSortKey === "factor_id") {
      const aLabel = factorDisplayName(a.factor_id, factorCatalog);
      const bLabel = factorDisplayName(b.factor_id, factorCatalog);
      return riskSortAsc
        ? aLabel.localeCompare(bLabel)
        : bLabel.localeCompare(aLabel);
    }
    if (typeof av === "number" && typeof bv === "number") {
      return riskSortAsc ? av - bv : bv - av;
    }
    return riskSortAsc
      ? String(av).localeCompare(String(bv))
      : String(bv).localeCompare(String(av));
  });
  const visibleRiskRows = showAllRiskRows ? sortedRiskRows : sortedRiskRows.slice(0, COLLAPSED_ROWS);
  const riskArrow = (key: SortKey) => (riskSortKey === key ? (riskSortAsc ? " ↑" : " ↓") : "");
  const handleRiskSort = (key: SortKey) => {
    if (key === riskSortKey) setRiskSortAsc((prev) => !prev);
    else {
      setRiskSortKey(key);
      setRiskSortAsc(false);
    }
  };

  return (
    <div>
      <div className="chart-card">
        <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 10 }}>
          <h3 style={{ margin: 0 }}>
            Factor Exposures — {MODES.find((m) => m.key === mode)?.label}
          </h3>
          {crossSection && (
            <span style={{
              fontSize: 10,
              letterSpacing: "0.04em",
              color: "rgba(169, 182, 210, 0.5)",
              fontVariantNumeric: "tabular-nums",
            }}>
              {compactTruthSummary}
            </span>
          )}
        </div>
        <ExposureBarChart
          factors={factors}
          mode={mode as "raw" | "sensitivity" | "risk_contribution"}
          factorCatalog={factorCatalog}
          onBarClick={(f) => setSelectedFactor(f === selectedFactor ? null : f)}
        />
      </div>

      {selected && (
        <FactorDrilldown
          factorId={selected.factor_id}
          factorName={factorDisplayName(selected.factor_id, factorCatalog)}
          items={selected.drilldown}
          mode={mode}
          factorVol={selected.factor_vol}
          factorCatalog={factorCatalog}
          onClose={() => setSelectedFactor(null)}
        />
      )}

      <div className="chart-card" style={{ marginTop: 12 }}>
        <h3>Positions (Barra Risk Mix)</h3>
        {portfolioLoading ? (
          <div className="detail-history-empty loading-pulse">Loading positions...</div>
        ) : (
          <ExposurePositionsTable positions={positions} />
        )}
      </div>

      <div className="chart-card mb-4" style={{ marginTop: 12 }}>
        <h3>Variance Attribution</h3>
        {riskLoading ? (
          <AnalyticsLoadingViz message="Loading risk data..." />
        ) : (
          <div className="dash-table">
            <table>
              <thead>
                <tr>
                  <th onClick={() => handleRiskSort("factor_id")}>
                    <span className="col-help-wrap">
                      <HelpLabel
                        label="Factor"
                        plain="The named risk driver in your model."
                        math="Each row is one factor f"
                        interpret={{
                          lookFor: "Which factors dominate your table.",
                          good: "Risk is not unintentionally concentrated in one factor.",
                        }}
                      />
                      {riskArrow("factor_id")}
                    </span>
                  </th>
                  <th onClick={() => handleRiskSort("category")}>
                    <span className="col-help-wrap">
                      <HelpLabel
                        label="Category"
                        plain="Whether the factor is market, industry, or style based."
                        math="Category ∈ {market, industry, style}"
                        interpret={{
                          lookFor: "If one category overwhelmingly dominates.",
                          good: "Mix aligns with your intended portfolio construction.",
                        }}
                      />
                      {riskArrow("category")}
                    </span>
                  </th>
                  <th className="text-right" onClick={() => handleRiskSort("exposure")}>
                    <span className="col-help-wrap">
                      <HelpLabel
                        label="Exposure"
                        plain="Portfolio loading on that factor before volatility scaling."
                        math="h_f = Σ (w_i × x_i,f)"
                        interpret={{
                          lookFor: "Large absolute exposures and sign concentration.",
                          good: "Exposures are intentional and not accidental bets.",
                        }}
                      />
                      {riskArrow("exposure")}
                    </span>
                  </th>
                  <th className="text-right" onClick={() => handleRiskSort("factor_vol")}>
                    <span className="col-help-wrap">
                      <HelpLabel
                        label="Factor Vol"
                        plain="Annualized volatility of that factor’s return."
                        math="σ_f = sqrt(F_f,f)"
                        interpret={{
                          lookFor: "High-vol factors paired with high exposure.",
                          good: "Highest vol factors are controlled unless intentionally targeted.",
                        }}
                      />
                      {riskArrow("factor_vol")}
                    </span>
                  </th>
                  <th className="text-right" onClick={() => handleRiskSort("sensitivity")}>
                    <span className="col-help-wrap">
                      <HelpLabel
                        label="Sensitivity"
                        plain="Exposure scaled by factor volatility."
                        math="Sensitivity_f = h_f × σ_f"
                        interpret={{
                          lookFor: "Large signed values; this is first-pass risk direction.",
                          good: "Top sensitivities match your intended factor bets/hedges.",
                        }}
                      />
                      {riskArrow("sensitivity")}
                    </span>
                  </th>
                  <th className="text-right" onClick={() => handleRiskSort("marginal_var_contrib")}>
                    <span className="col-help-wrap">
                      <HelpLabel
                        label="Marg. Var"
                        plain="Raw contribution of this factor to portfolio variance, including covariance effects."
                        math="MVC_f = h_f × (Fh)_f"
                        interpret={{
                          lookFor: "Very large positives and unexpected negatives.",
                          good: "Signs and magnitude are consistent with your covariance structure.",
                          distribution: "Can be negative for hedging factors due to correlations.",
                        }}
                      />
                      {riskArrow("marginal_var_contrib")}
                    </span>
                  </th>
                  <th className="text-right" onClick={() => handleRiskSort("pct_of_total")}>
                    <span className="col-help-wrap">
                      <HelpLabel
                        label="% Total"
                        plain="Share of total portfolio variance attributed to this factor."
                        math="%_f = MVC_f / total variance"
                        interpret={{
                          lookFor: "Top contributors and whether negatives are true hedges.",
                          good: "No unintended single-factor dominance unless by design.",
                          distribution: "A balanced spread usually indicates better diversification.",
                        }}
                      />
                      {riskArrow("pct_of_total")}
                    </span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {visibleRiskRows.map((d) => (
                  <tr key={d.factor_id}>
                    <td><strong>{factorDisplayName(d.factor_id, factorCatalog)}</strong></td>
                    <td>
                      <span className={`text-xs ${
                        d.category === "style"
                          ? "text-[#f5bae4]"
                          : d.category === "market"
                            ? "text-[#58b6c7]"
                            : "text-[#cc3558]"
                      }`}>
                        {d.category}
                      </span>
                    </td>
                    <td className="text-right">{d.exposure.toFixed(4)}</td>
                    <td className="text-right">{(d.factor_vol * 100).toFixed(2)}%</td>
                    <td className="text-right">{d.sensitivity.toFixed(4)}</td>
                    <td className="text-right">{d.marginal_var_contrib.toFixed(6)}</td>
                    <td className="text-right">
                      <span className={d.pct_of_total >= 0 ? "positive" : "negative"}>
                        {d.pct_of_total.toFixed(2)}%
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <TableRowToggle
              totalRows={sortedRiskRows.length}
              collapsedRows={COLLAPSED_ROWS}
              expanded={showAllRiskRows}
              onToggle={() => setShowAllRiskRows((prev) => !prev)}
              label="factors"
            />
          </div>
        )}
      </div>

      <div className="chart-card mb-4" style={{ marginTop: 12 }}>
        <h3>Risk Decomposition</h3>
        <div className="section-subtitle">
          Share of total portfolio risk split across market, industry, style, and idiosyncratic components.
        </div>
        {riskLoading ? (
          <AnalyticsLoadingViz message="Loading portfolio risk mix..." />
        ) : (
          <RiskDecompChart shares={riskShares} />
        )}
      </div>

      <div className="chart-card">
        <h3>
          <HelpLabel
            label="Factor Correlation Heatmap"
            plain="Shows how factor returns move together."
            math="corr(factor_return_i, factor_return_j)"
            interpret={{
              lookFor: "Large blocks of very high positive or negative correlation.",
              good: "Mostly moderate correlations with intuitive clusters.",
              distribution: "A broad spread around 0 usually means better diversification potential.",
            }}
          />
        </h3>
        <LazyMountOnVisible
          minHeight={320}
          fallback={<div className="detail-history-empty">Scroll to load the factor correlation heatmap.</div>}
        >
          <div className="heatmap-centered-70">
            <CovarianceHeatmap data={cov} factorCatalog={factorCatalog} />
          </div>
        </LazyMountOnVisible>
      </div>

      <div className="floating-mode-toggle">
        {MODES.map((m) => (
          <button
            key={m.key}
            className={mode === m.key ? "active" : ""}
            onClick={() => {
              setMode(m.key);
              setSelectedFactor(null);
            }}
          >
            {m.label}
          </button>
        ))}
      </div>
    </div>
  );
}
