"use client";

import { useEffect, useMemo, useState, type CSSProperties } from "react";
import {
  useCuseRiskPageCovariance,
  useCuseRiskPageExposureMode,
  useCuseRiskPageSnapshot,
} from "@/hooks/useCuse4Api";
import ExposureBarChart from "@/features/cuse4/components/ExposureBarChart";
import FactorDrilldown from "@/features/cuse4/components/FactorDrilldown";
import ExposurePositionsTable from "@/features/cuse4/components/ExposurePositionsTable";
import CovarianceHeatmap from "@/features/cuse4/components/CovarianceHeatmap";
import RiskDecompChart from "@/features/cuse4/components/RiskDecompChart";
import TableRowToggle from "@/components/TableRowToggle";
import HelpLabel from "@/components/HelpLabel";
import ApiErrorState from "@/features/cuse4/components/ApiErrorState";
import LazyMountOnVisible from "@/components/LazyMountOnVisible";
import type { CuseRiskPageExposureModeData, FactorCatalogEntry, FactorDetail } from "@/lib/types/cuse4";
import { exposureTier as exposureMethodTier, normalizeExposureOrigin } from "@/lib/exposureOrigin";
import { factorDisplayName } from "@/lib/factorLabels";
import { buildAnalyticsTruthCompactSummary, summarizeAnalyticsTruth } from "@/lib/cuse4Truth";
import {
  COMBINED_DECOMP_SUBTITLE,
  deriveRawLoadingSharesFromRiskDetails,
  RISK_DECOMP_SECTION_TITLE,
} from "@/lib/riskDecompBars";

const MODES = [
  { key: "raw", label: "Exposure" },
  { key: "sensitivity", label: "Sensitivity" },
  { key: "risk_contribution", label: "Risk Contrib" },
] as const;
type ExposureModeKey = (typeof MODES)[number]["key"];
type SortKey = keyof FactorDetail;
const COLLAPSED_ROWS = 10;
const SNAPSHOT_WARNING_STYLE = {
  marginTop: 12,
  padding: "12px 14px",
  border: "1px solid color-mix(in srgb, var(--negative) 32%, transparent)",
  background: "color-mix(in srgb, var(--negative) 10%, transparent)",
  color: "var(--text-primary)",
  fontSize: 13,
  lineHeight: 1.5,
} satisfies CSSProperties;

const RISK_CATEGORY_TONES = {
  style: "var(--analytics-style)",
  market: "var(--analytics-market)",
  industry: "var(--analytics-industry)",
} as const;

function CuseRiskPageLoadingCards() {
  return (
    <div>
      <div className="chart-card" style={{ marginBottom: 12 }}>
        <h3>{RISK_DECOMP_SECTION_TITLE}</h3>
        <div className="section-subtitle">{COMBINED_DECOMP_SUBTITLE}</div>
        <div className="detail-history-empty loading-pulse">Loading first risk snapshot...</div>
      </div>
      <div className="chart-card" style={{ marginBottom: 12 }}>
        <h3>Risk — Exposure</h3>
        <div className="detail-history-empty loading-pulse">Loading headline factor bars...</div>
      </div>
      <div className="chart-card">
        <h3>Positions (Factor Risk Mix)</h3>
        <div className="detail-history-empty loading-pulse">Loading holdings snapshot...</div>
      </div>
    </div>
  );
}

function CuseRiskPageCovarianceSection({
  factorCatalog,
}: {
  factorCatalog?: FactorCatalogEntry[];
}) {
  const { data, error, isLoading } = useCuseRiskPageCovariance(true);
  const cov = data?.cov_matrix
    ? {
        factors: data.cov_matrix.factors ?? [],
        correlation: data.cov_matrix.correlation ?? data.cov_matrix.matrix ?? [],
      }
    : { factors: [], correlation: [] };

  return (
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
      {error ? (
        <div className="detail-history-empty">Factor correlation heatmap is temporarily unavailable.</div>
      ) : isLoading && !data ? (
        <div className="detail-history-empty loading-pulse">Loading factor correlation heatmap...</div>
      ) : (
        <div className="heatmap-centered-70">
          <CovarianceHeatmap data={cov} factorCatalog={factorCatalog} />
        </div>
      )}
    </div>
  );
}

export default function ExposuresPage() {
  const [mode, setMode] = useState<ExposureModeKey>("raw");
  const [displayMode, setDisplayMode] = useState<ExposureModeKey>("raw");
  const [selectedFactor, setSelectedFactor] = useState<string | null>(null);
  const [riskSortKey, setRiskSortKey] = useState<SortKey>("pct_of_total");
  const [riskSortAsc, setRiskSortAsc] = useState(false);
  const [showAllRiskRows, setShowAllRiskRows] = useState(false);
  const [loadedModes, setLoadedModes] = useState<Record<Exclude<ExposureModeKey, "raw">, boolean>>({
    sensitivity: false,
    risk_contribution: false,
  });
  const { data: snapshot, isLoading, error } = useCuseRiskPageSnapshot();
  const sensitivityMode = useCuseRiskPageExposureMode("sensitivity", loadedModes.sensitivity);
  const riskContributionMode = useCuseRiskPageExposureMode("risk_contribution", loadedModes.risk_contribution);

  useEffect(() => {
    if (mode === "raw") {
      setDisplayMode("raw");
      return;
    }
    setLoadedModes((prev) => (
      prev[mode]
        ? prev
        : { ...prev, [mode]: true }
    ));
  }, [mode]);

  const exposureDataByMode: Partial<Record<ExposureModeKey, CuseRiskPageExposureModeData>> = {
    raw: snapshot?.exposures.raw,
    sensitivity: sensitivityMode.data,
    risk_contribution: riskContributionMode.data,
  };
  useEffect(() => {
    if (mode !== "raw" && exposureDataByMode[mode]) {
      setDisplayMode(mode);
    }
  }, [exposureDataByMode, mode]);

  const renderedMode = mode === "raw" ? "raw" : exposureDataByMode[mode] ? mode : displayMode;
  const data = exposureDataByMode[renderedMode] ?? snapshot?.exposures.raw;
  const portfolioData = snapshot?.portfolio;
  const riskData = snapshot?.risk;
  const isAccountScoped = Boolean(snapshot?._account_scoped || riskData?._account_scoped);
  const positions = portfolioData?.positions ?? [];
  const riskDetails = riskData?.factor_details ?? [];
  const factorCatalog = riskData?.factor_catalog ?? [];
  const riskShares = riskData?.risk_shares ?? { market: 0, industry: 0, style: 0, idio: 100 };
  const hasVolScaledShares = Boolean(
    riskData?.vol_scaled_shares
    && Object.values(riskData.vol_scaled_shares).some((value) => Math.abs(Number(value || 0)) > 1e-12),
  );
  const volScaledShares = hasVolScaledShares ? riskData!.vol_scaled_shares! : riskShares;
  const rawLoadingShares = useMemo(
    () => deriveRawLoadingSharesFromRiskDetails(riskDetails, positions),
    [positions, riskDetails],
  );
  const visibleFactorIds = useMemo(() => {
    const thresholds = {
      raw: 0.04,
      sensitivity: 0.0015,
      risk_contribution: 0.075,
    } as const;
    const visible = new Set<string>();
    (["raw", "sensitivity", "risk_contribution"] as const).forEach((key) => {
      const factors = exposureDataByMode[key]?.factors ?? [];
      const threshold = thresholds[key];
      for (const factor of factors) {
        const net = Math.abs(Number(factor.value || 0));
        const gross = (factor.drilldown ?? []).reduce(
          (sum, item) => sum + Math.abs(Number(item.contribution || 0)),
          0,
        );
        if (net >= threshold || gross >= threshold) {
          visible.add(factor.factor_id);
        }
      }
    });
    if (visible.size > 0) return Array.from(visible);

    return riskDetails
      .filter((detail) => (
        Math.abs(Number(detail.exposure || 0)) >= thresholds.raw
        || Math.abs(Number(detail.sensitivity || 0)) >= thresholds.sensitivity
        || Math.abs(Number(detail.pct_of_total || 0)) >= thresholds.risk_contribution
      ))
      .map((detail) => detail.factor_id);
  }, [exposureDataByMode, riskDetails]);
  const pendingModeLabel = mode !== renderedMode ? MODES.find((entry) => entry.key === mode)?.label ?? null : null;
  const loadingMode = (
    mode === "sensitivity"
      ? sensitivityMode
      : mode === "risk_contribution"
        ? riskContributionMode
        : null
  );
  const modeLoadError = mode !== "raw" ? loadingMode?.error : null;
  const isModeTransitioning = !modeLoadError && (mode !== renderedMode || Boolean(
    mode !== "raw" && loadingMode?.isValidating && !exposureDataByMode[mode],
  ));

  const chartFactors = useMemo(() => {
    const originByTicker = new Map(
      positions.map((pos) => [
        String(pos.ticker || "").toUpperCase(),
        {
          model_status: pos.model_status,
          exposure_origin: normalizeExposureOrigin(pos.exposure_origin, pos.model_status),
        },
      ]),
    );
    return (data?.factors ?? []).map((factor) => ({
      ...factor,
      drilldown: (factor.drilldown ?? []).map((item) => {
        const meta = originByTicker.get(String(item.ticker || "").toUpperCase());
        return meta
          ? {
              ...item,
              model_status: meta.model_status,
              exposure_origin: meta.exposure_origin,
            }
          : item;
      }),
    }));
  }, [data?.factors, positions]);
  // Extract cross-section summary from the factor data
  const crossSection = useMemo(() => {
    const ns = chartFactors
      .map((f) => Number(f.cross_section_n || 0))
      .filter((n) => n > 0);
    if (ns.length === 0) return null;
    const min = Math.min(...ns);
    const max = Math.max(...ns);
    const date = chartFactors.find((f) => f.factor_coverage_asof || f.coverage_date)?.factor_coverage_asof
      ?? chartFactors.find((f) => f.factor_coverage_asof || f.coverage_date)?.coverage_date
      ?? null;
    return { min, max, date };
  }, [chartFactors]);
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
  const hasProjectedExtensions = useMemo(
    () =>
      chartFactors.some((factor) =>
        factor.drilldown.some(
          (item) => exposureMethodTier(item.exposure_origin, item.model_status) !== "core",
        ),
      ),
    [chartFactors],
  );

  if (isLoading && !snapshot) {
    return <CuseRiskPageLoadingCards />;
  }
  if (error) {
    return <ApiErrorState title="Risk Data Not Ready" error={error} />;
  }
  if (snapshotMismatch) {
    return (
      <div>
        <div className="chart-card">
          <h3 style={{ marginTop: 0 }}>Risk Snapshot In Flight</h3>
          <div style={SNAPSHOT_WARNING_STYLE}>
            Portfolio, risk, and exposures are spanning multiple published snapshots right now ({truth.snapshotIds.join(" / ")}).
            This page withholds analytics until RECALC finishes or the page reloads into one coherent publish.
          </div>
        </div>
      </div>
    );
  }

  const selected = selectedFactor
    ? chartFactors.find((f) => f.factor_id === selectedFactor)
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
      <div className="chart-card" style={{ marginBottom: 12 }}>
        <h3>{RISK_DECOMP_SECTION_TITLE}</h3>
        <div className="section-subtitle">
          {COMBINED_DECOMP_SUBTITLE}
        </div>
        <RiskDecompChart
          rows={[
            { label: "Raw Loadings", shares: rawLoadingShares },
            { label: "Vol-Scaled", shares: volScaledShares },
          ]}
        />
      </div>

      <div className="chart-card">
        <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 10 }}>
          <h3 style={{ margin: 0 }}>
            Risk — {MODES.find((m) => m.key === renderedMode)?.label}
          </h3>
          {crossSection && (
            <span style={{
              fontSize: 10,
              letterSpacing: "0.04em",
              color: "var(--text-muted)",
              fontVariantNumeric: "tabular-nums",
            }}>
              {compactTruthSummary}
            </span>
          )}
        </div>
        {pendingModeLabel && !modeLoadError && (
          <div className="section-subtitle">
            Loading {pendingModeLabel} bars; keeping {MODES.find((entry) => entry.key === renderedMode)?.label} visible.
          </div>
        )}
        {modeLoadError && (
          <div className="detail-history-empty" style={{ marginBottom: 10 }}>
            Unable to load {MODES.find((entry) => entry.key === mode)?.label ?? "selected"} right now. Keeping{" "}
            {MODES.find((entry) => entry.key === renderedMode)?.label ?? "current"} visible.
          </div>
        )}
        {isAccountScoped && (
          <div className="section-subtitle">
            Scoped previews load deeper modes on demand from the current account snapshot.
          </div>
        )}
        {hasProjectedExtensions && (
          <div
            style={{
              marginBottom: 10,
              fontSize: 12,
              color: "var(--text-secondary)",
            }}
          >
            Non-core layers extend the base bars: Fundamental Projection first, Returns Projection outermost.
          </div>
        )}
        <ExposureBarChart
          factors={chartFactors}
          mode={renderedMode as "raw" | "sensitivity" | "risk_contribution"}
          factorCatalog={factorCatalog}
          visibleFactorIds={visibleFactorIds}
          onBarClick={(f) => setSelectedFactor(f === selectedFactor ? null : f)}
        />
        {isModeTransitioning && (
          <div className="detail-history-empty" style={{ marginTop: 10 }}>
            Loading {pendingModeLabel ?? MODES.find((entry) => entry.key === mode)?.label ?? "selected"} view…
          </div>
        )}
      </div>

      {selected && (
        <FactorDrilldown
          factorId={selected.factor_id}
          factorName={factorDisplayName(selected.factor_id, factorCatalog)}
          items={selected.drilldown}
          mode={renderedMode}
          factorVol={selected.factor_vol}
          factorCatalog={factorCatalog}
          onClose={() => setSelectedFactor(null)}
        />
      )}

      <div className="chart-card" style={{ marginTop: 12 }}>
        <h3>Positions (Factor Risk Mix)</h3>
        <ExposurePositionsTable positions={positions} />
      </div>

      <div className="chart-card mb-4" style={{ marginTop: 12 }}>
        <h3>Variance Attribution</h3>
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
                    <span
                      className="text-xs"
                      style={{
                        color: RISK_CATEGORY_TONES[
                          d.category as keyof typeof RISK_CATEGORY_TONES
                        ] ?? "var(--text-secondary)",
                      }}
                    >
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
      </div>

      {isAccountScoped ? (
        <div className="chart-card">
          <h3>Factor Correlation Heatmap</h3>
          <div className="detail-history-empty">
            Scoped risk pages do not load the shared factor correlation heatmap yet.
          </div>
        </div>
      ) : (
        <LazyMountOnVisible
          minHeight={320}
          fallback={<div className="detail-history-empty">Scroll to load the factor correlation heatmap.</div>}
        >
          <CuseRiskPageCovarianceSection factorCatalog={factorCatalog} />
        </LazyMountOnVisible>
      )}

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
