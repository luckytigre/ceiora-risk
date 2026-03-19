"use client";

import { useState } from "react";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ApiErrorState from "@/features/cuse4/components/ApiErrorState";
import { useDataDiagnostics } from "@/hooks/useCuse4Api";
import type { DataTableStats } from "@/lib/types/cuse4";

function fmtInt(n?: number | null): string {
  if (typeof n !== "number" || !Number.isFinite(n)) return "—";
  return n.toLocaleString("en-US");
}

function fmtTs(s?: string | null): string {
  if (!s) return "—";
  const iso = s.includes("T") ? s : `${s}Z`;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return s;
  return d.toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

const DATASET_DESC: Record<string, string> = {
  "Security Master": "The universe definition — every security the model knows about, with RICs, tickers, and eligibility flags.",
  "Fundamentals PIT": "Point-in-time fundamental data (market cap, earnings, etc.) used for style factor exposures.",
  "Classification PIT": "Point-in-time TRBC industry classifications for building the industry factor structure.",
  "Prices EOD": "End-of-day prices and volumes used for return computation and momentum/volatility factors.",
  "ESTU Membership Daily": "Daily estimation universe membership — which securities are eligible for cross-sectional regression.",
  "Raw Cross-Section History": "The core model input: daily cross-sectional snapshots of exposures, returns, and weights.",
  "Cross-Section Snapshot": "Latest-only snapshot of the cross-section, used as a performance cache for serving.",
};

const CACHE_DESC: Record<string, string> = {
  portfolio: "Current portfolio positions, weights, and exposure projections used across Risk, Explore, and Positions.",
  exposures: "Factor exposure matrix for all held positions, served to the Risk page.",
  risk: "Risk decomposition, factor contributions, and covariance-derived metrics for the Risk page.",
  health_diagnostics: "Precomputed health page payload — R², coverage, bias stats, and factor diagnostics.",
  daily_universe_eligibility_summary: "Legacy cache-era eligibility summary table kept for compatibility. Current summary metadata should come from durable serving payloads.",
  daily_factor_returns: "Legacy cache-era factor-return history kept for compatibility. Current factor coverage metadata should come from durable model outputs.",
  risk_engine_meta: "Risk engine configuration — method version, covariance parameters, history window.",
  cuse4_foundation: "Core cUSE4 model foundation — factor definitions, hierarchy, and estimation parameters.",
};

export default function DataPage() {
  const [deepMode, setDeepMode] = useState(false);
  const { data, isLoading, error } = useDataDiagnostics({
    includeExpensiveChecks: deepMode,
    includeExactRowCounts: deepMode,
  });

  if (isLoading) {
    return <AnalyticsLoadingViz message="Loading data diagnostics..." />;
  }
  if (error) {
    return <ApiErrorState title="Data Diagnostics Unavailable" error={error} />;
  }

  const src = data?.source_tables;
  const elig = data?.cross_section_usage?.eligibility_summary;
  const cross = data?.cross_section_usage?.factor_cross_section;
  const cacheRows = data?.cache_outputs ?? [];
  const truth = data?.truth_surfaces;
  const dupes = data?.exposure_duplicates?.active_exposure_source;

  const refreshRows: Array<{ label: string; table: DataTableStats | null | undefined }> = [
    { label: "Security Master", table: src?.security_master },
    { label: "Fundamentals PIT", table: src?.security_fundamentals_pit },
    { label: "Classification PIT", table: src?.security_classification_pit },
    { label: "Prices EOD", table: src?.security_prices_eod },
    { label: "ESTU Membership Daily", table: src?.estu_membership_daily },
    { label: "Raw Cross-Section History", table: src?.barra_raw_cross_section_history },
    { label: "Cross-Section Snapshot", table: src?.universe_cross_section_snapshot },
  ];

  const dupesClean = dupes?.computed
    ? (dupes.duplicate_groups === 0 && dupes.duplicate_extra_rows === 0)
    : null;

  return (
    <div>
      <div className="section-subtitle" style={{ marginBottom: 14 }}>
        Health is the live operator/control-room surface. Data is the maintenance surface for source tables, lineage, coverage, and cache diagnostics.
      </div>
      {/* ── Pipeline Overview ── */}
      <div className="chart-card data-section">
        <div className="data-section-header">
          <h3>Data Pipeline Overview</h3>
          <label className={`toggle-switch${deepMode ? " active" : ""}`} onClick={() => setDeepMode((v) => !v)}>
            <span className="toggle-switch-track" />
            {deepMode ? "Deep diagnostics" : "Fast diagnostics"}
          </label>
        </div>
        <div className="data-section-desc">
          {deepMode
            ? "Deep mode computes exact row counts, distinct ticker counts, duplicate checks, and last-update timestamps. This queries every source table."
            : "Fast mode uses table statistics for approximate counts and skips expensive queries. Toggle deep mode for exact numbers."}
        </div>

        <div className="data-kpi-grid">
          <div className="data-kpi">
            <div className="data-kpi-label">Universe Size</div>
            <div className="data-kpi-value">{fmtInt(elig?.max_structural_eligible_n)}</div>
            <div className="data-kpi-desc">
              Peak number of securities passing structural eligibility — market cap, price, and listing filters that define the investable universe.
            </div>
          </div>
          <div className="data-kpi">
            <div className="data-kpi-label">US Core Universe</div>
            <div className="data-kpi-value">{fmtInt(elig?.latest?.core_structural_eligible_n)}</div>
            <div className="data-kpi-desc">
              Structurally eligible US names in the live core model. This is the maximum pool that can define factor returns.
            </div>
          </div>
          <div className="data-kpi">
            <div className="data-kpi-label">Regression Members</div>
            <div className="data-kpi-value">{fmtInt(elig?.latest?.regression_member_n)}</div>
            <div className="data-kpi-desc">
              Securities actually used in the latest cross-sectional regression. This is the effective estimation sample size.
            </div>
          </div>
          <div className="data-kpi">
            <div className="data-kpi-label">Projected-Only</div>
            <div className="data-kpi-value">{fmtInt(elig?.latest?.projected_only_n)}</div>
            <div className="data-kpi-desc">
              Names carried through the model for portfolio analytics but excluded from core factor-return estimation.
            </div>
          </div>
          <div className="data-kpi">
            <div className="data-kpi-label">Data Integrity</div>
            <div className="data-kpi-value small">
              {dupes?.computed
                ? dupesClean
                  ? <span className="data-integrity-status clean">Clean</span>
                  : <span className="data-integrity-status dirty">{fmtInt(dupes.duplicate_groups)} dupes</span>
                : <span className="data-mode-badge">fast</span>}
            </div>
            <div className="data-kpi-desc">
              Duplicate (ticker, date) pairs in the active exposure source. Duplicates can bias regression estimates. Enable deep mode to check.
            </div>
          </div>
        </div>
      </div>

      {/* ── Truth Surfaces ── */}
      <div className="chart-card data-section">
        <h3>Truth Surfaces</h3>
        <div className="data-section-desc">
          The system has three distinct sources of truth. Each page and panel reads from its own surface so that user-facing dashboards stay stable even while backend maintenance is running.
        </div>
        <div className="data-truth-grid">
          <div className="data-truth-card">
            <h4>Dashboard Serving</h4>
            <div className="data-truth-source">{truth?.dashboard_serving?.source || "—"}</div>
            <div className="data-truth-desc">
              {truth?.dashboard_serving?.plain_english || "Risk, Explore, Positions, and Health summaries read from durable serving payloads built by refresh and then published for dashboard use."}
            </div>
          </div>
          <div className="data-truth-card">
            <h4>Operator Status</h4>
            <div className="data-truth-source">{truth?.operator_status?.source || "—"}</div>
            <div className="data-truth-desc">
              {truth?.operator_status?.plain_english || "The Health page reads live runtime state: lane statuses, holdings sync, Neon parity, and source recency. This is the control-room view."}
            </div>
          </div>
          <div className="data-truth-card">
            <h4>Local Diagnostics</h4>
            <div className="data-truth-source">{truth?.local_diagnostics?.source || "—"}</div>
            <div className="data-truth-desc">
              {truth?.local_diagnostics?.plain_english || "This Data page inspects local source tables and cache state directly. Treat it as a diagnostics surface, not the live operator control room."}
            </div>
          </div>
        </div>
        <div className="section-subtitle" style={{ marginBottom: 0 }}>
          {data?.diagnostic_scope?.plain_english || "Diagnostics reflect the current backend instance."}
        </div>
      </div>

      {/* ── Source Table Health ── */}
      <div className="chart-card data-section">
        <h3>Source Table Health</h3>
        <div className="data-section-desc">
          These are the seven canonical source tables that feed the model. Each row shows size, date coverage, and freshness. Gaps or stale dates here propagate into factor returns and risk outputs.
        </div>
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th>Dataset</th>
                <th className="text-right">Rows</th>
                <th className="text-right">Tickers</th>
                <th>Date Range</th>
                <th>Last Updated</th>
                <th>Last Job</th>
              </tr>
            </thead>
            <tbody>
              {refreshRows.map(({ label, table }) => (
                <tr key={label}>
                  <td>
                    {label}
                    <div className="data-table-dataset-desc">{DATASET_DESC[label]}</div>
                  </td>
                  <td className="text-right">
                    {fmtInt(table?.row_count)}
                    {table?.row_count_mode === "approx" ? <span className="data-mode-badge" style={{ marginLeft: 4 }}>approx</span> : ""}
                  </td>
                  <td className="text-right">
                    {typeof table?.ticker_count === "number" ? fmtInt(table?.ticker_count) : deepMode ? "—" : <span className="data-mode-badge">fast</span>}
                  </td>
                  <td>{table?.min_date && table?.max_date ? `${table.min_date} → ${table.max_date}` : "—"}</td>
                  <td>{table?.last_updated_at ? fmtTs(table?.last_updated_at) : deepMode ? "—" : <span className="data-mode-badge">fast</span>}</td>
                  <td>{table?.last_job_run_id || (deepMode ? "—" : <span className="data-mode-badge">fast</span>)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── Universe & Regression Coverage ── */}
      <div className="chart-card data-section">
        <h3>Universe & Regression Coverage</h3>
        <div className="data-section-desc">
          The model filters the raw universe through two eligibility gates. <strong>Structural eligibility</strong> applies hard filters (market cap, price, listing status) to define the investable universe. <strong>Regression membership</strong> further screens for data completeness — only securities with enough exposure and return data enter the cross-sectional regression.
        </div>
        <div className="data-metric-grid">
          <div className="data-metric-card">
            <h4>Structural Eligible Universe</h4>
            <div className="data-metric-desc">
              Securities passing market cap, price, and listing filters. This defines the broadest set of names the model considers investable.
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Historical min</span>
              <span className="data-metric-value">{fmtInt(elig?.min_structural_eligible_n)}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Historical max</span>
              <span className="data-metric-value">{fmtInt(elig?.max_structural_eligible_n)}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Latest</span>
              <span className="data-metric-value">
                {elig?.latest
                  ? `${fmtInt(elig.latest.structural_eligible_n)} (${elig.latest.date})`
                  : "—"}
              </span>
            </div>
            {elig?.latest?.exp_date && (
              <div className="data-metric-row">
                <span className="data-metric-label">Exposure date</span>
                <span className="data-metric-value">{elig.latest.exp_date}</span>
              </div>
            )}
          </div>

          <div className="data-metric-card">
            <h4>US Core Estimation Universe</h4>
            <div className="data-metric-desc">
              The live model estimates factor returns on a US-core universe. These counts separate the full US structural pool from the subset that actually made it into the regression.
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Historical min</span>
              <span className="data-metric-value">{fmtInt(elig?.min_core_structural_eligible_n)}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Historical max</span>
              <span className="data-metric-value">{fmtInt(elig?.max_core_structural_eligible_n)}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Latest</span>
              <span className="data-metric-value">{elig?.latest ? fmtInt(elig.latest.core_structural_eligible_n) : "—"}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Latest regression members</span>
              <span className="data-metric-value">{elig?.latest ? fmtInt(elig.latest.regression_member_n) : "—"}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Regression coverage</span>
              <span className="data-metric-value">
                {elig?.latest ? `${Number(elig.latest.regression_coverage_pct || 0).toFixed(1)}%` : "—"}
              </span>
            </div>
          </div>

          <div className="data-metric-card">
            <h4>Projected Coverage</h4>
            <div className="data-metric-desc">
              Structurally eligible names with usable returns that remain in portfolio analytics even when they do not participate in core estimation.
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Historical min projectable</span>
              <span className="data-metric-value">{fmtInt(elig?.min_projectable_n)}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Historical max projectable</span>
              <span className="data-metric-value">{fmtInt(elig?.max_projectable_n)}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Latest projectable</span>
              <span className="data-metric-value">{elig?.latest ? fmtInt(elig.latest.projectable_n) : "—"}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Latest projected-only</span>
              <span className="data-metric-value">{elig?.latest ? fmtInt(elig.latest.projected_only_n) : "—"}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Projectable coverage</span>
              <span className="data-metric-value">
                {elig?.latest ? `${Number(elig.latest.projectable_coverage_pct || 0).toFixed(1)}%` : "—"}
              </span>
            </div>
          </div>

          <div className="data-metric-card">
            <h4>Factor Return Cross-Section</h4>
            <div className="data-metric-desc">
              The number of securities actually used in each daily factor return regression. A narrow range here means stable estimation; wide swings may signal data quality issues.
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Historical min N</span>
              <span className="data-metric-value">{fmtInt(cross?.min_cross_section_n)}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Historical max N</span>
              <span className="data-metric-value">{fmtInt(cross?.max_cross_section_n)}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Latest range</span>
              <span className="data-metric-value">
                {cross?.latest
                  ? `${fmtInt(cross.latest.cross_section_n_min)}–${fmtInt(cross.latest.cross_section_n_max)}`
                  : "—"}
              </span>
            </div>
            {cross?.latest?.date && (
              <div className="data-metric-row">
                <span className="data-metric-label">As of</span>
                <span className="data-metric-value">{cross.latest.date}</span>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* ── Data Integrity ── */}
      <div className="chart-card data-section">
        <h3>Data Integrity</h3>
        <div className="data-section-desc">
          Checks for duplicate (ticker, date) pairs in the active exposure source table. Duplicates mean the same security appears twice on the same date, which can bias regression weights and inflate factor return estimates.
        </div>
        <div className="data-metric-grid" style={{ maxWidth: 600 }}>
          <div className="data-metric-card">
            <h4>Exposure Source Duplicates</h4>
            <div className="data-metric-row">
              <span className="data-metric-label">Table</span>
              <span className="data-metric-value">{dupes?.table || "—"}</span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Duplicate groups</span>
              <span className="data-metric-value">
                {dupes?.computed
                  ? fmtInt(dupes.duplicate_groups)
                  : <span className="data-mode-badge">fast</span>}
              </span>
            </div>
            <div className="data-metric-row">
              <span className="data-metric-label">Extra rows</span>
              <span className="data-metric-value">
                {dupes?.computed
                  ? fmtInt(dupes.duplicate_extra_rows)
                  : <span className="data-mode-badge">fast</span>}
              </span>
            </div>
            {dupes?.computed && (
              <div className="data-metric-row">
                <span className="data-metric-label">Status</span>
                <span className="data-metric-value">
                  {dupesClean
                    ? <span className="data-integrity-status clean">No duplicates found</span>
                    : <span className="data-integrity-status dirty">Duplicates detected — investigate</span>}
                </span>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* ── Cache Freshness ── */}
      <div className="chart-card data-section">
        <h3>Cache Freshness</h3>
        <div className="data-section-desc">
          Every dashboard page reads from precomputed cache outputs rather than querying source tables on each load. Stale caches mean the UI is showing old data — run the appropriate refresh lane to update.
        </div>
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th>Cache Key</th>
                <th>Description</th>
                <th>Last Refreshed (UTC)</th>
              </tr>
            </thead>
            <tbody>
              {cacheRows.map((r) => (
                <tr key={r.key}>
                  <td><span className="data-cache-key">{r.key}</span></td>
                  <td>
                    <span className="data-cache-desc">
                      {CACHE_DESC[r.key] || "Internal cache output."}
                    </span>
                  </td>
                  <td>{fmtTs(r.updated_at_utc)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
