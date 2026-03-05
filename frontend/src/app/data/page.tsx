"use client";

import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ApiErrorState from "@/components/ApiErrorState";
import { useDataDiagnostics } from "@/hooks/useApi";
import type { DataTableStats } from "@/lib/types";

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

export default function DataPage() {
  const { data, isLoading, error } = useDataDiagnostics();

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
  const refreshRows: Array<{ label: string; table: DataTableStats | null | undefined }> = [
    { label: "Security Master", table: src?.security_master },
    { label: "Fundamentals PIT", table: src?.security_fundamentals_pit },
    { label: "Classification PIT", table: src?.security_classification_pit },
    { label: "Prices EOD", table: src?.security_prices_eod },
    { label: "ESTU Membership Daily", table: src?.estu_membership_daily },
    { label: "Raw Cross-Section History", table: src?.barra_raw_cross_section_history },
    { label: "Cross-Section Snapshot", table: src?.universe_cross_section_snapshot },
  ];

  return (
    <div>
      <div className="kpi-row">
        <div className="kpi-card">
          <div className="label">Exposure Source</div>
          <div className="value" style={{ fontSize: 15 }}>
            {data?.exposure_source_table || "—"}
          </div>
          <div className="sub">Active table used for engine exposures</div>
        </div>
        <div className="kpi-card">
          <div className="label">Min Structural N</div>
          <div className="value">{fmtInt(elig?.min_structural_eligible_n)}</div>
          <div className="sub">Across cached eligibility history</div>
        </div>
        <div className="kpi-card">
          <div className="label">Max Structural N</div>
          <div className="value">{fmtInt(elig?.max_structural_eligible_n)}</div>
          <div className="sub">Across cached eligibility history</div>
        </div>
        <div className="kpi-card">
          <div className="label">Latest Regression N</div>
          <div className="value">{fmtInt(elig?.latest?.regression_member_n)}</div>
          <div className="sub">{elig?.latest?.date || "—"}</div>
        </div>
      </div>

      <div className="chart-card mb-4">
        <h3>Source Refresh & Coverage</h3>
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th>Dataset</th>
                <th className="text-right">Rows</th>
                <th className="text-right">Tickers</th>
                <th>Date Range</th>
                <th>Last Updated</th>
                <th>Last Job Run</th>
              </tr>
            </thead>
            <tbody>
              {refreshRows.map(({ label, table }) => (
                <tr key={label}>
                  <td>{label}</td>
                  <td className="text-right">{fmtInt(table?.row_count)}</td>
                  <td className="text-right">{fmtInt(table?.ticker_count)}</td>
                  <td>{table?.min_date && table?.max_date ? `${table.min_date} → ${table.max_date}` : "—"}</td>
                  <td>{fmtTs(table?.last_updated_at)}</td>
                  <td>{table?.last_job_run_id || "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="chart-card mb-4">
        <h3>Cross-Section Usage Through Time</h3>
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th>Metric</th>
                <th className="text-right">Min</th>
                <th className="text-right">Max</th>
                <th>Latest Snapshot</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Structural Eligible N</td>
                <td className="text-right">{fmtInt(elig?.min_structural_eligible_n)}</td>
                <td className="text-right">{fmtInt(elig?.max_structural_eligible_n)}</td>
                <td>
                  {elig?.latest
                    ? `${fmtInt(elig.latest.structural_eligible_n)} (date ${elig.latest.date}, exp ${elig.latest.exp_date || "—"})`
                    : "—"}
                </td>
              </tr>
              <tr>
                <td>Regression Member N</td>
                <td className="text-right">{fmtInt(elig?.min_regression_member_n)}</td>
                <td className="text-right">{fmtInt(elig?.max_regression_member_n)}</td>
                <td>{elig?.latest ? fmtInt(elig.latest.regression_member_n) : "—"}</td>
              </tr>
              <tr>
                <td>Factor Return Cross-Section N</td>
                <td className="text-right">{fmtInt(cross?.min_cross_section_n)}</td>
                <td className="text-right">{fmtInt(cross?.max_cross_section_n)}</td>
                <td>{cross?.latest ? `${fmtInt(cross.latest.cross_section_n_min)}–${fmtInt(cross.latest.cross_section_n_max)} (${cross.latest.date || "—"})` : "—"}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div className="chart-card mb-4">
        <h3>Exposure Duplicate Audit</h3>
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th>Table</th>
                <th className="text-right">Duplicate Groups</th>
                <th className="text-right">Extra Rows</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>{data?.exposure_duplicates?.active_exposure_source?.table || "—"}</td>
                <td className="text-right">{fmtInt(data?.exposure_duplicates?.active_exposure_source?.duplicate_groups)}</td>
                <td className="text-right">{fmtInt(data?.exposure_duplicates?.active_exposure_source?.duplicate_extra_rows)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div className="chart-card">
        <h3>Cached Output Refresh Times</h3>
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th>Cache Key</th>
                <th>Last Refreshed (UTC)</th>
              </tr>
            </thead>
            <tbody>
              {cacheRows.map((r) => (
                <tr key={r.key}>
                  <td>{r.key}</td>
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
