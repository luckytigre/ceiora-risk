export interface DataTableStats {
  table: string;
  exists: boolean;
  row_count?: number;
  row_count_mode?: string | null;
  ticker_count?: number | null;
  date_column?: string | null;
  min_date?: string | null;
  max_date?: string | null;
  last_updated_at?: string | null;
  last_job_run_id?: string | null;
}

export interface DataDiagnosticsData {
  status: string;
  database_path: string;
  cache_db_path: string;
  diagnostic_scope?: {
    source?: string;
    plain_english?: string;
  };
  truth_surfaces?: {
    dashboard_serving?: {
      source?: string;
      plain_english?: string;
    };
    operator_status?: {
      source?: string;
      plain_english?: string;
    };
    local_diagnostics?: {
      source?: string;
      plain_english?: string;
    };
  };
  exposure_source_table: string;
  exposure_source?: {
    table: string;
    selection_mode: string;
    is_dynamic: boolean;
    latest_asof?: string | null;
    plain_english?: string | null;
  };
  source_tables: {
    security_master: DataTableStats | null;
    security_fundamentals_pit: DataTableStats | null;
    security_classification_pit: DataTableStats | null;
    security_prices_eod: DataTableStats | null;
    estu_membership_daily: DataTableStats | null;
    barra_raw_cross_section_history: DataTableStats | null;
    universe_cross_section_snapshot: DataTableStats | null;
  };
  exposure_duplicates: {
    active_exposure_source: {
      table: string;
      exists: boolean;
      duplicate_groups: number | null;
      duplicate_extra_rows: number | null;
      computed?: boolean;
    };
  };
  cross_section_usage: {
    eligibility_summary: {
      available: boolean;
      latest?: {
        date: string;
        exp_date: string | null;
        exposure_n: number;
        structural_eligible_n: number;
        core_structural_eligible_n: number;
        regression_member_n: number;
        projectable_n: number;
        projected_only_n: number;
        structural_coverage_pct: number;
        regression_coverage_pct: number;
        projectable_coverage_pct: number;
        alert_level: string;
      } | null;
      min_structural_eligible_n?: number | null;
      max_structural_eligible_n?: number | null;
      min_core_structural_eligible_n?: number | null;
      max_core_structural_eligible_n?: number | null;
      min_regression_member_n?: number | null;
      max_regression_member_n?: number | null;
      min_projectable_n?: number | null;
      max_projectable_n?: number | null;
      min_projected_only_n?: number | null;
      max_projected_only_n?: number | null;
    };
    factor_cross_section: {
      available: boolean;
      latest?: {
        date: string | null;
        cross_section_n_min: number;
        cross_section_n_max: number;
        eligible_n_min: number;
        eligible_n_max: number;
      } | null;
      min_cross_section_n?: number | null;
      max_cross_section_n?: number | null;
      min_eligible_n?: number | null;
      max_eligible_n?: number | null;
    };
  };
  risk_engine_meta: Record<string, unknown>;
  cache_outputs: Array<{
    key: string;
    updated_at_unix: number | null;
    updated_at_utc: string | null;
  }>;
}
