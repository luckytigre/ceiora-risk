import type { SourceDates } from "@/lib/types/analytics";

export interface RefreshStatusState {
  status: string;
  job_id: string | null;
  pipeline_run_id: string | null;
  profile: string | null;
  requested_profile: string | null;
  mode: string | null;
  as_of_date: string | null;
  resume_run_id: string | null;
  from_stage: string | null;
  to_stage: string | null;
  force_core: boolean;
  force_risk_recompute: boolean;
  requested_at: string | null;
  started_at: string | null;
  finished_at: string | null;
  current_stage?: string | null;
  stage_index?: number | null;
  stage_count?: number | null;
  stage_started_at?: string | null;
  current_stage_message?: string | null;
  current_stage_progress_pct?: number | null;
  current_stage_items_processed?: number | null;
  current_stage_items_total?: number | null;
  current_stage_unit?: string | null;
  current_stage_heartbeat_at?: string | null;
  result: Record<string, unknown> | null;
  error: {
    type?: string;
    message?: string;
    traceback?: string;
  } | null;
}

export interface RefreshStatusData {
  status: string;
  refresh: RefreshStatusState;
}

export interface OperatorLaneLatestRun {
  run_id: string | null;
  profile: string | null;
  status: string;
  started_at: string | null;
  finished_at: string | null;
  updated_at: string | null;
  duration_seconds?: number | null;
  stage_count: number;
  completed_stage_count: number;
  failed_stage_count: number;
  running_stage_count: number;
  stage_duration_seconds_total?: number;
  current_stage?: OperatorLaneStage | null;
  stages: OperatorLaneStage[];
}

export interface OperatorLaneStage {
  stage_name: string;
  stage_order: number;
  status: string;
  started_at: string | null;
  completed_at: string | null;
  duration_seconds?: number | null;
  heartbeat_at?: string | null;
  details?: {
    message?: string | null;
    progress_kind?: string | null;
    progress_pct?: number | null;
    items_processed?: number | null;
    items_total?: number | null;
    unit?: string | null;
    current_date?: string | null;
    current_as_of?: string | null;
    dates_per_second?: number | null;
    computed_dates?: number | null;
    cached_dates?: number | null;
    skip_counts?: Record<string, number>;
    [key: string]: unknown;
  };
  error_type: string | null;
  error_message: string | null;
}

export interface OperatorLaneStatus {
  profile: string;
  label: string;
  description: string;
  core_policy: string;
  serving_mode: string;
  raw_history_policy: string;
  reset_core_cache: boolean;
  default_stages: string[];
  enable_ingest: boolean;
  ingest_policy?: string;
  rebuild_backend?: string;
  requires_neon_sync_before_core?: boolean;
  source_sync_required?: boolean;
  neon_readiness_required?: boolean;
  latest_run: OperatorLaneLatestRun;
}

export interface OperatorStatusData {
  status: string;
  generated_at: string;
  lanes: OperatorLaneStatus[];
  source_dates: SourceDates;
  local_archive_source_dates?: SourceDates | null;
  risk_engine: {
    status?: string;
    method_version?: string;
    last_recompute_date?: string;
    factor_returns_latest_date?: string;
    lookback_days?: number;
    cross_section_min_age_days?: number;
    recompute_interval_days?: number;
  };
  core_due: {
    due: boolean;
    reason: string;
  };
  refresh: RefreshStatusState;
  holdings_sync?: {
    pending?: boolean;
    pending_count?: number;
    dirty_revision?: number;
    dirty_since?: string | null;
    last_mutation_at?: string | null;
    last_mutation_kind?: string | null;
    last_mutation_summary?: string | null;
    last_mutation_account_id?: string | null;
    last_import_batch_id?: string | null;
    last_refresh_started_at?: string | null;
    last_refresh_finished_at?: string | null;
    last_refresh_status?: string | null;
    last_refresh_profile?: string | null;
    last_refresh_run_id?: string | null;
    last_refresh_message?: string | null;
    last_refresh_started_dirty_revision?: number | null;
  } | null;
  neon_sync_health?: {
    status?: string;
    message?: string;
    updated_at?: string;
    artifact_path?: string | null;
    mirror_status?: string | null;
    sync_status?: string | null;
    parity_status?: string | null;
    parity_issue_count?: number;
  } | null;
  active_snapshot?: {
    snapshot_id?: string;
    published_at?: number;
  } | null;
  latest_parity_artifact?: string | null;
  runtime?: {
    app_runtime_role?: string;
    allowed_profiles?: string[];
    local_only_profiles?: string[];
    canonical_serving_profile?: string;
    dashboard_truth_surface?: string;
    dashboard_truth_plain_english?: string;
    storage_contract_plain_english?: string;
    source_authority?: string;
    source_authority_plain_english?: string;
    local_archive_enabled?: boolean;
    local_archive_plain_english?: string;
    rebuild_authority?: string;
    rebuild_authority_plain_english?: string;
    diagnostics_scope?: string;
    diagnostics_scope_plain_english?: string;
    data_backend?: string;
    neon_database_configured?: boolean;
    neon_auto_sync_enabled?: boolean;
    neon_auto_sync_enabled_effective?: boolean;
    neon_auto_parity_enabled?: boolean;
    neon_auto_parity_enabled_effective?: boolean;
    neon_auto_prune_enabled?: boolean;
    neon_auto_prune_enabled_effective?: boolean;
    neon_authoritative_rebuilds?: boolean;
    neon_read_surfaces?: string[];
    serving_outputs_primary_reads?: boolean;
    serving_outputs_primary_reads_effective?: boolean;
    warnings?: string[];
  } | null;
}
