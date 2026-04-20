import type {
  FactorCatalogEntry,
  FactorDetail,
  FactorExposure,
  PortfolioData,
  Position,
  RiskShares,
  ServingSnapshotMeta,
  SourceDates,
} from "@/lib/types/analytics";

export interface WhatIfScenarioRow {
  account_id: string;
  ticker: string;
  ric?: string | null;
  quantity: number;
  source?: string | null;
}

export interface WhatIfHoldingDelta {
  account_id: string;
  ticker: string;
  ric: string;
  current_quantity: number;
  hypothetical_quantity: number;
  delta_quantity: number;
}

export interface WhatIfPreviewSide {
  positions: Position[];
  total_value: number;
  position_count: number;
  risk_shares: RiskShares;
  component_shares: Omit<RiskShares, "idio">;
  factor_details: FactorDetail[];
  exposure_modes: {
    raw: FactorExposure[];
    sensitivity: FactorExposure[];
    risk_contribution: FactorExposure[];
  };
  factor_catalog?: FactorCatalogEntry[];
}

export interface WhatIfFactorDeltaRow {
  factor_id: string;
  current: number;
  hypothetical: number;
  delta: number;
}

export interface WhatIfPreviewScope {
  kind: string;
  account_ids: string[];
  accounts_count: number;
}

export interface WhatIfPreviewData {
  scenario_rows: WhatIfScenarioRow[];
  holding_deltas: WhatIfHoldingDelta[];
  current: WhatIfPreviewSide;
  hypothetical: WhatIfPreviewSide;
  diff: {
    total_value: number;
    position_count: number;
    risk_shares: RiskShares;
    factor_deltas: {
      raw: WhatIfFactorDeltaRow[];
      sensitivity: WhatIfFactorDeltaRow[];
      risk_contribution: WhatIfFactorDeltaRow[];
    };
  };
  source_dates?: SourceDates;
  serving_snapshot?: ServingSnapshotMeta;
  truth_surface?: string;
  preview_scope?: WhatIfPreviewScope;
  _preview_only: boolean;
}

export interface WhatIfApplyRowResult {
  account_id: string;
  ticker: string;
  ric: string;
  current_quantity: number;
  applied_quantity: number;
  delta_quantity?: number;
  action: string;
}

export interface WhatIfApplyRejectedRow {
  row_number: number;
  reason_code: string;
  message: string;
}

export interface WhatIfApplyResponse {
  status: "ok" | "dry_run" | "rejected";
  accepted_rows: number;
  rejected_rows: number;
  rejection_counts: Record<string, number>;
  warnings: string[];
  applied_upserts: number;
  applied_deletes: number;
  row_results: WhatIfApplyRowResult[];
  rejected: WhatIfApplyRejectedRow[];
  import_batch_ids?: Record<string, string>;
}

export type HoldingsImportMode = "replace_account" | "upsert_absolute" | "increment_delta";

export interface HoldingsModeData {
  modes: HoldingsImportMode[];
  default: HoldingsImportMode;
}

export interface HoldingsAccount {
  account_id: string;
  account_name: string;
  is_active: boolean;
  positions_count: number;
  gross_quantity: number;
  last_position_updated_at: string | null;
}

export interface HoldingsAccountsData {
  accounts: HoldingsAccount[];
}

export interface HoldingsPosition {
  account_id: string;
  ric: string;
  ticker: string;
  quantity: number;
  source: string;
  updated_at: string | null;
}

export interface HoldingsPositionsData {
  positions: HoldingsPosition[];
  account_id: string | null;
  count: number;
}

export interface HoldingsImportRowPayload {
  account_id?: string;
  ric?: string;
  ticker?: string;
  quantity: number;
  source?: string;
}

export interface HoldingsImportResponse {
  status: string;
  mode: HoldingsImportMode;
  account_id: string;
  import_batch_id: string;
  accepted_rows: number;
  rejected_rows: number;
  rejection_counts: Record<string, number>;
  warnings: string[];
  applied_upserts: number;
  applied_deletes: number;
  refresh?: {
    started: boolean;
    state: Record<string, unknown>;
  } | null;
  preview_rejections?: Array<Record<string, unknown>>;
}

export interface HoldingsPositionEditResponse {
  status: string;
  action: string;
  account_id: string;
  ric: string;
  ticker: string | null;
  quantity: number;
  import_batch_id: string;
  refresh?: {
    started: boolean;
    state: Record<string, unknown>;
  } | null;
}

export type { PortfolioData };
