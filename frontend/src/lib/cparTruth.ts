"use client";

import { ApiError } from "@/lib/cparApi";
import type {
  CparCoverageBreakdown,
  CparCovMatrix,
  CparRiskData,
  CparFactorChartRow,
  CparFactorDrilldownRow,
  CparFactorGroup,
  CparFactorVarianceContribution,
  CparFactorSpec,
  CparFitStatus,
  CparHedgeStatus,
  CparLoading,
  CparPortfolioHedgeData,
  CparPortfolioHedgeRecommendationData,
  CparPortfolioPositionRow,
  CparPortfolioWhatIfData,
  CparSearchItem,
  CparWarning,
} from "@/lib/types/cpar";

export type BadgeTone = "success" | "warning" | "error" | "neutral";

interface CparApiErrorDetail {
  status?: string;
  error?: string;
  message?: string;
  build_profile?: string;
}

interface CparPackageFreshnessInput {
  package_date?: string | null;
  source_prices_asof?: string | null;
  completed_at?: string | null;
}

export interface CparBadgeDescriptor {
  label: string;
  tone: BadgeTone;
  detail: string;
}

export interface CparErrorSummary {
  kind: "not_ready" | "unavailable" | "ambiguous" | "missing" | "unknown";
  message: string;
  statusCode: number | null;
  buildProfile: string | null;
}

interface CparPackageIdentity {
  package_run_id?: string | null;
  package_date?: string | null;
}

const EMPTY_CPAR_COVERAGE_BREAKDOWN: CparCoverageBreakdown = {
  covered: { positions_count: 0, gross_market_value: 0 },
  missing_price: { positions_count: 0, gross_market_value: 0 },
  missing_cpar_fit: { positions_count: 0, gross_market_value: 0 },
  insufficient_history: { positions_count: 0, gross_market_value: 0 },
};

function cleanDate(value: string | null | undefined): string | null {
  const text = String(value || "").trim();
  return text || null;
}

function parseIsoDate(value: string | null | undefined): Date | null {
  const isoDate = cleanDate(value);
  if (!isoDate) return null;
  const parts = isoDate.split("-");
  if (parts.length !== 3) return null;
  const year = Number(parts[0]);
  const month = Number(parts[1]);
  const day = Number(parts[2]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  return new Date(year, month - 1, day, 12, 0, 0);
}

export function formatCparPackageDate(value: string | null | undefined): string {
  const isoDate = cleanDate(value);
  const parsed = parseIsoDate(value);
  if (!isoDate) return "—";
  if (!parsed) return isoDate;
  return parsed.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "2-digit",
  });
}

export function formatCparTimestamp(value: string | null | undefined): string {
  const raw = String(value || "").trim();
  if (!raw) return "—";
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function formatCparNumber(value: number | null | undefined, digits = 2): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "—";
  return value.toFixed(digits);
}

export function formatCparMarketValueThousands(
  value: number | null | undefined,
  options: {
    digits?: number;
    absolute?: boolean;
  } = {},
): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "—";
  const { digits = 1, absolute = false } = options;
  const normalized = absolute ? Math.abs(value) : value;
  const scaled = normalized / 1000;
  return `${scaled.toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}K`;
}

export function formatCparPercent(value: number | null | undefined, digits = 1): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "—";
  return `${(value * 100).toFixed(digits)}%`;
}

export function describeCparFitStatus(status: CparFitStatus | string | null | undefined): CparBadgeDescriptor {
  if (status === "ok") {
    return {
      label: "OK",
      tone: "success",
      detail: "Complete weekly coverage with no material continuity warning.",
    };
  }
  if (status === "limited_history") {
    return {
      label: "Limited History",
      tone: "warning",
      detail: "Fit is usable, but history depth or continuity is weaker than the ideal 52-week package.",
    };
  }
  if (status === "insufficient_history") {
    return {
      label: "Insufficient History",
      tone: "error",
      detail: "The package does not have enough weekly observations to expose loadings or hedge output.",
    };
  }
  return {
    label: "Unknown",
    tone: "neutral",
    detail: "Fit status was not recognized by the cPAR frontend contract.",
  };
}

export function describeCparPositionMethod(
  coverage: string | null | undefined,
  fitStatus: CparFitStatus | string | null | undefined,
): CparBadgeDescriptor {
  if (coverage === "missing_price") {
    return {
      label: "Missing Price",
      tone: "error",
      detail: "A current price row is missing, so the position cannot be included in the aggregate cPAR surface.",
    };
  }
  if (coverage === "missing_cpar_fit") {
    return {
      label: "Missing cPAR Fit",
      tone: "error",
      detail: "The active cPAR package does not contain a fit row for this security.",
    };
  }
  if (coverage === "insufficient_history" || fitStatus === "insufficient_history") {
    return {
      label: "Insufficient History",
      tone: "error",
      detail: "The active package does not have enough weekly observations to expose this security.",
    };
  }
  if (fitStatus === "limited_history") {
    return {
      label: "Package Fit (Limited)",
      tone: "warning",
      detail: "The position is usable in the active package, but its fit is based on weaker-than-ideal history depth or continuity.",
    };
  }
  return {
    label: "Package Fit",
    tone: "success",
    detail: "The position is covered by the active cPAR package fit.",
  };
}

export function describeCparWarning(warning: CparWarning | string): CparBadgeDescriptor {
  if (warning === "continuity_gap") {
    return {
      label: "Continuity Gap",
      tone: "warning",
      detail: "The weekly return history has a material gap even though the fit remains readable.",
    };
  }
  if (warning === "ex_us_caution") {
    return {
      label: "Ex-US Caution",
      tone: "warning",
      detail: "The security is outside the US-first proxy design of cPAR1, so interpret the hedge more cautiously.",
    };
  }
  return {
    label: String(warning || "Warning"),
    tone: "neutral",
    detail: "Unmapped cPAR warning code.",
  };
}

export function describeCparHedgeStatus(status: CparHedgeStatus | string | null | undefined): CparBadgeDescriptor {
  if (status === "hedge_ok") {
    return {
      label: "Hedge OK",
      tone: "success",
      detail: "The persisted fit produced a valid deterministic hedge package.",
    };
  }
  if (status === "hedge_degraded") {
    return {
      label: "Hedge Degraded",
      tone: "warning",
      detail: "The hedge package exists, but non-market reduction fell below the preferred threshold.",
    };
  }
  if (status === "hedge_unavailable") {
    return {
      label: "Hedge Unavailable",
      tone: "error",
      detail: "The fit status blocks hedge generation for this instrument.",
    };
  }
  return {
    label: "Unknown Hedge",
    tone: "neutral",
    detail: "Hedge status was not recognized by the cPAR frontend contract.",
  };
}

export function summarizeFactorRegistry(factors: CparFactorSpec[]): Record<CparFactorGroup, number> {
  return factors.reduce(
    (acc, factor) => {
      acc[factor.group] += 1;
      return acc;
    },
    { market: 0, sector: 0, style: 0 } as Record<CparFactorGroup, number>,
  );
}

export function canNavigateCparSearchResult(item: Pick<CparSearchItem, "ticker"> | null | undefined): boolean {
  return Boolean(item?.ticker && item.ticker.trim());
}

export function describeCparPackageFreshness(meta: CparPackageFreshnessInput): CparBadgeDescriptor {
  const referenceDate = parseIsoDate(meta.source_prices_asof) || parseIsoDate(meta.package_date);
  if (!referenceDate) {
    return {
      label: "Unknown",
      tone: "neutral",
      detail: "The cPAR package does not expose a usable package date for freshness checks.",
    };
  }

  const today = new Date();
  const noonToday = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 12, 0, 0);
  const ageDays = Math.round((noonToday.getTime() - referenceDate.getTime()) / (24 * 60 * 60 * 1000));
  const asOf = formatCparPackageDate(meta.source_prices_asof || meta.package_date || null);
  const builtAt = formatCparTimestamp(meta.completed_at);
  const buildDetail = builtAt === "—" ? "Build timestamp unavailable." : `Built ${builtAt}.`;

  if (ageDays <= 7) {
    return {
      label: "Current",
      tone: "success",
      detail: `Source/package as of ${asOf}. ${buildDetail}`,
    };
  }
  if (ageDays <= 14) {
    return {
      label: "Aging",
      tone: "warning",
      detail: `Source/package as of ${asOf} (${ageDays} days old). ${buildDetail}`,
    };
  }
  return {
    label: "Stale",
    tone: "error",
    detail: `Source/package as of ${asOf} (${ageDays} days old). Publish a newer cPAR package before relying on this read surface. ${buildDetail}`,
  };
}

export function sameCparPackageIdentity(
  expected: CparPackageIdentity | null | undefined,
  actual: CparPackageIdentity | null | undefined,
): boolean {
  const expectedRunId = String(expected?.package_run_id || "").trim();
  const actualRunId = String(actual?.package_run_id || "").trim();
  if (expectedRunId && actualRunId && expectedRunId !== actualRunId) {
    return false;
  }

  const expectedDate = String(expected?.package_date || "").trim();
  const actualDate = String(actual?.package_date || "").trim();
  if (expectedDate && actualDate && expectedDate !== actualDate) {
    return false;
  }

  return true;
}

function normalizeCparNumeric(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function normalizeCparCoverageBreakdown(value: unknown): CparCoverageBreakdown {
  const breakdown = value && typeof value === "object" ? value as Partial<CparCoverageBreakdown> : null;
  return {
    covered: {
      positions_count: normalizeCparNumeric(breakdown?.covered?.positions_count),
      gross_market_value: normalizeCparNumeric(breakdown?.covered?.gross_market_value),
    },
    missing_price: {
      positions_count: normalizeCparNumeric(breakdown?.missing_price?.positions_count),
      gross_market_value: normalizeCparNumeric(breakdown?.missing_price?.gross_market_value),
    },
    missing_cpar_fit: {
      positions_count: normalizeCparNumeric(breakdown?.missing_cpar_fit?.positions_count),
      gross_market_value: normalizeCparNumeric(breakdown?.missing_cpar_fit?.gross_market_value),
    },
    insufficient_history: {
      positions_count: normalizeCparNumeric(breakdown?.insufficient_history?.positions_count),
      gross_market_value: normalizeCparNumeric(breakdown?.insufficient_history?.gross_market_value),
    },
  };
}

function normalizeCparLoadings(value: unknown): CparLoading[] {
  return Array.isArray(value) ? value as CparLoading[] : [];
}

function normalizeCparCovMatrix(value: unknown): CparCovMatrix {
  const matrix = value && typeof value === "object" ? value as Partial<CparCovMatrix> : null;
  return {
    factors: Array.isArray(matrix?.factors) ? matrix.factors.map((factor) => String(factor)) : [],
    correlation: Array.isArray(matrix?.correlation)
      ? matrix.correlation.map((row) => (
          Array.isArray(row)
            ? row.map((cell) => (typeof cell === "number" && Number.isFinite(cell) ? cell : 0))
            : []
        ))
      : [],
  };
}

function normalizeCparVarianceContributions(value: unknown): CparFactorVarianceContribution[] {
  return Array.isArray(value) ? value as CparFactorVarianceContribution[] : [];
}

function normalizeCparFactorChartRows(value: unknown): CparFactorChartRow[] {
  if (!Array.isArray(value)) return [];
  return value.map((row) => {
    const drilldown = Array.isArray((row as CparFactorChartRow).drilldown)
      ? (row as CparFactorChartRow).drilldown.map((item) => ({
          ...item,
          warnings: Array.isArray(item.warnings) ? item.warnings : [],
          vol_scaled_loading: typeof item.vol_scaled_loading === "number" ? item.vol_scaled_loading : 0,
          vol_scaled_contribution: typeof item.vol_scaled_contribution === "number" ? item.vol_scaled_contribution : 0,
          covariance_adjusted_loading: typeof item.covariance_adjusted_loading === "number" ? item.covariance_adjusted_loading : 0,
          risk_contribution_pct: typeof item.risk_contribution_pct === "number" ? item.risk_contribution_pct : 0,
        }))
      : [];
    return {
      ...(row as CparFactorChartRow),
      factor_volatility: typeof (row as CparFactorChartRow).factor_volatility === "number" ? (row as CparFactorChartRow).factor_volatility : 0,
      covariance_adjustment: typeof (row as CparFactorChartRow).covariance_adjustment === "number" ? (row as CparFactorChartRow).covariance_adjustment : 0,
      sensitivity_beta: typeof (row as CparFactorChartRow).sensitivity_beta === "number" ? (row as CparFactorChartRow).sensitivity_beta : 0,
      risk_contribution_pct: typeof (row as CparFactorChartRow).risk_contribution_pct === "number" ? (row as CparFactorChartRow).risk_contribution_pct : 0,
      drilldown,
    };
  });
}

function deriveCparFactorChartRows(
  portfolio: Pick<CparPortfolioHedgeData, "positions"> & {
    aggregate_loadings: CparLoading[];
    factor_variance_contributions: CparFactorVarianceContribution[];
    contribution_field: "thresholded_contributions" | "display_contributions";
  },
): CparFactorChartRow[] {
  const loadings = normalizeCparLoadings(portfolio.aggregate_loadings);
  const varianceRows = normalizeCparVarianceContributions(portfolio.factor_variance_contributions);
  const positions = Array.isArray(portfolio.positions) ? portfolio.positions : [];
  const metaByFactor = new Map<string, Pick<CparFactorChartRow, "factor_id" | "label" | "group" | "display_order">>();
  const aggregateByFactor = new Map(loadings.map((row) => [row.factor_id, row]));
  const varianceByFactor = new Map(varianceRows.map((row) => [row.factor_id, row]));

  loadings.forEach((row) => {
    metaByFactor.set(row.factor_id, row);
  });
  varianceRows.forEach((row) => {
    if (!metaByFactor.has(row.factor_id)) metaByFactor.set(row.factor_id, row);
  });
  positions.forEach((row) => {
    if (row.coverage !== "covered") return;
    normalizeCparLoadings(row[portfolio.contribution_field]).forEach((contribution) => {
      if (Math.abs(contribution.beta) <= 1e-12) return;
      if (!metaByFactor.has(contribution.factor_id)) metaByFactor.set(contribution.factor_id, contribution);
    });
  });

  return [...metaByFactor.values()]
    .sort((left, right) => (
      left.display_order - right.display_order
      || left.factor_id.localeCompare(right.factor_id)
    ))
    .map((meta) => {
      const drilldown = positions
        .filter((row) => row.coverage === "covered")
        .map((row) => {
          const contribution = normalizeCparLoadings(row[portfolio.contribution_field]).find(
            (item) => item.factor_id === meta.factor_id,
          );
          if (!contribution || Math.abs(contribution.beta) <= 1e-12) return null;
          const weight = row.portfolio_weight;
          const factorBeta = typeof weight === "number" && Math.abs(weight) > 1e-12
            ? contribution.beta / weight
            : null;
          return {
            ric: row.ric,
            ticker: row.ticker,
            display_name: row.display_name,
            market_value: row.market_value,
            portfolio_weight: row.portfolio_weight,
            fit_status: row.fit_status,
            warnings: row.warnings,
            coverage: row.coverage,
            coverage_reason: row.coverage_reason,
            factor_beta: factorBeta,
            contribution_beta: contribution.beta,
            vol_scaled_loading: 0,
            vol_scaled_contribution: 0,
            covariance_adjusted_loading: 0,
            risk_contribution_pct: 0,
          } satisfies CparFactorDrilldownRow;
        })
        .filter((row): row is CparFactorDrilldownRow => row !== null)
        .sort((left, right) => Math.abs(right.contribution_beta) - Math.abs(left.contribution_beta));
      const aggregate = aggregateByFactor.get(meta.factor_id);
      const variance = varianceByFactor.get(meta.factor_id);
      return {
        ...meta,
        beta: aggregate?.beta ?? 0,
        aggregate_beta: aggregate?.beta ?? 0,
        factor_volatility: 0,
        covariance_adjustment: 0,
        sensitivity_beta: 0,
        risk_contribution_pct: (variance?.variance_share ?? 0) * 100,
        positive_contribution_beta: drilldown.reduce(
          (sum, row) => sum + Math.max(row.contribution_beta, 0),
          0,
        ),
        negative_contribution_beta: drilldown.reduce(
          (sum, row) => sum + Math.min(row.contribution_beta, 0),
          0,
        ),
        variance_contribution: variance?.variance_contribution ?? 0,
        variance_share: variance?.variance_share ?? 0,
        drilldown,
      } satisfies CparFactorChartRow;
    });
}

function normalizeCparPortfolioPositionRow(row: CparPortfolioPositionRow): CparPortfolioPositionRow {
  return {
    ...row,
    display_contributions: normalizeCparLoadings(row.display_contributions ?? row.thresholded_contributions),
    thresholded_contributions: normalizeCparLoadings(row.thresholded_contributions),
  };
}

type CparRiskLikePayload = {
  aggregate_display_loadings?: CparLoading[];
  aggregate_thresholded_loadings: CparLoading[];
  coverage_breakdown: CparCoverageBreakdown;
  display_cov_matrix?: CparCovMatrix;
  cov_matrix: CparCovMatrix;
  display_factor_variance_contributions?: CparFactorVarianceContribution[];
  factor_variance_contributions: CparFactorVarianceContribution[];
  display_factor_chart?: CparFactorChartRow[];
  factor_chart?: CparFactorChartRow[];
  positions: CparPortfolioPositionRow[];
};

function normalizeCparRiskLikeData<T extends CparRiskLikePayload>(payload: T): T {
  const positions = Array.isArray(payload.positions)
    ? payload.positions.map(normalizeCparPortfolioPositionRow)
    : [];
  const aggregateDisplayLoadings = normalizeCparLoadings(
    payload.aggregate_display_loadings ?? payload.aggregate_thresholded_loadings,
  );
  const aggregateThresholdedLoadings = normalizeCparLoadings(payload.aggregate_thresholded_loadings);
  const displayFactorVarianceContributions = normalizeCparVarianceContributions(
    payload.display_factor_variance_contributions ?? payload.factor_variance_contributions,
  );
  const factorVarianceContributions = normalizeCparVarianceContributions(payload.factor_variance_contributions);
  const displayCovMatrix = payload.display_cov_matrix ? normalizeCparCovMatrix(payload.display_cov_matrix) : undefined;
  const hasDisplayFactorChartField = Object.prototype.hasOwnProperty.call(payload, "display_factor_chart");
  const displayFactorChart = normalizeCparFactorChartRows(
    payload.display_factor_chart ?? payload.factor_chart,
  );
  const hasFactorChartField = Object.prototype.hasOwnProperty.call(payload, "factor_chart");
  const factorChart = normalizeCparFactorChartRows(payload.factor_chart);
  return {
    ...payload,
    aggregate_display_loadings: aggregateDisplayLoadings,
    aggregate_thresholded_loadings: aggregateThresholdedLoadings,
    coverage_breakdown: normalizeCparCoverageBreakdown(payload.coverage_breakdown || EMPTY_CPAR_COVERAGE_BREAKDOWN),
    display_cov_matrix: displayCovMatrix,
    cov_matrix: normalizeCparCovMatrix(payload.display_cov_matrix ?? payload.cov_matrix),
    display_factor_variance_contributions: displayFactorVarianceContributions,
    factor_variance_contributions: factorVarianceContributions,
    display_factor_chart: hasDisplayFactorChartField
      ? displayFactorChart
      : deriveCparFactorChartRows({
          aggregate_loadings: aggregateDisplayLoadings,
          factor_variance_contributions: displayFactorVarianceContributions,
          positions,
          contribution_field: "display_contributions",
        }),
    factor_chart: hasFactorChartField
      ? factorChart
      : deriveCparFactorChartRows({
          aggregate_loadings: aggregateThresholdedLoadings,
          factor_variance_contributions: factorVarianceContributions,
          positions,
          contribution_field: "thresholded_contributions",
        }),
    positions,
  };
}

export function normalizeCparPortfolioHedgeData(
  portfolio: CparPortfolioHedgeData | null | undefined,
): CparPortfolioHedgeData | null {
  if (!portfolio) return null;
  return normalizeCparRiskLikeData(portfolio);
}

export function normalizeCparPortfolioHedgeRecommendationData(
  portfolio: CparPortfolioHedgeRecommendationData | null | undefined,
): CparPortfolioHedgeRecommendationData | null {
  if (!portfolio) return null;
  return normalizeCparRiskLikeData(portfolio) as CparPortfolioHedgeRecommendationData;
}

export function normalizeCparRiskData(
  risk: CparRiskData | null | undefined,
): CparRiskData | null {
  if (!risk) return null;
  return normalizeCparRiskLikeData(risk);
}

export function normalizeCparPortfolioWhatIfData(
  whatIf: CparPortfolioWhatIfData | null | undefined,
): CparPortfolioWhatIfData | null {
  if (!whatIf) return null;
  return {
    ...whatIf,
    current: normalizeCparPortfolioHedgeData(whatIf.current) as CparPortfolioHedgeData,
    hypothetical: normalizeCparPortfolioHedgeData(whatIf.hypothetical) as CparPortfolioHedgeData,
  };
}

export function readCparError(error: unknown): CparErrorSummary {
  if (error instanceof ApiError) {
    const detail = error.detail as CparApiErrorDetail | string | null;
    if (error.status === 503 && detail && typeof detail === "object") {
      if (detail.error === "cpar_not_ready") {
        return {
          kind: "not_ready",
          message: String(detail.message || error.message),
          statusCode: error.status,
          buildProfile: String(detail.build_profile || "") || null,
        };
      }
      if (detail.error === "cpar_authority_unavailable") {
        return {
          kind: "unavailable",
          message: String(detail.message || error.message),
          statusCode: error.status,
          buildProfile: null,
        };
      }
    }
    if (error.status === 409) {
      return {
        kind: "ambiguous",
        message: typeof detail === "string" ? detail : error.message,
        statusCode: error.status,
        buildProfile: null,
      };
    }
    if (error.status === 404) {
      return {
        kind: "missing",
        message: typeof detail === "string" ? detail : error.message,
        statusCode: error.status,
        buildProfile: null,
      };
    }
    return {
      kind: "unknown",
      message: typeof detail === "string" ? detail : error.message,
      statusCode: error.status,
      buildProfile: null,
    };
  }
  if (error instanceof Error) {
    return {
      kind: "unknown",
      message: error.message,
      statusCode: null,
      buildProfile: null,
    };
  }
  return {
    kind: "unknown",
    message: "Unknown cPAR frontend error.",
    statusCode: null,
    buildProfile: null,
  };
}

export function readCparDependencyErrorMessage(error: unknown): string {
  if (error instanceof ApiError) return error.message;
  if (error instanceof Error) return error.message;
  return "Unknown cPAR dependency error.";
}
