"use client";

import { ApiError } from "@/lib/api";
import type {
  CparFactorGroup,
  CparFactorSpec,
  CparFitStatus,
  CparHedgeStatus,
  CparSearchItem,
  CparWarning,
} from "@/lib/types";

type BadgeTone = "success" | "warning" | "error" | "neutral";

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
