"use client";

import { describeCparFitStatus, describeCparWarning } from "@/lib/cparTruth";
import type { CparFitStatus, CparWarning } from "@/lib/types/cpar";

export default function CparWarningsBar({
  fitStatus,
  warnings,
  compact = false,
}: {
  fitStatus?: CparFitStatus | null;
  warnings?: CparWarning[] | null;
  compact?: boolean;
}) {
  const fit = fitStatus ? describeCparFitStatus(fitStatus) : null;
  const warningBadges = (warnings || []).map(describeCparWarning);
  if (!fit && warningBadges.length === 0) return null;

  return (
    <div className={`cpar-badge-row ${compact ? "compact" : ""}`}>
      {fit && (
        <span className={`cpar-badge ${fit.tone}`} title={fit.detail}>
          {fit.label}
        </span>
      )}
      {warningBadges.map((warning) => (
        <span
          key={warning.label}
          className={`cpar-badge ${warning.tone}`}
          title={warning.detail}
        >
          {warning.label}
        </span>
      ))}
    </div>
  );
}
