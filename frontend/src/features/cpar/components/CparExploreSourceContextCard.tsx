"use client";

import { formatCparPackageDate } from "@/lib/cparTruth";
import type { CparSourceContext, CparTickerDetailData } from "@/lib/types/cpar";

function metric(label: string, value: string, detail?: string) {
  return (
    <div className="cpar-package-metric">
      <div className="cpar-package-label">{label}</div>
      <div className="cpar-package-value">{value}</div>
      {detail ? <div className="cpar-package-detail">{detail}</div> : null}
    </div>
  );
}

function formatPrice(value: number | null | undefined, currency?: string | null): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: currency || "USD",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  } catch {
    return `${currency || "PX"} ${value.toFixed(2)}`;
  }
}

function sourceContextMessage(context: CparSourceContext): { tone: "neutral" | "warning"; title: string; body: string } | null {
  if (context.status === "ok") return null;
  if (context.status === "unavailable") {
    return {
      tone: "warning",
      title: "Supplemental shared-source context is temporarily unavailable.",
      body: "Persisted cPAR fit detail still comes from the active package row. Reload later to restore the package-date source context block.",
    };
  }
  if (context.status === "missing") {
    return {
      tone: "neutral",
      title: "No shared-source context rows were found on or before the active package date.",
      body: "The persisted cPAR fit row remains available even when this supplemental context block is empty.",
    };
  }
  return {
    tone: "neutral",
    title: "Supplemental shared-source context is only partially available.",
    body:
      context.reason === "shared_source_unavailable"
        ? "Some shared-source context reads were unavailable, so this block only renders the package-date context that was still readable."
        : context.reason === "mixed"
          ? "Some package-date context rows were missing and some shared-source reads were unavailable."
          : "Some package-date context rows were not available for this instrument on or before the active package date.",
  };
}

export default function CparExploreSourceContextCard({
  detail,
  embedded = false,
}: {
  detail: CparTickerDetailData;
  embedded?: boolean;
}) {
  const context: CparSourceContext = detail.source_context ?? {
    status: "missing",
    reason: "missing_rows",
    latest_common_name: null,
    classification_snapshot: null,
    latest_price_context: null,
  };
  const classification = context.classification_snapshot;
  const latestPrice = context.latest_price_context;
  const commonName = context.latest_common_name;
  const note = sourceContextMessage(context);
  const classificationValue = [
    classification?.trbc_economic_sector,
    classification?.trbc_industry_group,
    classification?.trbc_activity,
  ].filter(Boolean).join(" · ") || "—";

  return (
    <section
      className={embedded ? "cpar-explore-context-panel" : "chart-card cpar-explore-context-panel standalone"}
      data-testid="cpar-source-context-card"
    >
      <div className="cpar-explore-context-head">
        <div>
          <div className="cpar-explore-panel-title">Package-Date Source Context</div>
          <div className="cpar-explore-panel-subtitle">
            Supplemental shared-source context pinned to {formatCparPackageDate(detail.package_date)}. This does not
            change the persisted cPAR fit identity, loadings, or hedge semantics.
          </div>
        </div>
        <div className="cpar-detail-chip">Supplemental only</div>
      </div>
      {note ? (
        <div className={`cpar-inline-message ${note.tone}`}>
          <strong>{note.title}</strong>
          <span>{note.body}</span>
        </div>
      ) : null}
      <div className="explore-quote-data-grid compact cpar-explore-context-grid">
        {metric(
          "Common Name",
          commonName?.value || "—",
          commonName ? `As of ${formatCparPackageDate(commonName.as_of_date)}` : "No row on or before the package date",
        )}
        {metric(
          "Classification",
          classificationValue,
          classification
            ? `As of ${formatCparPackageDate(classification.as_of_date)}`
            : "No classification row on or before the package date",
        )}
        {metric(
          "Latest Source Price",
          latestPrice ? formatPrice(latestPrice.price, latestPrice.currency) : "—",
          latestPrice
            ? `${formatCparPackageDate(latestPrice.price_date)} · ${latestPrice.price_field_used || "price"}${latestPrice.currency ? ` · ${latestPrice.currency}` : ""}`
            : "No price row on or before the package date",
        )}
      </div>
    </section>
  );
}
