"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";
import { exposureMethodLabel, normalizeExposureOrigin } from "@/lib/exposureOrigin";
import type { FactorCatalogEntry, FactorExposure, UniverseTickerItem, WeeklyPricePoint } from "@/lib/types/cuse4";

const ExposureBarChart = dynamic(() => import("@/features/cuse4/components/ExposureBarChart"), {
  ssr: false,
});
const TickerWeeklyPriceChart = dynamic(() => import("@/features/cuse4/components/TickerWeeklyPriceChart"), {
  ssr: false,
});

interface PositionSummary {
  shares: number;
  weight: number;
  market_value: number;
  long_short: string;
}

function formatMoney(value: number | null | undefined, digits = 2): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return `$${value.toFixed(digits)}`;
}

function formatCompactCurrency(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  const abs = Math.abs(value);
  if (abs >= 1e12) return `$${(value / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `$${(value / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `$${(value / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `$${(value / 1e3).toFixed(1)}K`;
  return `$${value.toFixed(0)}`;
}

function formatCompactNumber(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  const abs = Math.abs(value);
  if (abs >= 1e12) return `${(value / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${(value / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${(value / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${(value / 1e3).toFixed(1)}K`;
  return value.toFixed(0);
}

function formatPercent(value: number | null | undefined, digits = 2): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return `${(value * 100).toFixed(digits)}%`;
}

function formatFixed(value: number | null | undefined, digits = 4): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return value.toFixed(digits);
}

function formatShares(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
}

function formatDateLabel(value: string | null | undefined): string {
  const raw = String(value || "").trim();
  if (!raw) return "—";
  const parsed = new Date(`${raw}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) return raw;
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(parsed);
}

function humanizeReason(value: string | null | undefined): string {
  const raw = String(value || "").trim();
  if (!raw) return "—";
  return raw.replaceAll("_", " ");
}

export default function TickerQuoteCard({
  item,
  expanded,
  onExpandedChange,
  historyRequested,
  selectedPosition,
  historyPoints,
  historyLoading,
  historyError,
  chartFactors,
  factorCatalog,
  factorVisualsLoading,
  factorVisualsUnavailable,
}: {
  item: UniverseTickerItem;
  expanded: boolean;
  onExpandedChange: (expanded: boolean) => void;
  historyRequested: boolean;
  selectedPosition?: PositionSummary;
  historyPoints: WeeklyPricePoint[];
  historyLoading: boolean;
  historyError: unknown;
  chartFactors: FactorExposure[];
  factorCatalog?: FactorCatalogEntry[];
  factorVisualsLoading: boolean;
  factorVisualsUnavailable: boolean;
}) {
  const [spotlight, setSpotlight] = useState(false);
  const modelStatus = item.model_status ?? "ineligible";
  const exposureOrigin = normalizeExposureOrigin(item.exposure_origin, modelStatus);
  const hasModelExposures = chartFactors.length > 0;
  const quoteSource = String(item.quote_source || "").trim();

  useEffect(() => {
    onExpandedChange(false);
    setSpotlight(true);
    const timer = window.setTimeout(() => setSpotlight(false), 2400);
    return () => window.clearTimeout(timer);
  }, [item.ticker, onExpandedChange]);

  const historySummary = useMemo(() => {
    if (!historyPoints.length) return null;
    const first = Number(historyPoints[0]?.close ?? 0);
    const latest = Number(historyPoints[historyPoints.length - 1]?.close ?? 0);
    const totalReturnPct = first > 0 ? ((latest / first) - 1) * 100 : null;
    return {
      latest,
      totalReturnPct,
      isPositive: (totalReturnPct ?? 0) >= 0,
      firstDate: historyPoints[0]?.date ?? null,
      lastDate: historyPoints[historyPoints.length - 1]?.date ?? null,
    };
  }, [historyPoints]);

  const returnTone = historySummary
    ? (historySummary.isPositive ? "positive" : "negative")
    : "muted";
  const returnText = !historyRequested
    ? "Open for 5Y trend"
    : historyLoading
      ? "Loading 5Y trend..."
      : historySummary?.totalReturnPct != null
        ? `${historySummary.totalReturnPct >= 0 ? "+" : ""}${historySummary.totalReturnPct.toFixed(1)}%`
        : "—";
  const sharesOutstanding = (
    typeof item.market_cap === "number"
      && Number.isFinite(item.market_cap)
      && typeof item.price === "number"
      && Number.isFinite(item.price)
      && item.price > 0
  )
    ? item.market_cap / item.price
    : null;

  const triggerMetrics: Array<{ label: string; value: string; tone: string }> = [
    { label: "Price", value: formatMoney(item.price), tone: "strong" },
    { label: "Risk", value: formatFixed(item.risk_loading, 4), tone: "" },
  ];
  if (selectedPosition) {
    triggerMetrics.splice(1, 0, {
      label: "Shares",
      value: formatShares(selectedPosition.shares),
      tone: "positive",
    });
    triggerMetrics.splice(2, 0, {
      label: "Mkt Val",
      value: formatCompactCurrency(selectedPosition.market_value),
      tone: "",
    });
  }

  const detailRows: Array<{ label: string; value: string }> = [
    { label: "Risk Tier", value: String(item.risk_tier_label || "—") },
    { label: "Tier Detail", value: String(item.risk_tier_detail || "—") },
    { label: "Quote Source", value: String(item.quote_source_label || "—") },
    { label: "Market Cap", value: formatCompactCurrency(item.market_cap) },
    { label: "Shares Out", value: formatCompactNumber(sharesOutstanding) },
    { label: "As Of", value: formatDateLabel(item.as_of_date) },
    { label: "Specific Vol", value: formatPercent(item.specific_vol, 2) },
    { label: "Specific Var", value: formatFixed(item.specific_var, 6) },
    {
      label: "Exposure Method",
      value: exposureMethodLabel(exposureOrigin, modelStatus, {
        projectionOutputStatus: item.projection_output_status,
        servedExposureAvailable: item.served_exposure_available,
      }),
    },
    ...(exposureOrigin === "projected_returns" && item.projection_r_squared != null
      ? [{ label: "Projection R\u00B2", value: formatFixed(item.projection_r_squared, 4) }]
      : []),
    ...(exposureOrigin === "projected_returns" && item.projection_obs_count != null
      ? [{ label: "Obs Count", value: String(item.projection_obs_count) }]
      : []),
    ...(exposureOrigin === "projected_returns" && item.projection_asof
      ? [{ label: "Projection As Of", value: formatDateLabel(item.projection_asof) }]
      : []),
  ];
  const noteMessage = (
    quoteSource === "registry_runtime"
      ? String(item.quote_source_detail || "").trim()
      : String(item.model_warning || "").trim()
  ) || (!hasModelExposures
    ? `Model ineligible: ${humanizeReason(item.model_status_reason || item.eligibility_reason)}`
    : "");

  return (
    <section className={`explore-quote-module${expanded ? " open" : ""}${spotlight && !expanded ? " fresh" : ""}`}>
      <button
        type="button"
        className={`explore-quote-trigger${expanded ? " open" : ""}`}
        onClick={() => onExpandedChange(!expanded)}
        aria-expanded={expanded}
      >
        <span className="explore-quote-trigger-copy">
          <span className="explore-quote-trigger-kicker">Quote</span>
          <span className="explore-quote-trigger-title">
            <span className="ticker">{item.ticker}</span>
            <span className="name">{item.name}</span>
          </span>
          <span className="explore-quote-trigger-meta">
            {item.trbc_economic_sector_short || "Unclassified"}
            {item.trbc_industry_group ? ` • ${item.trbc_industry_group}` : ""}
          </span>
        </span>

        <span className="explore-quote-trigger-strip">
          {triggerMetrics.map((metric) => (
            <span key={metric.label} className="explore-quote-trigger-metric">
              <span className="explore-quote-trigger-metric-label">{metric.label}</span>
              <span className={`explore-quote-trigger-metric-value${metric.tone ? ` ${metric.tone}` : ""}`}>
                {metric.value}
              </span>
            </span>
          ))}
        </span>

        <span className="explore-quote-trigger-action">
          <span className="explore-quote-trigger-action-copy">
            <span className="explore-quote-trigger-action-label">
              {expanded ? "Hide quote" : "Show quote"}
            </span>
            <span className="explore-quote-trigger-action-hint">
              {expanded ? "Collapse overlay" : "Open overlay"}
            </span>
          </span>
          <span className="explore-quote-trigger-glyph" aria-hidden="true">+</span>
        </span>
      </button>

      <div className="explore-quote-overlay" aria-hidden={!expanded}>
        <div className="explore-quote-overlay-card">
          <div className="explore-quote-overlay-body">
            <div className="explore-quote-overlay-left">
              <div className="explore-quote-spark-panel">
                <div className="explore-quote-spark-summary">
                  <span className={`explore-quote-spark-latest explore-quote-return ${returnTone}`}>
                    {returnText === "—"
                      ? "5Y trend unavailable"
                      : returnText.startsWith("Open") || returnText.startsWith("Loading")
                        ? returnText
                        : `5Y ${returnText}`}
                  </span>
                  <span className="explore-quote-spark-range">
                    {!historyRequested
                      ? "Expanded view loads 5Y history"
                      : historyLoading
                        ? "Fetching history range"
                        : historySummary?.firstDate && historySummary?.lastDate
                      ? `${formatDateLabel(historySummary.firstDate)} to ${formatDateLabel(historySummary.lastDate)}`
                      : "No history range"}
                  </span>
                </div>

                {!historyRequested ? (
                  <div className="explore-quote-spark-empty">Expand again once opened to view the 5Y trend.</div>
                ) : historyLoading ? (
                  <div className="explore-quote-spark-empty loading-pulse">Loading 5Y history...</div>
                ) : historyError ? (
                  <div className="explore-quote-spark-empty">5Y history is temporarily unavailable.</div>
                ) : (
                  <TickerWeeklyPriceChart
                    ticker={item.ticker}
                    points={historyPoints}
                    variant="sparkline"
                    className="explore-quote-sparkline"
                  />
                )}
              </div>

              <div className="explore-quote-data-panel">
                <div className="explore-quote-data-grid compact">
                  {detailRows.map((row) => (
                    <div key={row.label} className="explore-quote-row">
                      <span className="explore-quote-row-label">{row.label}</span>
                      <span className="explore-quote-row-value">{row.value}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            <div className="explore-quote-chart-panel">
              <div className="explore-quote-chart-head">
                <span>Factor Exposures</span>
                <span>
                  {factorVisualsLoading && hasModelExposures
                    ? "Enhancing labels"
                    : factorVisualsUnavailable
                      ? hasModelExposures
                        ? "Fallback labels"
                        : "Temporarily unavailable"
                      : hasModelExposures
                        ? "All Factors"
                        : "Unavailable"}
                </span>
              </div>
              {expanded && hasModelExposures && (!factorVisualsLoading || factorCatalog?.length || factorVisualsUnavailable) ? (
                <div className="explore-quote-chart-scroll">
                  <ExposureBarChart factors={chartFactors} factorCatalog={factorCatalog} />
                </div>
              ) : (
                <div className="explore-quote-chart-empty">
                  {expanded && hasModelExposures && factorVisualsLoading
                    ? "Loading factor metadata..."
                    : hasModelExposures
                    ? "Expand to load factor exposures."
                    : factorVisualsUnavailable
                      ? "Factor visuals are temporarily unavailable."
                      : "Factor exposures are unavailable for this ticker."}
                </div>
              )}
            </div>
          </div>

          {noteMessage && (
            <div className="explore-quote-note">
              {noteMessage}
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
