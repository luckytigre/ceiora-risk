"use client";

import { useEffect, useMemo, useState } from "react";
import CparExposureBarChart from "@/features/cpar/components/CparExposureBarChart";
import CparTickerPriceChart from "@/features/cpar/components/CparTickerPriceChart";
import { describeCparFitStatus, formatCparPackageDate } from "@/lib/cparTruth";
import { shortFactorLabel } from "@/lib/factorLabels";
import type { FactorCatalogEntry, FactorExposure } from "@/lib/types/analytics";
import type { CparTickerDetailData, CparTickerHistoryPoint } from "@/lib/types/cpar";
import type { CparExplorePositionSummary } from "@/features/cpar/components/cparExploreUtils";

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

function formatShares(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
}

function formatFixed(value: number | null | undefined, digits = 4): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return value.toFixed(digits);
}

function formatPercentValue(value: number | null | undefined, digits = 1): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return `${value.toFixed(digits)}%`;
}

function metricToneFromBadgeTone(tone: "success" | "warning" | "error" | "neutral"): string {
  if (tone === "success") return "positive";
  if (tone === "error") return "negative";
  if (tone === "warning") return "warning";
  return "";
}

function chipClassFromBadgeTone(tone: "success" | "warning" | "error" | "neutral"): string {
  if (tone === "success") return "positive";
  if (tone === "error") return "negative";
  if (tone === "warning") return "warning";
  return "";
}

function resolveDisplayLoadings(item: CparTickerDetailData) {
  return (item.display_loadings?.length ? item.display_loadings : item.thresholded_loadings) || [];
}

function chartFactors(item: CparTickerDetailData): FactorExposure[] {
  return resolveDisplayLoadings(item).map((loading) => ({
    factor_id: loading.factor_id,
    value: Number(loading.beta || 0),
    factor_vol: 0,
    drilldown: [
      {
        ticker: String(item.ticker || item.ric || ""),
        weight: Number(loading.beta || 0) >= 0 ? 1 : -1,
        exposure: Number(loading.beta || 0),
        sensitivity: Number(loading.beta || 0),
        contribution: Number(loading.beta || 0),
        model_status: "core_estimated",
        exposure_origin: "native",
      },
    ],
  }));
}

function factorCatalog(item: CparTickerDetailData): FactorCatalogEntry[] {
  return resolveDisplayLoadings(item).map((loading) => {
    const family = loading.group === "market" ? "market" as const : loading.group === "sector" ? "industry" as const : "style" as const;
    return {
      factor_id: loading.factor_id,
      factor_name: loading.label,
      short_label: shortFactorLabel(loading.label),
      family,
      block: loading.group === "market" ? "Market" : loading.group === "sector" ? "Industry" : "Style",
      display_order: loading.display_order,
      active: true,
    };
  });
}

export default function CparTickerQuoteCard({
  item,
  selectedPosition,
  historyPoints,
  historyLoading,
  historyError,
}: {
  item: CparTickerDetailData;
  selectedPosition?: CparExplorePositionSummary;
  historyPoints: CparTickerHistoryPoint[];
  historyLoading: boolean;
  historyError: unknown;
}) {
  const [expanded, setExpanded] = useState(false);
  const [spotlight, setSpotlight] = useState(false);
  const fit = item.fit_status ? describeCparFitStatus(item.fit_status) : null;
  const tierLabel = item.risk_tier_label || fit?.label || "Unknown";
  const tierTone = fit?.tone || "neutral";

  useEffect(() => {
    setExpanded(true);
    setSpotlight(true);
    const timer = window.setTimeout(() => setSpotlight(false), 2400);
    return () => window.clearTimeout(timer);
  }, [item.ric]);

  const historySummary = useMemo(() => {
    if (!historyPoints.length) return null;
    const first = Number(historyPoints[0]?.close ?? 0);
    const latest = Number(historyPoints[historyPoints.length - 1]?.close ?? 0);
    const totalReturnPct = first > 0 ? ((latest / first) - 1) * 100 : null;
    return {
      totalReturnPct,
      isPositive: (totalReturnPct ?? 0) >= 0,
      firstDate: historyPoints[0]?.date ?? null,
      lastDate: historyPoints[historyPoints.length - 1]?.date ?? null,
    };
  }, [historyPoints]);

  const returnTone = historySummary ? (historySummary.isPositive ? "positive" : "negative") : "muted";
  const returnText = historySummary?.totalReturnPct != null
    ? `${historySummary.totalReturnPct >= 0 ? "+" : ""}${historySummary.totalReturnPct.toFixed(1)}%`
    : "—";
  const metrics: Array<{ label: string; value: string; tone?: string }> = [
    { label: "Price", value: formatMoney(item.source_context.latest_price_context?.price ?? null), tone: "strong" },
    { label: "Tier", value: tierLabel, tone: metricToneFromBadgeTone(tierTone) },
    { label: "Obs", value: String(item.observed_weeks || 0) },
  ];
  if (selectedPosition) {
    metrics.splice(1, 0, {
      label: "Shares",
      value: formatShares(selectedPosition.shares),
      tone: selectedPosition.shares >= 0 ? "positive" : "negative",
    });
    metrics.splice(2, 0, {
      label: "Mkt Val",
      value: formatCompactCurrency(selectedPosition.market_value),
    });
  }

  const detailRows: Array<{ label: string; value: string }> = [
    { label: "RIC", value: item.ric },
    { label: "Risk Tier", value: tierLabel },
    { label: "Tier Detail", value: item.risk_tier_detail || "—" },
    { label: "Quote Source", value: item.quote_source_label || "—" },
    { label: "TRBC Industry", value: item.source_context.classification_snapshot?.trbc_industry_group || "Unmapped" },
    { label: "HQ Country", value: item.hq_country_code || "—" },
    { label: "Package Date", value: formatCparPackageDate(item.package_date) },
    { label: "Price Date", value: formatCparPackageDate(item.source_context.latest_price_context?.price_date || null) },
    { label: "Fit Status", value: fit?.label || "Not in active package" },
    { label: "Observed Weeks", value: String(item.observed_weeks || 0) },
    { label: "Longest Gap", value: String(item.longest_gap_weeks || 0) },
    { label: "Market β", value: formatFixed(item.beta_market_step1 ?? item.beta_spy_trade, 4) },
    { label: "Factor Vol", value: formatPercentValue(item.pre_hedge_factor_volatility_proxy ?? null, 1) },
  ];

  const factors = chartFactors(item);
  const catalog = factorCatalog(item);

  return (
    <section className={`explore-quote-module${expanded ? " open" : ""}${spotlight && !expanded ? " fresh" : ""}`}>
      <button
        type="button"
        className={`explore-quote-trigger${expanded ? " open" : ""}`}
        onClick={() => setExpanded((prev) => !prev)}
        aria-expanded={expanded}
      >
        <span className="explore-quote-trigger-copy">
          <span className="explore-quote-trigger-kicker">Quote</span>
          <span className="explore-quote-trigger-title">
            <span className="ticker">{item.ticker || item.ric}</span>
            <span className="name">{item.display_name || item.ric}</span>
          </span>
          <span className="explore-quote-trigger-meta">
            {item.source_context.classification_snapshot?.trbc_economic_sector || "Unclassified"}
            {item.source_context.classification_snapshot?.trbc_industry_group
              ? ` • ${item.source_context.classification_snapshot.trbc_industry_group}`
              : ""}
          </span>
        </span>

        <span className="explore-quote-trigger-strip">
          {metrics.map((metric) => (
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
                    {returnText === "—" ? "5Y trend unavailable" : `5Y ${returnText}`}
                  </span>
                  <span className="explore-quote-spark-range">
                    {historySummary?.firstDate && historySummary?.lastDate
                      ? `${formatCparPackageDate(historySummary.firstDate)} to ${formatCparPackageDate(historySummary.lastDate)}`
                      : "No history range"}
                  </span>
                </div>

                {historyLoading ? (
                  <div className="explore-quote-spark-empty loading-pulse">Loading 5Y history...</div>
                ) : historyError ? (
                  <div className="explore-quote-spark-empty">5Y history is temporarily unavailable.</div>
                ) : (
                  <CparTickerPriceChart
                    ticker={item.ticker || item.ric}
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
                <span className={`cpar-detail-chip ${chipClassFromBadgeTone(tierTone)}`.trim()}>{tierLabel}</span>
              </div>
              {factors.length > 0 ? (
                <CparExposureBarChart factors={factors} mode="raw" factorCatalog={catalog} />
              ) : (
                <div className="explore-quote-spark-empty">No factor exposures are available for this cPAR fit.</div>
              )}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
