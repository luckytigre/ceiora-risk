"use client";

import type { ReactNode } from "react";
import { formatCparNumber, formatCparPackageDate } from "@/lib/cparTruth";
import type { CparTickerDetailData } from "@/lib/types/cpar";
import CparExploreSourceContextCard from "./CparExploreSourceContextCard";
import CparWarningsBar from "./CparWarningsBar";

function stat(label: string, value: string) {
  return (
    <div className="explore-hero-stat">
      <span className="label">{label}</span>
      <span className="value">{value}</span>
    </div>
  );
}

function row(label: string, value: string) {
  return (
    <div className="explore-quote-row">
      <span className="explore-quote-row-label">{label}</span>
      <span className="explore-quote-row-value">{value}</span>
    </div>
  );
}

export default function CparExploreDetailModule({
  detail,
  footer,
}: {
  detail: CparTickerDetailData;
  footer?: ReactNode;
}) {
  return (
    <section className="chart-card cpar-explore-detail-module" data-testid="cpar-detail-panel">
      <div className="cpar-explore-module-header">
        <div>
          <div className="cpar-explore-kicker">Persisted Fit Detail</div>
          <h3 className="cpar-explore-module-title">Selected Instrument</h3>
          <div className="cpar-explore-module-subtitle">
            One active-package fit row, its persisted metadata, and supplemental package-date source context for the
            same instrument.
          </div>
        </div>
        <div className="cpar-explore-module-status">{formatCparPackageDate(detail.package_date)}</div>
      </div>

      <div className="cpar-explore-detail-head">
        <div className="cpar-explore-detail-copy">
          <div className="explore-hero-title cpar-explore-hero-title">
            <span className="ticker">{detail.ticker || detail.ric}</span>
            <span className="name">{detail.display_name || detail.ric}</span>
          </div>
          <div className="cpar-explore-detail-subtitle">
            {detail.ric} · HQ {detail.hq_country_code || "—"} · Persisted fit status {detail.fit_status.replaceAll("_", " ")}
          </div>
        </div>
        <CparWarningsBar fitStatus={detail.fit_status} warnings={detail.warnings} />
      </div>

      <div className="cpar-badge-row cpar-explore-detail-badges">
        <span className="cpar-detail-chip">Ticker-keyed detail route</span>
        <span className="cpar-detail-chip">Package date {formatCparPackageDate(detail.package_date)}</span>
        <span className="cpar-detail-chip">Source prices {formatCparPackageDate(detail.source_prices_asof)}</span>
      </div>

      <div className="explore-hero-stats cpar-explore-hero-stats">
        {stat("Observed", `${detail.observed_weeks}w`)}
        {stat("Longest Gap", `${detail.longest_gap_weeks}w`)}
        {stat("Pre-Hedge Vol", formatCparNumber(detail.pre_hedge_factor_volatility_proxy, 3))}
        {stat("SPY Trade Beta", formatCparNumber(detail.beta_spy_trade, 3))}
      </div>

      <div className="explore-detail-grid cpar-explore-detail-grid">
        <div className="cpar-explore-facts-panel">
          <div className="cpar-explore-panel-title">Persisted Fit Facts</div>
          <div className="cpar-explore-panel-subtitle">
            These values come from the active-package fit row and stay authoritative even when supplemental source
            context is partial or unavailable.
          </div>
          <div className="explore-quote-data-grid compact cpar-explore-facts-grid">
            {row("Ticker", detail.ticker || "—")}
            {row("RIC", detail.ric)}
            {row("Package Date", formatCparPackageDate(detail.package_date))}
            {row("Price Field", detail.price_field_used || "—")}
            {row("Lookback", `${detail.lookback_weeks}w`)}
            {row("Observed", `${detail.observed_weeks}w`)}
            {row("Longest Gap", `${detail.longest_gap_weeks}w`)}
            {row("Classification As Of", formatCparPackageDate(detail.classification_asof))}
          </div>
        </div>

        <CparExploreSourceContextCard detail={detail} embedded />
      </div>

      {footer ? <div className="cpar-explore-detail-footer">{footer}</div> : null}
    </section>
  );
}
