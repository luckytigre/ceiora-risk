"use client";

import type { ReactNode } from "react";
import { formatCparPackageDate } from "@/lib/cparTruth";
import type { CparTickerDetailData } from "@/lib/types/cpar";
import CparWarningsBar from "./CparWarningsBar";

function metric(label: string, value: string, detail?: string) {
  return (
    <div className="cpar-package-metric">
      <div className="cpar-package-label">{label}</div>
      <div className="cpar-package-value">{value}</div>
      {detail ? <div className="cpar-package-detail">{detail}</div> : null}
    </div>
  );
}

export default function CparInstrumentSummaryCard({
  detail,
  title = "Selected Instrument",
  testId = "cpar-detail-panel",
  footer,
}: {
  detail: CparTickerDetailData;
  title?: string;
  testId?: string;
  footer?: ReactNode;
}) {
  return (
    <section className="chart-card" data-testid={testId}>
      <h3>{title}</h3>
      <div className="cpar-detail-header">
        <div>
          <div className="cpar-detail-title">{detail.display_name || detail.ticker || detail.ric}</div>
          <div className="cpar-detail-subtitle">
            {detail.ticker || "—"} · {detail.ric} · HQ {detail.hq_country_code || "—"}
          </div>
        </div>
        <CparWarningsBar fitStatus={detail.fit_status} warnings={detail.warnings} />
      </div>
      <div className="cpar-package-grid compact">
        {metric("Observed", `${detail.observed_weeks}w`, `Longest gap ${detail.longest_gap_weeks}w`)}
        {metric("Price Field", detail.price_field_used || "—", `Package date ${formatCparPackageDate(detail.package_date)}`)}
        {metric(
          "Pre-Hedge Vol",
          detail.pre_hedge_factor_volatility_proxy?.toFixed(3) || "—",
          `Variance ${detail.pre_hedge_factor_variance_proxy?.toFixed(3) || "—"}`,
        )}
        {metric(
          "SPY Trade Beta",
          detail.beta_spy_trade?.toFixed(3) || "—",
          `Market step ${detail.beta_market_step1?.toFixed(3) || "—"}`,
        )}
      </div>
      {footer ? footer : null}
    </section>
  );
}
