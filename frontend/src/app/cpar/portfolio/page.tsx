"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import CparLoadingsTable from "@/features/cpar/components/CparLoadingsTable";
import CparPackageBanner from "@/features/cpar/components/CparPackageBanner";
import CparPortfolioCoverageTable from "@/features/cpar/components/CparPortfolioCoverageTable";
import CparPortfolioHedgePanel from "@/features/cpar/components/CparPortfolioHedgePanel";
import { ApiError } from "@/lib/api";
import { useCparMeta, useCparPortfolioHedge, useHoldingsAccounts } from "@/hooks/useApi";
import { formatCparNumber, formatCparPercent, readCparError, sameCparPackageIdentity } from "@/lib/cparTruth";
import type { CparHedgeMode } from "@/lib/types";

function genericErrorMessage(error: unknown): string {
  if (error instanceof ApiError) return error.message;
  if (error instanceof Error) return error.message;
  return "Unknown holdings/account error.";
}

function CparPortfolioPageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const selectedAccountParam = searchParams?.get("account_id")?.trim() || null;
  const [mode, setMode] = useState<CparHedgeMode>("factor_neutral");

  const { data: meta, error: metaError, isLoading: metaLoading } = useCparMeta();
  const metaState = metaError ? readCparError(metaError) : null;
  const { data: accountsData, error: accountsError, isLoading: accountsLoading } = useHoldingsAccounts();

  const defaultAccountId = useMemo(() => {
    const accounts = accountsData?.accounts ?? [];
    return (
      accounts.find((row) => row.is_active && row.positions_count > 0)?.account_id
      || accounts.find((row) => row.positions_count > 0)?.account_id
      || accounts[0]?.account_id
      || null
    );
  }, [accountsData?.accounts]);

  useEffect(() => {
    if (selectedAccountParam || !defaultAccountId) return;
    const params = new URLSearchParams(searchParams?.toString() || "");
    params.set("account_id", defaultAccountId);
    router.replace(`/cpar/portfolio?${params.toString()}`);
  }, [defaultAccountId, router, searchParams, selectedAccountParam]);

  const selectedAccountId = selectedAccountParam || defaultAccountId;
  const {
    data: portfolio,
    error: portfolioError,
    isLoading: portfolioLoading,
  } = useCparPortfolioHedge(selectedAccountId, mode, Boolean(selectedAccountId) && Boolean(meta) && !metaState);

  if (metaLoading && !meta) {
    return <AnalyticsLoadingViz message="Loading cPAR portfolio hedge workflow..." />;
  }

  const portfolioState = portfolioError ? readCparError(portfolioError) : null;
  const packageMismatch = Boolean(meta && portfolio && !sameCparPackageIdentity(meta, portfolio));
  const selectedAccount = (accountsData?.accounts || []).find((row) => row.account_id === selectedAccountId) || null;

  return (
    <div className="cpar-page">
      <section className="cpar-page-header">
        <div className="cpar-section-kicker">cPAR / Portfolio</div>
        <h1>Account Hedge Workflow</h1>
        <p className="cpar-page-copy">
          This is the first narrow portfolio-level cPAR flow: one holdings account, one active cPAR package, one read-only hedge preview derived from covered holdings rows only.
        </p>
      </section>

      {meta ? (
        <CparPackageBanner
          meta={meta}
          factors={meta.factors}
          title="Current Portfolio Hedge Package"
          subtitle="The account hedge workflow reuses the active persisted package and the live holdings account selected below. It does not reuse cUSE4 what-if semantics."
        />
      ) : null}

      {metaState ? (
        <section className="chart-card cpar-alert-card" data-testid="cpar-portfolio-not-ready">
          <h3>{metaState.kind === "not_ready" ? "cPAR Portfolio Not Ready" : "cPAR Portfolio Unavailable"}</h3>
          <div className="section-subtitle">{metaState.message}</div>
          <div className="detail-history-empty compact">
            This workflow is package-based and read-only. Publish a durable cPAR package first, then reload.
          </div>
        </section>
      ) : null}

      <div className="cpar-two-column">
        <section className="chart-card" data-testid="cpar-portfolio-account-panel">
          <h3>Account Scope</h3>
          <div className="section-subtitle">
            Reused infrastructure: live holdings accounts and positions. Not reused: cUSE4 portfolio or what-if payload semantics.
          </div>
          {accountsLoading && !accountsData ? (
            <AnalyticsLoadingViz message="Loading holdings accounts..." />
          ) : accountsError ? (
            <div className="cpar-inline-message error">
              <strong>Holdings accounts unavailable.</strong>
              <span>{genericErrorMessage(accountsError)}</span>
            </div>
          ) : !(accountsData?.accounts.length) ? (
            <div className="detail-history-empty compact">No holdings accounts are available yet.</div>
          ) : (
            <>
              <label className="cpar-package-label" htmlFor="cpar-account-select">Selected account</label>
              <div className="cpar-search-row">
                <select
                  id="cpar-account-select"
                  className="cpar-search-input"
                  data-testid="cpar-portfolio-account-select"
                  value={selectedAccountId || ""}
                  onChange={(event) => {
                    const nextAccountId = event.target.value;
                    const params = new URLSearchParams(searchParams?.toString() || "");
                    params.set("account_id", nextAccountId);
                    router.push(`/cpar/portfolio?${params.toString()}`);
                  }}
                >
                  {(accountsData?.accounts || []).map((account) => (
                    <option key={account.account_id} value={account.account_id}>
                      {account.account_id} · {account.account_name} [{account.positions_count}]
                    </option>
                  ))}
                </select>
              </div>
              {selectedAccount ? (
                <div className="cpar-package-grid compact">
                  <div className="cpar-package-metric">
                    <div className="cpar-package-label">Account</div>
                    <div className="cpar-package-value">{selectedAccount.account_id}</div>
                    <div className="cpar-package-detail">{selectedAccount.account_name}</div>
                  </div>
                  <div className="cpar-package-metric">
                    <div className="cpar-package-label">Positions</div>
                    <div className="cpar-package-value">{selectedAccount.positions_count}</div>
                    <div className="cpar-package-detail">Current holdings rows</div>
                  </div>
                  <div className="cpar-package-metric">
                    <div className="cpar-package-label">Last Update</div>
                    <div className="cpar-package-value">{selectedAccount.last_position_updated_at ? "Live" : "—"}</div>
                    <div className="cpar-package-detail">{selectedAccount.last_position_updated_at || "No positions yet"}</div>
                  </div>
                </div>
              ) : null}
            </>
          )}
        </section>

        <section className="chart-card" data-testid="cpar-portfolio-summary">
          <h3>Workflow Scope</h3>
          <div className="section-subtitle">
            The account workflow prices current holdings at the latest shared-source price on or before the active package date, then aggregates only covered persisted cPAR loadings into one hedge vector.
          </div>
          <div className="cpar-inline-message neutral">
            <strong>Narrow by design.</strong>
            <span>This is not a cPAR what-if engine, not a portfolio mutation tool, and not a cPAR-vs-cUSE4 comparison layer.</span>
            <div className="cpar-badge-row compact">
              <Link href="/cpar/hedge" className="cpar-detail-chip" prefetch={false}>Instrument Hedge</Link>
              <Link href="/cpar/explore" className="cpar-detail-chip" prefetch={false}>Instrument Explore</Link>
            </div>
          </div>
        </section>
      </div>

      {!selectedAccountId && !accountsLoading ? (
        <section className="chart-card">
          <h3>Account Hedge Preview</h3>
          <div className="detail-history-empty compact">Choose a holdings account to open the read-only cPAR portfolio hedge workflow.</div>
        </section>
      ) : metaState || accountsError ? null : portfolioLoading && !portfolio ? (
        <section className="chart-card" data-testid="cpar-portfolio-loading">
          <h3>Account Hedge Preview</h3>
          <AnalyticsLoadingViz message={`Loading cPAR portfolio hedge for ${selectedAccountId}...`} />
        </section>
      ) : portfolioState ? (
        <section className="chart-card" data-testid="cpar-portfolio-error">
          <h3>Account Hedge Preview</h3>
          <div className={`cpar-inline-message ${portfolioState.kind === "missing" ? "warning" : "error"}`}>
            <strong>
              {portfolioState.kind === "missing"
                ? "Account not found."
                : portfolioState.kind === "not_ready"
                  ? "Portfolio package not ready."
                  : "Portfolio hedge unavailable."}
            </strong>
            <span>{portfolioState.message}</span>
          </div>
        </section>
      ) : packageMismatch ? (
        <section className="chart-card" data-testid="cpar-portfolio-package-mismatch">
          <h3>Account Hedge Preview</h3>
          <div className="cpar-inline-message error">
            <strong>Active package changed during read.</strong>
            <span>The portfolio workflow no longer matches the current package banner.</span>
            <span>Reload the page to pin one cPAR package before using the account hedge preview.</span>
          </div>
        </section>
      ) : portfolio ? (
        <>
          <section className="chart-card" data-testid="cpar-portfolio-overview">
            <h3>Account Coverage Summary</h3>
            <div className="cpar-badge-row compact">
              <span className={`cpar-badge ${
                portfolio.portfolio_status === "ok"
                  ? "success"
                  : portfolio.portfolio_status === "partial"
                    ? "warning"
                    : "error"
              }`}>
                {portfolio.portfolio_status === "ok"
                  ? "Coverage OK"
                  : portfolio.portfolio_status === "partial"
                    ? "Partial Coverage"
                    : portfolio.portfolio_status === "empty"
                      ? "Empty Account"
                      : "Coverage Unavailable"}
              </span>
              <span className="cpar-detail-chip">{portfolio.covered_positions_count} covered</span>
              <span className="cpar-detail-chip">{portfolio.excluded_positions_count} excluded</span>
              <span className="cpar-detail-chip">Priced coverage {formatCparPercent(portfolio.coverage_ratio, 1)}</span>
            </div>
            <div className="cpar-package-grid compact">
              <div className="cpar-package-metric">
                <div className="cpar-package-label">Gross MV</div>
                <div className="cpar-package-value">{formatCparNumber(portfolio.gross_market_value, 2)}</div>
                <div className="cpar-package-detail">Priced holdings rows only</div>
              </div>
              <div className="cpar-package-metric">
                <div className="cpar-package-label">Covered Gross</div>
                <div className="cpar-package-value">{formatCparNumber(portfolio.covered_gross_market_value, 2)}</div>
                <div className="cpar-package-detail">Rows included in the hedge vector</div>
              </div>
              <div className="cpar-package-metric">
                <div className="cpar-package-label">Net MV</div>
                <div className="cpar-package-value">{formatCparNumber(portfolio.net_market_value, 2)}</div>
                <div className="cpar-package-detail">Covered rows only</div>
              </div>
            </div>
            {portfolio.portfolio_reason ? (
              <div className="cpar-inline-message warning">
                <strong>Workflow note.</strong>
                <span>{portfolio.portfolio_reason}</span>
              </div>
            ) : null}
          </section>

          {portfolio.aggregate_thresholded_loadings.length > 0 ? (
            <div className="cpar-two-column">
              <CparLoadingsTable
                title="Aggregate Thresholded Loadings"
                rows={portfolio.aggregate_thresholded_loadings}
                emptyText="No covered holdings rows contributed to the aggregate thresholded portfolio vector."
              />
              <section className="chart-card">
                <h3>Scope Notes</h3>
                <div className="section-subtitle">
                  Aggregate loadings are weighted by signed market value over covered gross market value, using the current holdings quantities and latest shared-source prices on or before the package date. The displayed coverage ratio is covered gross divided by priced gross.
                </div>
                <div className="cpar-inline-message neutral">
                  <strong>No mutation path.</strong>
                  <span>The account workflow is read-only. Change holdings elsewhere, publish the active package if needed, then reload this page.</span>
                </div>
              </section>
            </div>
          ) : null}

          {portfolio.portfolio_status === "ok" || portfolio.portfolio_status === "partial" ? (
            <CparPortfolioHedgePanel
              data={portfolio}
              mode={mode}
              onModeChange={setMode}
            />
          ) : null}

          <CparPortfolioCoverageTable rows={portfolio.positions} />
        </>
      ) : null}
    </div>
  );
}

export default function CparPortfolioPage() {
  return (
    <Suspense fallback={<AnalyticsLoadingViz message="Loading cPAR portfolio hedge workflow..." />}>
      <CparPortfolioPageInner />
    </Suspense>
  );
}
