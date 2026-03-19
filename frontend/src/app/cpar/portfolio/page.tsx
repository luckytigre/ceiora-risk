"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import CparLoadingsTable from "@/features/cpar/components/CparLoadingsTable";
import CparPortfolioCoverageTable from "@/features/cpar/components/CparPortfolioCoverageTable";
import CparPortfolioHedgePanel from "@/features/cpar/components/CparPortfolioHedgePanel";
import CparSearchPanel from "@/features/cpar/components/CparSearchPanel";
import { ApiError } from "@/lib/api";
import { useCparMeta, useCparPortfolioHedge, useCparPortfolioWhatIf, useHoldingsAccounts } from "@/hooks/useApi";
import {
  canNavigateCparSearchResult,
  formatCparNumber,
  formatCparPercent,
  readCparError,
  sameCparPackageIdentity,
} from "@/lib/cparTruth";
import type { CparHedgeMode, CparSearchItem } from "@/lib/types";

interface CparDraftScenarioRow {
  ric: string;
  ticker: string | null;
  display_name: string | null;
  quantity_delta: number;
}

function genericErrorMessage(error: unknown): string {
  if (error instanceof ApiError) return error.message;
  if (error instanceof Error) return error.message;
  return "Unknown holdings/account error.";
}

function parseQuantityDelta(value: string): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || Math.abs(parsed) <= 1e-12) return null;
  return parsed;
}

function CparPortfolioPageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const selectedAccountParam = searchParams?.get("account_id")?.trim() || null;
  const [mode, setMode] = useState<CparHedgeMode>("factor_neutral");
  const [selectedScenarioItem, setSelectedScenarioItem] = useState<CparSearchItem | null>(null);
  const [quantityDeltaInput, setQuantityDeltaInput] = useState("10");
  const [scenarioRows, setScenarioRows] = useState<CparDraftScenarioRow[]>([]);
  const [scenarioMessage, setScenarioMessage] = useState<string | null>(null);

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
    router.replace(`/cpar/risk?${params.toString()}`);
  }, [defaultAccountId, router, searchParams, selectedAccountParam]);

  const selectedAccountId = selectedAccountParam || defaultAccountId;

  useEffect(() => {
    setScenarioRows([]);
    setSelectedScenarioItem(null);
    setScenarioMessage(null);
  }, [selectedAccountId, meta?.package_run_id]);

  const {
    data: portfolio,
    error: portfolioError,
    isLoading: portfolioLoading,
  } = useCparPortfolioHedge(selectedAccountId, mode, Boolean(selectedAccountId) && Boolean(meta) && !metaState);
  const portfolioState = portfolioError ? readCparError(portfolioError) : null;
  const packageMismatch = Boolean(meta && portfolio && !sameCparPackageIdentity(meta, portfolio));
  const {
    data: whatIf,
    error: whatIfError,
    isLoading: whatIfLoading,
  } = useCparPortfolioWhatIf(
    selectedAccountId,
    mode,
    scenarioRows.map((row) => ({
      ric: row.ric,
      ticker: row.ticker,
      quantity_delta: row.quantity_delta,
    })),
    Boolean(selectedAccountId)
      && Boolean(meta)
      && !metaState
      && Boolean(portfolio)
      && !portfolioState
      && !packageMismatch
      && scenarioRows.length > 0,
  );

  if (metaLoading && !meta) {
    return <AnalyticsLoadingViz message="Loading cPAR portfolio hedge workflow..." />;
  }

  const whatIfState = whatIfError ? readCparError(whatIfError) : null;
  const whatIfPackageMismatch = Boolean(
    (meta && whatIf && !sameCparPackageIdentity(meta, whatIf))
    || (whatIf && !sameCparPackageIdentity(whatIf, whatIf.current))
    || (whatIf && !sameCparPackageIdentity(whatIf, whatIf.hypothetical)),
  );
  const selectedAccount = (accountsData?.accounts || []).find((row) => row.account_id === selectedAccountId) || null;
  const parsedQuantityDelta = parseQuantityDelta(quantityDeltaInput);

  function addScenarioRow() {
    if (!selectedScenarioItem || !canNavigateCparSearchResult(selectedScenarioItem)) {
      setScenarioMessage("Choose an active-package search hit with a ticker before staging a what-if row.");
      return;
    }
    if (parsedQuantityDelta == null) {
      setScenarioMessage("Enter a non-zero finite share delta before staging a what-if row.");
      return;
    }
    if (scenarioRows.some((row) => row.ric === selectedScenarioItem.ric)) {
      setScenarioMessage(`RIC ${selectedScenarioItem.ric} is already staged.`);
      return;
    }
    setScenarioRows((current) => [
      ...current,
      {
        ric: selectedScenarioItem.ric,
        ticker: selectedScenarioItem.ticker,
        display_name: selectedScenarioItem.display_name,
        quantity_delta: parsedQuantityDelta,
      },
    ]);
    setQuantityDeltaInput("10");
    setSelectedScenarioItem(null);
    setScenarioMessage(null);
  }

  return (
    <div className="cpar-page">
      {metaState ? (
        <section className="chart-card cpar-alert-card" data-testid="cpar-portfolio-not-ready">
          <h3>{metaState.kind === "not_ready" ? "cPAR Risk Not Ready" : "cPAR Risk Unavailable"}</h3>
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
                    router.push(`/cpar/risk?${params.toString()}`);
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
            <span>This is a narrow cPAR what-if preview, not a portfolio mutation tool, not a broad analytics engine, and not a cPAR-vs-cUSE4 comparison layer.</span>
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
                  ? "Risk package not ready."
                  : "Risk preview unavailable."}
            </strong>
            <span>{portfolioState.message}</span>
          </div>
        </section>
      ) : packageMismatch ? (
        <section className="chart-card" data-testid="cpar-portfolio-package-mismatch">
            <h3>Account Hedge Preview</h3>
            <div className="cpar-inline-message error">
              <strong>Active package changed during read.</strong>
              <span>The risk workflow no longer matches the current package banner.</span>
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

          <div className="cpar-two-column">
            <CparSearchPanel
              initialQuery=""
              selectedRic={selectedScenarioItem?.ric || null}
              title="Stage What-If Rows"
              helperText="Search the active cPAR package, choose one tickered result, then stage a signed share delta."
              onSelectResult={(item) => {
                if (!canNavigateCparSearchResult(item)) return;
                setSelectedScenarioItem(item);
                setScenarioMessage(null);
              }}
            />

            <section className="chart-card" data-testid="cpar-portfolio-whatif-builder">
              <h3>Narrow What-If Scope</h3>
              <div className="section-subtitle">
                This preview reuses the active package plus the selected live holdings account. It does not mutate holdings, apply trades, or reuse cUSE4 what-if semantics.
              </div>
              {selectedScenarioItem ? (
                <div className="cpar-package-grid compact">
                  <div className="cpar-package-metric">
                    <div className="cpar-package-label">Selected</div>
                    <div className="cpar-package-value">{selectedScenarioItem.ticker || selectedScenarioItem.ric}</div>
                    <div className="cpar-package-detail">{selectedScenarioItem.display_name || selectedScenarioItem.ric}</div>
                  </div>
                  <div className="cpar-package-metric">
                    <div className="cpar-package-label">RIC</div>
                    <div className="cpar-package-value">{selectedScenarioItem.ric}</div>
                    <div className="cpar-package-detail">Active-package search hit</div>
                  </div>
                </div>
              ) : (
                <div className="detail-history-empty compact">
                  Select one active-package search hit, then stage a signed share delta.
                </div>
              )}
              <div className="cpar-search-row" style={{ marginTop: 12 }}>
                <input
                  className="cpar-search-input"
                  data-testid="cpar-whatif-quantity-input"
                  type="number"
                  step="0.01"
                  value={quantityDeltaInput}
                  onChange={(event) => setQuantityDeltaInput(event.target.value)}
                  placeholder="Share delta"
                />
                <button
                  type="button"
                  className={`cpar-mode-btn ${selectedScenarioItem && parsedQuantityDelta != null ? "active" : ""}`}
                  data-testid="cpar-whatif-add-btn"
                  onClick={addScenarioRow}
                >
                  Stage Row
                </button>
              </div>
              <div className="cpar-inline-message neutral">
                <strong>Signed deltas only.</strong>
                <span>Positive values add shares, negative values reduce shares, and zero is rejected.</span>
              </div>
              {scenarioMessage ? (
                <div className="cpar-inline-message warning">
                  <strong>What-if row not staged.</strong>
                  <span>{scenarioMessage}</span>
                </div>
              ) : null}
              <div className="dash-table">
                <table>
                  <thead>
                    <tr>
                      <th>Staged Row</th>
                      <th className="text-right">Share Delta</th>
                      <th className="text-right">Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {scenarioRows.length === 0 ? (
                      <tr>
                        <td colSpan={3} className="cpar-empty-row">
                          No cPAR what-if rows are staged for this account yet.
                        </td>
                      </tr>
                    ) : (
                      scenarioRows.map((row) => (
                        <tr key={row.ric}>
                          <td>
                            <strong>{row.ticker || row.ric}</strong>
                            <span className="cpar-table-sub">{row.display_name || row.ric}</span>
                          </td>
                          <td className="text-right cpar-number-cell">{formatCparNumber(row.quantity_delta, 2)}</td>
                          <td className="text-right">
                            <button
                              type="button"
                              className="cpar-detail-chip"
                              onClick={() => {
                                setScenarioRows((current) => current.filter((item) => item.ric !== row.ric));
                                setScenarioMessage(null);
                              }}
                            >
                              Remove
                            </button>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </section>
          </div>

          {scenarioRows.length === 0 ? (
            portfolio.portfolio_status === "ok" || portfolio.portfolio_status === "partial" ? (
              <CparPortfolioHedgePanel
                data={portfolio}
                mode={mode}
                onModeChange={setMode}
              />
            ) : null
          ) : whatIfLoading && !whatIf ? (
            <section className="chart-card" data-testid="cpar-portfolio-whatif-loading">
              <h3>What-If Preview</h3>
              <AnalyticsLoadingViz message={`Loading cPAR what-if preview for ${selectedAccountId}...`} />
            </section>
          ) : whatIfState ? (
            <section className="chart-card" data-testid="cpar-portfolio-whatif-error">
              <h3>What-If Preview</h3>
              <div className={`cpar-inline-message ${whatIfState.kind === "missing" ? "warning" : "error"}`}>
                <strong>
                  {whatIfState.kind === "missing"
                    ? "Account not found."
                    : whatIfState.kind === "not_ready"
                      ? "What-if package not ready."
                      : "What-if preview unavailable."}
                </strong>
                <span>{whatIfState.message}</span>
              </div>
            </section>
          ) : whatIfPackageMismatch ? (
            <section className="chart-card" data-testid="cpar-portfolio-whatif-package-mismatch">
              <h3>What-If Preview</h3>
              <div className="cpar-inline-message error">
                <strong>Active package changed during what-if read.</strong>
                <span>The what-if preview no longer matches the current package banner.</span>
                <span>Reload the page to pin one cPAR package before comparing current and hypothetical hedges.</span>
              </div>
            </section>
          ) : whatIf ? (
            <>
              <section className="chart-card" data-testid="cpar-portfolio-whatif-scenarios">
                <h3>Scenario Preview Rows</h3>
                <div className="section-subtitle">
                  Each row is previewed against the active package only. Coverage and fit warnings remain explicit, and no holdings mutation occurs.
                </div>
                <div className="dash-table">
                  <table>
                    <thead>
                      <tr>
                        <th>Instrument</th>
                        <th className="text-right">Current Qty</th>
                        <th className="text-right">Delta</th>
                        <th className="text-right">Hyp Qty</th>
                        <th className="text-right">MV Delta</th>
                        <th>Coverage</th>
                      </tr>
                    </thead>
                    <tbody>
                      {whatIf.scenario_rows.map((row) => (
                        <tr key={row.ric}>
                          <td>
                            <strong>{row.ticker || row.ric}</strong>
                            <span className="cpar-table-sub">{row.display_name || row.ric}</span>
                          </td>
                          <td className="text-right cpar-number-cell">{formatCparNumber(row.current_quantity, 2)}</td>
                          <td className="text-right cpar-number-cell">{formatCparNumber(row.quantity_delta, 2)}</td>
                          <td className="text-right cpar-number-cell">{formatCparNumber(row.hypothetical_quantity, 2)}</td>
                          <td className="text-right cpar-number-cell">{formatCparNumber(row.market_value_delta, 2)}</td>
                          <td>{row.coverage_reason || row.coverage}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>

              <div className="cpar-two-column">
                <CparPortfolioHedgePanel
                  data={whatIf.current}
                  mode={mode}
                  onModeChange={setMode}
                  title="Current Account Hedge"
                  subtitle="This is the live covered account vector under the active cPAR package, before staged share deltas are applied."
                  testId="cpar-portfolio-current-hedge-panel"
                />
                <CparPortfolioHedgePanel
                  data={whatIf.hypothetical}
                  mode={mode}
                  onModeChange={setMode}
                  title="Hypothetical Account Hedge"
                  subtitle="This is the same account after applying the staged cPAR what-if deltas, still using the same active package and persisted covariance surface."
                  testId="cpar-portfolio-hypothetical-hedge-panel"
                />
              </div>
            </>
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
