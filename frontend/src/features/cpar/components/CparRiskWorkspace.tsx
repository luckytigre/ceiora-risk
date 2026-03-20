"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { CparInlineLoadingState, CparPageLoadingState } from "@/features/cpar/components/CparLoadingState";
import CparPortfolioHedgePanel from "@/features/cpar/components/CparPortfolioHedgePanel";
import CparPortfolioWhatIfBuilder, { type CparDraftScenarioRow } from "@/features/cpar/components/CparPortfolioWhatIfBuilder";
import CparRiskAccountScopeCard from "@/features/cpar/components/CparRiskAccountScopeCard";
import CparRiskCoverageSummaryCard from "@/features/cpar/components/CparRiskCoverageSummaryCard";
import CparRiskFactorSummaryCard from "@/features/cpar/components/CparRiskFactorSummaryCard";
import CparRiskPositionsContributionTable from "@/features/cpar/components/CparRiskPositionsContributionTable";
import CparRiskWhatIfPreviewSection from "@/features/cpar/components/CparRiskWhatIfPreviewSection";
import { useCparMeta, useCparPortfolioHedge, useCparPortfolioWhatIf, useHoldingsAccounts } from "@/hooks/useApi";
import {
  readCparError,
  sameCparPackageIdentity,
} from "@/lib/cparTruth";
import type { CparHedgeMode, CparSearchItem } from "@/lib/types";

function parseQuantityDelta(value: string): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || Math.abs(parsed) <= 1e-12) return null;
  return parsed;
}

function formatScenarioQuantity(value: number): string {
  if (!Number.isFinite(value)) return "";
  return String(Number(value.toFixed(4)));
}

function CparRiskWorkspaceInner() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const selectedAccountParam = searchParams?.get("account_id")?.trim() || null;
  const [mode, setMode] = useState<CparHedgeMode>("factor_neutral");
  const [scenarioRows, setScenarioRows] = useState<CparDraftScenarioRow[]>([]);

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
  }, [selectedAccountId, meta?.package_run_id]);

  const {
    data: portfolio,
    error: portfolioError,
    isLoading: portfolioLoading,
  } = useCparPortfolioHedge(selectedAccountId, mode, Boolean(selectedAccountId) && Boolean(meta) && !metaState);
  const portfolioState = portfolioError ? readCparError(portfolioError) : null;
  const packageMismatch = Boolean(meta && portfolio && !sameCparPackageIdentity(meta, portfolio));
  const parsedScenarioRows = useMemo(() => {
    const rows = scenarioRows.map((row) => {
      const quantityDelta = parseQuantityDelta(row.quantity_text);
      if (quantityDelta == null) return null;
      return {
        ric: row.ric,
        ticker: row.ticker,
        quantity_delta: quantityDelta,
      };
    });
    return rows.every((row) => row !== null)
      ? rows as Array<{ ric: string; ticker: string | null; quantity_delta: number }>
      : null;
  }, [scenarioRows]);
  const hasInvalidScenarioRows = scenarioRows.length > 0 && parsedScenarioRows === null;
  const {
    data: whatIf,
    error: whatIfError,
    isLoading: whatIfLoading,
  } = useCparPortfolioWhatIf(
    selectedAccountId,
    mode,
    parsedScenarioRows ?? [],
    Boolean(selectedAccountId)
      && Boolean(meta)
      && !metaState
      && Boolean(portfolio)
      && !portfolioState
      && !packageMismatch
      && Boolean(parsedScenarioRows?.length),
  );

  if (metaLoading && !meta) {
    return <CparPageLoadingState message="Loading cPAR portfolio hedge workflow..." />;
  }

  const whatIfState = whatIfError ? readCparError(whatIfError) : null;
  const whatIfPackageMismatch = Boolean(
    (meta && whatIf && !sameCparPackageIdentity(meta, whatIf))
    || (whatIf && !sameCparPackageIdentity(whatIf, whatIf.current))
    || (whatIf && !sameCparPackageIdentity(whatIf, whatIf.hypothetical)),
  );
  const selectedAccount = (accountsData?.accounts || []).find((row) => row.account_id === selectedAccountId) || null;

  function stageScenarioRow(item: CparSearchItem, quantityDelta: number): string | null {
    if (scenarioRows.some((row) => row.ric === item.ric)) {
      return `RIC ${item.ric} is already staged.`;
    }
    setScenarioRows((current) => [
      ...current,
      {
        key: item.ric,
        ric: item.ric,
        ticker: item.ticker,
        display_name: item.display_name,
        fit_status: item.fit_status,
        hq_country_code: item.hq_country_code || null,
        quantity_text: formatScenarioQuantity(quantityDelta),
      },
    ]);
    return null;
  }

  function updateScenarioRow(ric: string, quantityText: string) {
    setScenarioRows((current) => current.map((row) => (
      row.ric === ric
        ? { ...row, quantity_text: quantityText }
        : row
    )));
  }

  function adjustScenarioRow(ric: string, delta: number) {
    setScenarioRows((current) => current.map((row) => {
      if (row.ric !== ric) return row;
      const currentQuantity = parseQuantityDelta(row.quantity_text) ?? 0;
      return {
        ...row,
        quantity_text: formatScenarioQuantity(currentQuantity + delta),
      };
    }));
  }

  function removeScenarioRow(ric: string) {
    setScenarioRows((current) => current.filter((row) => row.ric !== ric));
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
        <CparRiskAccountScopeCard
          accountsLoading={accountsLoading}
          accountsData={accountsData}
          accountsError={accountsError}
          selectedAccountId={selectedAccountId}
          selectedAccount={selectedAccount}
          onSelectAccount={(nextAccountId) => {
            const params = new URLSearchParams(searchParams?.toString() || "");
            params.set("account_id", nextAccountId);
            router.push(`/cpar/risk?${params.toString()}`);
          }}
        />
        {portfolio && !portfolioState && !packageMismatch ? (
          <CparRiskCoverageSummaryCard portfolio={portfolio} />
        ) : (
          <section className="chart-card" data-testid="cpar-portfolio-summary">
            <h3>Workflow Scope</h3>
            <div className="section-subtitle">
              The account workflow prices current holdings at the latest shared-source price on or before the active package date, then aggregates only covered persisted cPAR loadings into one hedge vector.
            </div>
            <div className="cpar-inline-message neutral">
              <strong>Narrow by design.</strong>
              <span>This is a narrow cPAR what-if preview, not a portfolio mutation tool, not a broad analytics engine, and not a cPAR-vs-cUSE4 comparison layer.</span>
            </div>
          </section>
        )}
      </div>

      {!selectedAccountId && !accountsLoading ? (
        <section className="chart-card">
          <h3>Account Hedge Preview</h3>
          <div className="detail-history-empty compact">Choose a holdings account to open the read-only cPAR portfolio hedge workflow.</div>
        </section>
      ) : metaState || accountsError ? null : portfolioLoading && !portfolio ? (
        <section className="chart-card" data-testid="cpar-portfolio-loading">
          <h3>Account Hedge Preview</h3>
          <CparInlineLoadingState message={`Loading cPAR portfolio hedge for ${selectedAccountId}...`} />
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
            <span>The risk workflow no longer matches the active package metadata.</span>
            <span>Reload the page to pin one cPAR package before using the account hedge preview.</span>
          </div>
        </section>
      ) : portfolio ? (
        <>
          {portfolio.aggregate_thresholded_loadings.length > 0 ? (
            scenarioRows.length === 0 ? (
              <div className="cpar-two-column">
                <CparRiskFactorSummaryCard portfolio={portfolio} />
                <CparPortfolioHedgePanel
                  data={portfolio}
                  mode={mode}
                  onModeChange={setMode}
                />
              </div>
            ) : (
              <CparRiskFactorSummaryCard portfolio={portfolio} />
            )
          ) : null}

          <CparRiskPositionsContributionTable rows={portfolio.positions} />

          <CparPortfolioWhatIfBuilder
            resetKey={`${selectedAccountId || "none"}:${meta?.package_run_id || "none"}`}
            scenarioRows={scenarioRows}
            hasInvalidScenarioRows={hasInvalidScenarioRows}
            onStageRow={stageScenarioRow}
            onUpdateScenarioRow={updateScenarioRow}
            onAdjustScenarioRow={adjustScenarioRow}
            onRemoveScenarioRow={removeScenarioRow}
            onClearScenarioRows={() => setScenarioRows([])}
          />

          {scenarioRows.length > 0 ? (
            hasInvalidScenarioRows ? (
              <section className="chart-card" data-testid="cpar-portfolio-whatif-invalid">
                <h3>What-If Preview</h3>
                <div className="cpar-inline-message warning">
                  <strong>Draft rows need attention.</strong>
                  <span>At least one staged row has an invalid or zero share delta.</span>
                  <span>Update the staged queue above before the hypothetical preview can recompute.</span>
                </div>
              </section>
            ) : whatIfLoading && !whatIf ? (
              <section className="chart-card" data-testid="cpar-portfolio-whatif-loading">
                <h3>What-If Preview</h3>
                <CparInlineLoadingState message={`Loading cPAR what-if preview for ${selectedAccountId}...`} />
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
                  <span>The what-if preview no longer matches the active package metadata.</span>
                  <span>Reload the page to pin one cPAR package before comparing current and hypothetical hedges.</span>
                </div>
              </section>
            ) : whatIf ? (
              <CparRiskWhatIfPreviewSection
                whatIf={whatIf}
                mode={mode}
                onModeChange={setMode}
              />
            ) : null
          ) : null}

          {portfolio.aggregate_thresholded_loadings.length === 0 ? (
            <section className="chart-card">
              <h3>Risk Summary Deferred</h3>
              <div className="section-subtitle">
                No covered holdings rows contributed to the aggregate thresholded portfolio vector, so the factor-only risk summary stays withheld until the account has priced and package-covered rows.
              </div>
              <div className="cpar-inline-message neutral">
                <strong>Still explicit.</strong>
                <span>Use the contribution mix table above to inspect excluded rows and the reason they were withheld from the cPAR risk vector.</span>
              </div>
            </section>
          ) : null}
        </>
      ) : null}
    </div>
  );
}

export default function CparRiskWorkspace() {
  return (
    <Suspense fallback={<CparPageLoadingState message="Loading cPAR portfolio hedge workflow..." />}>
      <CparRiskWorkspaceInner />
    </Suspense>
  );
}
