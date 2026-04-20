"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import CparPortfolioHedgeRecommendationPanel from "@/features/cpar/components/CparPortfolioHedgeRecommendationPanel";
import CparRiskFactorSummaryCard from "@/features/cpar/components/CparRiskFactorSummaryCard";
import { CparPageLoadingState } from "@/features/cpar/components/CparLoadingState";
import { useCparPortfolioHedgeRecommendation } from "@/hooks/useCparApi";
import { useHoldingsAccounts } from "@/hooks/useHoldingsApi";
import {
  normalizeCparPortfolioHedgeRecommendationData,
  readCparDependencyErrorMessage,
  readCparError,
} from "@/lib/cparTruth";

const ALL_SCOPE = "__all__";

function CparHedgeWorkspaceInner() {
  const { data: accountsData } = useHoldingsAccounts();
  const accountOptions = accountsData?.accounts ?? [];
  const [selectedScope, setSelectedScope] = useState<string>(ALL_SCOPE);
  const scopeKind = selectedScope === ALL_SCOPE ? "all_permitted_accounts" : "account";
  const accountId = scopeKind === "account" ? selectedScope : null;
  const { data, error, isLoading } = useCparPortfolioHedgeRecommendation(scopeKind, accountId, true);
  const normalized = useMemo(() => normalizeCparPortfolioHedgeRecommendationData(data), [data]);
  const errorState = error ? readCparError(error) : null;

  useEffect(() => {
    if (selectedScope === ALL_SCOPE) return;
    if (accountOptions.some((account) => account.account_id === selectedScope)) return;
    setSelectedScope(ALL_SCOPE);
  }, [accountOptions, selectedScope]);

  if (isLoading && !data) {
    return <CparPageLoadingState message="Loading cPAR hedge workspace..." />;
  }

  return (
    <div className="cpar-page">
      <section className="chart-card" data-testid="cpar-hedge-scope">
        <div className="cpar-hedge-workspace-head">
          <div>
            <div className="cpar-section-kicker">Portfolio Hedge Workspace</div>
            <h3>Factor-Neutral Hedge</h3>
            <div className="section-subtitle">
              Select all permitted accounts or one account, then review the current cPAR exposure chart and the ETF
              package that pulls displayed factor loadings back toward zero.
            </div>
          </div>
          <label className="cpar-hedge-scope-control">
            <span>Scope</span>
            <select value={selectedScope} onChange={(event) => setSelectedScope(event.target.value)}>
              <option value={ALL_SCOPE}>All Accounts</option>
              {accountOptions.map((account) => (
                <option key={account.account_id} value={account.account_id}>
                  {account.account_name || account.account_id}
                </option>
              ))}
            </select>
          </label>
        </div>
      </section>

      {errorState ? (
        <section className="chart-card" data-testid="cpar-hedge-error">
          <h3>Hedge Workspace</h3>
          <div className={`cpar-inline-message ${errorState.kind === "missing" ? "warning" : "error"}`}>
            <strong>
              {errorState.kind === "not_ready"
                ? "Hedge package not ready."
                : errorState.kind === "missing"
                  ? "Selected scope unavailable."
                  : "Hedge workspace unavailable."}
            </strong>
            <span>{readCparDependencyErrorMessage(error)}</span>
          </div>
        </section>
      ) : normalized ? (
        <>
          <CparRiskFactorSummaryCard portfolio={normalized} />
          <CparPortfolioHedgeRecommendationPanel data={normalized} />
        </>
      ) : null}
    </div>
  );
}

export default function CparHedgeWorkspace() {
  return (
    <Suspense fallback={<CparPageLoadingState message="Loading cPAR hedge workspace..." />}>
      <CparHedgeWorkspaceInner />
    </Suspense>
  );
}
