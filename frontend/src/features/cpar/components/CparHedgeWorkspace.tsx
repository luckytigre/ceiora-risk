"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import CparPortfolioHedgeRecommendationPanel from "@/features/cpar/components/CparPortfolioHedgeRecommendationPanel";
import InlineApiError from "@/components/InlineApiError";
import CparRiskFactorSummaryCard from "@/features/cpar/components/CparRiskFactorSummaryCard";
import { CparPageLoadingState } from "@/features/cpar/components/CparLoadingState";
import { useCparPortfolioHedgeRecommendation } from "@/hooks/useCparApi";
import { useHoldingsAccounts } from "@/hooks/useHoldingsApi";
import {
  normalizeCparPortfolioHedgeRecommendationData,
} from "@/lib/cparTruth";
import { accountTypeLabel, describeUiError } from "@/lib/uiErrors";

const ALL_SCOPE = "__all__";

function CparHedgeWorkspaceInner() {
  const { data: accountsData } = useHoldingsAccounts();
  const accountOptions = accountsData?.accounts ?? [];
  const [selectedScope, setSelectedScope] = useState<string>(ALL_SCOPE);
  const scopeKind = selectedScope === ALL_SCOPE ? "all_permitted_accounts" : "account";
  const accountId = scopeKind === "account" ? selectedScope : null;
  const { data, error, isLoading } = useCparPortfolioHedgeRecommendation(scopeKind, accountId, true);
  const normalized = useMemo(() => normalizeCparPortfolioHedgeRecommendationData(data), [data]);
  const errorKind = error ? describeUiError(error).kind : null;

  useEffect(() => {
    if (selectedScope === ALL_SCOPE) return;
    if (accountOptions.some((account) => account.account_id === selectedScope)) return;
    setSelectedScope(ALL_SCOPE);
  }, [accountOptions, selectedScope]);

  if (isLoading && !data) {
    return <CparPageLoadingState message="Loading cPAR hedge workspace…" />;
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
                  {account.account_name || account.account_id} · {accountTypeLabel(account.account_type)}
                </option>
              ))}
            </select>
          </label>
        </div>
      </section>

      {error ? (
        <section className="chart-card" data-testid="cpar-hedge-error">
          <h3>Hedge Workspace</h3>
          <InlineApiError
            error={error}
            surface="cPAR hedge workspace"
            accountName={accountOptions.find((account) => account.account_id === accountId)?.account_name}
            className={`cpar-inline-message ${errorKind === "not_found" ? "warning" : "error"}`}
          />
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
    <Suspense fallback={<CparPageLoadingState message="Loading cPAR hedge workspace…" />}>
      <CparHedgeWorkspaceInner />
    </Suspense>
  );
}
