"use client";

import Link from "next/link";
import { formatCparTimestamp, readCparDependencyErrorMessage } from "@/lib/cparTruth";
import type { HoldingsAccount, HoldingsAccountsData } from "@/lib/types";

export default function CparRiskAccountScopeCard({
  accountsLoading,
  accountsData,
  accountsError,
  selectedAccountId,
  selectedAccount,
  onSelectAccount,
}: {
  accountsLoading: boolean;
  accountsData: HoldingsAccountsData | undefined;
  accountsError: unknown;
  selectedAccountId: string | null;
  selectedAccount: HoldingsAccount | null;
  onSelectAccount: (accountId: string) => void;
}) {
  return (
    <section className="chart-card" data-testid="cpar-portfolio-account-panel">
      <h3>Account Scope</h3>
      <div className="section-subtitle">
        `/cpar/risk` stays account-scoped and package-pinned. It reuses shared holdings account plumbing, but it does not reuse cUSE portfolio or what-if payload semantics.
      </div>

      {accountsLoading && !accountsData ? (
        <div className="detail-history-empty compact">Loading holdings accounts…</div>
      ) : accountsError ? (
        <div className="cpar-inline-message error">
          <strong>Holdings accounts unavailable.</strong>
          <span>{readCparDependencyErrorMessage(accountsError)}</span>
        </div>
      ) : !(accountsData?.accounts.length) ? (
        <div className="detail-history-empty compact">No holdings accounts are available yet.</div>
      ) : (
        <>
          <label className="cpar-package-label" htmlFor="cpar-account-select">Selected account</label>
          <div className="cpar-search-row">
            <select
              id="cpar-account-select"
              className="explore-input whatif-entry-field whatif-entry-account"
              data-testid="cpar-portfolio-account-select"
              value={selectedAccountId || ""}
              onChange={(event) => onSelectAccount(event.target.value)}
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
                <div className="cpar-package-value">
                  {selectedAccount.last_position_updated_at ? "Live" : "—"}
                </div>
                <div className="cpar-package-detail">
                  {selectedAccount.last_position_updated_at
                    ? formatCparTimestamp(selectedAccount.last_position_updated_at)
                    : "No positions yet"}
                </div>
              </div>
            </div>
          ) : null}
        </>
      )}

      <div className="cpar-inline-message neutral">
        <strong>Workflow boundary.</strong>
        <span>
          The risk page is still read-only. Use cPAR search and hedge flows for single-name work, and use the
          scenario builder here only for preview-only account deltas.
        </span>
        <div className="cpar-badge-row compact">
          <Link href="/cpar/explore" className="cpar-detail-chip" prefetch={false}>Explore</Link>
          <Link href="/cpar/hedge" className="cpar-detail-chip" prefetch={false}>Instrument Hedge</Link>
        </div>
      </div>
    </section>
  );
}
