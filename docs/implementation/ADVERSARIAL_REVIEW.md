# Adversarial review — Lite UI integration

Date: 2026-10-22
Reviewer: independent adversarial_review agent, read-only review.
Scope: comprehensive plan and isolated P0–P4 implementation; no rollout approval.

## Findings incorporated

- cPAR chart display basis differs from thresholded position risk_mix; retain and
  explain both rather than relabeling the hedge basis as explanatory exposure.
- Aggregate multi-account rows cannot be mutable allocation legs. DASH reads the
  authorized holdings ledger; analytics only supplement account/RIC identities.
- Retain null/uncovered data instead of fabricating zero prices or zero risk.
- Hide stale position risk mixes when served and current quantities disagree, and
  display served-snapshot context alongside current ledger quantities.
- Keep model selector usable during loading/error; mount only selected family hooks.
- Include all 74 source commits plus dirty navigation and manual-order contract.
- Independent owner workspaces may each categorize a shared holding; holding
  reductions must validate all affected workspaces independently under shared locks.
- Production provider must handle auth identity changes, old refresh responses,
  stale revisions, and focus-refresh/mutation races before it is mounted.
- Feature-off pages retain flat holdings rows and expose no thesis mutation controls.
- DASH sortable headings need keyboard activation and aria-sort.
- Sticky header observers must unobserve detached page nodes.

## Browser-discovered auth omission

The initial static review checked appAccess but missed the explicit middleware
matcher. Browser testing found /dash absent from middleware config. Added
/dash/:path*, added regression coverage, rebuilt, and verified unauthenticated
redirect through the browser. Reviewer rechecked all protected prefixes and the
portfolio API catch-all; no analogous matcher omissions remained.

## P0/P1 acceptance

Reviewer accepted isolated, gated P0/P1 with no remaining Critical/High static
findings after corrections. This does not certify production thesis persistence,
auth lifecycle, atomic holdings mutations, or production deployment. Those remain
P2–P5 gates. See P1_EVIDENCE.md for actual runtime checks and their limits.

## P2–P4 findings resolved

- Preserve surviving account/RIC rows during what-if apply; reject allocated RIC
  consolidation and roll back the whole multi-account operation on conflicts.
- Acquire a common transaction advisory lock before read-modify-write operations,
  including direct SQL mutation triggers and parent membership/account operations.
  Reject unsupported isolation and TRUNCATE rather than bypassing invariants.
- Check allocation limits independently in every owner workspace sharing a holding.
  Revocation and account deactivation atomically remove scope, allocations and orders.
- Validate the canonical authenticated principal before opening owned transactions;
  capability defaults off and shared-auth activation requires explicit mapping.
- Verify workspace IDs as well as revisions in every DTO; reject cross-workspace
  responses, reset on identity changes and suppress obsolete refresh results.
- Risk aggregates and missing-RIC analytics cannot become mutable thesis legs.
  Provide a separate authorized concrete-account ledger in each family's Risk view,
  with current-ledger versus last-served analytics context visible.
- Preserve decimal wire precision and signed short allocations. Reject quantities
  the Number-based editor cannot safely represent instead of silently rounding.

## Final local acceptance

Independent reviewer accepted P2–P4 with no outstanding Critical/High findings.
Reviewed code plus reported real PostgreSQL tests and browser-to-database flow
support local integration acceptance. The final browser run includes both Risk
ledgers, signed decimal reload persistence, stale-writer 409, delayed stale refresh,
foreign-workspace rejection, recovery and logout cleanup.

Live Neon migration, production auth/account configuration, current-main
reconciliation, clean-install CI, deployment/owner enablement and live smoke remain
separate gates. No production credentials, data or deployment were reviewed.

Final auth regression review also caught malformed returnTo URL parser exceptions.
The redirect now preserves query parameters, checks same origin, and falls back
on invalid URLs; browser checks cover query retention, external and malformed
slash-backslash forms.

## P5 accessibility and lifecycle review — 2026-10-23

Resolved missing allocation focus trap, nested confirmation Escape competition,
header/rename opener restoration, drag-only manual ordering and truncated
micro-share displays. Split legs now have distinct focus identities, collapsed
active rows retain a visible tab stop, and successful moves expand their destination
before restoring focus. Browser checks prove all of these behaviors.

Long-run regression review caught the page-remount consequence of context/capability
hydration. The stable scoped provider masks old state, invalidates tickets and
rejects stale callbacks, without discarding same-login page drafts. Review caught
and corrected a shadowed order-scope parameter during this refactor. Actual
principal changes key page state, cancel scoped global confirmations and reload the
document to clear generic cached reads. The signed-cookie test proves cached A
holdings are absent while B requests are pending.

Final static review found no remaining concrete blocker. Eight legacy regressions,
final real PostgreSQL browser flow, extended accessibility/identity checks and six
pure contracts pass. Local acceptance only: clean-install CI, production auth/Neon
configuration, actual merge, deployment and live activation remain unperformed.

Final clean-check closeout: fresh npm ci --legacy-peer-deps (matching existing
CI/Dockerfile), six pure contracts and production build passed with unchanged
manifests/lockfile. Reviewer found no local fast-forward integration blocker after
original clean/main/remote-baseline checks. Production rollout remains unapproved
and unperformed; local integration acceptance does not imply a live migration.
