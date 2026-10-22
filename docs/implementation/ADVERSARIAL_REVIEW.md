# Adversarial review — Lite UI integration

Date: 2026-10-22
Reviewer: independent adversarial_review agent, read-only review.
Scope: comprehensive plan and isolated P0/P1 implementation; no rollout approval.

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

## Acceptance

Reviewer accepted isolated, gated P0/P1 with no remaining Critical/High static
findings after corrections. This does not certify production thesis persistence,
auth lifecycle, atomic holdings mutations, or production deployment. Those remain
P2–P5 gates. See P1_EVIDENCE.md for actual runtime checks and their limits.
