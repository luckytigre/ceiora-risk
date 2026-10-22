# P0/P1 execution evidence

Date: 2026-10-22
Historical P0/P1 checkpoint: provider-absent statements below describe that stage.
P2_P4_EVIDENCE.md records the subsequent gated persistence/UI integration.

Scope: independent integration checkout, production-built frontend, synthetic local
backend used only by the preview/test process. No credentials retrieved, packages
installed, live data accessed, production migration run, merge, or deployment.

## Implemented

- Inventoried final Lite branch (74 commits) plus tracked navigation changes;
  preserved path classification, SHA-256 source hashes and navigation source patch.
- Ported final UI foundations/refinements and login visual changes with production
  auth, fonts, configuration, dependencies and lockfile preserved.
- Shared DASH with URL-backed cUSE/cPAR selector, active-family navigation links,
  selected-family-only data children and usable selector on loading/failure.
- cUSE uses its risk-page owner and chart; cPAR uses aggregate risk and the native
  display-factor chart. Package/coverage and served snapshot context is visible.
- Account/RIC ledger owns quantities. Null analytics remain unavailable; aggregate
  or stale-quantity risk mixes are not copied onto current account holdings.
- Thesis provider deliberately absent from layout; all table pages use flat rows,
  hide thesis controls, disable manual-order writes and make no thesis state calls.
- DASH protected by server middleware matcher as well as client page classification.
- Keyboard sortable DASH columns and non-overlapping narrow navigation; detached
  sticky-header observers cleaned up across SPA page changes.

## Passing checks

- Node 20 frontend typecheck.
- Production build, including type validation and CSS variable/raw-value/contrast gates.
- Existing auth session, control-plane proxy, UI error and family redirect contracts.
- New dash_positions_contract_check: concrete holdings identity across same-RIC
  accounts, signed values, ticker collision, aggregate risk denial, zero vs null,
  stale quantity guard, and explicit server middleware matcher.
- New dash_smoke against built app: unauth DASH redirect; login; selected-family
  request isolation; matching ledger row counts; cPAR selection survives reload
  and browser back/forward; correct main navigation and no duplicate menu DASH;
  keyboard sorting; 390px layout and brand/tabs/menu bounds; model 503 hides old
  data and permits switching to working cUSE; all three existing table pages
  remain usable with thesis capability off; no uncaught browser errors.
- Targeted frontend scan finds no Lite overlay identifiers in source/scripts.
- git diff --check (raw archived Git patch context whitespace is exempted by
  a path-specific .gitattributes entry; application code has no exemption).
- Original repository remains on 23c6270 with clean working tree.

## Evidence and reproduction

Browser screenshots (synthetic preview data):
- /tmp/ceiora-port-dash-cuse.png
- /tmp/ceiora-port-dash-cpar.png
- /tmp/ceiora-port-dash-narrow.png

Production build output: /tmp/ceiora-port-build.log.
Preview: http://localhost:3101/dash (separate from Lite on port 3100).
Preview launcher environment is temporary and is not part of production source.

Run frontend/scripts/dash_positions_contract_check.mjs with Node 20.
For browser smoke, run frontend/scripts/dash_smoke.mjs with DASH_TEST_ORIGIN,
DASH_TEST_USERNAME and DASH_TEST_PASSWORD set for an isolated test account.
A copied existing dependency tree was used; clean dependency installation and CI
reproduction remain a P5 check. No package installation was performed.

## Review and limits

Independent adversarial agent accepted gated P0/P1 after fixes; see
ADVERSARIAL_REVIEW.md. Browser testing additionally discovered and fixed the missing
DASH middleware matcher and corrected the holdings hook's explicit all-accounts
argument. Mobile screenshot review caught and fixed brand/navigation overlap.

Synthetic preview testing does not prove Neon persistence, shared-account ownership,
transactional allocations, concurrent holdings edits, provider identity lifecycle,
live data quality, or real cloud auth. P2–P5 remain pending. No thesis activation or
production rollout is implied by this evidence. Original production main was not
modified; this checkout remains an integration branch for later review and merge.
