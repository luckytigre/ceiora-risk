# Ceiora Lite UI production integration

Date: 2026-10-22
Status: adversarial plan review accepted; P0 completed, P1 implemented and verified locally; P2–P5 pending.
Branch: feat/lite-ui-port
Integration checkout: /Users/shaun/Projects/ceiora-risk-ui-port
Production checkout: /Users/shaun/Library/CloudStorage/Dropbox/40 - Creating/Factor Model Project/ceiora-risk
Lite source: /Users/shaun/ceiora-risk-lite/app

## Goal and acceptance contract

Port the final Lite UI into the original production architecture, including login,
DASH, thesis grouping and signed partial allocations, manual ordering, refined
analytics tables/charts/tooltips, tokens, sticky headers, dialogs, and accessibility.
DASH is shared and model neutral: an on-page cUSE/cPAR selector changes the model
analytics; it does not change holdings, thesis allocations, or workspace identity.
DASH is first in the main selector before Risk and absent from the menu Navigation
section. Other family pages remain namespaced and retain existing behavior.

Completion means a reviewed integration branch with passing relevant checks and
an explicit rollout record. Creating a plan, copying UI files, or testing against
Lite mocks does not complete the production integration.

## Authority and source freeze

- Known original local main and cached origin/main: 23c6270a95b7397a2bad70f0d44ebb6b6bf863ff.
  Do not claim the remote or deployed app is current without checking it at merge time.
- Lite branch lite/ui-playground, HEAD 495ba0e, contains 74 commits after baseline.
- Include current uncommitted TabNav.tsx and globals.css navigation changes.
- Preserve a binary patch of tracked working changes plus a source manifest. Never
  sweep untracked frontend/runtime into the port or stage directories wholesale.
- Earlier PORTING-PLAN.md records a 47-commit replay; this is historical evidence,
  not validation of the current 74-commit source.
- Use final file state with a per-file inclusion manifest rather than blindly
  replaying intermediate browser-local persistence, added/reverted context strips,
  and superseded sorting implementations. Original history remains intact.
- Original checkout stays unchanged during integration; independent clone has no
  shared Git objects or dependencies and has no remote until merge handoff.

## Exclusions

Exclude the 17ff621 local runtime overlay: localhost origin config, synthetic badge,
Lite-only serif/font substitutions, mock server and fixtures, synthetic credentials,
NEXT_PUBLIC_CEIORA_LITE, runtime/cache/logs, exports, local launcher scripts, and
browser-local thesis storage. Preserve original auth providers, cloud proxies,
backend origins, fonts, dependencies, lockfiles, model kernels, and deployment config.
The portfolio proxy is a real new production feature, subject to auth/method review.
Keep new CSS checks and their package scripts because they validate the UI.

## Product decisions

- One authenticated owner workspace initially, spanning that owner's permitted
  holdings accounts; no workspace selector. Stable workspace ID enables future expansion.
- Theses and allocations are shared across model views. They reference concrete
  account + RIC holdings identities, never ticker or aggregate all_accounts rows.
- Model selection lives in DASH's URL (?model=cuse or ?model=cpar), defaults to cUSE,
  survives reload/back/forward, and must not globally switch unrelated model pages.
- Only the selected model's analytics hook is mounted. Switching immediately removes
  old-model content; pending/error/empty states identify the selected model. Never
  show cUSE results beneath a cPAR selector or silently fall back across models.
- cUSE reads its risk-page snapshot; cPAR reads its aggregate /api/cpar/risk owner.
  cPAR explanatory charts use display_factor_chart, aggregate_display_loadings,
  display_contributions, not hedge/trade-basis aliases. Table risk_mix retains the
  existing thresholded-contribution basis (already percentage-scaled) and is labeled
  separately; missing risk data is never presented as zero.
- Reuse family-native chart semantics through explicit adapters/components. Preserve
  package/as-of/coverage metadata and distinguish uncovered, unavailable, and zero.
- DASH's positions ledger should preserve real holdings identity and signed shares;
  model coverage may change displayed risk metadata, not silently erase holdings.
  Aggregate cPAR rows cannot be used as mutable thesis legs. Join family analytics
  to authorized holdings or provide a read-only aggregate view until identity is proven.
- Keep manual order by workspace and validated scope. Flat positions/unallocated
  scopes: holdings, cuse-risk, cpar-risk, dash. Thesis header and thesis-leg order is
  workspace-wide in current Lite; preserve this explicitly. No arbitrary scope keys.
- Decimal share arithmetic and database precision must match existing holdings;
  do not silently round fractional quantities using JS floats or a new fixed scale.

## Phases and gates

### P0 — provenance and rehearsal

Create independent integration clone and branch; record original/Lite HEAD and dirty
state; snapshot today's navigation patch; classify every changed tracked frontend
file. Recheck original status at closeout. No installs, credentials, live database
access, deployment, or model jobs are necessary for this phase.
Gate: manifest covers all paths, sandbox overlay excluded, original unchanged.

### P1 — UI foundations and model-neutral DASH

Port final tokens, CSS, confirmation/focus infrastructure, loading states, login
appearance, analytical display refinements, route access protection, and DASH shell.
Build an explicit DASH selector and family-owned data children/adapters. Verify cPAR
chart basis, coverage, positions identity, responsive body sizing, and active nav.
Keep production thesis mutations disabled until P2/P3 pass; instrument tables must
continue working without a thesis backend. Do not mount an eager authenticated
ThesisProvider on public login pages. Record the partially integrated state honestly.
Feature-off must preserve flat rows on every table page and remove thesis/manual
order mutation controls. DASH shell stays neutral; its Risk/Explore links follow
the selected model without changing other pages. Toggle stays usable on error.
Gate: Node 20 typecheck/build, CSS checks, route/auth checks, browser screenshots,
selected-model-only requests and error states; no model kernel/deployment changes.

### P2 — production thesis API and durable storage

Implement additive versioned Neon migration (no serving-time DDL), data adapter,
service, thin serve-app routes, and authenticated frontend portfolio proxy.
Proposed owned tables: portfolio_workspaces, portfolio_workspace_accounts,
portfolio_theses, portfolio_thesis_allocations, portfolio_manual_orders.
Map trusted app principal to existing app_users identity; define shared-auth behavior
explicitly and reject unprovisioned identities rather than inventing ownership.
Recheck memberships on every read/write; account revocation must not expose stale
allocations or enable mutations. Distinguish membership authorization from ownership.

Contracts: GET/POST /api/portfolio/theses; PATCH/DELETE /api/portfolio/theses/{id};
PUT /api/portfolio/thesis-allocations/position; PUT /api/portfolio/manual-order.
Return full versioned state and revision after mutations. Validate nonempty unique
names <=80 chars, finite signed quantities, existing holdings/theses, matching sign,
no duplicates or over-allocation, scoped order keys, payload bounds, and deletion
cleanup. Revision compare-and-set and writes share one transaction. Use typed domain
errors for 400/401/403/404/409; don't map all service failures to user validation.
Gate: isolated DB migration upgrade/idempotency tests, ownership/account denial,
CRUD/order contracts, rollback, decimal precision, stale revisions and two-client races.

### P3 — all holdings writers participate

Trace actual holdings store transaction boundaries before coding. Cover manual
upsert/remove, all CSV import modes (replace/upsert/delta), portfolio apply,
cPAR/shared what-if apply, zero quantities, account operations, and any non-UI
writer. Acquire locks in one deterministic order for holdings and affected workspaces.
Prevent a concurrent holdings reduction and allocation create from violating limits.
Allocation sum checks must account for every workspace that can allocate a shared
holding. Reject reductions/sign flips that invalidate allocations, remove allocations
on deletion, clean manual orders, and advance affected revisions atomically.
Do not commit holdings first and repair thesis state afterward. Preserve existing
holdings refresh semantics; thesis-only edits never trigger model computation.
Gate: mutation-matrix tests, shared-account cases, multi-workspace locks, concurrency,
all-or-nothing imports/apply, and no regressions in current holdings workflows.

### P4 — authenticated thesis UI integration

Use ordinary session-authenticated apiFetch, not editor/operator tokens. Add explicit
server capability/owner rollout gate and auth-aware provider lifetime. Reset state
on identity/workspace changes/logout; discard obsolete in-flight responses. Serialize
mutations and prevent a focus refresh from overwriting a newer mutation response.
Expose saving/saved/failed and conflict recovery. If unsupported, show instrument
views without broken requests; DASH must still work with thesis features disabled.
Connect full/partial moves, short legs, create/rename/delete, collapse, saved ordering,
keyboard paths and focus restoration on Positions, cUSE Risk, cPAR Risk and DASH.
Gate: reload/restart/session persistence, two-browser conflict, sign/quantity checks,
identity switch isolation, and feature-off/no-backend usability.

### P5 — full regression, adversarial closeout, handoff

Run smallest meaningful backend suites plus affected auth/proxy/account/holdings
contracts, frontend Node 20 typecheck/build, CSS variable/raw/contrast checks, existing
cUSE/cPAR/explore/what-if regressions and new DASH checks. Validate dark/light,
narrow/short desktop, reduced motion, keyboard dialogs, sticky layers and scrolling.
Use fixture-only tests for unavailable cloud services; document what remains unproven
without real Neon. Do not count mock-only testing as persistence/concurrency proof.
Adversarial reviewer examines the plan before execution and the final changed slice;
resolve Critical/High findings or keep the affected feature blocked from rollout.
Gate: passing evidence, clean diff checks, dependency/provenance scan, documented
remaining risks, canonical architecture/frontend docs updated and original unchanged.

## Rollout and rollback

Prepare additive migration and backend release before gated frontend activation.
Validate on an isolated production-like DB first; live credentials/schema/deployment
require the normal explicit authorization. No production action is implied by local
implementation. Owner-only enablement follows verified persistence and account scope.
Rollback disables thesis UI and restores instrument views, retaining saved data and
additive tables. DASH rollback is independent of thesis data. Avoid destructive
schema rollback. Merge only after checking current remote main and rerunning relevant
checks after any rebase. Do not auto-deploy or merge this work as part of planning.

## Work log and evidence

- 2026-10-22: original and Lite inventories read; independent integration clone
  created from known local main. No live data/credentials retrieved.
- Adversarial review: initial plan reviewed; no blocker to gated P0/P1. Corrections
  incorporated for cPAR risk-mix basis, independent workspace limits, feature-off
  flat rows, auth/refresh races, coverage/null states, and neutral navigation.
- P0/P1 verification: passing local typecheck, production build/CSS gates, existing
  auth/proxy/UI-error/family contracts, new ledger adapter/middleware contracts, and
  production-built browser smoke. See P1_EVIDENCE.md and ADVERSARIAL_REVIEW.md.
- Browser testing found/fixed missing DASH middleware matcher and explicit
  all-accounts holdings-hook argument; screenshot review fixed narrow header overlap.
- P2–P5: not implemented.
