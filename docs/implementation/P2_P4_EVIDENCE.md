# P2–P4 local integration evidence

Date: 2026-10-22
Historical P2–P4 checkpoint. P5_MERGE_HANDOFF.md records the subsequent lifecycle,
accessibility fixes and completed local regression checks.
Checkout: /Users/shaun/Projects/ceiora-risk-ui-port, feat/lite-ui-port.
Scope: local implementation and disposable PostgreSQL verification. No live
credentials/data, production migration, merge or deployment.

## Implemented behavior

- Additive, transactional and re-runnable schema v1; no request-time DDL.
  Owner workspaces reference existing app identities and authorized accounts.
- Thin authenticated serve-only routes and a bounded Next proxy. Capability is
  disabled by default; explicit owner allowlist and shared-auth mapping gates.
- Revision compare-and-set, signed six-decimal allocations, bounded validated
  order keys, deletion cleanup and independent shared-workspace limits.
- Common transaction lock and database guards cover direct SQL, imports, edits,
  what-if apply, membership revocation and account changes. Conflicting holdings
  reductions/rekeys fail atomically rather than discarding saved allocations.
- Provider lifecycle bound to authenticated identity and workspace, monotonic
  response handling, saving/conflict feedback and feature-off flat-table fallback.
- DASH and Positions use concrete account/RIC holdings. Both Risk pages offer a
  separate real-account thesis ledger; aggregate instrument rows stay immutable.
  Model selection changes analytics, preserving holdings and workspace identity.

## Passing evidence

- Production frontend build on Node 20, including type validation, CSS variables,
  raw CSS values and contrast gates. Log: /tmp/ceiora-theses-build.log.
- Backend focused run: 74 passed, including 13 real PostgreSQL cases, thesis
  service/store tests, holdings, what-if and account scope contracts.
- Additional route run: 26 passed, including canonical-principal denial,
  holdings dirty-state, account-scoped cUSE and runtime-state contracts.
  The runs overlap; these are not 100 unique tests.
- PostgreSQL tests exercise migration reapplication with saved data, exact decimal
  quantities and shorts, over-allocation/shrink/sign guards, shared workspaces,
  invalid identities/orders, deletes/revokes, isolation/TRUNCATE rejection,
  concurrent allocations, allocation versus reduction/revocation, actual what-if
  persistence and multi-account rollback, and all three import modes.
- Built-browser → Next proxy → real FastAPI thesis routes → PostgreSQL passed:
  create; 3.125 long and -5 short allocations; reload/model-switch persistence;
  delayed stale focus GET; stale second-writer 409; bounded proxy; Positions and
  both Risk ledger views with aggregate/missing-RIC analytics fixtures; foreign
  workspace DTO rejection; reload recovery and logout cleanup.
- Pure thesis quantity/identity, DASH ledger, auth session, proxy and UI error
  contracts passed on the final source. Feature-off built-browser checks passed
  for all three table pages, model request isolation, URL history, failure
  recovery, navigation, keyboard sorting and narrow layout.
- Final regression exposed and fixed an existing authenticated-login returnTo
  bug: DASH query parameters were encoded into the pathname. The redirect now
  parses the query and rejects origin-changing URL forms. Browser regression
  covers retained cPAR selection, slash-backslash origin escape prevention and
  malformed URL fallback.
- Original checkout remains clean on baseline 23c6270; Lite source preserved.

## Reproduction and test boundaries

PostgreSQL image: postgres:17-alpine, pulled with Shaun's explicit approval.
Digest: sha256:eee6c6e1b0894fa4fe0075eb054af9fd1cf39a35321c2d4110626cfe506411fb.
Disposable database ceiora_thesis_test is bound only to 127.0.0.1:55432; all rows
are synthetic. Existing cached backend image supplies Python/psycopg, and existing
local pytest/httpx dependencies were copied into scratch; no other package install.

Set CEIORA_THESIS_TEST_DSN to the disposable local database and run
backend/tests/test_portfolio_theses_pg.py with pytest. Its fixture explicitly
requires ceiora_thesis_test and resets that database's public schema. Never point
the test at a retained database. Unit routes are in test_portfolio_theses.py.

Browser test: frontend/scripts/thesis_persistence_smoke.mjs, using
DASH_TEST_ORIGIN (default localhost:3102) and isolated DASH_TEST_USERNAME/PASSWORD.
backend/tests/thesis_http_app.py refuses to start without explicit test mode;
only thesis routes write. Other data is a read-only proxy to synthetic Lite
fixtures. Local launcher/secret and fixture seed scripts stay outside the repo.
Each run requires a freshly seeded disposable database because names are fixed.

## Acceptance and remaining gates

Independent adversarial reviewer accepted local P2–P4 with no outstanding
Critical/High findings; see ADVERSARIAL_REVIEW.md. This proves PostgreSQL behavior
and the real thesis request path locally, not the deployed Neon/auth configuration.
The global advisory lock intentionally serializes this small app's portfolio
writes; privileged DDL or disabling triggers is outside its protection.
Frontend allocations reject values beyond its safe editing range/precision.

Legacy self-starting dev smoke scripts were also attempted: parallel runs collided
through the shared .next directory, and older unauthenticated portfolio/family
harnesses reached login configuration gates. Those attempts are not passing
evidence. The production build was regenerated afterward; keep future dev
harnesses isolated and update their auth fixtures before relying on them.

Before merge: reconcile current remote main, perform clean-install CI and finish
the planned broad visual/accessibility review (both themes, reduced motion,
short viewport, dialogs and sticky layers). Before activation: isolated
production-like Neon migration, real owner/account auth verification and live
smoke. Follow docs/operations/THESIS_WORKSPACE_ROLLOUT.md. Disable capability to
roll back UI; retain saved tables and holdings guards.
