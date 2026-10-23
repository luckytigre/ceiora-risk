# Local merge handoff

Date: 2026-10-23
Integration: /Users/shaun/Projects/ceiora-risk-ui-port, feat/lite-ui-port.
Original: /Users/shaun/Library/CloudStorage/Dropbox/40 - Creating/Factor Model Project/ceiora-risk.

## Result and scope

The Lite UI port and production-shaped thesis backend are implemented locally.
DASH is model neutral with an on-page URL-backed cUSE/cPAR toggle. Workspaces and
holdings remain shared between model views. Allocation writes use concrete
account/RIC identities; both Risk pages provide separate account-level thesis
ledgers rather than mutating aggregate analytics rows.

Latest P5 fixes add nested-modal focus trapping and safe Escape handling, restore
focus to create/rename/allocation openers, retain a single visible split-leg tab
stop, add Alt+Up/Down keyboard ordering for thesis groups and position legs, and
show all six supported quantity decimals. A move into a collapsed destination
expands that group before restoring focus. Explore now refreshes its served
context after RECALC rather than during the immediate live-ledger refresh.

No model kernels, dependency manifests/lockfiles, live credentials or deployment
configuration changed. Original main stayed unchanged during isolated implementation and review. The
reviewed history is now carried over locally with a fast-forward; Lite source
remains intact. No push or cloud deployment is part of this local carryover.

## Evidence

- P1_EVIDENCE.md: UI provenance, family adapters, navigation and feature-off checks.
- P2_P4_EVIDENCE.md: authenticated request path, PostgreSQL persistence, atomicity,
  races, revocation and conflict recovery.
- ADVERSARIAL_REVIEW.md: findings and independent review acceptance.
- Production Node 20 build/CSS/type gates: /tmp/ceiora-p5-build.log.
- Existing installed dependency tree: npm ls --depth=0 passes.
- P5 screenshot artifacts: /tmp/ceiora-p5-{dark,light}-{1440,1280,390}.png and
  /tmp/ceiora-p5-{dark,light}-dialog.png. They contain synthetic data only.

The older smoke harnesses now support synthetic authenticated sessions and
CEIORA_SMOKE_SERVER_MODE=start with CEIORA_SMOKE_PORT. Use production start
against one completed build and run sequentially. Their default dev mode still
requires exclusive ownership of .next. Updated fixtures expose current bootstrap
endpoints, explicit cPAR display/trade bases and resolved ticker selections.
Explore fixtures assert an empty unexpected-request list instead of throwing
unhandled route promises. Browser test authentication is loopback-only and uses
an in-memory random signing secret; it never reads production credentials.

## Final passing local checks

- All eight existing browser suites pass: family routes, cPAR pages, hedge,
  risk/portfolio, factor-history error handling, Positions, delayed Explore
  refresh and already-running Explore refresh. Summary: /tmp/ceiora-p5-regressions.log.
- Real PostgreSQL browser persistence flow passes on the final build, including
  fractional/short allocations, model switching, stale edits/responses, both
  concrete Risk ledgers, workspace mismatch rejection and logout.
- thesis_accessibility_smoke.mjs passes: six-place micro-share display, single
  split-leg tab stop and collapse fallback, nested modal Tab/Escape and opener
  restoration, thesis/leg/flat keyboard ordering, move into collapsed group,
  both themes, reduced motion, narrow/short layouts and narrow dialog bounds.
  Switching principal cancels an open global confirmation without submitting
  deletion or showing the previous thesis name.
- principal_cache_smoke.mjs passes with actual signed A-to-B cookies: previous
  cached holdings stay absent while B reads are pending; the fresh document
  renders B data and same-principal background refresh causes no further navigation.
- Six pure frontend contracts pass: auth session, control proxy, UI errors,
  family redirects, DASH ledger/null/identity and thesis quantity/identity.
- Adversarial reviewer accepted the final code; browser results close its runtime
  evidence requests. No outstanding Critical/High findings.

## Auth hydration and cache correction

Long-running tests caught two timing/state issues. Live holdings refresh no longer
revalidates served Explore context before RECALC completion. Thesis capability and
auth context hydration now update a stable provider's scoped state instead of
remounting the page and discarding drafts. Workspace IDs, scope markers and request
tickets hide previous state synchronously and discard obsolete callbacks/results.
Consumer overlays reset when workspace changes; global confirmations are scoped
and cancelled on principal/membership changes.

Session bootstrap/login/session payloads now include the authenticated subject so
principal identity remains stable through context hydration. A true login change
keys the page subtree and masks old content, then performs a full document reload
that clears the default SWR cache and in-flight JS. Remembering the previous
principal across public/logout pages also covers logout followed by another login.
This preserves existing global SWR mutation behavior without introducing a second
cache provider. The signed-cookie test verifies the populated-cache switch.

## Remaining merge and activation gates

- Remote main was checked without credentials using git ls-remote and matches
  baseline 23c6270a95b7397a2bad70f0d44ebb6b6bf863ff. Recheck at actual merge time.
- Clean dependency verification passed in the disposable checkout
  /Users/shaun/tmp/ceiora-risk-clean-check-20261023: npm ci --legacy-peer-deps
  (the existing CI/Dockerfile policy), six pure contracts and production build.
  Package manifests/lockfile stayed unchanged. Default npm ci hit the baseline
  peer-resolution/npm edgesOut failure; using the repository's documented flags
  succeeded. Logs: /tmp/ceiora-clean-install.log and /tmp/ceiora-clean-build.log.
  This reproduces frontend CI locally; no hosted CI run is claimed.
- Local integration fast-forwards original main from 23c6270, retaining the
  reviewed UI implementation commit 2c488ab and the preceding P0–P4 commits.
  Original status and remote main were clean/matching immediately before the
  carryover; final main must match feat/lite-ui-port with a clean working tree.
  No remote push, deployment or thesis activation was performed.
- Production-like Neon migration, real auth/account mapping, owner activation and
  live smoke require their own authorized rollout. Local PostgreSQL proof is not
  a claim about the deployed environment.

## Recorded local carryover procedure

After checking the original checkout is clean and main matches the recorded
baseline, import the reviewed branch and validate its diff:

```sh
git fetch /Users/shaun/Projects/ceiora-risk-ui-port feat/lite-ui-port:feat/lite-ui-port
git diff --stat main...feat/lite-ui-port
git diff --check main...feat/lite-ui-port -- . ':!docs/implementation/LITE_NAVIGATION_SOURCE.patch'
git merge --ff-only feat/lite-ui-port
git status --short
```

A local fast-forward is possible while main remains at the recorded baseline.
Reconcile any newly fetched upstream changes before merging. Follow
../operations/THESIS_WORKSPACE_ROLLOUT.md for additive migration, gated activation
and rollback; rollback retains data and database invariants.

## Verified carryover event

Original main was clean at 23c6270a95b7397a2bad70f0d44ebb6b6bf863ff and
fast-forwarded to 0f7296f16803de27a8f998f24492166d91284f00 on 2026-10-23.
The working tree was clean and matched the reviewed integration checkout.
Subsequent evidence-only bookkeeping does not change the verified app code.
No push, live schema migration, deployment or owner activation occurred.

Before checkout, Git used main's old attributes and flagged a preserved blank
context line in the archived source patch. The pre-merge check therefore excludes
only that byte-for-byte provenance artifact, matching the incoming branch's
existing path-specific .gitattributes rule. After the fast-forward, the complete
baseline-to-HEAD diff check passed with incoming attributes active. Application
code has no whitespace exemption.
