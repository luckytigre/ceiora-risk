# Frontend Auth And Custom Domain Plan

Date: 2026-04-14
Owner: Codex
Status: Implemented; frontend auth, private backends, Firebase frontend-only domain cutover, and `/home` migration are live

## Purpose

Define the lowest-complexity path from the current public cloud topology to the intended hobby-project shape:

- one public frontend
- the app moved behind auth
- heavy backend surfaces no longer directly internet-reachable
- `app.ceiora.com` restored only where users actually need it

This document is intentionally phased. It replaces the earlier big-bang idea of changing URL topology, auth, backend reachability, and operator workflows all at once.

## Goals

- Keep the project simple and low-cost.
- Preserve scale-to-zero behavior.
- Restore a single clean public custom domain for the frontend.
- Reduce incidental control-plane chatter now; prevent backend wakeups in later auth/private-backend phases.
- Move the current app home off `/` so a new public landing page can be built later.

## Non-Goals

- Reintroducing three public hostnames (`app`, `api`, `control`) as the default target shape.
- Rebuilding the old edge/load-balancer architecture as the first move.
- Landing a full multi-role auth system in one pass.

## Current State

- Frontend auth/session is live.
- `/` is the public landing placeholder, `/login` is the shared-session entry, and `/home` is the authenticated app home.
- `serve` and `control` are private Cloud Run services reachable only through the frontend proxy with service-to-service auth.
- `app.ceiora.com` now resolves to Firebase Hosting for the public frontend.
- The legacy HTTPS load balancer has been removed.
- The old public `api.ceiora.com` and `control.ceiora.com` DNS aliases were removed.

This proposal is intentionally phased.
Frontend, route-ownership, shared-auth, private-backend, and frontend-domain cutovers are complete.
The only intentionally deferred cleanup is removal of the temporary ACME challenge route after Firebase control-plane status settles beyond certificate propagation.

Execution evidence:
- `docs/operations/cutover_evidence/FRONTEND_AUTH_EXECUTION_20260415T010336Z.md`

## Accepted Direction

The intended end state is:

- public frontend only
- `app.ceiora.com` mapped only to that frontend
- `/` reserved for a future public landing page
- current app home moved to `/home`
- `/home`, `/cuse/*`, `/cpar/*`, `/positions`, `/data`, and `/settings` protected by auth
- `serve` and `control` callable only by the frontend
- Firebase Hosting forwards the shared auth session via the special `__session` cookie name

## Key Decisions

### 1. Keep exactly one public web surface

The frontend stays public.
`serve` and `control` should become private only after the frontend has a real server-to-server auth path.

Reason:
- this preserves a simple user mental model
- this prevents direct random hits on the heavy backends
- this keeps the custom-domain problem smaller

### 2. Stage `/home` before replacing `/`

Create `/home` now as the future authenticated app home.
Do not replace `/` until auth and login-return behavior exist.

Reason:
- route migration and auth migration should not land in the same cut
- bookmarks and shared links need a compatibility window

### 3. Do not start with full viewer/editor/operator RBAC

The first auth cut should aim for one real authenticated app session shared by the current primary account.
Per-user accounts and role splits can layer on later when they are tied to concrete workflows.

Reason:
- this is a hobby project
- the repo currently has no real session subsystem
- multi-role design adds complexity before the base auth boundary exists

### 4. `/settings` becomes a privileged in-app page

`/settings` should stop acting as the primary auth surface.
It remains inside the authenticated app and is reserved for the primary privileged account.

Reason:
- this preserves a maintenance/admin surface without keeping browser-token editing as the public trust model
- it leaves room to convert `/settings` from token management to real operator/admin controls over time

### 5. Restore `app.ceiora.com` only for the frontend

Do not restore separate public `api.ceiora.com` and `control.ceiora.com` unless a later requirement proves they are needed.

Reason:
- one public hostname is enough for the intended app
- the old three-host edge shape is more infrastructure than the hobby profile needs

### 6. Try Firebase before reintroducing the load balancer

Treat Firebase as the first frontend-only custom-domain experiment for `app.ceiora.com`.
Use the existing load balancer path only if the Firebase approach proves insufficient for the frontend-only shape.

Reason:
- this matches the hobby-project goal of keeping fixed-cost infrastructure light
- it keeps the load balancer as a fallback rather than the presumptive answer

## Preconditions Before Auth Work

The repo does not yet have a real app-session subsystem.
Before Phase 1 starts, decide and document:

- session primitive: cookie-backed session, signed bearer session, or external auth helper
- login entry path and logout path
- `returnTo` behavior for protected routes
- how the single shared account is provisioned and rotated
- how the existing browser-local operator/editor tokens are deprecated, migrated, or invalidated during auth rollout
- CSRF/origin posture for mutating proxy routes once browser-held backend tokens are no longer the main trust model

Without these decisions, route gating work will sprawl into ad hoc exceptions.

## Phased Rollout

### Phase 0: Incidental control-plane chatter reduction

Status:
- completed in repo

Changes:
- stop anonymous shared-shell and obvious operator-surface control-plane fetches
- add `/home` as the future authenticated app home while keeping `/` unchanged

Exit criteria:
- anonymous visits no longer hit `/api/operator/status` from the shared shell, health surface, or generic refresh CTA surface
- `/home` exists and matches the current app-facing landing/home content

Explicit non-goal:
- Phase 0 does not stop anonymous dashboard routes from hitting read-only `serve` payloads or Neon-backed app reads

Important boundary:
- the operator-token presence checks used in this phase are UI suppression only; they are not an auth wall and must not be reused as the Phase 1 session model

### Phase 1: Auth boundary in the frontend

Status:
- completed in repo

Changes:
- add a real shared auth/session boundary in Next
- protect `/home`, `/cuse/*`, `/cpar/*`, `/positions`, `/data`
- protect `/settings` as a privileged page for the primary account
- protect frontend `/api/*` proxy routes before they forward upstream
- add login, logout, and `returnTo` flow

Constraints:
- this is a new subsystem; it is not a route rename
- mutating flows need CSRF/origin protections if cookie sessions are used
- the old browser-local token editor in `/settings` must be gated, converted, or retired as part of this phase
- until Phase 2A is complete, route-handler auth does not yet own the full `/api/*` browser surface because the repo still carries a global rewrite

Exit criteria:
- unauthenticated users are redirected away from protected pages
- only the primary privileged account can reach `/settings`
- protected API proxy routes reject unauthenticated calls before contacting backend services
- login returns the user to the originally requested URL

### Phase 2A: Frontend proxy path hardening

Status:
- completed in repo

Changes:
- narrow or remove the global `/api/*` rewrite once App Router proxy routes are the supported path
- make frontend route handlers the only supported browser path for app/backend calls
- verify every browser-used backend call has an owned route handler rather than falling through to ambient rewrites
- add an explicit audit/test gate so undocumented browser calls cannot continue reaching public upstreams through the catch-all rewrite

Reason:
- backend privatization is unsafe while the browser still has broad accidental paths to public upstreams

Exit criteria:
- browser traffic uses owned frontend proxy handlers rather than catch-all rewrites
- the remaining rewrite surface, if any, is explicitly documented and justified

### Phase 2B: Backend trust-model cutover

Status:
- completed live

Changes:
- move backend trust from browser-held secrets to frontend-server identity
- make `serve` and `control` private at Cloud Run/IAM
- remove direct browser reliance on raw operator/editor backend secrets

Constraints:
- do not flip backend privacy until frontend-to-backend service auth exists and Phase 2A is complete
- smoke tests and rollback procedures need explicit updates because direct public `run.app` calls go away for private services

Exit criteria:
- browsers cannot directly call `serve` or `control`
- frontend can still proxy successfully to both services
- operator smoke and rollback docs reflect the new private-backend path

### Phase 3: Public URL decision gate

Status:
- completed live; Firebase is the active frontend-only cutover path

This phase is a deliberate infrastructure decision, not an automatic next step.
Choose the lightest option that supports one public frontend hostname without restoring three public hostnames by default.
If no such option is acceptable, stop and reassess before defaulting to the old three-host edge.

Changes:
- try Firebase first as the frontend-only custom-domain mechanism for `app.ceiora.com`
- document the fallback criteria that would justify the load balancer path
- restore `app.ceiora.com` for the frontend only once auth and private backend boundaries are stable
- keep the backend services non-public regardless of which frontend-domain option is chosen

Constraints:
- the current repo custom-domain path is a three-host load balancer module
- frontend-only custom domain restoration is therefore a separate infra decision, not a toggle
- the load balancer is the fallback, not the presumed first move

Exit criteria:
- users reach the frontend via `app.ceiora.com`
- backend services remain non-public
- the project does not regress to the old three-public-host shape unless explicitly chosen
- the old `api.ceiora.com` and `control.ceiora.com` aliases are absent from live DNS

### Phase 4: New public `/`

Status:
- completed in repo with a temporary public landing placeholder

Changes:
- replace the current `/` content with the future public landing page
- keep `/home` as the authenticated app entry
- preserve login entry from the public landing page

Exit criteria:
- `/` is public and light
- `/home` is the stable authenticated app home

## Tradeoffs

### Why not do all of this at once?

Because the current repo lacks:

- a session subsystem
- route middleware
- service-to-service backend auth from the frontend
- a frontend-only custom-domain module

Landing all of that in one cut would make failures ambiguous and rollback harder.

### Why not restore the old load balancer immediately?

Because the current edge module is intentionally three-host and brings back more surface area than the target shape needs.

Firebase is now the live frontend-only custom-domain path.
The load balancer remains rollback-only fallback infrastructure, not part of the active topology.

### Why not keep browser-held operator/editor tokens as the main auth model?

Because that model:

- is not a real app auth wall
- cannot protect page routes before client render
- keeps mutating/control privileges too close to the browser

## Executed In This Change

- `/home` is added as the future authenticated app home
- `/` is now a lightweight public landing placeholder and `/login` is the shared-session entry
- the frontend now owns a shared signed-cookie session boundary with middleware protection for:
  - `/home`
  - `/cuse/*`
  - `/cpar/*`
  - `/positions`
  - `/data`
  - privileged `/settings`
- protected frontend `/api/*` routes now reject unauthenticated traffic before contacting backend services
- browser-used `/api` namespaces are now covered by owned App Router handlers, including:
  - `cpar`
  - `holdings`
  - `universe`
  - `exposures/history`
- the global catch-all `/api/*` rewrite is removed
- anonymous operator/control fetches are suppressed in:
  - the shared shell navigation chrome
  - the health page operator/update surface
  - the generic API error refresh CTA surface
- frontend proxy helpers can now attach Cloud Run IAM service auth when `CLOUD_RUN_BACKEND_IAM_AUTH=true`
- Terraform now has a `private_backend_invocation_enabled` switch that:
  - points the frontend runtime at backend `run.app` service URLs
  - enables frontend-to-backend Cloud Run IAM auth
  - removes unauthenticated invoker from `serve` and `control`
  - grants the frontend service account invoker on `serve` and `control`

Chosen direction now reflected in this blueprint:
- shared app login/session first
- `/settings` remains a privileged page for the primary account
- Firebase is the live `app.ceiora.com` path; the load balancer is fallback only
- the shared app session uses the `__session` cookie name because Firebase Hosting forwards that cookie on Cloud Run rewrites

## Remaining Cleanup

The staged plan is complete.
The only intentionally deferred cleanup is removal of the temporary ACME challenge route once Firebase control-plane status settles beyond certificate propagation.

See:
- `docs/operations/cutover_evidence/FRONTEND_AUTH_EXECUTION_20260415T010336Z.md`
