# Neon Auth Integration Plan

Date: 2026-04-18
Owner: Codex
Status: Draft execution plan

Delete this file after implementation is complete, validation passes, and the relevant canonical docs are updated. This is a transitional rollout plan, not a permanent reference surface.

## Purpose

Replace the current shared frontend login/session boundary with Neon-backed per-user authentication so a small set of real users can:

- create accounts
- sign in
- upload and manage their own portfolios
- see only their own holdings, scenarios, and risk outputs

without overbuilding a full enterprise tenancy/RBAC system.

## Problem Statement

The current auth/session model is intentionally minimal:

- one shared frontend login
- one shared cookie-backed app session
- privileged backend routes still rely on browser-held operator/editor tokens
- holdings are already persisted in Neon, but current read/write surfaces are effectively global by `account_id`

That is enough for one shared operator session, but it is not safe or coherent for 4–5 friends each uploading personal portfolios.

## Goals

- Use Neon Auth as the user identity and session layer.
- Preserve the current app shell and route structure where possible.
- Keep the product simple: one personal account per user by default.
- Reuse the existing holdings authority model rather than inventing a second account system.
- Enforce per-user/per-account isolation in backend services and at the database boundary.
- Split ordinary user settings from operator/admin controls.
- Keep operator/backend authority distinct from normal product auth.

## Environment Contract

Current shared-auth envs that are transitional and must be explicitly deprecated during rollout:

- `CEIORA_SHARED_LOGIN_USERNAME`
- `CEIORA_SHARED_LOGIN_PASSWORD`
- `CEIORA_SESSION_SECRET`
- `CEIORA_PRIMARY_ACCOUNT_USERNAME`

Adjacent runtime envs that auth rollout must not disturb:

- `DATA_BACKEND`
- `NEON_DATABASE_URL`
- `APP_RUNTIME_ROLE`
- `NEON_AUTHORITATIVE_REBUILDS`

Operator/runtime envs that remain separate from product auth:

- `OPERATOR_API_TOKEN`
- `EDITOR_API_TOKEN`
- `CLOUD_RUN_JOBS_ENABLED`
- `CLOUD_RUN_PROJECT_ID`
- `CLOUD_RUN_REGION`

New auth/provider envs must be defined explicitly rather than implied:

- `APP_AUTH_PROVIDER=shared|neon`
- `APP_AUTH_BOOTSTRAP_ENABLED=true|false`
- `APP_ACCOUNT_ENFORCEMENT_ENABLED=true|false`
- `APP_ADMIN_SETTINGS_ENABLED=true|false`
- `APP_SHARED_AUTH_ACCEPT_LEGACY=true|false`
- `NEON_AUTH_ISSUER`
- `NEON_AUTH_JWKS_URL`
- `NEON_AUTH_BASE_URL`
- `NEON_AUTH_ALLOWED_EMAILS` or equivalent allowlist/invite config
- `NEON_AUTH_BOOTSTRAP_ADMINS`
- local-dev callback/origin settings

For the initial friend-scale rollout, treat these as required operational contracts:

- `NEON_AUTH_ALLOWED_EMAILS` must be non-empty
- `APP_AUTH_BOOTSTRAP_ENABLED` must be `true` whenever `APP_ACCOUNT_ENFORCEMENT_ENABLED=true`

## Non-Goals

- Building orgs, invites, or multi-workspace switching in v1.
- Shipping collaboration features.
- Replacing operator/runtime authority with user auth.
- Letting auth-provider metadata become the sole app authorization model.
- Landing a one-shot auth + tenancy + admin rewrite without fallback.

## Accepted Direction

### 1. Neon Auth is identity, not the whole app model

Neon Auth should issue user identity and authenticated session context.
Ceiora should still own:

- app user profile
- account ownership / membership
- authorization
- data scoping

Legacy shared auth must not mint fake account-scoped identity fields that look like real Neon-backed principals. During coexistence, shared auth remains explicitly legacy and must be prevented from drifting into the new account model by naming or shape.

### 2. Reuse `holdings_accounts` as the initial app account surface

Do not create a parallel `accounts` table in v1 if it duplicates the existing holdings authority.

The repo already has Neon holdings tables:

- `holdings_accounts`
- `holdings_positions_current`
- `holdings_import_batches`
- `holdings_position_events`

The first multi-user cut should extend that model rather than fork it.

### 3. One personal account per user, no account switching in v1

For the initial rollout:

- each authenticated user gets one default personal account
- one browser session maps to one active personal account
- no in-app account switcher
- no org/workspace selector

This keeps the UX small and avoids introducing a second navigation model prematurely.

### 4. Settings must split

The current `/settings` surface mixes:

- browser-held privileged tokens
- theme/background preferences
- model-specific settings

That cannot remain one page under per-user auth.

The app should split settings into:

- user settings
  - theme
  - background
  - model display preferences
- admin/operator settings
  - privileged maintenance controls
  - operator/editor token transitional controls, if they still exist

### 5. Database isolation is mandatory

Backend service-layer filtering alone is not enough.

The rollout must define database-level deny-by-default isolation for user-owned/account-owned tables, preferably via Neon/Postgres row-level security or an equivalent hard barrier.

## Current State Summary

Frontend:

- provider-seamed auth/session in [frontend/src/lib/appAuth.ts](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/frontend/src/lib/appAuth.ts)
- protected route map in [frontend/src/lib/appAccess.ts](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/frontend/src/lib/appAccess.ts)
- `/login` is provider-owned and can run either shared or Neon flows
- Neon login/session now preserves an authenticated-but-unready state for provisioning/account-context errors instead of collapsing immediately into a generic signed-out state
- Neon friend-onboarding assumes a non-empty allowlist and automatic personal-account bootstrap
- account-context failures now distinguish generic provisioning from `account_bootstrap_disabled`
- protected Neon page admission checks live `/api/auth/context` instead of trusting only cached cookie account state
- browser-held operator tokens remain only for explicit maintenance/control routes in admin settings
- app-facing holdings and what-if mutation proxies no longer forward privileged tokens by default

Backend:

- holdings reads/writes are Neon-backed
- holdings/account scope now has an authenticated app-session seam
- app-facing holdings and what-if mutations require app sessions when account enforcement is enabled
- app-admin no longer implies global analytics reads on the main cUSE/cPAR/holdings account-scoped surfaces
- some remaining maintenance/operator lanes still exist explicitly outside the normal per-user browser flow
- broader account-scoping audit remains incomplete for the whole app surface

## Schema Direction

### Add app-owned identity tables

Add:

- `app_users`
  - `user_id` UUID/text PK
  - `auth_user_id` text unique not null
  - `email` text
  - `display_name` text
  - `is_admin` boolean default false
  - `created_at`
  - `updated_at`

- `account_memberships`
  - `membership_id` UUID/text PK
  - `account_id` text not null references `holdings_accounts(account_id)`
  - `user_id` not null references `app_users(user_id)`
  - `role` text not null (`owner`, later maybe `member`)
  - `created_at`
  - unique `(account_id, user_id)`

Potential later additions, but not required for first cut:

- `account_invites`
- `user_preferences`
- `account_preferences`

### Extend `holdings_accounts`

Either extend `holdings_accounts` with app-facing ownership metadata or keep ownership solely in `account_memberships`.

Do not duplicate account identity in a second table unless a later requirement justifies it.

### Preserve current holdings/event tables

Do not replace:

- `holdings_positions_current`
- `holdings_import_batches`
- `holdings_position_events`

Instead, add user/account ownership enforcement around them.

## Authorization Model

### User identity

- authenticated Neon user id is the external identity
- app resolves that to exactly one `app_users` row

### Active account

For v1:

- active account is the user’s default personal account
- no account switcher in the frontend
- backend resolves active account from authenticated user membership rather than trusting the client

### Roles

For v1:

- `owner`
- `admin` (app-level, for privileged settings)

Keep this separate from operator/editor runtime authority.

### Operator/runtime authority

Operator/editor backend roles are not replaced by normal user auth.

They remain a separate authority lane for:

- refresh/control actions
- backend diagnostics
- maintenance tasks

The app must not accidentally treat normal Neon-authenticated users as operator-equivalent.

## Database Isolation Requirement

This rollout is not complete unless there is database-level isolation for user/account-owned tables.

Minimum requirement:

- deny-by-default access to user/account-owned rows unless the request is executing in the correct authenticated account context

Candidate implementation:

- Neon/Postgres RLS on app-owned/user-owned/account-owned tables
- or an equivalent DB-scoped pattern if direct RLS integration is not viable in the current runtime shape

At minimum, plan and document:

- which tables are user-owned/account-owned
- which remain global/shared
- which database role/session variables carry the authenticated principal/account

## What Must Become Account-Scoped

Immediately:

- holdings positions
- holdings imports
- holdings edits/removals
- cUSE what-if rows
- cPAR what-if rows
- saved scenarios/drafts when persisted
- user/account preferences

Must be reviewed explicitly:

- aggregate portfolio reads
- cPAR aggregate risk surfaces
- contributing account lists
- portfolio snapshots

Likely remains global/shared:

- factor catalogs
- model metadata
- risk engine state
- package catalogs
- operator health and diagnostics

## Rollout Phases

### Phase 0. Document and gate the target shape

Deliverables:

- this plan
- explicit auth/provider feature flags
- explicit cutover/rollback model

Required flags:

- `APP_AUTH_PROVIDER=shared|neon`
- `APP_AUTH_BOOTSTRAP_ENABLED=true|false`
- `APP_ACCOUNT_ENFORCEMENT_ENABLED=true|false`
- `APP_ADMIN_SETTINGS_ENABLED=true|false`
- `APP_SHARED_AUTH_ACCEPT_LEGACY=true|false`

Do not use one global flag for the whole migration.

### Coexistence rules

During migration, the provider boundary must be explicit:

- `/login` is owned by exactly one auth provider at a time
- session precedence must be deterministic if multiple cookies ever exist
- protected APIs must not silently accept both auth modes without documented precedence
- shared-auth sessions must not touch Neon-user-scoped data once account enforcement is enabled
- unsupported providers must be rejected, not parsed optimistically

### Phase 1. Introduce app identity tables and bootstrap

Deliverables:

- `app_users`
- `account_memberships`
- personal-account bootstrap logic

Requirements:

- bootstrap must be idempotent
- bootstrap must be transactional
- uniqueness must be enforced on `auth_user_id`
- one user gets one personal account membership by default

Local development requirements:

- define whether local dev uses a real Neon auth project or a mock/bypass
- define how first-login bootstrap works locally
- define how seeded/local users behave under coexistence
- keep `.env.local` expectations explicit so auth behavior does not drift machine-to-machine

Do not cut over frontend auth yet.

### Phase 2. Introduce Neon Auth session verification behind a provider boundary

Deliverables:

- auth provider abstraction in frontend auth/session helper
- Neon session verification path
- shared auth path still available behind provider selection

Requirements:

- different cookie/session acceptance must be explicit
- no ambiguous “both auth systems accepted everywhere” behavior
- legacy shared auth must not access new multi-user scoped data once enforcement begins
- Neon provider sessions are not accepted until Neon verification and app-user/account resolution both exist

### Phase 3. Split settings

Deliverables:

- `/settings` becomes user settings only, or at least normal-user-safe settings
- privileged/operator controls moved to an admin/operator surface

Requirements:

- theme/background/model display preferences should remain available to ordinary users
- operator/editor token controls must not remain in the ordinary settings experience
- privileged backend headers must not ride on the shared app transport for ordinary user traffic
- privileged frontend routes should use a separate admin transport while browser-held tokens still exist

### Phase 4. Service-layer account resolution

Add a single ownership seam in backend services/data:

- authenticated principal -> app user -> allowed memberships -> active account

Do not scatter account resolution across routes.

Deliverables:

- one backend helper/service for account resolution and membership checks
- routes stay thin and call it

### Phase 5. Migrate holdings reads/writes

Deliverables:

- `/api/holdings/accounts`
- `/api/holdings/positions`
- `/api/holdings/import`
- `/api/holdings/position`
- `/api/holdings/position/remove`

must all enforce authenticated account access.

Requirements:

- no global account enumeration for ordinary users
- no client-trusted `account_id` without membership validation
- mutation provenance continues to be recorded

### Phase 6. Migrate cUSE/cPAR account-derived flows

Must explicitly cover:

- cUSE portfolio reads
- cUSE what-if preview/apply
- cPAR portfolio hedge / what-if
- cPAR aggregate risk
- cPAR explore what-if if it uses account-derived state

Every one of these must make a deliberate choice between:

- current user’s personal account only
- allowed account set
- still-global shared surface

Do not leave this implicit.

### Phase 7. Cut over frontend login

Deliverables:

- `/login` becomes Neon-backed sign in / create account flow
- first-login bootstrap works
- protected pages use Neon-authenticated context

Requirements:

- shared auth remains available only as an explicitly bounded rollback path
- stale shared sessions must not land inside multi-user data paths

### Phase 8. Remove shared auth and browser-held privileged auth from normal flows

Remove:

- shared-login issuance
- shared-login acceptance
- browser-held operator/editor token dependency for ordinary user flows

Retain operator/runtime authority only where explicitly needed.

## Validation Matrix

### Authentication

- unauthenticated user cannot access protected pages
- authenticated user can access protected pages
- logout clears session
- expired session is rejected

### Bootstrap

- first login creates exactly one `app_users` row
- first login creates exactly one owner membership
- retries do not duplicate bootstrap artifacts

### Authorization

- user A cannot read user B’s holdings
- user A cannot write user B’s holdings
- user A cannot run what-if/apply against user B’s account
- non-admin cannot access admin/operator settings
- ordinary app reads do not forward privileged backend headers
- privileged routes only forward backend tokens through dedicated admin transport paths

### Runtime seams

- cUSE portfolio reads remain correct under account scoping
- cPAR aggregate/hedge/what-if remain correct under account scoping
- global/shared model metadata remains readable where intended

### Migration

- shared-auth fallback still works while enabled
- shared-auth cannot access Neon-user-scoped data once enforcement is on
- provider rollback can be done without schema rollback

## Rollback

Rollback should be provider/config rollback, not schema rollback.

Allowed rollback:

- switch frontend auth provider back to shared auth
- disable Neon bootstrap
- disable account enforcement
- keep new tables in place

Not allowed rollback:

- dropping new user/account tables
- leaving mixed auth acceptance undefined

Rollback must also define:

- cookie/session invalidation behavior
- bootstrap disable switch
- whether uploads/writes are temporarily disabled during rollback
- whether legacy shared auth is allowed to touch newly created user-owned rows

## Cutover Checkpoints

1. Schema ready
- `app_users`, `account_memberships`, and holdings-account extensions landed
- no frontend auth cutover yet

2. Verification ready
- Neon session verification implemented
- shared auth still primary

3. Bootstrap ready
- first-login bootstrap tested and idempotent
- admin bootstrap path defined

4. Dual-stack dark launch
- selected Neon users can sign in
- shared auth remains bounded fallback
- user-owned data paths are intentionally limited

5. User-data cutover
- holdings/scenarios/uploads enforce authenticated account scope
- shared auth no longer touches user-owned data

6. Admin/operator split complete
- user settings separated from admin/operator controls
- privileged maintenance flows use admin-only transport

7. Legacy auth removal
- shared-auth envs removed
- shared cookie acceptance removed
- canonical docs updated

## Implementation Notes

- Prefer stable external auth subject ids over email as the primary identity link key.
- Keep one signed-in account per browser session in v1.
- Do not add invites or account switching unless needed.
- Do not make route guards the only authorization layer.
- Update canonical docs after implementation and then delete this file.
