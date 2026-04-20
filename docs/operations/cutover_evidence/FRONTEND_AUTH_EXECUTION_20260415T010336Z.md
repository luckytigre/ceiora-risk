# Frontend Auth Execution Record

Date: 2026-04-15T01:03:36Z
Owner: Codex
Status: completed live; auth, private-backend, Firebase frontend-only domain cutover, and legacy-edge teardown are all complete

## Scope

Track the execution status for the frontend auth and custom-domain plan after the repo-side phases were implemented.

Reference plan:
- `docs/archive/implementation-trackers/FRONTEND_AUTH_AND_CUSTOM_DOMAIN_PLAN_2026-04-14.md`

## Completed In Repo

- Phase 0
  - `/home` exists as the future authenticated app entry
  - anonymous shared-shell operator/control reads are suppressed
- Phase 1
  - shared signed-cookie app session implemented in the frontend
  - `/home`, `/cuse/*`, `/cpar/*`, `/positions`, `/data` are protected
  - `/settings` is privileged for the primary account
  - `/login`, `/logout`, and `returnTo` behavior exist
- Phase 2A
  - the global `/api/*` rewrite was removed
  - browser-used `/api/*` traffic now terminates in owned App Router route handlers
- Phase 2B
  - frontend proxy code can use Cloud Run service-to-service auth
  - Terraform has a `private_backend_invocation_enabled` switch and frontend-to-backend invoker bindings prepared
- Phase 4
  - `/` is now a temporary public landing placeholder

## Secret And Auth Material

- shared username: stored outside the repo
- shared password: stored outside the repo
- generated session secret: stored outside the repo
- primary privileged account username: stored outside the repo

These values were saved to 1Password and must not be copied into repo docs or tracked files.

## Validation Completed

- `git diff --check`
- `./frontend/node_modules/.bin/tsc --noEmit --incremental false -p frontend/tsconfig.json`
- `node frontend/scripts/control_plane_proxy_contract_check.mjs`

`npm run build` was not run in this workspace because the local machine still lacks the repo-required Node `20.x` toolchain.

## Live Execution Completed

- Firebase project attachment succeeded for `project-4e18de12-63a3-4206-aaa`
- Firebase Hosting is deployed at `https://project-4e18de12-63a3-4206-aaa.web.app`
- the shared login/session env contract is live on the frontend
- `/home`, `/cuse/*`, `/cpar/*`, `/positions`, `/data`, and privileged `/settings` are frontend-protected
- direct public backend access is blocked; `serve` and `control` now require frontend service-to-service invocation
- shared auth material is stored in 1Password under `Ceiora Shared App Auth`
- the Cloudflare provider contract is confirmed:
  - `export CLOUDFLARE_API_TOKEN=\"$(op item get 'Cloudflare - Ceiora Risk' --vault Personal --fields label=credential --reveal)\"`

## Live Result

- `app.ceiora.com` now resolves to Firebase Hosting and serves the frontend successfully.
- The shared login works on the custom domain via the `__session` cookie.
- `/home` redirects unauthenticated users to `/login` and loads successfully after login.
- Authenticated frontend proxy calls work against the private backend services.
- Direct unauthenticated access to the backend `run.app` URLs returns `403`.
- The legacy HTTPS load balancer and edge resources were removed.
- The stale Cloudflare DNS aliases for `api.ceiora.com` and `control.ceiora.com` were deleted.

## Remaining Cleanup

The only intentionally deferred cleanup is removal of the temporary ACME challenge route from the frontend codebase once Firebase control-plane certificate status settles beyond propagation.

## Honest Status

Repo implementation and the live infra cutover are complete.
The only remaining low-risk cleanup is removal of the temporary ACME challenge route after Firebase control-plane certificate status catches up with the already working custom-domain TLS path.
