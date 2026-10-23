// Synthetic shared-auth fixture for self-starting local regression servers.
import { createHmac, randomBytes } from "node:crypto";
const secret = randomBytes(32).toString("hex");
const username = "smoke";
const now = Math.floor(Date.now() / 1000);
function signSession(user) {
  const payload = Buffer.from(JSON.stringify({ authProvider: "shared", username: user, subject: user, isAdmin: true, primary: true, issuedAt: now, expiresAt: now + 3600 })).toString("base64url");
  return `${payload}.${createHmac("sha256", secret).update(payload).digest("base64url")}`;
}
const token = signSession(username);
export const smokeEnv = {
  APP_AUTH_PROVIDER: "shared", CEIORA_SHARED_LOGIN_USERNAME: username,
  CEIORA_SHARED_LOGIN_PASSWORD: "unused-local-fixture", CEIORA_SESSION_SECRET: secret,
  APP_ACCOUNT_ENFORCEMENT_ENABLED: "false", APP_SHARED_AUTH_ACCEPT_LEGACY: "true",
  BACKEND_API_ORIGIN: "http://127.0.0.1:9", BACKEND_CONTROL_ORIGIN: "http://127.0.0.1:9",
  CLOUD_RUN_BACKEND_IAM_AUTH: "false",
};
export async function authenticateSmokePage(page, origin, user = username) {
  if (!["localhost", "127.0.0.1"].includes(new URL(origin).hostname)) throw new Error("Smoke auth is local only");
  await page.context().addCookies([{ name: "__session", value: signSession(user), url: origin, httpOnly: true, sameSite: "Lax" }]);
}
export function smokeFetch(url, options = {}) {
  const parsed = new URL(url);
  if (!["localhost", "127.0.0.1"].includes(parsed.hostname)) throw new Error("Smoke auth is local only");
  const headers = new Headers(options.headers);
  headers.set("cookie", `__session=${token}`);
  return globalThis.fetch(url, { ...options, headers });
}

export async function handleSmokeAuth(route, { enableTheses = false, user = username } = {}) {
  const request = route.request();
  const path = new URL(request.url()).pathname;
  if (request.method() !== "GET") return false;
  let body;
  if (path === "/api/auth/session") {
    body = { authenticated: true,
      session: { authProvider: "shared", username: user, subject: user, isAdmin: true, primary: true, expiresAt: now + 3600 },
      context: { auth_provider: "shared", subject: user, is_admin: true, account_enforcement_enabled: false,
        default_account_id: null, account_ids: [], admin_settings_enabled: true } };
  } else if (path === "/api/portfolio/theses/capability") {
    body = { available: enableTheses, ...(enableTheses ? { workspace_id: "00000000-0000-4000-8000-000000000001" } : {}) };
  } else if (enableTheses && path === "/api/portfolio/theses") {
    body = { version: 1, revision: 0, workspace_id: "00000000-0000-4000-8000-000000000001", theses: [], allocations: [], manual_orders: {} };
  } else return false;
  await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(body) });
  return true;
}
