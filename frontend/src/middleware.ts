import { NextRequest, NextResponse } from "next/server";
import type { AppAuthContextPayload } from "@/app/api/auth/_context";
import { DEFAULT_APP_HOME_PATH, isPrivilegedApiPath, isPrivilegedPagePath, isProtectedApiPath, isProtectedPagePath, normalizeReturnTo } from "@/lib/appAccess";
import { APP_SESSION_COOKIE_NAMES, appAuthProvider, authConfigMissingKeys, clearedAppSessionCookieOptions, isAppAuthConfigured, readSessionFromRequest, readSessionTokenFromRequest } from "@/lib/appAuth";
import { APP_AUTH_BOOTSTRAP_HEADER, encodeAuthSessionBootstrapHeader, type AuthSessionBootstrapPayload } from "@/lib/authSessionBootstrap";

function unauthorizedApi(detail: string, status = 401): NextResponse {
  return NextResponse.json({ detail }, { status });
}

function clearSessionCookies(res: NextResponse): NextResponse {
  for (const cookieName of APP_SESSION_COOKIE_NAMES) {
    res.cookies.set(cookieName, "", clearedAppSessionCookieOptions());
  }
  return res;
}

function shouldPreserveNeonSession(code: string | null | undefined): boolean {
  return (
    code === "account_provisioning_required" ||
    code === "account_context_unavailable" ||
    code === "account_bootstrap_disabled"
  );
}

function backendOrigin(): string {
  return (process.env.BACKEND_API_ORIGIN || "http://127.0.0.1:8000").replace(/\/+$/, "");
}

function backendIamAuthEnabled(): boolean {
  return String(process.env.CLOUD_RUN_BACKEND_IAM_AUTH || "").trim().toLowerCase() === "true";
}

async function fetchCloudRunIdentityToken(audience: string): Promise<string> {
  const metadataUrl = new URL("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity");
  metadataUrl.searchParams.set("audience", audience);
  metadataUrl.searchParams.set("format", "full");
  const res = await fetch(metadataUrl.toString(), {
    headers: { "Metadata-Flavor": "Google" },
    cache: "no-store",
  });
  if (!res.ok) {
    throw new Error(`Could not mint Cloud Run identity token for ${audience}.`);
  }
  return res.text();
}

type MiddlewareAuthContextResult =
  | { ok: true; context: AppAuthContextPayload; defaultAccountId: string | null; isAdmin: boolean }
  | { ok: false; status: number; code: string | null };

async function fetchLiveNeonAuthContext(
  req: NextRequest,
  options: { sessionToken: string },
): Promise<MiddlewareAuthContextResult> {
  try {
    const { sessionToken } = options;
    const upstream = `${backendOrigin()}/api/auth/context`;
    const headers = new Headers({ "x-app-session-token": sessionToken });
    if (backendIamAuthEnabled()) {
      headers.set("authorization", `Bearer ${await fetchCloudRunIdentityToken(new URL(upstream).origin)}`);
    }
    const res = await fetch(upstream, {
      method: "GET",
      headers,
      cache: "no-store",
    });
    if (!res.ok) {
      const payload = await res.json().catch(() => ({}));
      const detail = payload?.detail;
      const code =
        typeof detail?.code === "string"
          ? detail.code
          : typeof detail?.error === "string"
            ? detail.error
            : res.status === 409
              ? "account_provisioning_required"
              : null;
      return { ok: false, status: res.status, code };
    }
    const payload = (await res.json().catch(() => ({}))) as {
      auth_provider?: "shared" | "neon";
      subject?: string;
      email?: string | null;
      display_name?: string | null;
      is_admin?: boolean;
      account_enforcement_enabled?: boolean;
      default_account_id?: string | null;
      account_ids?: string[];
      admin_settings_enabled?: boolean;
    };
    const context: AppAuthContextPayload = {
      auth_provider: payload.auth_provider === "shared" ? "shared" : "neon",
      subject: String(payload.subject || "").trim(),
      email: typeof payload.email === "string" ? payload.email : null,
      display_name: typeof payload.display_name === "string" ? payload.display_name : null,
      is_admin: Boolean(payload.is_admin),
      account_enforcement_enabled: Boolean(payload.account_enforcement_enabled),
      default_account_id: payload.default_account_id ?? null,
      account_ids: Array.isArray(payload.account_ids) ? payload.account_ids.map((value) => String(value)) : [],
      admin_settings_enabled: payload.admin_settings_enabled !== false,
    };
    return {
      ok: true,
      context,
      defaultAccountId: context.default_account_id ?? null,
      isAdmin: Boolean(context.is_admin),
    };
  } catch {
    return { ok: false, status: 503, code: "account_context_unavailable" };
  }
}

function authBootstrapPayload(
  session: Awaited<ReturnType<typeof readSessionFromRequest>>,
  provider: ReturnType<typeof appAuthProvider>,
  neonContextStatus: MiddlewareAuthContextResult | null,
): AuthSessionBootstrapPayload | null {
  if (!session) return null;
  const context = provider === "neon" && neonContextStatus?.ok ? neonContextStatus.context : null;
  return {
    authenticated: true,
    session: {
      authProvider: session.authProvider,
      username: session.username,
      email: context?.email ?? session.email ?? null,
      displayName: context?.display_name ?? session.displayName ?? null,
      defaultAccountId:
        provider === "neon" && context
          ? context.default_account_id ?? null
          : session.defaultAccountId ?? null,
      isAdmin: provider === "neon" && context ? Boolean(context.is_admin) : session.isAdmin,
      primary: provider === "neon" && context ? Boolean(context.is_admin) : session.primary,
      expiresAt: session.expiresAt,
    },
    context,
    contextError: null,
  };
}

function nextWithAuthBootstrap(req: NextRequest, payload: AuthSessionBootstrapPayload | null): NextResponse {
  if (!payload) return NextResponse.next();
  const requestHeaders = new Headers(req.headers);
  requestHeaders.set(APP_AUTH_BOOTSTRAP_HEADER, encodeAuthSessionBootstrapHeader(payload));
  return NextResponse.next({
    request: {
      headers: requestHeaders,
    },
  });
}

export async function middleware(req: NextRequest) {
  const { pathname, search } = req.nextUrl;
  const protectedPage = isProtectedPagePath(pathname);
  const protectedApi = isProtectedApiPath(pathname);
  const onLoginPage = pathname === "/login";
  const loginReturnTo = normalizeReturnTo(req.nextUrl.searchParams.get("returnTo"));
  const loginHasDefaultReturnTo =
    onLoginPage &&
    req.nextUrl.searchParams.has("returnTo") &&
    loginReturnTo === DEFAULT_APP_HOME_PATH &&
    !req.nextUrl.searchParams.has("error");

  if (!protectedPage && !protectedApi && !onLoginPage) {
    return NextResponse.next();
  }

  if (loginHasDefaultReturnTo) {
    const url = req.nextUrl.clone();
    url.pathname = "/login";
    url.search = "";
    return NextResponse.redirect(url);
  }

  if (!isAppAuthConfigured()) {
    const detail = `App auth is not configured. Missing: ${authConfigMissingKeys().join(", ")}`;
    if (protectedApi) return unauthorizedApi(detail, 503);
    if (protectedPage) {
      const url = req.nextUrl.clone();
      url.pathname = "/login";
      url.searchParams.set("error", "misconfigured");
      url.searchParams.set("returnTo", normalizeReturnTo(`${pathname}${search}`));
      return NextResponse.redirect(url);
    }
    if (onLoginPage) return NextResponse.next();
  }

  const provider = appAuthProvider();
  const rawSession = await readSessionFromRequest(req);
  const session = await readSessionFromRequest(req, { expectedProvider: provider });
  const hasStaleSession = !session && Boolean(rawSession);
  let neonContextStatus: MiddlewareAuthContextResult | null = null;
  if (provider === "neon" && session && (protectedPage || protectedApi || onLoginPage)) {
    const sessionToken = await readSessionTokenFromRequest(req, { expectedProvider: provider });
    if (sessionToken) {
      neonContextStatus = await fetchLiveNeonAuthContext(req, { sessionToken });
    } else {
      neonContextStatus = { ok: false, status: 401, code: "session_expired" };
    }
  }
  const neonSessionContextReady =
    provider !== "neon" ||
    (neonContextStatus
      ? neonContextStatus.ok &&
        (Boolean(neonContextStatus.defaultAccountId) ||
          (neonContextStatus.isAdmin &&
            (onLoginPage || isPrivilegedPagePath(pathname) || isPrivilegedApiPath(pathname))))
      : Boolean(session?.defaultAccountId));
  if (onLoginPage && session) {
    if (!neonSessionContextReady) {
      const code =
        neonContextStatus && !neonContextStatus.ok && neonContextStatus.code
          ? neonContextStatus.code
          : "account_context_unavailable";
      if (shouldPreserveNeonSession(code) && req.nextUrl.searchParams.get("error") === code) {
        return NextResponse.next();
      }
      const url = req.nextUrl.clone();
      url.pathname = "/login";
      url.search = "";
      url.searchParams.set("error", code);
      const res = NextResponse.redirect(url);
      return shouldPreserveNeonSession(code) ? res : clearSessionCookies(res);
    }
    if (
      provider === "neon" &&
      neonContextStatus?.ok &&
      neonContextStatus.isAdmin &&
      !neonContextStatus.defaultAccountId &&
      loginReturnTo === DEFAULT_APP_HOME_PATH
    ) {
      const url = req.nextUrl.clone();
      url.pathname = "/settings/admin";
      url.search = "";
      return NextResponse.redirect(url);
    }
    const url = req.nextUrl.clone();
    url.pathname = loginReturnTo;
    url.search = "";
    return NextResponse.redirect(url);
  }
  if (onLoginPage && hasStaleSession) {
    const url = req.nextUrl.clone();
    url.pathname = "/login";
    url.search = "";
    url.searchParams.set("error", "session_expired");
    return clearSessionCookies(NextResponse.redirect(url));
  }

  if (!protectedPage && !protectedApi) {
    return NextResponse.next();
  }

  if (!session) {
    if (protectedApi) {
      const res = unauthorizedApi("Unauthorized: sign in required.");
      return hasStaleSession ? clearSessionCookies(res) : res;
    }
    const url = req.nextUrl.clone();
    url.pathname = "/login";
    url.search = "";
    const requestedReturnTo = normalizeReturnTo(`${pathname}${search}`);
    if (requestedReturnTo !== DEFAULT_APP_HOME_PATH) {
      url.searchParams.set("returnTo", requestedReturnTo);
    }
    if (hasStaleSession) {
      url.searchParams.set("error", "session_expired");
    }
    const res = NextResponse.redirect(url);
    return hasStaleSession ? clearSessionCookies(res) : res;
  }

  if (!neonSessionContextReady) {
    if (protectedApi) {
      const code =
        neonContextStatus && !neonContextStatus.ok && neonContextStatus.code
          ? neonContextStatus.code
          : "account_context_unavailable";
      const status =
        neonContextStatus && !neonContextStatus.ok && neonContextStatus.status
          ? neonContextStatus.status
          : code === "account_provisioning_required"
            ? 409
            : 401;
      const res = unauthorizedApi(`Unauthorized: ${code}.`, status);
      return shouldPreserveNeonSession(code) ? res : clearSessionCookies(res);
    }
    const url = req.nextUrl.clone();
    url.pathname = "/login";
    url.search = "";
    const requestedReturnTo = normalizeReturnTo(`${pathname}${search}`);
    if (requestedReturnTo !== DEFAULT_APP_HOME_PATH) {
      url.searchParams.set("returnTo", requestedReturnTo);
    }
    const code =
      neonContextStatus && !neonContextStatus.ok && neonContextStatus.code
        ? neonContextStatus.code
        : "account_context_unavailable";
    url.searchParams.set("error", code);
    const res = NextResponse.redirect(url);
    return shouldPreserveNeonSession(code) ? res : clearSessionCookies(res);
  }

  const effectiveIsAdmin =
    provider === "neon" && neonContextStatus?.ok ? neonContextStatus.isAdmin : session.isAdmin;

  if (protectedPage && isPrivilegedPagePath(pathname) && !effectiveIsAdmin) {
    const url = req.nextUrl.clone();
    url.pathname = "/settings";
    url.search = "";
    url.searchParams.set("error", "admin_required");
    return NextResponse.redirect(url);
  }

  if (protectedApi && isPrivilegedApiPath(pathname) && !effectiveIsAdmin) {
    return unauthorizedApi("Forbidden: admin session required.", 403);
  }

  return nextWithAuthBootstrap(req, protectedPage ? authBootstrapPayload(session, provider, neonContextStatus) : null);
}

export const config = {
  matcher: [
    "/home/:path*",
    "/cuse/:path*",
    "/cpar/:path*",
    "/positions/:path*",
    "/data/:path*",
    "/settings/:path*",
    "/login",
    "/api/:path*",
  ],
};
