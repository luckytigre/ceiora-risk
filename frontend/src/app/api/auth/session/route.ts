import { NextRequest, NextResponse } from "next/server";
import { AppAuthContextError } from "@/app/api/auth/_context";
import { APP_SESSION_COOKIE_NAMES, appAuthProvider, clearedAppSessionCookieOptions, readSessionFromRequest } from "@/lib/appAuth";
import { fetchAppAuthContext } from "@/app/api/auth/_context";

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

export async function GET(req: NextRequest) {
  const rawSession = await readSessionFromRequest(req);
  const session = await readSessionFromRequest(req, { expectedProvider: appAuthProvider() });
  if (!session) {
    const res = NextResponse.json({ authenticated: false });
    return rawSession ? clearSessionCookies(res) : res;
  }
  let context = null;
  try {
    context = await fetchAppAuthContext(req, { bestEffort: session.authProvider !== "neon" });
  } catch (error) {
    const status = error instanceof AppAuthContextError ? error.status : 503;
    const message =
      error instanceof Error && error.message
        ? error.message
        : "Could not load authenticated account context.";
    const code =
      error instanceof AppAuthContextError
        ? error.code
        : status === 409
          ? "account_provisioning_required"
          : "account_context_unavailable";
    if (session.authProvider === "neon" && shouldPreserveNeonSession(code)) {
      return NextResponse.json({
        authenticated: true,
        session: {
          authProvider: session.authProvider,
          username: session.username,
          email: session.email ?? null,
          defaultAccountId: session.defaultAccountId ?? null,
          isAdmin: session.isAdmin,
          primary: session.primary,
          expiresAt: session.expiresAt,
        },
        context: null,
        contextError: { message, code },
      });
    }
    const res = NextResponse.json({ detail: { message, code } }, { status });
    return session.authProvider === "neon" && !shouldPreserveNeonSession(code) ? clearSessionCookies(res) : res;
  }
  if (session.authProvider === "neon" && !context) {
    return NextResponse.json({
      authenticated: true,
      session: {
        authProvider: session.authProvider,
        username: session.username,
        email: session.email ?? null,
        defaultAccountId: session.defaultAccountId ?? null,
        isAdmin: session.isAdmin,
        primary: session.primary,
        expiresAt: session.expiresAt,
      },
      context: null,
      contextError: { message: "Could not load authenticated account context.", code: "account_context_unavailable" },
    });
  }
  return NextResponse.json({
    authenticated: true,
      session: {
        authProvider: session.authProvider,
        username: session.username,
        email: context?.email ?? session.email ?? null,
        defaultAccountId:
          session.authProvider === "neon" && context
            ? context.default_account_id ?? null
            : session.defaultAccountId ?? null,
        isAdmin: session.authProvider === "neon" && context ? Boolean(context.is_admin) : session.isAdmin,
        primary:
          session.authProvider === "neon" && context ? Boolean(context.is_admin) : session.primary,
        expiresAt: session.expiresAt,
      },
      context,
  });
}
