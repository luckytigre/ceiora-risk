import { NextRequest, NextResponse } from "next/server";
import { AppAuthContextError } from "@/app/api/auth/_context";
import { appAuthProvider, appSessionCookieOptions, APP_SESSION_COOKIE_NAME, authenticateNeonLogin, authenticateSharedLogin, createSessionToken, isAppAuthConfigured, sharedLegacyLoginAllowed } from "@/lib/appAuth";
import { normalizeReturnTo } from "@/lib/appAccess";
import { fetchAppAuthContext } from "@/app/api/auth/_context";

export async function POST(req: NextRequest) {
  if (!isAppAuthConfigured()) {
    return NextResponse.json({ detail: "App auth is not configured." }, { status: 503 });
  }

  let payload: {
    provider?: string;
    username?: string;
    password?: string;
    returnTo?: string;
    idToken?: string;
    token?: string;
  } | null = null;
  try {
    payload = await req.json();
  } catch {
    return NextResponse.json({ detail: "Invalid login payload." }, { status: 400 });
  }

  const provider = appAuthProvider();
  if (provider === "shared" && !sharedLegacyLoginAllowed()) {
    return NextResponse.json(
      { detail: "Shared login is disabled while account-scoped Neon enforcement is enabled." },
      { status: 403 },
    );
  }
  const requestedProvider = String(payload?.provider || "").trim().toLowerCase();
  if (requestedProvider && requestedProvider !== provider) {
    return NextResponse.json({ detail: `Configured auth provider is ${provider}.` }, { status: 400 });
  }
  const session =
    provider === "neon"
      ? await authenticateNeonLogin(payload?.idToken || payload?.token || "")
      : await authenticateSharedLogin(payload?.username || "", payload?.password || "");
  if (!session) {
    return NextResponse.json(
      { detail: provider === "neon" ? "Invalid or unauthorized Neon session." : "Invalid username or password." },
      { status: 401 },
    );
  }

  let token = await createSessionToken(session);
  const returnTo = normalizeReturnTo(payload?.returnTo);
  const sessionCookieOptions = appSessionCookieOptions(session.expiresAt);
  let context = null;
  try {
    context = await fetchAppAuthContext(req, {
      sessionTokenOverride: token,
      bestEffort: provider !== "neon",
    });
  } catch (error) {
    if (provider === "neon") {
      const message =
        error instanceof Error && error.message
          ? error.message
          : "Could not initialize your Ceiora account.";
      const status = error instanceof AppAuthContextError ? error.status : 503;
      const code = error instanceof AppAuthContextError ? error.code : null;
      const res = NextResponse.json(
        { detail: { message, code: code ?? (status === 409 ? "account_provisioning_required" : null) } },
        { status },
      );
      res.cookies.set(APP_SESSION_COOKIE_NAME, token, sessionCookieOptions);
      return res;
    }
  }
  if (provider === "neon" && !context) {
    const res = NextResponse.json(
      { detail: { message: "Could not initialize your Ceiora account.", code: "account_context_unavailable" } },
      { status: 503 },
    );
    res.cookies.set(APP_SESSION_COOKIE_NAME, token, sessionCookieOptions);
    return res;
  }
  if (provider === "neon") {
    session.email = context?.email ?? session.email ?? undefined;
    session.displayName = context?.display_name ?? session.displayName ?? undefined;
    session.isAdmin = Boolean(context?.is_admin);
    session.primary = session.isAdmin;
    session.defaultAccountId = context?.default_account_id ?? null;
    token = await createSessionToken(session);
  }
  const res = NextResponse.json({
    ok: true,
    returnTo,
    session: {
      authProvider: session.authProvider,
      username: session.username,
      email: session.email ?? null,
      defaultAccountId: session.defaultAccountId ?? null,
      isAdmin: session.isAdmin,
      primary: session.primary,
      expiresAt: session.expiresAt,
    },
    context,
  });
  res.cookies.set(APP_SESSION_COOKIE_NAME, token, sessionCookieOptions);
  return res;
}
