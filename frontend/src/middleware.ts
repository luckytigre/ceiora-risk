import { NextRequest, NextResponse } from "next/server";
import { DEFAULT_APP_HOME_PATH, isPrivilegedPagePath, isProtectedApiPath, isProtectedPagePath, normalizeReturnTo } from "@/lib/appAccess";
import { authConfigMissingKeys, isAppAuthConfigured, readSessionFromRequest } from "@/lib/appAuth";

function unauthorizedApi(detail: string, status = 401): NextResponse {
  return NextResponse.json({ detail }, { status });
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

  const session = await readSessionFromRequest(req);
  if (onLoginPage && session) {
    const url = req.nextUrl.clone();
    url.pathname = loginReturnTo;
    url.search = "";
    return NextResponse.redirect(url);
  }

  if (!protectedPage && !protectedApi) {
    return NextResponse.next();
  }

  if (!session) {
    if (protectedApi) {
      return unauthorizedApi("Unauthorized: sign in required.");
    }
    const url = req.nextUrl.clone();
    url.pathname = "/login";
    url.search = "";
    const requestedReturnTo = normalizeReturnTo(`${pathname}${search}`);
    if (requestedReturnTo !== DEFAULT_APP_HOME_PATH) {
      url.searchParams.set("returnTo", requestedReturnTo);
    }
    return NextResponse.redirect(url);
  }

  if (protectedPage && isPrivilegedPagePath(pathname) && !session.primary) {
    const url = req.nextUrl.clone();
    url.pathname = DEFAULT_APP_HOME_PATH;
    url.search = "";
    return NextResponse.redirect(url);
  }

  return NextResponse.next();
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
