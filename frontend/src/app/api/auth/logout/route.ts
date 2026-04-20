import { NextResponse } from "next/server";
import { APP_SESSION_COOKIE_NAMES, clearedAppSessionCookieOptions } from "@/lib/appAuth";

export async function POST() {
  const res = NextResponse.json({ ok: true });
  for (const cookieName of APP_SESSION_COOKIE_NAMES) {
    res.cookies.set(cookieName, "", clearedAppSessionCookieOptions());
  }
  return res;
}
