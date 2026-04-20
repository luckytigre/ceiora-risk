import type { AppAuthContextPayload } from "@/app/api/auth/_context";
import type { AppAuthProvider } from "@/lib/appAuth";

export const APP_AUTH_BOOTSTRAP_HEADER = "x-app-auth-bootstrap";

export interface AppSessionPayload {
  authProvider: AppAuthProvider;
  username: string;
  email?: string | null;
  displayName?: string | null;
  defaultAccountId?: string | null;
  isAdmin: boolean;
  primary: boolean;
  expiresAt?: number;
}

export interface AuthSessionBootstrapPayload {
  authenticated: boolean;
  session: AppSessionPayload | null;
  context: AppAuthContextPayload | null;
  contextError?: {
    message?: string;
    code?: string | null;
  } | null;
}

function base64UrlEncode(value: string): string {
  const bytes = new TextEncoder().encode(value);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64UrlDecode(value: string): string {
  const base64 = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = `${base64}${"=".repeat((4 - (base64.length % 4 || 4)) % 4)}`;
  const binary = atob(padded);
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  return new TextDecoder().decode(bytes);
}

export function encodeAuthSessionBootstrapHeader(payload: AuthSessionBootstrapPayload): string {
  return base64UrlEncode(JSON.stringify(payload));
}

export function decodeAuthSessionBootstrapHeader(value: string | null | undefined): AuthSessionBootstrapPayload | null {
  const clean = String(value || "").trim();
  if (!clean) return null;
  try {
    const parsed = JSON.parse(base64UrlDecode(clean)) as AuthSessionBootstrapPayload;
    if (!parsed || typeof parsed !== "object") return null;
    return {
      authenticated: Boolean(parsed.authenticated),
      session: parsed.authenticated ? parsed.session ?? null : null,
      context: parsed.authenticated ? parsed.context ?? null : null,
      contextError: parsed.authenticated ? parsed.contextError ?? null : null,
    };
  } catch {
    return null;
  }
}
