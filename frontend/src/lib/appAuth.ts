import type { NextRequest } from "next/server";
import { createLocalJWKSet, createRemoteJWKSet, jwtVerify, type JWK } from "jose";

export const APP_SESSION_COOKIE_NAME = "__session";
const LEGACY_APP_SESSION_COOKIE_NAMES = ["ceiora-app-session", "ceiora.app-session"] as const;
export const APP_SESSION_COOKIE_NAMES = [APP_SESSION_COOKIE_NAME, ...LEGACY_APP_SESSION_COOKIE_NAMES] as const;

const APP_SESSION_TTL_SECONDS = 60 * 60 * 24 * 30;

export type AppAuthProvider = "shared" | "neon";

export interface AppSession {
  authProvider: AppAuthProvider;
  username: string;
  subject?: string;
  email?: string;
  displayName?: string;
  defaultAccountId?: string | null;
  isAdmin: boolean;
  /**
   * Legacy compatibility flag for older middleware/UI checks.
   * Keep until the remaining `session.primary` assumptions are removed.
   */
  primary: boolean;
  issuedAt: number;
  expiresAt: number;
}

interface AuthConfig {
  provider: AppAuthProvider;
  username: string;
  password: string;
  primaryUsername: string;
  secret: string;
  neonIssuer: string;
  neonJwksUrl: string;
  neonJwksJson: string;
  neonAudience: string;
  neonProjectUrl: string;
  neonAllowedIdentities: string[];
  neonAdminIdentities: string[];
}

interface CookieStoreLike {
  get(name: string): { value: string } | undefined;
}

function textEncoder(): TextEncoder {
  return new TextEncoder();
}

function utf8Bytes(value: string): Uint8Array {
  return textEncoder().encode(value);
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64UrlDecode(value: string): Uint8Array {
  const base64 = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = `${base64}${"=".repeat((4 - (base64.length % 4 || 4)) % 4)}`;
  const binary = atob(padded);
  return Uint8Array.from(binary, (char) => char.charCodeAt(0));
}

function authConfig(): AuthConfig {
  const provider = String(process.env.APP_AUTH_PROVIDER || "shared").trim().toLowerCase() === "neon" ? "neon" : "shared";
  const username = String(process.env.CEIORA_SHARED_LOGIN_USERNAME || "").trim();
  const password = String(process.env.CEIORA_SHARED_LOGIN_PASSWORD || "").trim();
  const secret = String(process.env.CEIORA_SESSION_SECRET || "").trim();
  const primaryUsername = String(process.env.CEIORA_PRIMARY_ACCOUNT_USERNAME || username).trim() || username;
  const neonProjectUrl =
    String(process.env.NEON_AUTH_BASE_URL || "").trim() ||
    String(process.env.NEON_AUTH_PROJECT_URL || "").trim();
  return {
    provider,
    username,
    password,
    primaryUsername,
    secret,
    neonIssuer: String(process.env.NEON_AUTH_ISSUER || "").trim(),
    neonJwksUrl: String(process.env.NEON_AUTH_JWKS_URL || "").trim(),
    neonJwksJson: String(process.env.NEON_AUTH_JWKS_JSON || "").trim(),
    neonAudience: String(process.env.NEON_AUTH_AUDIENCE || "").trim(),
    neonProjectUrl,
    neonAllowedIdentities: String(process.env.NEON_AUTH_ALLOWED_EMAILS || "")
      .split(",")
      .map((part) => part.trim().toLowerCase())
      .filter(Boolean),
    neonAdminIdentities: String(process.env.NEON_AUTH_BOOTSTRAP_ADMINS || "")
      .split(",")
      .map((part) => part.trim().toLowerCase())
      .filter(Boolean),
  };
}

export function appAuthProvider(): AppAuthProvider {
  return authConfig().provider;
}

function envBool(name: string, fallback = false): boolean {
  const raw = String(process.env[name] ?? "").trim().toLowerCase();
  if (!raw) return fallback;
  return !["0", "false", "no", "off"].includes(raw);
}

export function sharedLegacyLoginAllowed(): boolean {
  const enforcementEnabled = envBool("APP_ACCOUNT_ENFORCEMENT_ENABLED", false);
  const sharedLegacyAllowed = envBool("APP_SHARED_AUTH_ACCEPT_LEGACY", false);
  return !enforcementEnabled || sharedLegacyAllowed;
}

export function neonAuthProjectUrl(): string {
  return authConfig().neonProjectUrl;
}

export function authConfigMissingKeys(): string[] {
  const cfg = authConfig();
  const missing: string[] = [];
  if (cfg.provider === "shared") {
    if (!cfg.username) missing.push("CEIORA_SHARED_LOGIN_USERNAME");
    if (!cfg.password) missing.push("CEIORA_SHARED_LOGIN_PASSWORD");
  } else {
    if (!cfg.neonIssuer) missing.push("NEON_AUTH_ISSUER");
    if (!cfg.neonJwksUrl && !cfg.neonJwksJson) missing.push("NEON_AUTH_JWKS_URL or NEON_AUTH_JWKS_JSON");
    if (!cfg.neonProjectUrl) missing.push("NEON_AUTH_BASE_URL");
    if (cfg.neonAllowedIdentities.length === 0) missing.push("NEON_AUTH_ALLOWED_EMAILS");
  }
  if (!cfg.secret) {
    missing.push("CEIORA_SESSION_SECRET");
  } else {
  }
  return missing;
}

export function isAppAuthConfigured(): boolean {
  return authConfigMissingKeys().length === 0;
}

export async function authenticateSharedLogin(username: string, password: string): Promise<AppSession | null> {
  const cfg = authConfig();
  if (cfg.provider !== "shared" || !cfg.username || !cfg.password || !cfg.secret || !sharedLegacyLoginAllowed()) {
    return null;
  }

  const cleanUsername = String(username || "").trim();
  const cleanPassword = String(password || "");
  if (cleanUsername !== cfg.username || cleanPassword !== cfg.password) return null;

  const issuedAt = Math.floor(Date.now() / 1000);
  const isAdmin = cleanUsername === cfg.primaryUsername;
  return {
    authProvider: "shared",
    username: cleanUsername,
    subject: cleanUsername,
    isAdmin,
    primary: isAdmin,
    issuedAt,
    expiresAt: issuedAt + APP_SESSION_TTL_SECONDS,
  };
}

const jwksByUrl = new Map<string, ReturnType<typeof createRemoteJWKSet>>();
const localJwksByValue = new Map<string, ReturnType<typeof createLocalJWKSet>>();

function normalizeIdentity(value: string | null | undefined): string | null {
  const clean = String(value || "").trim().toLowerCase();
  return clean || null;
}

function normalizeIssuerValue(value: string | null | undefined): string | null {
  const clean = String(value || "").trim();
  if (!clean) return null;
  return clean.replace(/\/+$/, "");
}

function allowedNeonIssuers(cfg: AuthConfig): Set<string> {
  const issuers = new Set<string>();
  const configured = normalizeIssuerValue(cfg.neonIssuer);
  const project = normalizeIssuerValue(cfg.neonProjectUrl);
  for (const value of [configured, project]) {
    if (!value) continue;
    issuers.add(value);
    try {
      const url = new URL(value);
      const origin = url.origin;
      if (origin) {
        issuers.add(`${origin}/auth`);
        issuers.add(`${origin}/neondb/auth`);
      }
    } catch {}
  }
  return issuers;
}

function remoteJwks(url: string) {
  const cached = jwksByUrl.get(url);
  if (cached) return cached;
  const jwks = createRemoteJWKSet(new URL(url));
  jwksByUrl.set(url, jwks);
  return jwks;
}

function localJwks(raw: string) {
  const cached = localJwksByValue.get(raw);
  if (cached) return cached;
  const parsed = JSON.parse(raw) as JWK | { keys: JWK[] };
  const jwks = createLocalJWKSet("keys" in parsed ? parsed : { keys: [parsed] });
  localJwksByValue.set(raw, jwks);
  return jwks;
}

export async function authenticateNeonLogin(idToken: string): Promise<AppSession | null> {
  const cfg = authConfig();
  if (cfg.provider !== "neon" || !cfg.secret || !cfg.neonIssuer || (!cfg.neonJwksUrl && !cfg.neonJwksJson)) return null;

  const token = String(idToken || "").trim();
  if (!token) return null;

  const verifyOptions: { audience?: string } = {};
  if (cfg.neonAudience) verifyOptions.audience = cfg.neonAudience;
  const jwks = cfg.neonJwksJson ? localJwks(cfg.neonJwksJson) : remoteJwks(cfg.neonJwksUrl);
  const { payload } = await jwtVerify(token, jwks, verifyOptions);

  const issuer = normalizeIssuerValue(typeof payload.iss === "string" ? payload.iss : null);
  if (!issuer || !allowedNeonIssuers(cfg).has(issuer)) {
    return null;
  }

  const subject = String(payload.sub || "").trim();
  const email = String(payload.email || "").trim();
  const displayName = String(payload.name || payload.preferred_username || "").trim();
  if (!subject) return null;

  const normalizedSubject = normalizeIdentity(subject);
  const normalizedEmail = normalizeIdentity(email);
  if (
    cfg.neonAllowedIdentities.length > 0 &&
    !cfg.neonAllowedIdentities.includes(normalizedEmail || "") &&
    !cfg.neonAllowedIdentities.includes(normalizedSubject || "")
  ) {
    return null;
  }

  const issuedAt = Math.floor(Date.now() / 1000);
  const tokenExpiry = Number(payload.exp || 0);
  const expiresAt = Number.isFinite(tokenExpiry) && tokenExpiry > issuedAt ? tokenExpiry : issuedAt + APP_SESSION_TTL_SECONDS;
  const isAdmin =
    cfg.neonAdminIdentities.includes(normalizedEmail || "") ||
    cfg.neonAdminIdentities.includes(normalizedSubject || "");

  return {
    authProvider: "neon",
    username: email || displayName || subject,
    subject,
    email: email || undefined,
    displayName: displayName || undefined,
    isAdmin,
    primary: isAdmin,
    issuedAt,
    expiresAt,
  };
}

async function importSigningKey(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    "raw",
    toArrayBuffer(utf8Bytes(secret)),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"],
  );
}

async function signPayload(payload: string, secret: string): Promise<string> {
  const key = await importSigningKey(secret);
  const signature = await crypto.subtle.sign("HMAC", key, toArrayBuffer(utf8Bytes(payload)));
  return base64UrlEncode(new Uint8Array(signature));
}

export async function createSessionToken(session: AppSession): Promise<string> {
  const cfg = authConfig();
  if (!cfg.secret) {
    throw new Error("App auth is not configured.");
  }
  const payload = base64UrlEncode(utf8Bytes(JSON.stringify(session)));
  const signature = await signPayload(payload, cfg.secret);
  return `${payload}.${signature}`;
}

interface SessionReadOptions {
  expectedProvider?: AppAuthProvider;
}

export async function readSessionFromCookieValue(
  value: string | null | undefined,
  options: SessionReadOptions = {},
): Promise<AppSession | null> {
  const cfg = authConfig();
  const token = String(value || "").trim();
  if (!cfg.secret || !token) return null;

  const parts = token.split(".");
  if (parts.length !== 2) return null;
  const [payload, signature] = parts;
  const expected = await signPayload(payload, cfg.secret);
  if (expected !== signature) return null;

  try {
    const decoded = JSON.parse(new TextDecoder().decode(base64UrlDecode(payload))) as Partial<AppSession>;
    const authProvider: AppAuthProvider = decoded.authProvider === "neon" ? "neon" : "shared";
    const username = String(decoded.username || "").trim();
    const subject = String(decoded.subject || "").trim();
    const email = String(decoded.email || "").trim() || undefined;
    const displayName = String(decoded.displayName || "").trim() || undefined;
    const defaultAccountId = String(decoded.defaultAccountId || "").trim() || null;
    const isAdmin = Boolean(decoded.isAdmin ?? decoded.primary);
    const issuedAt = Number(decoded.issuedAt || 0);
    const expiresAt = Number(decoded.expiresAt || 0);
    if (!username || !Number.isFinite(issuedAt) || !Number.isFinite(expiresAt)) return null;
    if (options.expectedProvider && authProvider !== options.expectedProvider) return null;
    if (authProvider === "shared" && !sharedLegacyLoginAllowed()) return null;
    if (authProvider === "neon" && !subject) return null;
    if (expiresAt <= Math.floor(Date.now() / 1000)) return null;
    return {
      authProvider,
      username,
      subject: authProvider === "shared" ? subject || username : subject,
      email,
      displayName,
      defaultAccountId,
      isAdmin,
      primary: isAdmin,
      issuedAt,
      expiresAt,
    };
  } catch {
    return null;
  }
}

function extractCookieValue(rawCookieHeader: string | null | undefined, cookieName: string): string | null {
  const header = String(rawCookieHeader || "");
  if (!header) return null;
  const prefix = `${cookieName}=`;
  for (const segment of header.split(";")) {
    const trimmed = segment.trim();
    if (trimmed.startsWith(prefix)) {
      return trimmed.slice(prefix.length);
    }
  }
  return null;
}

async function readSessionFromCookieSources(
  cookieStore: CookieStoreLike | null | undefined,
  rawCookieHeader: string | null | undefined,
  options: SessionReadOptions = {},
): Promise<AppSession | null> {
  for (const cookieName of APP_SESSION_COOKIE_NAMES) {
    const storeValue = cookieStore?.get(cookieName)?.value;
    const session = await readSessionFromCookieValue(storeValue, options);
    if (session) return session;
  }

  for (const cookieName of APP_SESSION_COOKIE_NAMES) {
    const headerValue = extractCookieValue(rawCookieHeader, cookieName);
    const session = await readSessionFromCookieValue(headerValue, options);
    if (session) return session;
  }

  return null;
}

export async function readSessionTokenFromCookieSources(
  cookieStore: CookieStoreLike | null | undefined,
  rawCookieHeader: string | null | undefined,
  options: SessionReadOptions = {},
): Promise<string | null> {
  for (const cookieName of APP_SESSION_COOKIE_NAMES) {
    const storeValue = cookieStore?.get(cookieName)?.value;
    const clean = String(storeValue || "").trim();
    if (!clean) continue;
    if (!options.expectedProvider) return clean;
    const session = await readSessionFromCookieValue(clean, options);
    if (session) return clean;
  }

  for (const cookieName of APP_SESSION_COOKIE_NAMES) {
    const headerValue = extractCookieValue(rawCookieHeader, cookieName);
    const clean = String(headerValue || "").trim();
    if (!clean) continue;
    if (!options.expectedProvider) return clean;
    const session = await readSessionFromCookieValue(clean, options);
    if (session) return clean;
  }

  return null;
}

export async function readSessionFromRequest(req: NextRequest, options: SessionReadOptions = {}): Promise<AppSession | null> {
  return readSessionFromCookieSources(req.cookies, req.headers.get("cookie"), options);
}

export async function readSessionTokenFromRequest(req: NextRequest, options: SessionReadOptions = {}): Promise<string | null> {
  return readSessionTokenFromCookieSources(req.cookies, req.headers.get("cookie"), options);
}

export async function readSessionFromCookieStore(
  cookieStore: CookieStoreLike | null | undefined,
  rawCookieHeader?: string | null,
  options: SessionReadOptions = {},
): Promise<AppSession | null> {
  return readSessionFromCookieSources(cookieStore, rawCookieHeader, options);
}

export function appSessionCookieOptions(expiresAt: number) {
  return {
    httpOnly: true,
    sameSite: "lax" as const,
    secure: process.env.NODE_ENV === "production",
    path: "/",
    expires: new Date(expiresAt * 1000),
  };
}

export function clearedAppSessionCookieOptions() {
  return {
    httpOnly: true,
    sameSite: "lax" as const,
    secure: process.env.NODE_ENV === "production",
    path: "/",
    expires: new Date(0),
  };
}
