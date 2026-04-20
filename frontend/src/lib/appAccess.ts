export const DEFAULT_APP_HOME_PATH = "/home";

const PROTECTED_PAGE_PREFIXES = ["/home", "/cuse", "/cpar", "/positions", "/data", "/settings"] as const;
const PRIVILEGED_PAGE_PREFIXES = ["/settings/admin"] as const;
const PUBLIC_SHELL_PATHS = ["/", "/login"] as const;
const PUBLIC_API_PATH_PREFIXES = ["/api/auth/login", "/api/auth/logout", "/api/auth/session"] as const;
const PRIVILEGED_API_PATH_PREFIXES = ["/api/data/diagnostics", "/api/health/diagnostics", "/api/operator/status", "/api/refresh", "/api/cpar/build"] as const;

function matchesPrefix(pathname: string, prefix: string): boolean {
  return pathname === prefix || pathname.startsWith(`${prefix}/`);
}

export function isPublicShellPath(pathname: string): boolean {
  return PUBLIC_SHELL_PATHS.includes(pathname as (typeof PUBLIC_SHELL_PATHS)[number]);
}

export function isProtectedPagePath(pathname: string): boolean {
  return PROTECTED_PAGE_PREFIXES.some((prefix) => matchesPrefix(pathname, prefix));
}

export function isPrivilegedPagePath(pathname: string): boolean {
  return PRIVILEGED_PAGE_PREFIXES.some((prefix) => matchesPrefix(pathname, prefix));
}

export function isPublicApiPath(pathname: string): boolean {
  return PUBLIC_API_PATH_PREFIXES.some((prefix) => matchesPrefix(pathname, prefix));
}

export function isProtectedApiPath(pathname: string): boolean {
  return pathname.startsWith("/api/") && !isPublicApiPath(pathname);
}

export function isPrivilegedApiPath(pathname: string): boolean {
  return PRIVILEGED_API_PATH_PREFIXES.some((prefix) => matchesPrefix(pathname, prefix));
}

export function normalizeReturnTo(value: string | null | undefined): string {
  const clean = String(value || "").trim();
  if (!clean.startsWith("/") || clean.startsWith("//")) return DEFAULT_APP_HOME_PATH;
  if (clean.startsWith("/api/")) return DEFAULT_APP_HOME_PATH;
  return clean;
}
