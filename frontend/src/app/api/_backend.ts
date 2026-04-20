import { NextRequest, NextResponse } from "next/server";
import { appAuthProvider, readSessionFromRequest, readSessionTokenFromRequest } from "@/lib/appAuth";

export function backendOrigin(): string {
  return (process.env.BACKEND_API_ORIGIN || "http://127.0.0.1:8000").replace(/\/+$/, "");
}

export function controlBackendOrigin(): string {
  return (process.env.BACKEND_CONTROL_ORIGIN || process.env.BACKEND_API_ORIGIN || "http://127.0.0.1:8000").replace(
    /\/+$/,
    "",
  );
}

const AUTH_HEADER_NAMES = ["x-operator-token", "x-editor-token"] as const;
const APP_SESSION_TOKEN_HEADER = "x-app-session-token";

export async function forwardedAuthHeaders(
  req: NextRequest,
  extra: HeadersInit = {},
  options?: { forwardPrivilegedHeaders?: boolean },
): Promise<Headers> {
  const headers = new Headers(extra);
  if (options?.forwardPrivilegedHeaders === true) {
    for (const name of AUTH_HEADER_NAMES) {
      const value = req.headers.get(name);
      if (value) {
        headers.set(name, value);
      }
    }
  }
  const expectedProvider = appAuthProvider();
  const session = await readSessionFromRequest(req, { expectedProvider });
  if (session) {
    const sessionToken = headers.has(APP_SESSION_TOKEN_HEADER)
      ? null
      : await readSessionTokenFromRequest(req, { expectedProvider });
    if (sessionToken) {
      headers.set(APP_SESSION_TOKEN_HEADER, sessionToken);
    }
  }
  return headers;
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

export async function upstreamHeaders(
  req: NextRequest,
  upstream: string,
  extra: HeadersInit = {},
  options?: { forwardPrivilegedHeaders?: boolean },
): Promise<Headers> {
  const headers = await forwardedAuthHeaders(req, extra, options);
  if (backendIamAuthEnabled()) {
    headers.set("authorization", `Bearer ${await fetchCloudRunIdentityToken(new URL(upstream).origin)}`);
  }
  return headers;
}

export async function proxyJson(
  req: NextRequest,
  upstream: string,
  options?: { method?: string; headers?: HeadersInit; forwardPrivilegedHeaders?: boolean },
) {
  const body = req.method === "GET" || req.method === "HEAD" ? undefined : await req.text();
  const upstreamRequestHeaders = new Headers(options?.headers);
  if (body !== undefined && !upstreamRequestHeaders.has("content-type")) {
    const incomingContentType = req.headers.get("content-type");
    if (incomingContentType) {
      upstreamRequestHeaders.set("content-type", incomingContentType);
    }
  }
  const res = await fetch(upstream, {
    method: options?.method || req.method,
    headers: await upstreamHeaders(req, upstream, upstreamRequestHeaders, {
      forwardPrivilegedHeaders: options?.forwardPrivilegedHeaders,
    }),
    body,
    cache: "no-store",
  });
  const text = await res.text();
  return new NextResponse(text, {
    status: res.status,
    headers: { "content-type": res.headers.get("content-type") || "application/json" },
  });
}
