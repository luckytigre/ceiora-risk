"use client";

import { useEffect, useState, type ReactNode } from "react";
import { usePathname } from "next/navigation";
import { useAuthSession } from "@/components/AuthSessionContext";
import { ThesisProvider } from "@/components/ThesisContext";
import { apiFetch } from "@/lib/apiTransport";
import { isProtectedPagePath } from "@/lib/appAccess";

function CapabilityBoundary({ children }: { children: ReactNode }) {
  const [workspace, setWorkspace] = useState<string | null>(null);
  useEffect(() => {
    let current = true;
    apiFetch<{ available: boolean; workspace_id?: string }>("/api/portfolio/theses/capability", { cache: "no-store" })
      .then(result => { if (current) setWorkspace(result.available && result.workspace_id ? result.workspace_id : null); })
      .catch(() => { if (current) setWorkspace(null); });
    return () => { current = false; };
  }, []);
  return workspace ? <ThesisProvider key={workspace} workspaceId={workspace}>{children}</ThesisProvider> : <>{children}</>;
}

/** Identity/membership changes remount the complete provider and discard responses
 * from its previous lifetime. Public pages never request thesis state/capability. */
export default function ThesisWorkspaceBoundary({ children }: { children: ReactNode }) {
  const { authenticated, context, session } = useAuthSession();
  const pathname = usePathname();
  if (!authenticated || !context?.subject || !session || !isProtectedPagePath(pathname ?? "")) return <>{children}</>;
  const identity = `${session.authProvider}:${context.subject}:${[...context.account_ids].sort().join(",")}`;
  return <CapabilityBoundary key={identity}>{children}</CapabilityBoundary>;
}
