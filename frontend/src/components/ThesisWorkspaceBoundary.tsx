"use client";

import { useEffect, useState, type ReactNode } from "react";
import { usePathname } from "next/navigation";
import { useAuthSession } from "@/components/AuthSessionContext";
import { ThesisProvider } from "@/components/ThesisContext";
import { apiFetch } from "@/lib/apiTransport";
import { isProtectedPagePath } from "@/lib/appAccess";

/** Keep the page subtree stable while auth/capability loads. The provider resets
 * its scoped state and rejects obsolete responses without remounting page drafts. */
export default function ThesisWorkspaceBoundary({ children }: { children: ReactNode }) {
  const { authenticated, context, session } = useAuthSession();
  const pathname = usePathname();
  const protectedPage = isProtectedPagePath(pathname ?? "");
  const subject = context?.subject || (session?.authProvider === "shared" ? session.username : "");
  const identity = authenticated && session && protectedPage
    ? `${session.authProvider}:${subject}:${[...(context?.account_ids ?? [])].sort().join(",")}` : "";
  const canCheck = Boolean(identity && context?.subject);
  const [capability, setCapability] = useState<{ identity: string; workspace: string | null } | null>(null);
  useEffect(() => {
    if (!canCheck) return;
    let current = true;
    apiFetch<{ available: boolean; workspace_id?: string }>("/api/portfolio/theses/capability", { cache: "no-store" })
      .then(result => { if (current) setCapability({ identity, workspace: result.available && result.workspace_id ? result.workspace_id : null }); })
      .catch(() => { if (current) setCapability({ identity, workspace: null }); });
    return () => { current = false; };
  }, [canCheck, identity]);
  const workspace = canCheck && capability?.identity === identity ? capability.workspace : null;
  return <ThesisProvider workspaceId={workspace} identity={identity}>{children}</ThesisProvider>;
}
