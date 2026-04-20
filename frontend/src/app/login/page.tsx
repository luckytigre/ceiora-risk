import LoginClient from "./LoginClient";
import { appAuthProvider, isAppAuthConfigured, neonAuthProjectUrl, sharedLegacyLoginAllowed } from "@/lib/appAuth";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default function LoginPage() {
  const provider = appAuthProvider();
  return (
    <LoginClient
      provider={provider}
      authConfigured={isAppAuthConfigured()}
      neonProjectUrl={provider === "neon" ? neonAuthProjectUrl() : ""}
      sharedLoginAllowed={provider !== "shared" || sharedLegacyLoginAllowed()}
    />
  );
}
