import type { Metadata } from "next";
import { headers } from "next/headers";
import "./globals.css";
import { AppSettingsProvider } from "@/components/AppSettingsContext";
import AuthSessionGate from "@/components/AuthSessionGate";
import { AuthSessionProvider } from "@/components/AuthSessionContext";
import { BackgroundProvider } from "@/components/BackgroundContext";
import Neo2DotBackground from "@/components/Neo2DotBackground";
import TabNav from "@/components/TabNav";
import PageTransition from "@/components/PageTransition";
import { appAuthProvider, neonAuthProjectUrl } from "@/lib/appAuth";
import { APP_AUTH_BOOTSTRAP_HEADER, decodeAuthSessionBootstrapHeader } from "@/lib/authSessionBootstrap";

const THEME_BOOTSTRAP = `
(() => {
  try {
    const stored = String(localStorage.getItem('theme-mode') || '').trim().toLowerCase();
    const mode = stored === 'light' ? 'light' : 'dark';
    document.documentElement.dataset.theme = mode;
    document.body.dataset.theme = mode;
    document.documentElement.style.colorScheme = mode;
    document.body.style.colorScheme = mode;
  } catch {}
})();
`;

export const metadata: Metadata = {
  title: "Ceiora",
  description: "Portfolio factor risk model dashboard",
};

export default async function RootLayout({ children }: { children: React.ReactNode }) {
  const provider = appAuthProvider();
  const projectUrl = provider === "neon" ? neonAuthProjectUrl() : "";
  const requestHeaders = await headers();
  const initialAuthState = decodeAuthSessionBootstrapHeader(requestHeaders.get(APP_AUTH_BOOTSTRAP_HEADER));
  return (
    <html lang="en" data-theme="dark" suppressHydrationWarning>
      <body data-theme="dark">
        <script dangerouslySetInnerHTML={{ __html: THEME_BOOTSTRAP }} />
        <AuthSessionProvider neonProjectUrl={projectUrl} initialState={initialAuthState}>
          <AppSettingsProvider>
            <BackgroundProvider>
              <Neo2DotBackground />
              <TabNav />
              <main className="dash-main">
                <AuthSessionGate>
                  <PageTransition>{children}</PageTransition>
                </AuthSessionGate>
              </main>
            </BackgroundProvider>
          </AppSettingsProvider>
        </AuthSessionProvider>
      </body>
    </html>
  );
}
