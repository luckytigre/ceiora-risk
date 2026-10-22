import { Suspense } from "react";
import type { Metadata } from "next";
import { headers } from "next/headers";
import "./globals.css";
import { AppSettingsProvider } from "@/components/AppSettingsContext";
import ThesisWorkspaceBoundary from "@/components/ThesisWorkspaceBoundary";
import AuthSessionGate from "@/components/AuthSessionGate";
import { AuthSessionProvider } from "@/components/AuthSessionContext";
import { BackgroundProvider } from "@/components/BackgroundContext";
import Neo2DotBackground from "@/components/Neo2DotBackground";
import TabNav from "@/components/TabNav";
import ConfirmDialogProvider from "@/components/ConfirmDialogProvider";
import StickyTableHeaders from "@/components/StickyTableHeaders";
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
      <body data-theme="dark" suppressHydrationWarning>
        <script dangerouslySetInnerHTML={{ __html: THEME_BOOTSTRAP }} />
        <AuthSessionProvider neonProjectUrl={projectUrl} initialState={initialAuthState}>
          <ConfirmDialogProvider>
            <AppSettingsProvider>
              <BackgroundProvider>
                <Neo2DotBackground />
                <StickyTableHeaders />
                <Suspense fallback={null}><TabNav /></Suspense>
                <main className="dash-main">
                  <AuthSessionGate>
                    <ThesisWorkspaceBoundary><PageTransition>{children}</PageTransition></ThesisWorkspaceBoundary>
                  </AuthSessionGate>
                </main>
              </BackgroundProvider>
            </AppSettingsProvider>
          </ConfirmDialogProvider>
        </AuthSessionProvider>
      </body>
    </html>
  );
}
