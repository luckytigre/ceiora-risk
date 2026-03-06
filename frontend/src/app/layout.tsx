import type { Metadata } from "next";
import "./globals.css";
import { BackgroundProvider } from "@/components/BackgroundContext";
import { RecomputePromptProvider } from "@/components/RecomputePromptContext";
import Neo2DotBackground from "@/components/Neo2DotBackground";
import TabNav from "@/components/TabNav";

export const metadata: Metadata = {
  title: "Ceiora",
  description: "Portfolio factor risk model dashboard",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <BackgroundProvider>
          <RecomputePromptProvider>
            <Neo2DotBackground />
            <TabNav />
            <main className="dash-main">{children}</main>
          </RecomputePromptProvider>
        </BackgroundProvider>
      </body>
    </html>
  );
}
