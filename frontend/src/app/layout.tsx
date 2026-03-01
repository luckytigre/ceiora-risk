import type { Metadata } from "next";
import "./globals.css";
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
        <Neo2DotBackground />
        <TabNav />
        <main className="dash-main">{children}</main>
      </body>
    </html>
  );
}
