import Link from "next/link";
import { cookies } from "next/headers";
import LandingBackgroundLock from "@/components/LandingBackgroundLock";
import LandingSummary from "@/components/LandingSummary";
import { readSessionFromCookieStore } from "@/lib/appAuth";

const OPERATING_PILLARS = [
  {
    label: "cUSE",
    title: "Descriptor-native equity risk",
    body: "Cross-sectional exposures, factor decomposition, and orthogonalized style structure built from the investable universe.",
  },
  {
    label: "cPAR",
    title: "Tradable proxy risk",
    body: "Parsimonious ETF-space regression for hedging, what-if analysis, and direct mapping from portfolio intuition to executable sleeves.",
  },
  {
    label: "Ops",
    title: "Authoritative control plane",
    body: "Protected operator surfaces for package health, refresh state, holdings, and serving payload publication.",
  },
] as const;

const ENTRY_SURFACES = [
  { title: "Exposure reading", route: "/cuse/exposures" },
  { title: "Portfolio risk", route: "/cpar/risk" },
  { title: "Operator health", route: "/health" },
] as const;

export default async function PublicLandingPage() {
  const cookieStore = await cookies();
  const session = await readSessionFromCookieStore(cookieStore);

  return (
    <>
      <LandingBackgroundLock />
      <div className="public-landing">
        <section className="public-hero chart-card">
          <div className="public-hero-grid">
            <div className="public-hero-copy">
              <div className="public-kicker">Ceiora Risk Platform</div>
              <h1 className="public-headline">
                Cross-sectional and tradable factor views, in one operator-grade shell.
              </h1>
              <p className="public-body">
                Ceiora combines descriptor-native equity risk with a parsimonious ETF proxy surface so you can read exposures,
                understand portfolio structure, and move from interpretation to hedgeable action without switching systems.
              </p>
              <div className="public-stat-row" aria-label="Platform summary">
                <div className="public-stat-card">
                  <span className="public-stat-value">2</span>
                  <span className="public-stat-label">factor families</span>
                </div>
                <div className="public-stat-card">
                  <span className="public-stat-value">1</span>
                  <span className="public-stat-label">protected control plane</span>
                </div>
                <div className="public-stat-card">
                  <span className="public-stat-value">Live</span>
                  <span className="public-stat-label">serving refresh workflow</span>
                </div>
              </div>
              <div className="public-hero-actions">
                {session ? (
                  <Link href="/home" className="btn btn-secondary">
                    Continue to app
                  </Link>
                ) : (
                  <Link href="/login?returnTo=/home" className="btn btn-secondary">
                    Sign in to dashboard
                  </Link>
                )}
                <Link href="/home" className="public-secondary-link">
                  Preview internal landing
                </Link>
              </div>
            </div>

            <aside className="public-hero-panel" aria-label="Protected surfaces">
              <div className="public-panel-block">
                <div className="public-panel-kicker">Protected surfaces</div>
                <h2 className="public-panel-title">Where the app opens after sign-in</h2>
                <div className="public-surface-list">
                  {ENTRY_SURFACES.map((surface) => {
                    const href = session
                      ? surface.route
                      : `/login?returnTo=${encodeURIComponent(surface.route)}`;

                    return (
                      <Link key={surface.route} href={href} className="public-surface-card">
                        <span>{surface.title}</span>
                        <code>{surface.route}</code>
                      </Link>
                    );
                  })}
                </div>
              </div>
              <div className="public-panel-block">
                <div className="public-panel-kicker">Access posture</div>
                <p className="public-panel-note">
                  Shared login gates the dashboard, holdings tooling, data health pages, and package publication controls. The
                  public shell stays informational; the operating surfaces remain authenticated.
                </p>
              </div>
            </aside>
          </div>
        </section>

        <section className="public-pillars" aria-label="Platform pillars">
          {OPERATING_PILLARS.map((pillar) => (
            <article key={pillar.label} className="public-pillar-card">
              <div className="public-pillar-label">{pillar.label}</div>
              <h2 className="public-pillar-title">{pillar.title}</h2>
              <p className="public-pillar-body">{pillar.body}</p>
            </article>
          ))}
        </section>

        <LandingSummary />
      </div>
    </>
  );
}
