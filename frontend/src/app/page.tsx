import Link from "next/link";
import { cookies } from "next/headers";
import LandingBackgroundLock from "@/components/LandingBackgroundLock";
import { readSessionFromCookieStore } from "@/lib/appAuth";

const RISK_ENGINES = [
  {
    step: "01",
    name: "cUSE",
    title: "Barra-style equity risk",
    imageSrc: "/intro/cuse.jpg",
    body: "cUSE is the descriptor-native engine. It estimates exposures from company characteristics, industry structure, and orthogonalized style descriptors so the platform can explain portfolio risk in a stable, interpretable factor language.",
  },
  {
    step: "02",
    name: "cPAR",
    title: "Returns-based factor risk",
    imageSrc: "/intro/cpar.jpg",
    body: "cPAR is the tradable proxy engine. It fits market, sector, and style sleeves in residualized ETF space so you can read incremental risk clearly and move directly from diagnosis to hedgeable action.",
  },
  {
    step: "03",
    name: "cMAC",
    title: "Multi-asset macro risk",
    imageSrc: "/intro/cmac.jpg",
    body: "cMAC is the forthcoming macro layer. It will extend the platform beyond single-book factor diagnosis by framing exposures against rates, inflation, growth, credit, and liquidity regimes that shape how portfolio risk transmits through the broader market.",
  },
] as const;

export default async function PublicLandingPage() {
  const cookieStore = await cookies();
  const session = await readSessionFromCookieStore(cookieStore);
  const appHref = session ? "/home" : "/login";

  return (
    <>
      <LandingBackgroundLock bodyClassName="public-topo-boost" />
      <header className="dash-tabs">
        <div className="dash-tabs-brand-cluster">
          <Link href="/" className="dash-tabs-brand">
            Ceiora
          </Link>
        </div>
        <div className="dash-tabs-center" aria-hidden="true" />
        <div className="dash-tabs-actions">
          <Link href={appHref} className="public-intro-link">
            Go to App
            <span className="public-intro-link-icon" aria-hidden="true">
              ↗
            </span>
          </Link>
        </div>
      </header>
      <div className="public-page-shell">
        <div className="public-text-page">
          <div className="public-text-shell">
            <section className="public-text-intro">
              <h1 className="public-text-headline">Institutional risk models for the individual investor</h1>
              <p className="public-text-copy">
                Ceiora is a portfolio risk management platform built on a family of bespoke factor models that decompose risk
                exposures. Ceiora distinguishes itself from traditional factor models by focusing on the unique needs of
                individual allocators and retail investors. In this vein, each of our models are designed to be actionable,
                interpretable, and approachable.
              </p>
            </section>

            <section className="public-engine-header" aria-label="Family intro">
              <h2 className="public-engine-heading">Meet the family</h2>
            </section>

            <section className="public-engine-copy" aria-label="Risk engines">
              {RISK_ENGINES.map((engine) => (
                <article key={engine.name} className="public-engine-card">
                  <div className="public-engine-step">
                    <span className="public-engine-step-number">{engine.step}</span>
                  </div>
                  <div className="public-engine-preview" aria-hidden="true">
                    <img src={engine.imageSrc} alt="" className="public-engine-preview-image" />
                  </div>
                  <div className="public-engine-card-copy">
                    <h2>{engine.title}</h2>
                    <div className="public-engine-rule" aria-hidden="true" />
                    <p>{engine.body}</p>
                  </div>
                </article>
              ))}
            </section>
          </div>
        </div>

        <footer className="public-site-footer">
          <div className="public-site-footer-inner">
            <Link href="/" className="dash-tabs-brand public-footer-brand">
              Ceiora
            </Link>
            <nav className="public-site-footer-links" aria-label="Footer">
              <a href="#" className="public-site-footer-link">
                About Us
              </a>
              <a href="#" className="public-site-footer-link">
                Terms of Use
              </a>
              <a href="#" className="public-site-footer-link">
                Privacy
              </a>
              <a href="#" className="public-site-footer-link">
                Contact
              </a>
              <span className="public-site-footer-meta">Copyright 2026 Ceiora</span>
            </nav>
          </div>
        </footer>
      </div>
    </>
  );
}
