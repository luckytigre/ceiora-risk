import Link from "next/link";
import { cookies } from "next/headers";
import LandingBackgroundLock from "@/components/LandingBackgroundLock";
import { readSessionFromCookieStore } from "@/lib/appAuth";

const RISK_ENGINES = [
  {
    step: "01",
    name: "cUSE",
    title: "Barra-style equity risk",
    previewClassName: "public-engine-preview-cuse",
    body: "cUSE is the descriptor-native engine. It estimates exposures from company characteristics, industry structure, and orthogonalized style descriptors so the platform can explain portfolio risk in a stable, interpretable factor language.",
  },
  {
    step: "02",
    name: "cPAR",
    title: "Returns-based factor risk",
    previewClassName: "public-engine-preview-cpar",
    body: "cPAR is the tradable proxy engine. It fits market, sector, and style sleeves in residualized ETF space so you can read incremental risk clearly and move directly from diagnosis to hedgeable action.",
  },
  {
    step: "03",
    name: "cMAC",
    title: "Multi-asset macro risk",
    previewClassName: "public-engine-preview-cmac",
    body: "cMAC is the forthcoming macro layer. It will extend the platform beyond single-book factor diagnosis by framing exposures against rates, inflation, growth, credit, and liquidity regimes that shape how portfolio risk transmits through the broader market.",
  },
] as const;

export default async function PublicLandingPage() {
  const cookieStore = await cookies();
  const session = await readSessionFromCookieStore(cookieStore);
  const appHref = session ? "/home" : "/login?returnTo=/home";

  return (
    <>
      <LandingBackgroundLock />
      <header className="dash-tabs">
        <div className="dash-tabs-brand-cluster">
          <Link href="/" className="dash-tabs-brand">
            Ceiora
          </Link>
        </div>
        <div className="dash-tabs-center" aria-hidden="true" />
        <div className="dash-tabs-actions">
          <Link href={appHref} className="public-intro-link">
            <span className="public-intro-link-icon" aria-hidden="true">
              ↗
            </span>
            Go to App
          </Link>
        </div>
      </header>
      <div className="public-text-page">
        <div className="public-text-shell">
          <section className="public-text-intro">
            <h1 className="public-text-headline">Institutional risk modeling adapted for the individual investor</h1>
            <p className="public-text-copy">
              Ceiora does this by combining multiple model lenses in one surface. cUSE preserves the descriptor-based,
              cross-sectional structure used in institutional equity models, cPAR translates the same portfolio into a smaller
              tradable proxy space that is easier to interpret and act on, and cMAC will add the macro regime layer that
              connects portfolio risk to the wider market backdrop.
            </p>
            <p className="public-text-copy">
              The design choices are what make that usable for an individual investor: a constrained style and industry factor
              model for structural reading, a parsimonious ETF proxy model for actionability, and one interface that keeps the
              different lenses aligned so the same book can be understood in institutional terms without becoming operationally
              opaque.
            </p>
          </section>

          <section className="public-engine-copy" aria-label="Risk engines">
            {RISK_ENGINES.map((engine) => (
              <article key={engine.name} className="public-engine-card">
                <div className="public-engine-step">
                  <span className="public-engine-step-number">{engine.step}</span>
                </div>
                <div className={`public-engine-preview ${engine.previewClassName}`} aria-hidden="true">
                  <div className="public-engine-preview-placeholder">
                    <span className="public-engine-preview-sun" />
                    <span className="public-engine-preview-ridge public-engine-preview-ridge-back" />
                    <span className="public-engine-preview-ridge public-engine-preview-ridge-front" />
                  </div>
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
    </>
  );
}
