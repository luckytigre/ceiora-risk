import Link from "next/link";
import { cookies } from "next/headers";
import { Instrument_Serif } from "next/font/google";
import BrandLockup from "@/components/BrandLockup";
import LandingBackgroundLock from "@/components/LandingBackgroundLock";
import { appAuthProvider, readSessionFromCookieStore } from "@/lib/appAuth";

const instrumentSerif = Instrument_Serif({
  weight: "400",
  subsets: ["latin"],
  style: ["normal", "italic"],
});

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
    title: "Returns-based equity risk",
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

const MODEL_COMPARISON = [
  {
    key: "cuse",
    folio: "Characteristics-Based Model",
    label: "cUSE",
    subtitle: "Barra-Style US Equity Model",
    accentClass: "public-model-panel-title-cuse",
    metrics: [
      { value: "3K+", label: "Core universe", note: "core-estimated" },
      { value: "45", label: "Live factors", note: "14 style" },
      { value: "30", label: "Industry groups", note: "business sectors" },
    ],
    readsFrom: "Barra USE4 lineage, but narrowed to a smaller live factor set, a tighter industry list, and style blocks that are orthogonalized in a clean dependency order",
    bestFor: "The model gives up breadth and institutional granularity so the factor language stays interpretable, maintainable, and harder to overfit in day-to-day use",
    outputs: "A native-factor decomposition that favors stable structure and actionable explanation over exhaustive factor sprawl",
    note: "cUSE keeps the descriptor-native philosophy, but trims complexity on purpose: fewer style factors, a smaller industry burden, and ordered orthogonalization so the outputs remain understandable enough to manage instead of becoming a research object.",
  },
  {
    key: "cpar",
    folio: "Returns-Based Model",
    label: "cPAR",
    subtitle: "Returns-Based Equity Model",
    accentClass: "public-model-panel-title-cpar",
    metrics: [
      { value: "3K+", label: "Fitted universe", note: "active package" },
      { value: "17", label: "ETF proxies", note: "SPY + sector + style" },
      { value: "52", label: "Weekly bars", note: "one-year window" },
    ],
    readsFrom: "A fixed registry of real ETF proxies: SPY, sector sleeves, and a short list of style ETFs, with non-market sleeves orthogonalized to market before the fit",
    bestFor: "Returns-based breadth and direct hedgeability without inventing bespoke factor portfolios, plus one-shot weekly ridge to keep the fit stable instead of chasing noise",
    outputs: "Residualized tradable factor space that keeps market explicit and turns incremental structure into a hedgeable read",
    note: "cPAR applies the same restraint in returns space: fixed ETF proxies, package-level market orthogonalization, and one-shot weighted ridge so the model stays broad, tradable, and easily maintainable.",
  },
] as const;

export default async function PublicLandingPage() {
  const cookieStore = await cookies();
  const session = await readSessionFromCookieStore(cookieStore, undefined, { expectedProvider: appAuthProvider() });
  const appHref = session ? "/home" : "/login";

  return (
    <>
      <LandingBackgroundLock bodyClassName="public-topo-boost" />
      <header className="dash-tabs dash-tabs-landing">
        <div className="dash-tabs-brand-cluster">
          <BrandLockup
            href="/"
            className="dash-tabs-brand"
            markClassName="dash-tabs-brand-mark"
            wordmarkClassName="dash-tabs-brand-wordmark"
            markTitle="Ceiora"
          />
        </div>
        <div className="dash-tabs-center" aria-hidden="true" />
        <div className="dash-tabs-actions">
          <Link href={appHref} className="public-intro-link">
            Go to App
            <span className="public-intro-link-icon" aria-hidden="true">
              <svg viewBox="0 0 12 12" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path
                  d="M2 10L10 2M2 2H10V10"
                  stroke="currentColor"
                  strokeWidth="1.2"
                  strokeLinecap="square"
                  strokeLinejoin="miter"
                />
              </svg>
            </span>
          </Link>
        </div>
      </header>
      <div className="public-page-shell">
        <div className="public-text-page">
          <div className="public-text-shell">
            <section className="public-text-intro">
              <h1 className="public-text-headline">
                Institutional risk models for the{" "}
                <em className={`public-text-headline-em ${instrumentSerif.className}`}>individual</em> investor
              </h1>
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
                    <span className={`public-engine-preview-badge public-engine-preview-badge-${engine.name.toLowerCase()}`}>
                      {engine.name}
                    </span>
                  </div>
                  <div className="public-engine-card-copy">
                    <h2>{engine.title}</h2>
                    <div className="public-engine-rule" aria-hidden="true" />
                    <p>{engine.body}</p>
                  </div>
                </article>
              ))}
            </section>

            <section className="public-model-compare" aria-label="How the models read risk">
              <div className="public-model-compare-header">
                <span className="public-model-compare-folio">MODEL PHILOSOPHY</span>
                <div className="public-model-compare-intro-block">
                  <h2 className="public-model-compare-display">Simpler by design</h2>
                  <div className="public-model-compare-intro-body">
                    <p className="public-model-compare-intro">
                      Ceiora isn&apos;t trying to out-math established institutional risk models. Instead, our risk engines are
                      narrower on purpose: much smaller factor sets that enable clear risk visualization, explicit orthogonalization
                      to push risk into ETF-tradable exposures, and regularization to stabilize factor loadings and reduce hedge
                      management. Our goal is to provide models you can understand and act on quickly so you never have to miss{" "}
                      <em className={instrumentSerif.className}>The Big Game™</em> with the boys.
                    </p>
                    <aside className="public-model-compare-aside" aria-label="The Big Game aside">
                      <img
                        src="/intro/big-game.webp"
                        alt="Friends watching the big game together on a couch."
                        className="public-model-compare-aside-image"
                      />
                    </aside>
                  </div>
                </div>
              </div>

              <div className="public-model-panel-grid">
                {MODEL_COMPARISON.map((model) => (
                  <article key={model.key} className="public-model-panel">
                    <div className="public-model-panel-toprule" aria-hidden="true" />
                    <div className="public-model-panel-meta">
                      <span className="public-model-panel-folio">{model.folio}</span>
                      <span className="public-model-panel-enter">
                        Enter
                        <span className="public-model-panel-enter-icon" aria-hidden="true">
                          <svg viewBox="0 0 12 12" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <path
                              d="M2 10L10 2M2 2H10V10"
                              stroke="currentColor"
                              strokeWidth="1.2"
                              strokeLinecap="square"
                              strokeLinejoin="miter"
                            />
                          </svg>
                        </span>
                      </span>
                    </div>
                    <h3 className={`public-model-panel-title ${model.accentClass}`}>
                      <span className="public-model-panel-prefix">c</span>
                      {model.label.slice(1)}
                    </h3>
                    <p className="public-model-panel-subtitle">{model.subtitle}</p>
                    <p className="public-model-panel-note">{model.note}</p>

                    <dl className="public-model-panel-metrics">
                      {model.metrics.map((metric) => (
                        <div key={`${model.key}-${metric.label}`} className="public-model-panel-metric">
                          <dt>{metric.label}</dt>
                          <dd>{metric.value}</dd>
                          <span>{metric.note}</span>
                        </div>
                      ))}
                    </dl>

                    <dl className="public-model-panel-signals">
                      <div className="public-model-panel-signal">
                        <dt>Lineage</dt>
                        <dd>{model.readsFrom}</dd>
                      </div>
                      <div className="public-model-panel-signal">
                        <dt>Tradeoff</dt>
                        <dd>{model.bestFor}</dd>
                      </div>
                      <div className="public-model-panel-signal">
                        <dt>What you get</dt>
                        <dd>{model.outputs}</dd>
                      </div>
                    </dl>
                  </article>
                ))}
              </div>
            </section>
          </div>
        </div>

        <footer className="public-site-footer">
          <div className="public-site-footer-inner">
            <BrandLockup
              href="/"
              className="dash-tabs-brand public-footer-brand"
              markClassName="dash-tabs-brand-mark public-footer-brand-mark"
              wordmarkClassName="dash-tabs-brand-wordmark"
              markTitle="Ceiora"
            />
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
              <a href="#" className="public-site-footer-link">
                Careers
              </a>
              <span className="public-site-footer-meta">Copyright © 2026 Ceiora</span>
            </nav>
          </div>
        </footer>
      </div>
    </>
  );
}
