import Link from "next/link";

const CUSE_LINKS = [
  { href: "/cuse/exposures", label: "Exposures", detail: "Current portfolio factor exposure and risk decomposition." },
  { href: "/cuse/explore", label: "Explore", detail: "Single-name inspection and the existing cUSE what-if workflow." },
  { href: "/cuse/health", label: "Health", detail: "Operator status and model diagnostics for the current cUSE runtime." },
];

const CPAR_LINKS = [
  { href: "/cpar/risk", label: "Risk", detail: "Current account-scoped cPAR risk, hedge, and narrow what-if workflow." },
  { href: "/cpar/explore", label: "Explore", detail: "Persisted package search, detail, and loadings interpretation." },
  { href: "/cpar/health", label: "Health", detail: "Lightweight package diagnostics, registry summary, and status legend." },
  { href: "/cpar/hedge", label: "Hedge", detail: "Standalone hedge workflow for one persisted subject at a time." },
];

export default function Home() {
  return (
    <div className="family-landing">
      <section className="family-landing-hero">
        <div className="family-landing-kicker">Ceiora</div>
        <h1>Choose a model family</h1>
        <p className="family-landing-copy">
          The app now splits cleanly by model family. cUSE keeps its current information architecture under a namespaced
          shell, and cPAR keeps its own package-based read surfaces.
        </p>
      </section>

      <div className="family-landing-grid">
        <section className="family-landing-card" data-testid="landing-cuse-card">
          <div className="family-landing-card-kicker">cUSE</div>
          <h2>Existing cUSE dashboards, now namespaced</h2>
          <p>
            Use the current cUSE exposure, explore, and health surfaces without changing their internal behavior in this slice.
          </p>
          <div className="family-landing-links">
            {CUSE_LINKS.map((link) => (
              <Link key={link.href} href={link.href} className="family-landing-link" prefetch={false}>
                <strong>{link.label}</strong>
                <span>{link.detail}</span>
              </Link>
            ))}
          </div>
        </section>

        <section className="family-landing-card" data-testid="landing-cpar-card">
          <div className="family-landing-card-kicker">cPAR</div>
          <h2>Package-scoped cPAR workspaces</h2>
          <p>
            cPAR remains separate from cUSE. The current account risk page stays narrow and package-based while the rest
            of the cPAR workspace keeps its persisted-detail flow.
          </p>
          <div className="family-landing-links">
            {CPAR_LINKS.map((link) => (
              <Link key={link.href} href={link.href} className="family-landing-link" prefetch={false}>
                <strong>{link.label}</strong>
                <span>{link.detail}</span>
              </Link>
            ))}
          </div>
        </section>
      </div>

      <section className="family-landing-footer">
        <div className="family-landing-footer-label">Shared surface</div>
        <Link href="/positions" className="family-landing-footer-link" prefetch={false}>
          Positions
          <span>Holdings import, live ledger editing, and the current shared positions workflow remain global.</span>
        </Link>
      </section>
    </div>
  );
}
