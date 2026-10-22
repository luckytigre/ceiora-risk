import type { Config } from "tailwindcss";

/**
 * TOKEN-01 — the design tokens live in src/app/globals.css as CSS custom
 * properties, which is what components consume. Mirroring them here means the
 * same scale is reachable from Tailwind utilities instead of being re-invented
 * as an arbitrary value.
 *
 * These reference the custom properties rather than duplicating literals, so
 * globals.css stays the single source of truth and the two cannot drift.
 */
const config: Config = {
  content: ["./src/**/*.{js,ts,jsx,tsx,mdx}"],
  theme: {
    extend: {
      fontSize: {
        "3xs": "var(--fs-3xs)",
        "2xs": "var(--fs-2xs)",
        xs: "var(--fs-xs)",
        sm: "var(--fs-sm)",
        base: "var(--fs-base)",
        md: "var(--fs-md)",
        lg: "var(--fs-lg)",
        xl: "var(--fs-xl)",
        "2xl": "var(--fs-2xl)",
        "3xl": "var(--fs-3xl)",
        "4xl": "var(--fs-4xl)",
        "5xl": "var(--fs-5xl)",
        "6xl": "var(--fs-6xl)",
      },
      letterSpacing: {
        caps: "var(--track-caps)",
        display: "var(--track-display)",
        none: "var(--track-none)",
      },
      spacing: {
        1: "var(--sp-1)",
        2: "var(--sp-2)",
        3: "var(--sp-3)",
        4: "var(--sp-4)",
        6: "var(--sp-6)",
        8: "var(--sp-8)",
        12: "var(--sp-12)",
      },
      transitionDuration: {
        fast: "var(--dur-fast)",
        base: "var(--dur-base)",
        slow: "var(--dur-slow)",
      },
      boxShadow: {
        1: "var(--shadow-1)",
        2: "var(--shadow-2)",
        3: "var(--shadow-3)",
        focus: "var(--focus-ring)",
      },
      zIndex: {
        base: "var(--z-base)",
        content: "var(--z-content)",
        sticky: "var(--z-sticky)",
        nav: "var(--z-nav)",
        dropdown: "var(--z-dropdown)",
        popover: "var(--z-popover)",
        modal: "var(--z-modal)",
        tooltip: "var(--z-tooltip)",
        toast: "var(--z-toast)",
      },
      colors: {
        ink: {
          DEFAULT: "var(--text-primary)",
          secondary: "var(--text-secondary)",
          muted: "var(--text-muted)",
          folio: "var(--text-folio)",
        },
        rule: {
          DEFAULT: "var(--border)",
          strong: "var(--rule-strong)",
          soft: "var(--rule-soft)",
          focus: "var(--rule-focus)",
        },
        signal: {
          positive: "var(--text-positive)",
          negative: "var(--text-negative)",
        },
      },
    },
  },
  plugins: [],
};

export default config;
