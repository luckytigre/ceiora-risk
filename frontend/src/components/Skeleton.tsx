"use client";

/**
 * UX-04 — a placeholder for a single value whose layout is already known.
 *
 * Use where the shape of the result is fixed and only the value is pending. It is
 * aria-hidden with an aria-busy wrapper left to the caller, so a screen reader
 * hears nothing rather than the word "Loading" repeated once per field.
 */
export default function Skeleton({ wide = false }: { wide?: boolean }) {
  return <span className="skeleton" data-width={wide ? "wide" : undefined} aria-hidden="true" />;
}
