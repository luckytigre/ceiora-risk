"use client";

import { useEffect, type RefObject } from "react";

const FOCUSABLE = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  '[tabindex]:not([tabindex="-1"])',
].join(",");

/**
 * A11Y-05 — keep Tab inside an open dialog and restore focus on close.
 *
 * The thesis dialogs already declare role="dialog" aria-modal="true" and handle
 * Escape, but nothing kept focus inside them, so Tab walked out into the table
 * behind while the dialog was still covering it.
 *
 * `autoFocus` on the first input is left to the caller; this hook only takes over
 * when focus would otherwise leave, and returns focus to whatever was focused
 * before the dialog opened.
 */
export function useFocusTrap(
  ref: RefObject<HTMLElement | null>,
  active: boolean,
  { restoreFocus = true }: { restoreFocus?: boolean } = {},
) {
  useEffect(() => {
    if (!active) return;
    const container = ref.current;
    if (!container) return;

    const previouslyFocused = document.activeElement as HTMLElement | null;

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Tab") return;
      // Do not filter on offsetParent: it is null for position:fixed elements as
      // well as hidden ones, which would silently make a fixed control untabbable.
      const items = Array.from(container.querySelectorAll<HTMLElement>(FOCUSABLE)).filter((el) => {
        if (el === document.activeElement) return true;
        if (el.hasAttribute("hidden") || el.getAttribute("aria-hidden") === "true") return false;
        const style = getComputedStyle(el);
        if (style.visibility === "hidden" || style.display === "none") return false;
        return el.getClientRects().length > 0;
      });
      if (!items.length) return;
      const first = items[0];
      const last = items[items.length - 1];
      const active2 = document.activeElement;

      // Focus sitting outside the dialog entirely (or on the container) — pull it back.
      if (!container.contains(active2)) {
        event.preventDefault();
        (event.shiftKey ? last : first).focus();
        return;
      }
      if (!event.shiftKey && active2 === last) {
        event.preventDefault();
        first.focus();
      } else if (event.shiftKey && active2 === first) {
        event.preventDefault();
        last.focus();
      }
    };

    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("keydown", onKeyDown);
      // Only restore if focus is still inside the dialog being torn down;
      // otherwise something else has deliberately taken it.
      if (!restoreFocus) return;
      if (!container.contains(document.activeElement) && document.activeElement !== document.body) return;
      previouslyFocused?.focus?.();
    };
  }, [ref, active, restoreFocus]);
}

export default useFocusTrap;
