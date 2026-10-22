"use client";

import { useEffect } from "react";

const TABLE_SELECTOR = ".dash-table";
const MODULE_HEADER_SELECTOR = ":scope > .dash-table-sticky-chrome, :scope > .thesis-section-heading, :scope > .explore-whatif-table-title";

/**
 * Keep the table's sticky stack measured and clip body rows at its lower edge.
 * One capture-phase listener handles every table on the page — `scroll` does not
 * bubble, but it does capture — rather than wiring each table independently.
 */
export default function StickyTableHeaders() {
  useEffect(() => {
    let frame = 0;

    const apply = (el: HTMLElement) => {
      el.style.setProperty("--dash-table-scroll-top", `${el.scrollTop}px`);
      const moduleHeader = el.querySelector<HTMLElement>(MODULE_HEADER_SELECTOR);
      const columnHeader = el.querySelector<HTMLElement>("thead tr");
      let offset = 0;
      if (moduleHeader) {
        const moduleHeaderStyle = getComputedStyle(moduleHeader);
        const trailingSpace = Number.parseFloat(moduleHeaderStyle.marginBlockEnd) || 0;
        offset = Math.ceil(moduleHeader.getBoundingClientRect().height + trailingSpace);
        el.dataset.stickyModuleHeader = "true";
      } else {
        delete el.dataset.stickyModuleHeader;
      }
      if (offset > 0) el.style.setProperty("--dash-table-sticky-offset", `${offset}px`);
      else el.style.removeProperty("--dash-table-sticky-offset");
      if (columnHeader) el.style.setProperty("--dash-table-column-header-height", `${Math.ceil(columnHeader.getBoundingClientRect().height)}px`);
      else el.style.removeProperty("--dash-table-column-header-height");

      const scrolled = el.scrollTop > 0;
      if ((el.dataset.scrolled === "true") !== scrolled) {
        el.dataset.scrolled = scrolled ? "true" : "false";
      }
    };

    const observedChrome = new Set<HTMLElement>();
    const resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const table = entry.target.closest<HTMLElement>(TABLE_SELECTOR);
        if (table) apply(table);
      }
    });

    const discoverTables = () => {
      for (const header of observedChrome) {
        if (!header.isConnected) {
          resizeObserver.unobserve(header);
          observedChrome.delete(header);
        }
      }
      document.querySelectorAll<HTMLElement>(TABLE_SELECTOR).forEach((table) => {
        apply(table);
        const moduleHeader = table.querySelector<HTMLElement>(MODULE_HEADER_SELECTOR);
        const columnHeader = table.querySelector<HTMLElement>("thead tr");
        if (moduleHeader && !observedChrome.has(moduleHeader)) {
          observedChrome.add(moduleHeader);
          resizeObserver.observe(moduleHeader);
        }
        if (columnHeader && !observedChrome.has(columnHeader)) {
          observedChrome.add(columnHeader);
          resizeObserver.observe(columnHeader);
        }
      });
    };

    discoverTables();
    const mutationObserver = new MutationObserver(discoverTables);
    mutationObserver.observe(document.body, { childList: true, subtree: true });

    const onScroll = (event: Event) => {
      const target = event.target as HTMLElement | null;
      if (!target?.classList?.contains("dash-table")) return;
      if (frame) return;
      frame = window.requestAnimationFrame(() => {
        frame = 0;
        apply(target);
      });
    };

    document.addEventListener("scroll", onScroll, { capture: true, passive: true });
    return () => {
      document.removeEventListener("scroll", onScroll, { capture: true });
      mutationObserver.disconnect();
      resizeObserver.disconnect();
      observedChrome.clear();
      if (frame) window.cancelAnimationFrame(frame);
    };
  }, []);

  return null;
}
