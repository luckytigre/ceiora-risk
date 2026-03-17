import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { mutate } from "swr";
import {
  applyPortfolioWhatIf,
  previewPortfolioWhatIf,
  useHoldingsAccounts,
  useHoldingsPositions,
} from "@/hooks/useApi";
import { ApiError, apiPath } from "@/lib/api";
import { runServeRefreshAndRevalidate } from "@/lib/refresh";
import type {
  UniverseSearchItem,
  UniverseTickerItem,
  WhatIfPreviewData,
} from "@/lib/types";
import { factorTier } from "@/lib/factorLabels";
import {
  buildScenarioPayloadRows,
  formatScenarioCount,
  fmtQty,
  normalizeAccountId,
  normalizeTicker,
  parseQty,
  refreshFailureMessage,
  scenarioKey,
  type WhatIfMode,
  type ScenarioDraftRow,
} from "@/features/whatif/whatIfUtils";

interface UseWhatIfScenarioLabArgs {
  item: UniverseTickerItem | null | undefined;
  priceMap: Map<string, number>;
  searchQuery: string;
  searchResults: UniverseSearchItem[];
  onSelectTicker: (ticker: string) => void;
}

export function useWhatIfScenarioLab({
  item,
  priceMap,
  searchQuery,
  searchResults,
  onSelectTicker,
}: UseWhatIfScenarioLabArgs) {
  const { data: accountsData } = useHoldingsAccounts();
  const { data: holdingsData } = useHoldingsPositions(null);

  const [mode, setMode] = useState<WhatIfMode>("raw");
  const [accountId, setAccountId] = useState("");
  const [quantityText, setQuantityText] = useState("");
  const [busy, setBusy] = useState(false);
  const [awaitingRefresh, setAwaitingRefresh] = useState(false);
  const [previewData, setPreviewData] = useState<WhatIfPreviewData | null>(null);
  const [showResults, setShowResults] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const [resultMessage, setResultMessage] = useState("");
  const [scenarioDrafts, setScenarioDrafts] = useState<Record<string, ScenarioDraftRow>>({});
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [tickerFocused, setTickerFocused] = useState(false);

  const wrapRef = useRef<HTMLDivElement>(null);
  const toggleRef = useRef<HTMLDivElement>(null);

  const accountOptions = accountsData?.accounts ?? [];
  const validAccountIds = useMemo(
    () => new Set(accountOptions.map((account) => normalizeAccountId(account.account_id))),
    [accountOptions],
  );
  const holdingsRows = holdingsData?.positions ?? [];
  const selectedTicker = normalizeTicker(item?.ticker);
  const scenarioTicker = normalizeTicker(searchQuery) || selectedTicker;
  const entryPrice = priceMap.get(scenarioTicker) ?? null;
  const entryQty = parseQty(quantityText);
  const entryMv = entryPrice != null && entryQty != null ? entryQty * entryPrice : null;

  useEffect(() => {
    if (tickerFocused && searchQuery.trim().length > 0 && searchResults.length > 0) {
      setDropdownOpen(true);
      setActiveIndex(-1);
    } else {
      setDropdownOpen(false);
    }
  }, [tickerFocused, searchQuery, searchResults.length]);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) {
        setDropdownOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const selectFromTypeahead = useCallback(
    (ticker: string) => {
      onSelectTicker(ticker);
      setTickerFocused(false);
      setDropdownOpen(false);
      setActiveIndex(-1);
    },
    [onSelectTicker],
  );

  const handleTickerKeyDown = useCallback(
    (e: KeyboardEvent<HTMLInputElement>) => {
      if (!dropdownOpen || searchResults.length === 0) {
        if (e.key === "Enter") {
          const direct = searchQuery.trim().toUpperCase();
          if (direct) selectFromTypeahead(direct);
        }
        return;
      }
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setActiveIndex((prev) => (prev < searchResults.length - 1 ? prev + 1 : 0));
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setActiveIndex((prev) => (prev > 0 ? prev - 1 : searchResults.length - 1));
      } else if (e.key === "Enter") {
        e.preventDefault();
        if (activeIndex >= 0 && activeIndex < searchResults.length) {
          selectFromTypeahead(searchResults[activeIndex].ticker);
        } else {
          const direct = searchQuery.trim().toUpperCase();
          if (direct) selectFromTypeahead(direct);
        }
      } else if (e.key === "Escape") {
        setDropdownOpen(false);
      }
    },
    [activeIndex, dropdownOpen, searchQuery, searchResults, selectFromTypeahead],
  );

  const handleTickerFocus = useCallback(() => {
    setTickerFocused(true);
    if (searchQuery.trim().length > 0 && searchResults.length > 0) {
      setDropdownOpen(true);
    }
  }, [searchQuery, searchResults.length]);

  const handleTickerBlur = useCallback((relatedTarget: EventTarget | null) => {
    if (relatedTarget && wrapRef.current?.contains(relatedTarget as Node)) return;
    setTickerFocused(false);
    setDropdownOpen(false);
    setActiveIndex(-1);
  }, []);

  useEffect(() => {
    if (!accountId && accountOptions.length > 0) {
      setAccountId(accountOptions[0].account_id);
    }
  }, [accountId, accountOptions]);

  useEffect(() => {
    setPreviewData(null);
    setErrorMessage("");
    setResultMessage("");
    setShowResults(false);
  }, [selectedTicker]);

  const liveQuantityByScenarioKey = useMemo(() => {
    const out = new Map<string, number>();
    for (const row of holdingsRows) {
      const key = scenarioKey(row.account_id, row.ticker);
      out.set(key, Number(out.get(key) || 0) + Number(row.quantity || 0));
    }
    return out;
  }, [holdingsRows]);

  useEffect(() => {
    if (!accountId) return;
    const key = scenarioKey(accountId, scenarioTicker);
    const staged = scenarioDrafts[key];
    if (staged) {
      setQuantityText(staged.quantity_text);
      return;
    }
    setQuantityText("");
  }, [accountId, scenarioDrafts, scenarioTicker]);

  const scenarioRows = useMemo(
    () =>
      Object.values(scenarioDrafts).sort((a, b) => {
        const byTicker = normalizeTicker(a.ticker).localeCompare(normalizeTicker(b.ticker));
        if (byTicker !== 0) return byTicker;
        return normalizeAccountId(a.account_id).localeCompare(normalizeAccountId(b.account_id));
      }),
    [scenarioDrafts],
  );

  const currentModeFactorOrder = useMemo(() => {
    const currentFactors = previewData?.current.exposure_modes[mode] ?? [];
    const factorCatalog = previewData?.current.factor_catalog ?? [];
    return [...currentFactors]
      .sort((a, b) => {
        const tierDiff = factorTier(a.factor_id, factorCatalog) - factorTier(b.factor_id, factorCatalog);
        if (tierDiff !== 0) return tierDiff;
        const byMagnitude = Math.abs(Number(b.value || 0)) - Math.abs(Number(a.value || 0));
        if (byMagnitude !== 0) return byMagnitude;
        return a.factor_id.localeCompare(b.factor_id);
      })
      .map((factor) => factor.factor_id);
  }, [mode, previewData]);

  const clearMessages = useCallback(() => {
    setErrorMessage("");
    setResultMessage("");
  }, []);

  const stageSelectedTicker = useCallback(() => {
    clearMessages();
    setPreviewData(null);
    const account = normalizeAccountId(accountId);
    const ticker = scenarioTicker;
    const qty = parseQty(quantityText);
    if (!account) {
      setErrorMessage("Select an account for the what-if row.");
      return;
    }
    if (validAccountIds.size > 0 && !validAccountIds.has(account)) {
      setErrorMessage("Choose an existing account from the list before staging the what-if row.");
      return;
    }
    if (!ticker) {
      setErrorMessage("Enter a ticker for the what-if row.");
      return;
    }
    if (qty === null) {
      setErrorMessage("Quantity must be numeric.");
      return;
    }
    const key = scenarioKey(account, ticker);
    setScenarioDrafts((prev) => ({
      ...prev,
      [key]: {
        key,
        account_id: account,
        ticker,
        quantity_text: quantityText.trim(),
        source: "what_if",
      },
    }));
    setResultMessage(`Staged trade delta for ${ticker} in ${account}.`);
  }, [accountId, clearMessages, quantityText, scenarioTicker, validAccountIds]);

  const updateScenarioRow = useCallback((key: string, quantityValue: string) => {
    setPreviewData(null);
    setScenarioDrafts((prev) => {
      const existing = prev[key];
      if (!existing) return prev;
      return {
        ...prev,
        [key]: {
          ...existing,
          quantity_text: quantityValue,
        },
      };
    });
  }, []);

  const adjustScenarioRow = useCallback((key: string, delta: number) => {
    const existing = scenarioDrafts[key];
    if (!existing) return;
    const currentQty = parseQty(existing.quantity_text);
    if (currentQty === null) {
      setErrorMessage(`Fix quantity for ${existing.ticker} before stepping it.`);
      return;
    }
    updateScenarioRow(key, fmtQty(currentQty + delta));
  }, [scenarioDrafts, updateScenarioRow]);

  const removeScenarioRow = useCallback((key: string) => {
    setPreviewData(null);
    clearMessages();
    setScenarioDrafts((prev) => {
      const next = { ...prev };
      delete next[key];
      return next;
    });
  }, [clearMessages]);

  const runPreview = useCallback(async () => {
    clearMessages();
    const payload = buildScenarioPayloadRows({
      scenarioRows,
      validAccountIds,
      action: "preview",
    });
    if ("error" in payload) {
      setErrorMessage(payload.error);
      return;
    }
    try {
      setBusy(true);
      const out = await previewPortfolioWhatIf({ scenario_rows: payload.rows });
      setPreviewData(out);
      setShowResults(true);
      setResultMessage(`Preview refreshed for ${payload.rows.length} scenario row${payload.rows.length === 1 ? "" : "s"}.`);
      requestAnimationFrame(() => {
        toggleRef.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
      });
    } catch (err) {
      if (err instanceof ApiError) {
        setErrorMessage(typeof err.detail === "string" ? err.detail : err.message);
      } else if (err instanceof Error) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage("What-if preview failed.");
      }
    } finally {
      setBusy(false);
    }
  }, [clearMessages, scenarioRows, validAccountIds]);

  const applyScenario = useCallback(async () => {
    clearMessages();
    if (scenarioRows.length === 0) {
      setErrorMessage("Stage at least one scenario row first.");
      return;
    }
    const payload = buildScenarioPayloadRows({
      scenarioRows,
      validAccountIds,
      action: "apply",
    });
    if ("error" in payload) {
      setErrorMessage(payload.error);
      return;
    }
    const hasFullRemovalFromDelta = scenarioRows.some((row) => {
      const qty = parseQty(row.quantity_text);
      if (qty === null) return false;
      const liveQty = Number(liveQuantityByScenarioKey.get(scenarioKey(row.account_id, row.ticker)) || 0);
      return Math.abs(liveQty) > 1e-12 && Math.abs(liveQty + qty) <= 1e-12;
    });
    if (
      hasFullRemovalFromDelta
      && !window.confirm("This trade delta fully closes one or more positions. Apply these changes and run RECALC?")
    ) {
      return;
    }

    try {
      setBusy(true);
      const out = await applyPortfolioWhatIf({
        scenario_rows: payload.rows,
        default_source: "what_if",
      });
      if (out.status !== "ok") {
        const rejected = out.rejected?.[0];
        const warning = out.warnings?.[0];
        setErrorMessage(
          rejected?.message
            || warning
            || "What-if apply was rejected. Review the staged rows and try again.",
        );
        return;
      }
      if (out.rejected_rows > 0) {
        const rejected = out.rejected?.[0];
        setErrorMessage(rejected?.message || "One or more scenario rows were rejected.");
        return;
      }

      const appliedScenarioCount = payload.rows.length;
      const warningText = out.warnings.length > 0 ? out.warnings[0] : "";

      await Promise.all([
        mutate(apiPath.holdingsAccounts()),
        mutate(apiPath.holdingsPositions(null)),
        mutate(apiPath.refreshStatus()),
        mutate(apiPath.operatorStatus()),
      ]);
      setScenarioDrafts({});
      setPreviewData(null);
      setShowResults(false);

      try {
        setAwaitingRefresh(true);
        setResultMessage(`Applied ${formatScenarioCount(appliedScenarioCount)} and started RECALC.${warningText ? ` ${warningText}` : ""}`);
        const { refresh, holdingsSyncVerified } = await runServeRefreshAndRevalidate();
        if (String(refresh.status || "").trim().toLowerCase() === "ok") {
          setErrorMessage("");
          setResultMessage(
            holdingsSyncVerified
              ? `Applied ${formatScenarioCount(appliedScenarioCount)} and RECALC finished.${warningText ? ` ${warningText}` : ""}`
              : `Applied ${formatScenarioCount(appliedScenarioCount)} and RECALC finished, but holdings sync status could not be verified. Check Operator status before trusting published analytics.${warningText ? ` ${warningText}` : ""}`,
          );
          return;
        }
        setResultMessage("");
        setErrorMessage(`What-if changes were applied, but RECALC failed: ${refreshFailureMessage(refresh)}${warningText ? ` ${warningText}` : ""}`);
      } catch (refreshErr) {
        setResultMessage("");
        if (refreshErr instanceof ApiError) {
          setErrorMessage(
            `What-if changes were applied, but RECALC failed: ${typeof refreshErr.detail === "string" ? refreshErr.detail : refreshErr.message}${warningText ? ` ${warningText}` : ""}`,
          );
        } else if (refreshErr instanceof Error) {
          setErrorMessage(`What-if changes were applied, but RECALC failed: ${refreshErr.message}${warningText ? ` ${warningText}` : ""}`);
        } else {
          setErrorMessage(`What-if changes were applied, but RECALC failed.${warningText ? ` ${warningText}` : ""}`);
        }
      } finally {
        setAwaitingRefresh(false);
      }
    } catch (err) {
      if (err instanceof ApiError) {
        setErrorMessage(typeof err.detail === "string" ? err.detail : err.message);
      } else if (err instanceof Error) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage("Could not apply what-if scenario.");
      }
    } finally {
      setBusy(false);
    }
  }, [clearMessages, liveQuantityByScenarioKey, scenarioRows, validAccountIds]);

  const discardScenario = useCallback(() => {
    setScenarioDrafts({});
    setPreviewData(null);
    setShowResults(false);
    clearMessages();
    setResultMessage("Discarded what-if scenario rows.");
  }, [clearMessages]);

  const builderStatus = awaitingRefresh
    ? "RECALC running"
    : previewData
      ? "Preview ready"
      : scenarioRows.length > 0
        ? `${scenarioRows.length} staged`
        : selectedTicker
          ? `${selectedTicker} selected`
          : "Ready";
  const controlsBusy = busy || awaitingRefresh;
  const normalizedAccountId = normalizeAccountId(accountId);
  const hasValidAccount = Boolean(normalizedAccountId) && (validAccountIds.size === 0 || validAccountIds.has(normalizedAccountId));
  const hasEntryTicker = Boolean(scenarioTicker);
  const hasValidEntryQty = entryQty !== null;
  const stageReady = !controlsBusy && hasValidAccount && hasEntryTicker && hasValidEntryQty;
  const previewReady = !controlsBusy && scenarioRows.length > 0;
  const previewNeedsAttention = previewReady && !previewData;
  const applyReady = !controlsBusy && scenarioRows.length > 0;
  const discardReady = !controlsBusy && scenarioRows.length > 0;

  return {
    accountId,
    accountOptions,
    activeIndex,
    adjustScenarioRow,
    applyReady,
    applyScenario,
    awaitingRefresh,
    busy,
    builderStatus,
    controlsBusy,
    currentModeFactorOrder,
    discardReady,
    discardScenario,
    dropdownOpen,
    entryMv,
    entryPrice,
    errorMessage,
    handleTickerBlur,
    handleTickerFocus,
    handleTickerKeyDown,
    mode,
    previewData,
    previewNeedsAttention,
    previewReady,
    quantityText,
    removeScenarioRow,
    resultMessage,
    runPreview,
    scenarioRows,
    selectFromTypeahead,
    setAccountId,
    setActiveIndex,
    setMode,
    setQuantityText,
    setShowResults,
    showResults,
    stageReady,
    stageSelectedTicker,
    toggleRef,
    updateScenarioRow,
    wrapRef,
  };
}

export type WhatIfScenarioLabState = ReturnType<typeof useWhatIfScenarioLab>;
