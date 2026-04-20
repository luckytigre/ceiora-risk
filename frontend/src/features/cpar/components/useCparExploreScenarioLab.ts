import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { mutate } from "swr";
import { ApiError } from "@/lib/apiTransport";
import {
  previewCparExploreWhatIf,
} from "@/hooks/useCparApi";
import { applyPortfolioWhatIf, useHoldingsAccounts, useHoldingsPositions } from "@/hooks/useHoldingsApi";
import { cparApiPath } from "@/lib/cparApi";
import { holdingsApiPath } from "@/lib/holdingsApi";
import { canNavigateCparSearchResult } from "@/lib/cparTruth";
import type { CparExploreWhatIfData, CparSearchItem } from "@/lib/types/cpar";
import {
  formatScenarioCount,
  normalizeAccountId,
  normalizeRic,
  normalizeTicker,
  parseQty,
  scenarioKey,
  type CparExploreMode,
  type CparExploreScenarioDraftRow,
} from "@/features/cpar/components/cparExploreUtils";

interface UseCparExploreScenarioLabArgs {
  priceMap: Map<string, number>;
  selectedInstrument: CparSearchItem | null;
  searchQuery: string;
  searchResults: CparSearchItem[];
  onSelectInstrument: (item: CparSearchItem) => void;
  searchSettled: boolean;
  onPreviewInstrument: (item: CparSearchItem) => void;
}

export function useCparExploreScenarioLab({
  priceMap,
  selectedInstrument,
  searchQuery,
  searchResults,
  onSelectInstrument,
  searchSettled,
  onPreviewInstrument,
}: UseCparExploreScenarioLabArgs) {
  const { data: accountsData } = useHoldingsAccounts();
  const { data: holdingsData } = useHoldingsPositions(null);

  const [mode, setMode] = useState<CparExploreMode>("raw");
  const [accountId, setAccountId] = useState("");
  const [quantityText, setQuantityText] = useState("");
  const [busy, setBusy] = useState(false);
  const [previewData, setPreviewData] = useState<CparExploreWhatIfData | null>(null);
  const [showResults, setShowResults] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const [resultMessage, setResultMessage] = useState("");
  const [scenarioDrafts, setScenarioDrafts] = useState<Record<string, CparExploreScenarioDraftRow>>({});
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [tickerFocused, setTickerFocused] = useState(false);

  const wrapRef = useRef<HTMLDivElement>(null);
  const toggleRef = useRef<HTMLDivElement>(null);
  const accountOptions = accountsData?.accounts ?? [];
  const holdingsRows = holdingsData?.positions ?? [];

  useEffect(() => {
    if (!accountId && accountOptions.length > 0) {
      setAccountId(accountOptions[0].account_id);
    }
  }, [accountId, accountOptions]);

  useEffect(() => {
    if (tickerFocused && searchQuery.trim().length > 0) {
      setDropdownOpen(true);
      setActiveIndex(-1);
    } else {
      setDropdownOpen(false);
    }
  }, [tickerFocused, searchQuery]);

  useEffect(() => {
    function handleClick(event: MouseEvent) {
      if (wrapRef.current && !wrapRef.current.contains(event.target as Node)) {
        setDropdownOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const liveQuantityByKey = useMemo(() => {
    const out = new Map<string, number>();
    for (const row of holdingsRows) {
      const key = scenarioKey(row.account_id, row.ric);
      out.set(key, Number(out.get(key) || 0) + Number(row.quantity || 0));
    }
    return out;
  }, [holdingsRows]);

  useEffect(() => {
    if (!selectedInstrument?.ric || !accountId) return;
    const existing = scenarioDrafts[scenarioKey(accountId, selectedInstrument.ric)];
    if (existing) {
      setQuantityText(existing.quantity_text);
    }
  }, [accountId, scenarioDrafts, selectedInstrument]);

  const scenarioRows = useMemo(
    () => Object.values(scenarioDrafts).sort((a, b) => a.key.localeCompare(b.key)),
    [scenarioDrafts],
  );

  const currentModeFactorOrder = useMemo(() => {
    const currentFactors = previewData?.current.display_exposure_modes?.[mode]
      ?? previewData?.current.exposure_modes[mode]
      ?? [];
    return [...currentFactors]
      .sort((a, b) => Math.abs(b.value) - Math.abs(a.value) || a.factor_id.localeCompare(b.factor_id))
      .map((factor) => factor.factor_id);
  }, [mode, previewData]);

  const clearMessages = useCallback(() => {
    setErrorMessage("");
    setResultMessage("");
  }, []);

  const resolveDirectSelection = useCallback((value: string): CparSearchItem | null => {
    const ticker = normalizeTicker(value);
    const ric = normalizeRic(value);
    if (!ticker && !ric) return null;
    return searchResults.find(
      (item) => canNavigateCparSearchResult(item) && normalizeTicker(item.ticker) === ticker,
    ) ?? searchResults.find(
      (item) => canNavigateCparSearchResult(item) && normalizeRic(item.ric) === ric,
    ) ?? null;
  }, [searchResults]);

  const selectFromTypeahead = useCallback((item: CparSearchItem) => {
    if (!searchSettled) return;
    if (!canNavigateCparSearchResult(item)) return;
    onPreviewInstrument(item);
    onSelectInstrument(item);
    setTickerFocused(false);
    setDropdownOpen(false);
    setActiveIndex(-1);
  }, [onPreviewInstrument, onSelectInstrument, searchSettled]);

  useEffect(() => {
    if (!searchSettled) {
      setActiveIndex(-1);
      return;
    }
    if (!dropdownOpen || activeIndex < 0 || activeIndex >= searchResults.length) return;
    const activeItem = searchResults[activeIndex];
    if (!canNavigateCparSearchResult(activeItem)) return;
    onPreviewInstrument(activeItem);
  }, [activeIndex, dropdownOpen, onPreviewInstrument, searchResults, searchSettled]);

  const handleTickerKeyDown = useCallback((e: KeyboardEvent<HTMLInputElement>) => {
    if (!dropdownOpen || searchResults.length === 0) {
      if (e.key === "Enter") {
        if (!searchSettled) {
          e.preventDefault();
          return;
        }
        const direct = resolveDirectSelection(searchQuery);
        if (direct) {
          e.preventDefault();
          selectFromTypeahead(direct);
        }
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
      if (!searchSettled) return;
      if (activeIndex >= 0 && activeIndex < searchResults.length) {
        selectFromTypeahead(searchResults[activeIndex]);
      } else {
        if (!searchSettled) return;
        const direct = resolveDirectSelection(searchQuery);
        if (direct) {
          selectFromTypeahead(direct);
        }
      }
    } else if (e.key === "Escape") {
      setDropdownOpen(false);
      setActiveIndex(-1);
    }
  }, [activeIndex, dropdownOpen, resolveDirectSelection, searchQuery, searchResults, searchSettled, selectFromTypeahead]);

  const handleTickerFocus = useCallback(() => {
    setTickerFocused(true);
    if (searchQuery.trim().length > 0) {
      setDropdownOpen(true);
    }
  }, [searchQuery]);

  const handleTickerBlur = useCallback((relatedTarget: EventTarget | null) => {
    if (relatedTarget && wrapRef.current?.contains(relatedTarget as Node)) return;
    setTickerFocused(false);
    setDropdownOpen(false);
    setActiveIndex(-1);
  }, []);

  const stageSelectedTicker = useCallback(() => {
    clearMessages();
    setPreviewData(null);
    const account = normalizeAccountId(accountId);
    const qty = parseQty(quantityText);
    if (!account) {
      setErrorMessage("Select an account for the what-if row.");
      return;
    }
    if (!selectedInstrument?.ric || !(selectedInstrument.ticker || selectedInstrument.ric)) {
      setErrorMessage("Choose an active-package cPAR search hit before staging the what-if row.");
      return;
    }
    if (selectedInstrument.scenario_stage_supported === false) {
      setErrorMessage(
        selectedInstrument.scenario_stage_detail
          || "This cPAR quote is visible from the registry, but staging stays limited to active-package names.",
      );
      return;
    }
    if (qty === null) {
      setErrorMessage("Quantity must be numeric and non-zero.");
      return;
    }
    const ticker = normalizeTicker(selectedInstrument.ticker || selectedInstrument.ric);
    const ric = normalizeRic(selectedInstrument.ric);
    const key = scenarioKey(account, ric);
    setScenarioDrafts((prev) => ({
      ...prev,
      [key]: {
        key,
        account_id: account,
        ticker,
        ric,
        quantity_text: quantityText.trim(),
        display_name: selectedInstrument.display_name,
        fit_status: selectedInstrument.fit_status,
        hq_country_code: selectedInstrument.hq_country_code,
        source: "cpar_explore",
      },
    }));
    setResultMessage(`Staged trade delta for ${ticker} in ${account}.`);
  }, [accountId, clearMessages, quantityText, selectedInstrument]);

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
    const nextQty = currentQty + delta;
    if (Math.abs(nextQty) <= 1e-12) {
      setPreviewData(null);
      clearMessages();
      setScenarioDrafts((prev) => {
        const next = { ...prev };
        delete next[key];
        return next;
      });
      setResultMessage(`Removed staged trade delta for ${existing.ticker}.`);
      return;
    }
    updateScenarioRow(key, String(nextQty));
  }, [clearMessages, scenarioDrafts, updateScenarioRow]);

  const removeScenarioRow = useCallback((key: string) => {
    setPreviewData(null);
    clearMessages();
    setScenarioDrafts((prev) => {
      const next = { ...prev };
      delete next[key];
      return next;
    });
  }, [clearMessages]);

  const buildPayloadRows = useCallback((): Array<{
    account_id: string;
    ticker: string;
    ric: string;
    quantity: number;
    source?: string | null;
  }> | null => {
    if (scenarioRows.length === 0) {
      setErrorMessage("Stage at least one scenario row first.");
      return null;
    }
    const payload = scenarioRows.map((row) => {
      const quantity = parseQty(row.quantity_text);
      if (quantity === null) return null;
      return {
        account_id: row.account_id,
        ticker: row.ticker,
        ric: row.ric,
        quantity,
        source: row.source,
      };
    });
    if (payload.some((row) => row === null)) {
      setErrorMessage("Fix scenario row quantities before continuing.");
      return null;
    }
    return payload as Array<{
      account_id: string;
      ticker: string;
      ric: string;
      quantity: number;
      source?: string | null;
    }>;
  }, [scenarioRows]);

  const runPreview = useCallback(async () => {
    clearMessages();
    const payloadRows = buildPayloadRows();
    if (!payloadRows) return;
    try {
      setBusy(true);
      const out = await previewCparExploreWhatIf({ scenario_rows: payloadRows });
      setPreviewData(out);
      setShowResults(true);
      setResultMessage(`Preview refreshed for ${formatScenarioCount(payloadRows.length)}.`);
      requestAnimationFrame(() => {
        toggleRef.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
      });
    } catch (error) {
      if (error instanceof ApiError) {
        setErrorMessage(typeof error.detail === "string" ? error.detail : error.message);
      } else if (error instanceof Error) {
        setErrorMessage(error.message);
      } else {
        setErrorMessage("cPAR explore preview failed.");
      }
    } finally {
      setBusy(false);
    }
  }, [buildPayloadRows, clearMessages]);

  const applyScenario = useCallback(async () => {
    clearMessages();
    const payloadRows = buildPayloadRows();
    if (!payloadRows) return;
    const hasFullRemoval = payloadRows.some((row) => {
      const liveQty = Number(liveQuantityByKey.get(scenarioKey(row.account_id, row.ric)) || 0);
      return Math.abs(liveQty) > 1e-12 && Math.abs(liveQty + row.quantity) <= 1e-12;
    });
    if (
      hasFullRemoval
      && !window.confirm("This trade delta fully closes one or more positions. Apply these holdings changes?")
    ) {
      return;
    }
    try {
      setBusy(true);
      await applyPortfolioWhatIf({
        scenario_rows: payloadRows,
        default_source: "cpar_explore",
      });
      await Promise.all([
        mutate(holdingsApiPath.holdingsAccounts()),
        mutate(holdingsApiPath.holdingsPositions(null)),
        mutate(cparApiPath.cparExploreContext()),
        mutate(cparApiPath.cparRisk()),
      ]);
      setScenarioDrafts({});
      setPreviewData(null);
      setShowResults(false);
      setResultMessage(`Applied ${formatScenarioCount(payloadRows.length)} to your holdings.`);
    } catch (error) {
      if (error instanceof ApiError) {
        setErrorMessage(typeof error.detail === "string" ? error.detail : error.message);
      } else if (error instanceof Error) {
        setErrorMessage(error.message);
      } else {
        setErrorMessage("cPAR explore apply failed.");
      }
    } finally {
      setBusy(false);
    }
  }, [buildPayloadRows, clearMessages, liveQuantityByKey]);

  const previewReady = scenarioRows.length > 0 && !busy;
  const applyReady = previewReady;
  const controlsBusy = busy;
  const stageReady = parseQty(quantityText) != null && Boolean(accountId) && Boolean(selectedInstrument?.ric);
  const discardReady = scenarioRows.length > 0;
  const previewNeedsAttention = scenarioRows.length > 0 && !busy;
  const builderStatus = scenarioRows.length > 0 ? `${scenarioRows.length} staged` : "Preview-only";
  const entryPrice = priceMap.get(normalizeTicker(searchQuery)) ?? null;
  const entryQty = parseQty(quantityText);
  const entryMv = entryPrice != null && entryQty != null ? entryPrice * entryQty : null;

  return {
    accountId,
    accountOptions,
    activeIndex,
    applyReady,
    builderStatus,
    busy,
    controlsBusy,
    currentModeFactorOrder,
    discardReady,
    dropdownOpen,
    entryMv,
    entryPrice,
    errorMessage,
    mode,
    previewData,
    previewNeedsAttention,
    previewReady,
    quantityText,
    resultMessage,
    scenarioRows,
    setAccountId,
    setActiveIndex,
    setMode,
    setQuantityText,
    setShowResults,
    showResults,
    stageReady,
    wrapRef,
    toggleRef,
    handleTickerBlur,
    handleTickerFocus,
    handleTickerKeyDown,
    selectFromTypeahead,
    stageSelectedTicker,
    updateScenarioRow,
    adjustScenarioRow,
    removeScenarioRow,
    runPreview,
    applyScenario,
    discardScenario: () => {
      setScenarioDrafts({});
      setPreviewData(null);
      setShowResults(false);
      clearMessages();
    },
  };
}
