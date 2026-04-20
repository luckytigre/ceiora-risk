"use client";

import CparWhatIfBuilderPanel from "@/features/cpar/components/CparWhatIfBuilderPanel";
import CparWhatIfPreviewPanel from "@/features/cpar/components/CparWhatIfPreviewPanel";
import { useCparExploreScenarioLab } from "@/features/cpar/components/useCparExploreScenarioLab";
import type { CparSearchItem } from "@/lib/types/cpar";
import type { CparExplorePositionSummary } from "@/features/cpar/components/cparExploreUtils";

export default function CparExploreWhatIfSection({
  priceMap,
  selectedInstrument,
  searchQuery,
  searchLoading,
  searchSettled,
  onSearchQueryChange,
  searchResults,
  onSelectInstrument,
  positionMap,
  onPreviewInstrument,
}: {
  priceMap: Map<string, number>;
  selectedInstrument: CparSearchItem | null;
  searchQuery: string;
  searchLoading: boolean;
  searchSettled: boolean;
  onSearchQueryChange: (query: string) => void;
  searchResults: CparSearchItem[];
  onSelectInstrument: (item: CparSearchItem) => void;
  positionMap: Map<string, CparExplorePositionSummary>;
  onPreviewInstrument: (item: CparSearchItem) => void;
}) {
  const scenario = useCparExploreScenarioLab({
    priceMap,
    selectedInstrument,
    searchQuery,
    searchResults,
    onSelectInstrument,
    searchSettled,
    onPreviewInstrument,
  });

  return (
    <div className="whatif-builder" ref={scenario.wrapRef}>
      <CparWhatIfBuilderPanel
        accountId={scenario.accountId}
        accountOptions={scenario.accountOptions}
        activeIndex={scenario.activeIndex}
        applyReady={scenario.applyReady}
        busy={scenario.busy}
        builderStatus={scenario.builderStatus}
        controlsBusy={scenario.controlsBusy}
        discardReady={scenario.discardReady}
        dropdownOpen={scenario.dropdownOpen}
        entryMv={scenario.entryMv}
        entryPrice={scenario.entryPrice}
        errorMessage={scenario.errorMessage}
        onAccountIdChange={scenario.setAccountId}
        onApply={() => void scenario.applyScenario()}
        onDiscard={scenario.discardScenario}
        onPreview={() => void scenario.runPreview()}
        onQuantityTextChange={scenario.setQuantityText}
        onSearchQueryChange={onSearchQueryChange}
        onSetActiveIndex={scenario.setActiveIndex}
        onStage={scenario.stageSelectedTicker}
        onTickerBlur={scenario.handleTickerBlur}
        onTickerFocus={scenario.handleTickerFocus}
        onTickerKeyDown={scenario.handleTickerKeyDown}
        onTickerHover={onPreviewInstrument}
        onTickerSelect={scenario.selectFromTypeahead}
        positionMap={positionMap}
        previewNeedsAttention={scenario.previewNeedsAttention}
        previewReady={scenario.previewReady}
        priceMap={priceMap}
        quantityText={scenario.quantityText}
        resultMessage={scenario.resultMessage}
        scenarioRows={scenario.scenarioRows}
        searchQuery={searchQuery}
        searchLoading={searchLoading}
        searchSettled={searchSettled}
        searchResults={searchResults}
        stageReady={scenario.stageReady}
        updateScenarioRow={scenario.updateScenarioRow}
        adjustScenarioRow={scenario.adjustScenarioRow}
        removeScenarioRow={scenario.removeScenarioRow}
      />

      <CparWhatIfPreviewPanel
        currentModeFactorOrder={scenario.currentModeFactorOrder}
        mode={scenario.mode}
        onModeChange={scenario.setMode}
        onToggleResults={() => scenario.setShowResults((prev) => !prev)}
        previewData={scenario.previewData}
        showResults={scenario.showResults}
        toggleRef={scenario.toggleRef}
      />
    </div>
  );
}
