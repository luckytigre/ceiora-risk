"use client";

import { useState } from "react";
import type { FactorDrilldownItem } from "@/lib/types";
import TableRowToggle from "@/components/TableRowToggle";
import FactorHistoryChart from "@/components/FactorHistoryChart";
import { useFactorHistory } from "@/hooks/useApi";

interface FactorDrilldownProps {
  factor: string;
  items: FactorDrilldownItem[];
  mode?: string;
  factorVol?: number;
  onClose: () => void;
}
const COLLAPSED_ROWS = 12;
type SortKey = "ticker" | "weight" | "exposure" | "sensitivity" | "contribution";

export default function FactorDrilldown({ factor, items, mode, factorVol, onClose }: FactorDrilldownProps) {
  const isSensitivity = mode === "sensitivity";
  const [sortKey, setSortKey] = useState<SortKey>(isSensitivity ? "sensitivity" : "exposure");
  const [sortAsc, setSortAsc] = useState(false);
  const [showAllRows, setShowAllRows] = useState(false);
  const { data: historyData, isLoading: historyLoading } = useFactorHistory(factor, 5);
  const sorted = [...items].sort((a, b) => {
    if (sortKey === "ticker") {
      return sortAsc
        ? a.ticker.localeCompare(b.ticker)
        : b.ticker.localeCompare(a.ticker);
    }
    const av = sortKey === "sensitivity" ? (a.sensitivity ?? 0) : a[sortKey];
    const bv = sortKey === "sensitivity" ? (b.sensitivity ?? 0) : b[sortKey];
    return sortAsc ? av - bv : bv - av;
  });
  const uniqueExposureCount = new Set(items.map((item) => item.exposure.toFixed(6))).size;
  const handleSort = (key: SortKey) => {
    if (key === sortKey) setSortAsc((prev) => !prev);
    else {
      setSortKey(key);
      setSortAsc(false);
    }
  };
  const arrow = (key: SortKey) => (sortKey === key ? (sortAsc ? " ↑" : " ↓") : "");
  const visibleRows = showAllRows ? sorted : sorted.slice(0, COLLAPSED_ROWS);

  return (
    <div className="detail-panel">
      <div className="detail-panel-header">
        <h4>{factor} — {isSensitivity ? "Sensitivity Breakdown" : "Position Breakdown"}</h4>
        <button
          onClick={onClose}
          className="detail-panel-close"
        >
          CLOSE
        </button>
      </div>
      <p className="detail-panel-meta">
        {items.length} positions, {uniqueExposureCount} unique exposure values{factorVol != null ? `, factor vol ${(factorVol * 100).toFixed(2)}%` : ""}
      </p>
      <div className="detail-history">
        <h5>5Y Historical Return — {factor}</h5>
        {historyLoading
          ? <div className="detail-history-empty loading-pulse">Loading 5Y history...</div>
          : <FactorHistoryChart factor={factor} points={historyData?.points ?? []} factorVol={factorVol} />}
      </div>
      <div className="dash-table">
        <table>
          <thead>
            <tr>
              <th onClick={() => handleSort("ticker")}>Ticker{arrow("ticker")}</th>
              <th className="text-right" onClick={() => handleSort("weight")}>Weight{arrow("weight")}</th>
              <th className="text-right" onClick={() => handleSort("exposure")}>Loading{arrow("exposure")}</th>
              {isSensitivity && (
                <th className="text-right" onClick={() => handleSort("sensitivity")}>
                  Loading × Vol{arrow("sensitivity")}
                </th>
              )}
              <th className="text-right" onClick={() => handleSort("contribution")}>
                Contribution{arrow("contribution")}
              </th>
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((item) => (
              <tr key={item.ticker}>
                <td><strong>{item.ticker}</strong></td>
                <td className="text-right">{(item.weight * 100).toFixed(2)}%</td>
                <td className="text-right">
                  <span className={item.exposure >= 0 ? "positive" : "negative"}>
                    {item.exposure.toFixed(4)}
                  </span>
                </td>
                {isSensitivity && (
                  <td className="text-right">
                    <span className={(item.sensitivity ?? 0) >= 0 ? "positive" : "negative"}>
                      {(item.sensitivity ?? 0).toFixed(4)}
                    </span>
                  </td>
                )}
                <td className="text-right">
                  <span className={item.contribution >= 0 ? "positive" : "negative"}>
                    {item.contribution.toFixed(6)}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <TableRowToggle
          totalRows={sorted.length}
          collapsedRows={COLLAPSED_ROWS}
          expanded={showAllRows}
          onToggle={() => setShowAllRows((prev) => !prev)}
          label="positions"
        />
      </div>
    </div>
  );
}
