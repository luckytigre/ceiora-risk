"use client";

interface TableRowToggleProps {
  totalRows: number;
  collapsedRows: number;
  expanded: boolean;
  onToggle: () => void;
  label?: string;
}

export default function TableRowToggle({
  totalRows,
  collapsedRows,
  expanded,
  onToggle,
  label = "rows",
}: TableRowToggleProps) {
  const hiddenRows = Math.max(0, totalRows - collapsedRows);
  if (hiddenRows === 0) return null;

  return (
    <div className="dash-table-toggle-wrap">
      <button
        type="button"
        className={`dash-table-toggle ${expanded ? "expanded" : ""}`}
        onClick={onToggle}
        aria-expanded={expanded}
      >
        {expanded ? `Show fewer ${label}` : `Show ${hiddenRows} more ${label}`}
      </button>
    </div>
  );
}
