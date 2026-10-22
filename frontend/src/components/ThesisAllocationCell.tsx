"use client";

import { useEffect, useMemo, useState, type CSSProperties } from "react";
import { useTheses, thesisPositionKey } from "@/components/ThesisContext";
import { useConfirm } from "@/components/ConfirmDialogProvider";

interface ThesisAllocationCellProps {
  accountId: string;
  ric: string;
  ticker: string;
  positionQuantity: number;
  disabled?: boolean;
  summaryMode?: "allocations" | "unallocated";
}

function formatQuantity(value: number): string {
  return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
}
export default function ThesisAllocationCell({
  accountId,
  ric,
  ticker,
  positionQuantity,
  disabled = false,
  summaryMode = "allocations",
}: ThesisAllocationCellProps) {
  const { available, theses, allocations, busy, createThesis, deleteThesis, replacePositionAllocations } = useTheses();
  const confirm = useConfirm();
  const [open, setOpen] = useState(false);
  const [newThesisName, setNewThesisName] = useState("");
  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const [error, setError] = useState<string | null>(null);
  const key = thesisPositionKey(accountId, ric);
  const positionAllocations = useMemo(
    () => allocations.filter((allocation) => thesisPositionKey(allocation.accountId, allocation.ric) === key),
    [allocations, key],
  );
  const thesisById = useMemo(() => new Map(theses.map((thesis) => [thesis.id, thesis])), [theses]);
  const allocatedAbsolute = positionAllocations.reduce((sum, allocation) => sum + Math.abs(allocation.quantity), 0);
  useEffect(() => {
    if (!open) return;
    const closeOnEscape = (event: KeyboardEvent) => { if (event.key === "Escape") setOpen(false); };
    document.addEventListener("keydown", closeOnEscape);
    return () => document.removeEventListener("keydown", closeOnEscape);
  }, [open]);

  const beginEditing = () => {
    setDrafts(Object.fromEntries(positionAllocations.map((allocation) => [allocation.thesisId, String(Math.abs(allocation.quantity))])));
    setError(null);
    setOpen(true);
  };

  const handleCreate = async () => {
    setError(null);
    const thesis = await createThesis(newThesisName);
    if (!thesis) { setError("Thesis could not be created. It may already exist."); return; }
    setDrafts((current) => ({ ...current, [thesis.id]: "" }));
    setNewThesisName("");
  };

  const handleSave = async () => {
    const direction = Math.sign(positionQuantity);
    const next = Object.entries(drafts).map(([thesisId, rawQuantity]) => ({
      thesisId,
      quantity: rawQuantity.trim() ? Math.abs(Number(rawQuantity)) * direction : 0,
    }));
    if (next.some((draft) => !Number.isFinite(draft.quantity))) {
      setError("Every allocation must be a valid number.");
      return;
    }
    const nextError = await replacePositionAllocations(accountId, ric, next);
    if (nextError) {
      setError(nextError);
      return;
    }
    setOpen(false);
  };

  if (!available || disabled || !accountId || !ric || !ticker || Math.abs(positionQuantity) <= 1e-9) {
    return <span className="thesis-cell-disabled">—</span>;
  }

  return (
    <>
      <button type="button" className="thesis-cell-trigger" onClick={beginEditing}>
        {summaryMode === "unallocated" || positionAllocations.length === 0 ? (
          <span className="thesis-cell-empty">+ Thesis</span>
        ) : (
          <span className="thesis-chip-list">
            {positionAllocations.slice(0, 2).map((allocation) => {
              const thesis = thesisById.get(allocation.thesisId);
              return thesis ? (
                <span key={allocation.thesisId} className="thesis-chip" style={{ "--thesis-color": thesis.color } as CSSProperties}>
                  {thesis.name}
                </span>
              ) : null;
            })}
            {positionAllocations.length > 2 ? <span className="thesis-chip-more">+{positionAllocations.length - 2}</span> : null}
          </span>
        )}
      </button>

      {open ? (
        <div className="thesis-modal-backdrop" role="presentation" onMouseDown={() => setOpen(false)}>
          <section className="thesis-modal thesis-allocation-modal" style={{ "--thesis-active-color": positionAllocations.length ? thesisById.get(positionAllocations[0].thesisId)?.color : "var(--analytics-cuse)" } as CSSProperties} role="dialog" aria-modal="true" aria-label={`Allocate ${ticker} by thesis`} onMouseDown={(event) => event.stopPropagation()}>
            <div className="thesis-modal-header">
              <div>
                <span className="thesis-modal-folio">THESIS ALLOCATION</span>
                <h3>{ticker} · {accountId}</h3>
              </div>
              <button type="button" className="thesis-modal-close" onClick={() => setOpen(false)} aria-label="Close thesis allocation">×</button>
            </div>
            <p className="thesis-modal-copy">
              Enter share magnitudes across theses. This {positionQuantity > 0 ? "long" : "short"} position keeps its direction automatically, and any remainder stays unallocated.
            </p>
            <div className="thesis-allocation-meter">
              Currently allocated {formatQuantity(allocatedAbsolute)} of {formatQuantity(Math.abs(positionQuantity))} shares
            </div>
            <div className="thesis-allocation-list">
              {theses.map((thesis) => (
                <label key={thesis.id} className="thesis-allocation-row">
                  <span className="thesis-dot" style={{ background: thesis.color }} />
                  <span>{thesis.name}</span>
                  <input
                    aria-label={`${thesis.name} share quantity`}
                    type="number"
                    step="any"
                    value={drafts[thesis.id] ?? ""}
                    placeholder="0"
                    onChange={(event) => setDrafts((current) => ({ ...current, [thesis.id]: event.target.value }))}
                  />
                  <button
                    type="button"
                    className="thesis-delete-button"
                    aria-label={`Delete thesis ${thesis.name}`}
                    onClick={async () => {
                      const proceed = await confirm({
                        title: `Delete “${thesis.name}”?`,
                        body: "All of its allocations return to Unallocated. The underlying holdings are not changed.",
                        dangerText: "Delete thesis",
                      });
                      if (!proceed) return;
                      const deleteError = await deleteThesis(thesis.id);
                      if (deleteError) { setError(deleteError); return; }
                      setDrafts((current) => {
                        const next = { ...current };
                        delete next[thesis.id];
                        return next;
                      });
                    }}
                  >
                    Delete
                  </button>
                </label>
              ))}
              {theses.length === 0 ? <div className="thesis-modal-empty">Create your first thesis below.</div> : null}
            </div>
            <div className="thesis-create-row">
              <input
                value={newThesisName}
                onChange={(event) => setNewThesisName(event.target.value)}
                onKeyDown={(event) => { if (event.key === "Enter") { event.preventDefault(); handleCreate(); } }}
                placeholder="e.g. Memory Cycle"
                aria-label="New thesis name"
              />
              <button type="button" onClick={() => void handleCreate()} disabled={!newThesisName.trim() || busy}>Create</button>
            </div>
            {error ? <div className="thesis-modal-error">{error}</div> : null}
            <div className="thesis-modal-actions">
              <button type="button" className="btn-action" onClick={() => setOpen(false)}>Cancel</button>
              <button type="button" className="btn-action primary" onClick={() => void handleSave()} disabled={busy}>{busy ? "Saving…" : "Save Allocation"}</button>
            </div>
          </section>
        </div>
      ) : null}
    </>
  );
}
