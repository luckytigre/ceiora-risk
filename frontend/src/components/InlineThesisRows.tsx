"use client";

import { Fragment, useCallback, useEffect, useMemo, useRef, useState, type CSSProperties, type DragEvent, type KeyboardEvent as ReactKeyboardEvent, type MouseEvent, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { manualPositionKey, thesisPositionKey, useTheses } from "@/components/ThesisContext";
import { isConcreteThesisPosition } from "@/lib/thesisIdentity";
import useFocusTrap from "@/hooks/useFocusTrap";
import { useConfirm } from "@/components/ConfirmDialogProvider";

export interface InlineThesisPosition {
  accountId: string;
  ric: string;
  ticker: string;
  quantity: number;
  price?: number | null;
  marketValue?: number | null;
}

interface DragDescriptor extends InlineThesisPosition {
  sourceId: string;
  sourceQuantity: number;
}

interface InlineThesisRowsProps<T> {
  rows: T[];
  colSpan: number;
  getPosition: (row: T) => InlineThesisPosition;
  renderCells: (row: T, displayQuantity: number, grouped: boolean, sourceGroupId?: string) => ReactNode;
  grouped: boolean;
  emptyLabel?: string;
  groupedSortMetric?: "quantity" | "market_value";
  groupedSortAscending?: boolean;
  groupedMarketValueAbsolute?: boolean;
  groupedTickerDirection?: "asc" | "desc";
  manualOrder?: boolean;
  orderScope: string;
  marketValueColumnIndex?: number;
  headerControlsId?: string;
  ungroupedLimit?: number;
}

const LEG_MIME = "application/x-ceiora-thesis-leg";
const FLAT_ORDER_MIME = "application/x-ceiora-manual-position";
const THESIS_ORDER_MIME = "application/x-ceiora-manual-thesis";

function moveToIndex<T>(items: T[], sourceIndex: number, targetIndex: number): T[] {
  if (sourceIndex < 0 || targetIndex < 0 || sourceIndex === targetIndex) return items;
  const next = [...items];
  const [moved] = next.splice(sourceIndex, 1);
  next.splice(targetIndex, 0, moved);
  return next;
}

function orderedByKeys<T>(items: T[], keys: string[], keyOf: (item: T) => string): T[] {
  if (!keys.length) return items;
  const rank = new Map(keys.map((key, index) => [key, index]));
  return [...items].sort((left, right) => (rank.get(keyOf(left)) ?? Number.MAX_SAFE_INTEGER) - (rank.get(keyOf(right)) ?? Number.MAX_SAFE_INTEGER));
}

function fmtQuantity(value: number): string { return value.toLocaleString(undefined, { maximumFractionDigits: 4 }); }
function direction(value: number): string { return `${value < 0 ? "Short" : "Long"} ${fmtQuantity(Math.abs(value))}`; }
function signedQuantity(value: number): string { return `${value < 0 ? "−" : "+"}${fmtQuantity(Math.abs(value))}`; }
function fmtMoney(value: number | null): string {
  if (value === null) return "—";
  const sign = value < -1e-9 ? "−" : "";
  if (Math.abs(value) >= 1e6) return `${sign}$${(Math.abs(value) / 1e6).toFixed(2)}M`;
  if (Math.abs(value) >= 1e3) return `${sign}$${(Math.abs(value) / 1e3).toFixed(1)}K`;
  return `${sign}$${Math.abs(value).toFixed(0)}`;
}
function legMarketValue(leg: DragDescriptor): number | null {
  const price = leg.price == null ? Number.NaN : Number(leg.price);
  if (Number.isFinite(price)) return price * leg.sourceQuantity;
  const fullMarketValue = leg.marketValue == null ? Number.NaN : Number(leg.marketValue);
  if (Number.isFinite(fullMarketValue) && Math.abs(leg.quantity) > 1e-9) return fullMarketValue * (leg.sourceQuantity / leg.quantity);
  return null;
}

export default function InlineThesisRows<T>({ rows, colSpan, getPosition, renderCells, grouped, emptyLabel = "No positions are loaded.", groupedSortMetric, groupedSortAscending = false, groupedMarketValueAbsolute = true, groupedTickerDirection, manualOrder = false, orderScope, marketValueColumnIndex, headerControlsId, ungroupedLimit }: InlineThesisRowsProps<T>) {
  const { available, theses, allocations, manualOrders, busy, createThesis, renameThesis, deleteThesis, saveManualOrder, replacePositionAllocations } = useTheses();
  const concrete = rows.every(row => { const p = getPosition(row); return isConcreteThesisPosition(p.accountId, p.ric); });
  grouped = grouped && available && concrete;
  manualOrder = manualOrder && available && concrete;
  const confirm = useConfirm();
  const [pending, setPending] = useState<{ leg: DragDescriptor; destinationId: string } | null>(null);
  const [partial, setPartial] = useState("");
  const [allocationMode, setAllocationMode] = useState<"all" | "partial">("all");
  const [error, setError] = useState<string | null>(null);
  const [createName, setCreateName] = useState("");
  const [creatingFor, setCreatingFor] = useState<DragDescriptor | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [renameDraft, setRenameDraft] = useState<{ id: string; name: string } | null>(null);
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; leg: DragDescriptor } | null>(null);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(() => new Set());
  const [draggingLeg, setDraggingLeg] = useState<DragDescriptor | null>(null);
  const [dragOverGroupId, setDragOverGroupId] = useState<string | null>(null);
  const [draggingThesisId, setDraggingThesisId] = useState<string | null>(null);
  const [thesisOrderTargetId, setThesisOrderTargetId] = useState<string | null>(null);
  const [rowOrderTargetKey, setRowOrderTargetKey] = useState<string | null>(null);
  const [recentlyMovedKey, setRecentlyMovedKey] = useState<string | null>(null);
  const [headerControlsHost, setHeaderControlsHost] = useState<HTMLElement | null>(null);
  // A11Y-02: rows are identified by account+RIC, which is stable across a move.
  // The React key includes group.id, so a moved row unmounts and remounts under a
  // new key — a cached DOM node would be stale exactly on the success path.
  const menuInvokerKeyRef = useRef<string | null>(null);
  // Roving tabindex (WAI-ARIA grid pattern): one tab stop for the whole table,
  // arrows move between rows. Making every row a tab stop would put 24+ stops
  // between the table and whatever follows it.
  //
  // The rows deliberately carry NO role. role="button" would announce them as
  // buttons and destroy the row/cell semantics a screen reader relies on to read
  // a data table; the correct alternative is a full role="grid" conversion with
  // gridcell on every cell, which is a larger change than this item. Until then
  // the rows stay native <tr> and carry an aria-label describing the action.
  const [activeLegKey, setActiveLegKey] = useState<string | null>(null);
  const menuRef = useRef<HTMLDivElement | null>(null);
  const createRef = useRef<HTMLElement | null>(null);
  const renameRef = useRef<HTMLElement | null>(null);
  const pendingRef = useRef<HTMLElement | null>(null);

  // A11Y-02: open the move menu from the keyboard. Anchored to the row, since
  // there is no pointer position to anchor to.
  const legKeyOf = useCallback((leg: DragDescriptor) => `${leg.accountId}:${leg.ric}`, []);

  const focusLeg = useCallback((key: string) => {
    const el = document.querySelector<HTMLElement>(`[data-leg-key="${CSS.escape(key)}"]`);
    if (el) { setActiveLegKey(key); el.focus(); }
  }, []);

  const openMenuForRow = useCallback((el: HTMLElement, leg: DragDescriptor) => {
    const box = el.getBoundingClientRect();
    menuInvokerKeyRef.current = `${leg.accountId}:${leg.ric}`;
    setContextMenu({
      x: Math.min(box.left + 24, window.innerWidth - 260),
      y: Math.min(box.bottom, window.innerHeight - 280),
      leg,
    });
  }, []);

  useEffect(() => {
    if (!pending && !showCreate && !renameDraft && !contextMenu) return;
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setPending(null); setShowCreate(false); setRenameDraft(null); setContextMenu(null); setError(null);
    };
    document.addEventListener("keydown", closeOnEscape);
    return () => document.removeEventListener("keydown", closeOnEscape);
  }, [contextMenu, pending, renameDraft, showCreate]);

  useEffect(() => {
    if (!recentlyMovedKey) return;
    // MOTION-01: kept just past --dur-base (180ms) so the class outlives the animation.
    const timer = window.setTimeout(() => setRecentlyMovedKey(null), 250);
    return () => window.clearTimeout(timer);
  }, [recentlyMovedKey]);

  useEffect(() => {
    setHeaderControlsHost(grouped && headerControlsId ? document.getElementById(headerControlsId) : null);
  }, [grouped, headerControlsId]);

  useFocusTrap(createRef, showCreate, { restoreFocus: false });
  useFocusTrap(renameRef, Boolean(renameDraft), { restoreFocus: false });
  useFocusTrap(pendingRef, Boolean(pending), { restoreFocus: false });

  // A11Y-02: focus the first item when the menu opens.
  useEffect(() => {
    if (!contextMenu) return;
    menuRef.current?.querySelector<HTMLElement>('[role="menuitem"]')?.focus();
  }, [contextMenu]);

  // A11Y-02: hand focus back to the originating row once EVERY overlay has closed.
  // Keying this on the menu alone was wrong: choosing a menu item closes the menu
  // and opens a dialog in the same tick, so the restore fought the dialog's
  // autoFocus and focus landed on the row instead of inside the dialog.
  const anyOverlayOpen = Boolean(contextMenu) || showCreate || Boolean(renameDraft) || Boolean(pending);
  useEffect(() => {
    if (anyOverlayOpen) return;
    const key = menuInvokerKeyRef.current;
    if (!key) return;
    menuInvokerKeyRef.current = null;
    // Re-query by identity after the dialog tears down. A successful move
    // remounts the row under a different group, so a cached node would be gone.
    const id = window.setTimeout(() => focusLeg(key), 0);
    return () => window.clearTimeout(id);
  }, [anyOverlayOpen, focusLeg]);

  const allocationsByPosition = useMemo(() => {
    const map = new Map<string, Map<string, number>>();
    for (const allocation of allocations) {
      const key = thesisPositionKey(allocation.accountId, allocation.ric);
      const positionAllocations = map.get(key) ?? new Map<string, number>();
      positionAllocations.set(allocation.thesisId, allocation.quantity);
      map.set(key, positionAllocations);
    }
    return map;
  }, [allocations]);

  const groups = useMemo(() => {
    const thesisOrder = manualOrders.theses ?? [];
    const orderedTheses = manualOrder ? orderedByKeys(theses, thesisOrder, (thesis) => thesis.id) : [...theses];
    if (!manualOrder && groupedTickerDirection) orderedTheses.sort((left, right) => groupedTickerDirection === "asc" ? left.name.localeCompare(right.name) : right.name.localeCompare(left.name));
    const definitions = [...orderedTheses, { id: "unallocated", name: "Unallocated", color: "#8b929e", createdAt: "" }];
    return definitions.map((group) => {
      const legs = rows.flatMap((row) => {
        const position = getPosition(row);
        const positionAllocations = allocationsByPosition.get(thesisPositionKey(position.accountId, position.ric));
        const allocated = [...(positionAllocations?.values() ?? [])].reduce((sum, quantity) => sum + quantity, 0);
        const sourceQuantity = group.id === "unallocated" ? position.quantity - allocated : positionAllocations?.get(group.id) ?? 0;
        return Math.abs(sourceQuantity) > 1e-9 ? [{ row, leg: { ...position, sourceId: group.id, sourceQuantity } }] : [];
      });
      if (manualOrder) {
        const scope = group.id === "unallocated" ? `unallocated:${orderScope}` : `legs:${group.id}`;
        const order = manualOrders[scope] ?? [];
        const ordered = orderedByKeys(legs, order, ({ leg }) => manualPositionKey(leg.accountId, leg.ric));
        legs.splice(0, legs.length, ...ordered);
      } else if (groupedTickerDirection) {
        legs.sort((left, right) => groupedTickerDirection === "asc" ? left.leg.ticker.localeCompare(right.leg.ticker) : right.leg.ticker.localeCompare(left.leg.ticker));
      } else if (groupedSortMetric) {
        legs.sort((left, right) => {
          const sortValue = ({ leg }: (typeof legs)[number]) => {
            if (groupedSortMetric === "quantity") return leg.sourceQuantity;
            const marketValue = legMarketValue(leg);
            return marketValue === null ? null : groupedMarketValueAbsolute ? Math.abs(marketValue) : marketValue;
          };
          const leftValue = sortValue(left); const rightValue = sortValue(right);
          if (leftValue === null) return rightValue === null ? 0 : 1;
          if (rightValue === null) return -1;
          const delta = leftValue - rightValue;
          if (Math.abs(delta) > 1e-9) return groupedSortAscending ? delta : -delta;
          return left.leg.ticker.localeCompare(right.leg.ticker);
        });
      }
      const legValues = legs.map(item => legMarketValue(item.leg));
      const marketValue = legValues.some(value => value === null) ? null : legValues.reduce<number>((sum, value) => sum + (value ?? 0), 0);
      return { ...group, legs, marketValue };
    });
  }, [allocationsByPosition, getPosition, groupedMarketValueAbsolute, groupedSortAscending, groupedSortMetric, groupedTickerDirection, manualOrder, manualOrders, orderScope, rows, theses]);

  const orderedFlatRows = useMemo(() => manualOrder
      ? orderedByKeys(rows, manualOrders[`positions:${orderScope}`] ?? [], (row) => { const position = getPosition(row); return manualPositionKey(position.accountId, position.ric); })
      : rows, [getPosition, manualOrder, manualOrders, orderScope, rows]);
  const flatRows = ungroupedLimit == null ? orderedFlatRows : orderedFlatRows.slice(0, ungroupedLimit);

  // The single default tab stop for the whole grid until a row is focused.
  const firstLegKey = useMemo(() => {
    for (const group of groups) {
      const leg = group.legs[0];
      if (leg) return `${leg.leg.accountId}:${leg.leg.ric}`;
    }
    return null;
  }, [groups]);

  const beginMove = (leg: DragDescriptor, destinationId: string) => {
    if (leg.sourceId === destinationId) return;
    setAllocationMode("all"); setPartial(String(leg.sourceQuantity)); setError(null); setPending({ leg, destinationId });
  };
  const dropOn = (event: DragEvent, destinationId: string) => {
    event.preventDefault();
    setDragOverGroupId(null);
    setDraggingLeg(null);
    try { beginMove(JSON.parse(event.dataTransfer.getData(LEG_MIME)) as DragDescriptor, destinationId); }
    catch { setError("That position row could not be moved."); }
  };
  const dragOver = (event: DragEvent, destinationId: string) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = "move";
    setRowOrderTargetKey(null);
    setDragOverGroupId(draggingLeg?.sourceId === destinationId ? null : destinationId);
  };
  const endDrag = () => {
    setDraggingLeg(null); setDragOverGroupId(null); setDraggingThesisId(null); setThesisOrderTargetId(null); setRowOrderTargetKey(null);
  };
  const persistOrder = async (scope: string, keys: string[]) => {
    const saveError = await saveManualOrder(scope, keys);
    if (saveError) setError(saveError);
  };
  const reorderFlatRows = (sourceKey: string, targetKey: string) => {
    const keys = orderedFlatRows.map((row) => { const position = getPosition(row); return manualPositionKey(position.accountId, position.ric); });
    const next = moveToIndex(keys, keys.indexOf(sourceKey), keys.indexOf(targetKey));
    if (next !== keys) void persistOrder(`positions:${orderScope}`, next);
  };
  const reorderThesisGroups = (sourceId: string, targetId: string) => {
    const keys = groups.filter((group) => group.id !== "unallocated").map((group) => group.id);
    const next = moveToIndex(keys, keys.indexOf(sourceId), keys.indexOf(targetId));
    if (next !== keys) void persistOrder("theses", next);
  };
  const reorderGroupLegs = (groupId: string, source: DragDescriptor, target: DragDescriptor) => {
    const group = groups.find((item) => item.id === groupId);
    if (!group) return;
    const keys = group.legs.map(({ leg }) => manualPositionKey(leg.accountId, leg.ric));
    const next = moveToIndex(keys, keys.indexOf(manualPositionKey(source.accountId, source.ric)), keys.indexOf(manualPositionKey(target.accountId, target.ric)));
    if (next !== keys) void persistOrder(groupId === "unallocated" ? `unallocated:${orderScope}` : `legs:${groupId}`, next);
  };
  const applyMove = async () => {
    if (!pending) return;
    const maximum = Math.abs(pending.leg.sourceQuantity);
    const partialQuantity = Number(partial);
    const magnitude = allocationMode === "all" ? maximum : Math.abs(partialQuantity);
    if (!Number.isFinite(magnitude) || magnitude <= 0 || magnitude > maximum + 1e-8) { setError(`Enter a quantity from 0 to ${fmtQuantity(maximum)}.`); return; }
    if (allocationMode === "partial" && Math.sign(partialQuantity) !== Math.sign(pending.leg.quantity)) { setError(pending.leg.quantity < 0 ? "Short allocations must be negative." : "Long allocations must be positive."); return; }
    const signed = magnitude * Math.sign(pending.leg.quantity);
    const current = new Map((allocationsByPosition.get(thesisPositionKey(pending.leg.accountId, pending.leg.ric)) ?? new Map()).entries());
    if (pending.leg.sourceId !== "unallocated") current.set(pending.leg.sourceId, Number(current.get(pending.leg.sourceId) || 0) - signed);
    if (pending.destinationId !== "unallocated") current.set(pending.destinationId, Number(current.get(pending.destinationId) || 0) + signed);
    const saveError = await replacePositionAllocations(pending.leg.accountId, pending.leg.ric, [...current.entries()].map(([thesisId, quantity]) => ({ thesisId, quantity })));
    if (saveError) { setError(saveError); return; }
    setRecentlyMovedKey(`${pending.destinationId}:${pending.leg.accountId}:${pending.leg.ric}`);
    setPending(null); setError(null);
  };
  const handleCreate = async () => {
    const thesis = await createThesis(createName);
    if (!thesis) { setError("Thesis could not be created. It may already exist."); return; }
    const source = creatingFor;
    setCreateName(""); setCreatingFor(null); setShowCreate(false);
    if (source) beginMove(source, thesis.id);
  };
  const handleRename = async () => {
    if (!renameDraft) return;
    const nextError = await renameThesis(renameDraft.id, renameDraft.name);
    if (nextError) { setError(nextError); return; }
    setRenameDraft(null); setError(null);
  };
  const toggleGroup = (groupId: string) => setCollapsedGroups((current) => {
    const next = new Set(current);
    if (next.has(groupId)) next.delete(groupId); else next.add(groupId);
    return next;
  });

  if (!grouped) {
    return <>{flatRows.length ? flatRows.map((row) => {
      const position = getPosition(row);
      const positionKey = manualPositionKey(position.accountId, position.ric || position.ticker);
      return <tr key={positionKey} className={`${manualOrder ? "manual-sort-row" : ""}${rowOrderTargetKey === positionKey ? " order-target" : ""}`} draggable={manualOrder && !busy} onDragStart={(event) => { if (!manualOrder) return; event.dataTransfer.effectAllowed = "move"; event.dataTransfer.setData(FLAT_ORDER_MIME, positionKey); setDragOverGroupId(null); setThesisOrderTargetId(null); }} onDragEnd={endDrag} onDragOver={(event) => { if (!manualOrder || !event.dataTransfer.types.includes(FLAT_ORDER_MIME)) return; event.preventDefault(); event.dataTransfer.dropEffect = "move"; setRowOrderTargetKey(positionKey); }} onDrop={(event) => { if (!manualOrder || !event.dataTransfer.types.includes(FLAT_ORDER_MIME)) return; event.preventDefault(); const sourceKey = event.dataTransfer.getData(FLAT_ORDER_MIME); endDrag(); reorderFlatRows(sourceKey, positionKey); }}>{renderCells(row, position.quantity, false)}</tr>;
    }) : <tr><td colSpan={colSpan} className="holdings-empty-row">{emptyLabel}</td></tr>}</>;
  }

  const allGroupsCollapsed = groups.length > 0 && groups.every((group) => collapsedGroups.has(group.id));
  const toolbarDisabled = busy || Boolean(draggingLeg) || Boolean(draggingThesisId);
  const dragStatus = draggingLeg
    ? "Drop into a thesis · Esc to cancel"
    : draggingThesisId
      ? "Drop thesis in its new order · Esc to cancel"
      : null;
  const saveStatus = busy ? "Saving…" : recentlyMovedKey ? "✓ Saved" : error ? "Couldn’t save" : null;

  return <>
    {headerControlsHost ? createPortal(dragStatus ? <span className="thesis-header-drag-status" aria-live="polite">{dragStatus}</span> : <>{saveStatus ? <span className="thesis-header-save-status" title={error || undefined} aria-live="polite">{saveStatus}</span> : null}{theses.length > 0 ? <button type="button" disabled={toolbarDisabled} onClick={() => setCollapsedGroups(allGroupsCollapsed ? new Set() : new Set(groups.map((group) => group.id)))}>{allGroupsCollapsed ? "Expand groups" : "Collapse groups"}</button> : null}<button type="button" className="btn-action primary" disabled={toolbarDisabled} onClick={() => { setCreatingFor(null); setShowCreate(true); }}>+ New thesis</button></>, headerControlsHost) : null}
    {groups.map((group) => { const groupLead = <>{manualOrder && group.id !== "unallocated" ? <button type="button" className="thesis-manual-handle" disabled={busy} draggable={!busy} aria-label={`Drag to reorder ${group.name}`} onDragStart={(event) => { event.stopPropagation(); event.dataTransfer.effectAllowed = "move"; event.dataTransfer.setData(THESIS_ORDER_MIME, group.id); setDraggingLeg(null); setDragOverGroupId(null); setRowOrderTargetKey(null); setDraggingThesisId(group.id); }} onDragEnd={endDrag}>⋮⋮</button> : null}<button type="button" className="thesis-group-toggle" aria-label={`${collapsedGroups.has(group.id) ? "Expand" : "Collapse"} ${group.name}`} aria-expanded={!collapsedGroups.has(group.id)} onClick={() => toggleGroup(group.id)}><span aria-hidden="true">{collapsedGroups.has(group.id) ? "›" : "⌄"}</span></button><span className="thesis-dot" style={{ background: group.color }} /><strong>{group.name}</strong><span>{group.legs.length} legs</span></>; const groupActions = group.id !== "unallocated" ? <span className="thesis-group-actions"><button type="button" onClick={() => { setError(null); setRenameDraft({ id: group.id, name: group.name }); }}>Rename</button><button type="button" className="danger" aria-label={`Delete thesis ${group.name}`} onClick={async () => { const proceed = await confirm({ title: `Delete “${group.name}”?`, body: `${group.legs.length} ${group.legs.length === 1 ? "leg returns" : "legs return"} to Unallocated. The underlying holdings are not changed.`, dangerText: "Delete thesis" }); if (!proceed) return; const nextError = await deleteThesis(group.id); if (nextError) setError(nextError); }}>Delete</button></span> : null; const hasMarketValueColumn = marketValueColumnIndex != null && marketValueColumnIndex > 0 && marketValueColumnIndex < colSpan - 1; const trailingSpan = hasMarketValueColumn ? colSpan - marketValueColumnIndex - 2 : 0; return <Fragment key={group.id}>
      <tr className={`thesis-inline-group-row${collapsedGroups.has(group.id) ? " collapsed" : ""}${dragOverGroupId === group.id ? " drop-target" : ""}${thesisOrderTargetId === group.id ? " order-target" : ""}${draggingThesisId === group.id ? " ordering" : ""}`} style={{ "--thesis-active-color": group.color } as CSSProperties} onDragOver={(event) => { if (manualOrder && group.id !== "unallocated" && event.dataTransfer.types.includes(THESIS_ORDER_MIME)) { event.preventDefault(); event.dataTransfer.dropEffect = "move"; setDragOverGroupId(null); setRowOrderTargetKey(null); setThesisOrderTargetId(group.id); return; } dragOver(event, group.id); }} onDrop={(event) => { if (manualOrder && group.id !== "unallocated" && event.dataTransfer.types.includes(THESIS_ORDER_MIME)) { event.preventDefault(); const sourceId = event.dataTransfer.getData(THESIS_ORDER_MIME); endDrag(); reorderThesisGroups(sourceId, group.id); return; } dropOn(event, group.id); }}>
        {hasMarketValueColumn ? <><td colSpan={marketValueColumnIndex}>{groupLead}</td><td className="text-right thesis-group-market-value" title="Net allocated market value">{group.legs.length ? fmtMoney(group.marketValue) : "—"}</td>{trailingSpan > 0 ? <td colSpan={trailingSpan} /> : null}<td className="thesis-group-actions-cell">{groupActions}</td></> : <td colSpan={colSpan}>{groupLead}{group.legs.length ? <span>{fmtMoney(group.marketValue)} net</span> : null}{groupActions}</td>}
      </tr>
      {!collapsedGroups.has(group.id) ? group.legs.map(({ row, leg }) => { const legKey = manualPositionKey(leg.accountId, leg.ric); return <tr key={`${group.id}:${leg.accountId}:${leg.ric}`} data-thesis-group-id={group.id} data-thesis-group-name={group.name} className={`thesis-inline-draggable${draggingLeg?.sourceId === leg.sourceId && draggingLeg.accountId === leg.accountId && draggingLeg.ric === leg.ric ? " dragging" : ""}${dragOverGroupId === group.id && draggingLeg?.sourceId !== group.id ? " drop-target-row" : ""}${rowOrderTargetKey === `${group.id}:${legKey}` ? " order-target" : ""}${recentlyMovedKey === `${group.id}:${leg.accountId}:${leg.ric}` ? " just-moved" : ""}`} style={{ "--thesis-active-color": group.color } as CSSProperties} draggable onDragStart={(event) => { event.dataTransfer.effectAllowed = "move"; event.dataTransfer.setData(LEG_MIME, JSON.stringify(leg)); setDraggingThesisId(null); setThesisOrderTargetId(null); setRowOrderTargetKey(null); setDraggingLeg(leg); }} onDragEnd={endDrag} onDragEnter={(event) => { if (manualOrder && draggingLeg?.sourceId === group.id) { event.preventDefault(); setDragOverGroupId(null); setRowOrderTargetKey(`${group.id}:${legKey}`); return; } dragOver(event, group.id); }} onDragOver={(event) => { if (manualOrder && draggingLeg?.sourceId === group.id) { event.preventDefault(); event.dataTransfer.dropEffect = "move"; setDragOverGroupId(null); setRowOrderTargetKey(`${group.id}:${legKey}`); return; } dragOver(event, group.id); }} onDrop={(event) => { if (manualOrder && draggingLeg?.sourceId === group.id) { event.preventDefault(); const source = JSON.parse(event.dataTransfer.getData(LEG_MIME)) as DragDescriptor; endDrag(); reorderGroupLegs(group.id, source, leg); return; } dropOn(event, group.id); }} data-leg-key={`${leg.accountId}:${leg.ric}`} tabIndex={(activeLegKey ?? firstLegKey) === `${leg.accountId}:${leg.ric}` ? 0 : -1} aria-label={`${leg.ticker}, ${direction(leg.sourceQuantity)}, in ${group.name}. Press Enter to move.`} onFocus={() => setActiveLegKey(`${leg.accountId}:${leg.ric}`)} onKeyDown={(event: ReactKeyboardEvent<HTMLTableRowElement>) => { if (event.target !== event.currentTarget) return; if (event.key === "Enter" || event.key === " ") { event.preventDefault(); openMenuForRow(event.currentTarget, leg); return; } if (event.key === "ArrowDown" || event.key === "ArrowUp" || event.key === "Home" || event.key === "End") { event.preventDefault(); const all = Array.from(document.querySelectorAll<HTMLElement>("tr.thesis-inline-draggable[data-leg-key]")); const i = all.indexOf(event.currentTarget); const next = event.key === "ArrowDown" ? all[Math.min(i + 1, all.length - 1)] : event.key === "ArrowUp" ? all[Math.max(i - 1, 0)] : event.key === "Home" ? all[0] : all[all.length - 1]; if (next) { setActiveLegKey(next.dataset.legKey ?? null); next.focus(); } } }} onContextMenu={(event: MouseEvent) => { event.preventDefault(); setContextMenu({ x: Math.min(event.clientX, window.innerWidth - 260), y: Math.min(event.clientY, window.innerHeight - 280), leg }); }}>
        {renderCells(row, leg.sourceQuantity, true, group.id)}
      </tr>; }) : null}
      {!collapsedGroups.has(group.id) && !group.legs.length ? <tr className={`thesis-inline-empty-row${dragOverGroupId === group.id ? " drop-target" : ""}`} style={{ "--thesis-active-color": group.color } as CSSProperties} onDragEnter={(event) => dragOver(event, group.id)} onDragOver={(event) => dragOver(event, group.id)} onDrop={(event) => dropOn(event, group.id)}><td colSpan={colSpan}>{draggingLeg ? `Drop ${draggingLeg.ticker} into ${group.name}` : "Drop a position row here"}</td></tr> : null}
    </Fragment>; })}

    {(contextMenu || showCreate || renameDraft || pending) ? <tr className="thesis-inline-modal-host"><td colSpan={colSpan}>
      {contextMenu ? <div ref={menuRef} className="thesis-context-menu" role="menu" aria-label={`Move ${contextMenu.leg.ticker}`} onKeyDown={(event: ReactKeyboardEvent<HTMLDivElement>) => { const items = Array.from(event.currentTarget.querySelectorAll<HTMLElement>('[role="menuitem"]')); if (!items.length) return; const i = items.indexOf(document.activeElement as HTMLElement); if (event.key === "ArrowDown") { event.preventDefault(); items[(i + 1) % items.length].focus(); } else if (event.key === "ArrowUp") { event.preventDefault(); items[(i - 1 + items.length) % items.length].focus(); } else if (event.key === "Home") { event.preventDefault(); items[0].focus(); } else if (event.key === "End") { event.preventDefault(); items[items.length - 1].focus(); } }} style={{ left: contextMenu.x, top: contextMenu.y }}>
        <button type="button" role="menuitem" onClick={() => { setCreatingFor(contextMenu.leg); setShowCreate(true); setContextMenu(null); }}>Add to new thesis</button>
        {groups.filter((group) => group.id !== contextMenu.leg.sourceId).map((group) => <button key={group.id} type="button" role="menuitem" onClick={() => { beginMove(contextMenu.leg, group.id); setContextMenu(null); }}>{group.id === "unallocated" ? "Move to Unallocated" : `Move to ${group.name} thesis`}</button>)}
      </div> : null}
      {showCreate ? <div className="thesis-modal-backdrop" role="presentation" onMouseDown={() => setShowCreate(false)}><section className="thesis-modal thesis-inline-modal" style={{ "--thesis-active-color": "var(--analytics-cuse)" } as CSSProperties} ref={createRef as never} role="dialog" aria-modal="true" aria-label="Create thesis" onMouseDown={(event) => event.stopPropagation()}>
        <div className="thesis-modal-header"><div><span className="thesis-modal-folio">NEW THESIS</span><h3>{creatingFor ? `Create from ${creatingFor.ticker}` : "Create thesis"}</h3></div><button type="button" className="thesis-modal-close" aria-label="Close create thesis" onClick={() => setShowCreate(false)}>×</button></div>
        <input autoFocus value={createName} maxLength={80} onChange={(event) => setCreateName(event.target.value)} onKeyDown={(event) => { if (event.key === "Enter") void handleCreate(); }} placeholder="e.g. Memory Cycle" aria-label="Thesis name" />
        {error ? <div className="thesis-modal-error" role="alert">{error}</div> : null}
        <div className="thesis-modal-actions"><button type="button" className="btn-action" onClick={() => setShowCreate(false)}>Cancel</button><button type="button" className="btn-action primary" disabled={!createName.trim() || busy} onClick={() => void handleCreate()}>{busy ? "Creating…" : "Create"}</button></div>
      </section></div> : null}
      {renameDraft ? <div className="thesis-modal-backdrop" role="presentation" onMouseDown={() => setRenameDraft(null)}><section className="thesis-modal thesis-inline-modal" style={{ "--thesis-active-color": theses.find((thesis) => thesis.id === renameDraft.id)?.color || "var(--analytics-cuse)" } as CSSProperties} ref={renameRef as never} role="dialog" aria-modal="true" aria-label="Rename thesis" onMouseDown={(event) => event.stopPropagation()}>
        <div className="thesis-modal-header"><div><span className="thesis-modal-folio">RENAME THESIS</span><h3>{renameDraft.name}</h3></div><button type="button" className="thesis-modal-close" aria-label="Close rename thesis" onClick={() => setRenameDraft(null)}>×</button></div>
        <input autoFocus value={renameDraft.name} maxLength={80} onChange={(event) => setRenameDraft({ ...renameDraft, name: event.target.value })} onKeyDown={(event) => { if (event.key === "Enter") void handleRename(); }} aria-label="Thesis name" />
        {error ? <div className="thesis-modal-error" role="alert">{error}</div> : null}
        <div className="thesis-modal-actions"><button type="button" className="btn-action" onClick={() => setRenameDraft(null)}>Cancel</button><button type="button" className="btn-action primary" disabled={!renameDraft.name.trim() || busy} onClick={() => void handleRename()}>{busy ? "Saving…" : "Save name"}</button></div>
      </section></div> : null}
      {pending ? <div className="thesis-modal-backdrop" role="presentation" onMouseDown={() => setPending(null)}><section className="thesis-modal thesis-inline-modal" style={{ "--thesis-active-color": groups.find((group) => group.id === pending.destinationId)?.color || "var(--analytics-cuse)" } as CSSProperties} ref={pendingRef as never} role="dialog" aria-modal="true" aria-label={`Move ${pending.leg.ticker}`} onMouseDown={(event) => event.stopPropagation()}>
        <div className="thesis-modal-header"><div><span className="thesis-modal-folio">MOVE POSITION ROW</span><h3>{pending.leg.ticker} · {direction(pending.leg.sourceQuantity)}</h3></div><button type="button" className="thesis-modal-close" aria-label="Close move position row" onClick={() => setPending(null)}>×</button></div>
        <p className="thesis-modal-copy">Move from {groups.find((group) => group.id === pending.leg.sourceId)?.name} to {groups.find((group) => group.id === pending.destinationId)?.name}. The underlying holding will not change.</p>
        <div className="thesis-move-options" role="radiogroup" aria-label="Allocation quantity">
          <button type="button" role="radio" aria-checked={allocationMode === "all"} className={allocationMode === "all" ? "selected" : ""} onClick={() => setAllocationMode("all")}><span><strong>All available</strong><small>Move the entire row from this section</small></span><b>{signedQuantity(pending.leg.sourceQuantity)} shares</b></button>
          <button type="button" role="radio" aria-checked={allocationMode === "partial"} className={allocationMode === "partial" ? "selected" : ""} onClick={() => setAllocationMode("partial")}><span><strong>Partial allocation</strong><small>Split this row between sections</small></span><b>{Number.isFinite(Number(partial)) ? signedQuantity(Number(partial)) : "—"} shares</b></button>
        </div>
        {allocationMode === "partial" ? <label className="thesis-partial-allocation"><span>Signed share quantity</span><input autoFocus type="number" step="any" value={partial} onChange={(event) => setPartial(event.target.value)} /><small>{pending.leg.quantity < 0 ? "Use a negative quantity for this short position." : "Use a positive quantity for this long position."}</small></label> : null}
        {error ? <div className="thesis-modal-error" role="alert">{error}</div> : null}
        <div className="thesis-modal-actions"><button type="button" className="btn-action" onClick={() => setPending(null)}>Cancel</button><button type="button" className="btn-action primary" disabled={busy} onClick={() => void applyMove()}>{busy ? "Saving…" : "Confirm move"}</button></div>
      </section></div> : null}
    </td></tr> : null}
  </>;
}
