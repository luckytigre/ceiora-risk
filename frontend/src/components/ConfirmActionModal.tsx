"use client";

import { useEffect, useId, useMemo, useRef, useState } from "react";
import useFocusTrap, { isTopFocusTrap } from "@/hooks/useFocusTrap";

interface ConfirmActionModalProps {
  open: boolean;
  title: string;
  body: string;
  confirmLabel?: string;
  confirmValue?: string | null;
  dangerText?: string;
  onCancel: () => void;
  onConfirm: () => void | Promise<void>;
}

export default function ConfirmActionModal({
  open,
  title,
  body,
  confirmLabel = "Type to confirm",
  confirmValue = null,
  dangerText = "Confirm",
  onCancel,
  onConfirm,
}: ConfirmActionModalProps) {
  const [typed, setTyped] = useState("");
  const requiresTypedConfirm = useMemo(() => !!confirmValue && confirmValue.trim().length > 0, [confirmValue]);
  const cardRef = useRef<HTMLDivElement>(null);
  const cancelRef = useRef<HTMLButtonElement>(null);
  const titleId = useId();
  const bodyId = useId();

  useEffect(() => {
    if (!open) {
      setTyped("");
    }
  }, [open]);

  // UX-02: this replaces window.confirm() at five call sites. The native dialog
  // traps focus and closes on Escape for free; a custom one has to earn both, or
  // the swap is an accessibility regression rather than an improvement.
  useFocusTrap(cardRef, open);

  useEffect(() => {
    if (!open) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape" && isTopFocusTrap(cardRef.current)) {
        event.stopPropagation();
        onCancel();
      }
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [open, onCancel]);

  // Focus the safe action, not the destructive one.
  useEffect(() => {
    if (!open || requiresTypedConfirm) return;
    cancelRef.current?.focus();
  }, [open, requiresTypedConfirm]);

  if (!open) return null;

  const confirmEnabled = !requiresTypedConfirm || typed.trim().toUpperCase() === String(confirmValue).trim().toUpperCase();

  return (
    <div className="confirm-modal-backdrop" role="presentation" onClick={onCancel}>
      <div
        ref={cardRef}
        className="confirm-modal-card"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={bodyId}
        onClick={(e) => e.stopPropagation()}
      >
        <h3 id={titleId}>{title}</h3>
        <div className="confirm-modal-body" id={bodyId}>{body}</div>
        {requiresTypedConfirm && (
          <div className="confirm-modal-input-group">
            <label>
              {confirmLabel}: <strong>{confirmValue}</strong>
            </label>
            <input
              data-dialog-autofocus
              className="explore-input"
              value={typed}
              onChange={(e) => setTyped(e.target.value)}
              placeholder={String(confirmValue)}
            />
          </div>
        )}
        <div className="confirm-modal-actions">
          <button ref={cancelRef} className="btn-action" onClick={onCancel}>
            Cancel
          </button>
          <button
            className="btn-danger"
            onClick={() => void onConfirm()}
            disabled={!confirmEnabled}
          >
            {dangerText}
          </button>
        </div>
      </div>
    </div>
  );
}
