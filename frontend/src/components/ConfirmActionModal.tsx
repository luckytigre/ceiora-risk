"use client";

import { useEffect, useMemo, useState } from "react";

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

  useEffect(() => {
    if (!open) {
      setTyped("");
    }
  }, [open]);

  if (!open) return null;

  const confirmEnabled = !requiresTypedConfirm || typed.trim().toUpperCase() === String(confirmValue).trim().toUpperCase();

  return (
    <div className="confirm-modal-backdrop" onClick={onCancel}>
      <div className="confirm-modal-card" onClick={(e) => e.stopPropagation()}>
        <h3>{title}</h3>
        <div className="confirm-modal-body">{body}</div>
        {requiresTypedConfirm && (
          <div className="confirm-modal-input-group">
            <label>
              {confirmLabel}: <strong>{confirmValue}</strong>
            </label>
            <input
              autoFocus
              className="explore-input"
              value={typed}
              onChange={(e) => setTyped(e.target.value)}
              placeholder={String(confirmValue)}
            />
          </div>
        )}
        <div className="confirm-modal-actions">
          <button className="btn-action" onClick={onCancel}>
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
