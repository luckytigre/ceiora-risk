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
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(6, 8, 12, 0.66)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 1000,
        padding: 16,
      }}
      onClick={onCancel}
    >
      <div
        className="chart-card"
        style={{ width: "min(520px, 100%)", margin: 0, borderColor: "rgba(224, 87, 127, 0.28)" }}
        onClick={(e) => e.stopPropagation()}
      >
        <h3 style={{ marginTop: 0, marginBottom: 10 }}>{title}</h3>
        <div style={{ color: "rgba(232,237,249,0.82)", lineHeight: 1.5, marginBottom: 12 }}>{body}</div>
        {requiresTypedConfirm && (
          <div style={{ marginBottom: 14 }}>
            <label style={{ display: "block", fontSize: 12, color: "rgba(169,182,210,0.78)", marginBottom: 6 }}>
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
        <div style={{ display: "flex", justifyContent: "flex-end", gap: 10 }}>
          <button className="btn btn-secondary" onClick={onCancel}>
            Cancel
          </button>
          <button
            className="explore-search-btn"
            onClick={() => void onConfirm()}
            disabled={!confirmEnabled}
            style={{ paddingLeft: 14, paddingRight: 14 }}
          >
            {dangerText}
          </button>
        </div>
      </div>
    </div>
  );
}
