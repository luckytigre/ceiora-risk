"use client";

interface InlineShareDraftEditorProps {
  quantityText: string;
  disabled?: boolean;
  draftActive?: boolean;
  invalid?: boolean;
  step?: number;
  titleBase: string;
  onQuantityTextChange: (value: string) => void;
  onStep: (delta: number) => void;
}

export default function InlineShareDraftEditor({
  quantityText,
  disabled = false,
  draftActive = false,
  invalid = false,
  step = 5,
  titleBase,
  onQuantityTextChange,
  onStep,
}: InlineShareDraftEditorProps) {
  return (
    <span className={`share-draft-editor${draftActive ? " draft" : ""}${invalid ? " invalid" : ""}`}>
      <input
        className="share-draft-input"
        inputMode="decimal"
        value={quantityText}
        onChange={(e) => onQuantityTextChange(e.target.value)}
        aria-label={`${titleBase} quantity`}
        disabled={disabled}
      />
      <button
        className="share-adjuster-btn"
        onClick={() => onStep(step)}
        disabled={disabled}
        title={`Increase ${titleBase} by ${step} shares`}
        aria-label={`Increase ${titleBase} by ${step} shares`}
        type="button"
      >
        ↑
      </button>
      <button
        className="share-adjuster-btn"
        onClick={() => onStep(-step)}
        disabled={disabled}
        title={`Decrease ${titleBase} by ${step} shares`}
        aria-label={`Decrease ${titleBase} by ${step} shares`}
        type="button"
      >
        ↓
      </button>
    </span>
  );
}
