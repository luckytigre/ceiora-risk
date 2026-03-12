"use client";

interface HoldingsMutationFeedbackProps {
  resultMessage: string;
  errorMessage: string;
  rejectionPreview: Array<Record<string, unknown>>;
  draftCount?: number;
  draftDeleteCount?: number;
}

export default function HoldingsMutationFeedback({
  resultMessage,
  errorMessage,
  rejectionPreview,
  draftCount = 0,
  draftDeleteCount = 0,
}: HoldingsMutationFeedbackProps) {
  return (
    <>
      {draftCount > 0 && (
        <div className="feedback-warn">
          {draftCount} staged edit{draftCount === 1 ? "" : "s"} pending
          {draftDeleteCount > 0 ? ` (${draftDeleteCount} remove${draftDeleteCount === 1 ? "" : "s"})` : ""}.
          Changes stay local until you hit `RECALC`.
        </div>
      )}
      {resultMessage && (
        <div className="feedback-success">{resultMessage}</div>
      )}
      {errorMessage && (
        <div className="feedback-error">{errorMessage}</div>
      )}
      {rejectionPreview.length > 0 && (
        <div className="feedback-rejection">
          Preview rejections:
          <pre>{JSON.stringify(rejectionPreview, null, 2)}</pre>
        </div>
      )}
    </>
  );
}
