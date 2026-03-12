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
        <div style={{ marginTop: 10, color: "rgba(224, 190, 92, 0.92)", fontSize: 12 }}>
          {draftCount} staged edit{draftCount === 1 ? "" : "s"} pending
          {draftDeleteCount > 0 ? ` (${draftDeleteCount} remove${draftDeleteCount === 1 ? "" : "s"})` : ""}.
          Changes stay local until you hit `RECALC`.
        </div>
      )}
      {resultMessage && (
        <div style={{ marginTop: 10, color: "rgba(107, 207, 154, 0.88)", fontSize: 12 }}>
          {resultMessage}
        </div>
      )}
      {errorMessage && (
        <div style={{ marginTop: 10, color: "rgba(224, 87, 127, 0.92)", fontSize: 12 }}>
          {errorMessage}
        </div>
      )}
      {rejectionPreview.length > 0 && (
        <div style={{ marginTop: 10, fontSize: 11, color: "rgba(232, 237, 249, 0.75)" }}>
          Preview rejections:
          <pre style={{ marginTop: 6, whiteSpace: "pre-wrap" }}>
            {JSON.stringify(rejectionPreview, null, 2)}
          </pre>
        </div>
      )}
    </>
  );
}
