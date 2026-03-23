"use client";

export type MethodLabelTone = "success" | "projection" | "warning" | "error" | "neutral";

export default function MethodLabel({
  label,
  tone = "neutral",
}: {
  label: string;
  tone?: MethodLabelTone;
}) {
  return <span className={`method-label ${tone}`.trim()}>{label}</span>;
}
