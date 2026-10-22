import HelpLabel from "@/components/HelpLabel";

const METHOD_HELP = {
  cuse: "Core uses the native cUSE fit; Fundamental Projection extends coverage from company fundamentals, while Returns Projection estimates exposures from return history. Ineligible or unavailable rows are not modeled.",
  cpar: "Package Fit uses the active persisted cPAR security fit; Limited indicates weaker history, while missing-price, missing-fit, or insufficient-history labels identify excluded positions.",
} as const;

export default function MethodHelpLabel({
  family,
  label = "Method",
}: {
  family: keyof typeof METHOD_HELP;
  label?: string;
}) {
  return <span className="method-help-label"><span>{label}</span><HelpLabel label={label} plain={METHOD_HELP[family]} iconOnly /></span>;
}
