import type { ExposureOrigin, ModelStatus } from "@/lib/types/cuse4";
import type { MethodLabelTone } from "@/components/MethodLabel";

export type ExposureTier = "core" | "fundamental" | "returns";

export function hasExposureMethodMetadata(
  origin?: ExposureOrigin | null,
  modelStatus?: ModelStatus | null,
): boolean {
  return Boolean(String(origin || "").trim()) || Boolean(String(modelStatus || "").trim());
}

export function normalizeExposureOrigin(
  origin?: ExposureOrigin | null,
  modelStatus?: ModelStatus | null,
): ExposureOrigin {
  const raw = String(origin || "").trim();
  if (raw === "projected") {
    return "projected_returns";
  }
  if (modelStatus === "projected_only" && (raw === "" || raw === "native")) {
    return "projected_fundamental";
  }
  if (raw === "projected_fundamental" || raw === "projected_returns" || raw === "native") {
    return raw;
  }
  if (modelStatus === "projected_only") {
    return "projected_fundamental";
  }
  return "native";
}

export function exposureTier(
  origin?: ExposureOrigin | null,
  modelStatus?: ModelStatus | null,
): ExposureTier {
  const normalized = normalizeExposureOrigin(origin, modelStatus);
  if (normalized === "projected_returns") return "returns";
  if (normalized === "projected_fundamental") return "fundamental";
  return "core";
}

export function exposureMethodLabel(
  origin?: ExposureOrigin | null,
  modelStatus?: ModelStatus | null,
): string {
  if (modelStatus === "core_estimated") return "Core";
  if (modelStatus === "ineligible" && !origin) return "Ineligible";
  const tier = exposureTier(origin, modelStatus);
  if (tier === "fundamental") return "Fundamental Projection";
  if (tier === "returns") return "Returns Projection";
  return modelStatus === "ineligible" ? "Ineligible" : "Core";
}

export function exposureMethodDisplayLabel(
  origin?: ExposureOrigin | null,
  modelStatus?: ModelStatus | null,
): string {
  return hasExposureMethodMetadata(origin, modelStatus)
    ? exposureMethodLabel(origin, modelStatus)
    : "\u2014";
}

export function exposureMethodRank(
  origin?: ExposureOrigin | null,
  modelStatus?: ModelStatus | null,
): number {
  if (!hasExposureMethodMetadata(origin, modelStatus)) return 99;
  if (modelStatus === "ineligible" && !origin) return 3;
  const tier = exposureTier(origin, modelStatus);
  if (tier === "core") return 0;
  if (tier === "fundamental") return 1;
  return 2;
}

export function exposureMethodTone(
  origin?: ExposureOrigin | null,
  modelStatus?: ModelStatus | null,
): MethodLabelTone {
  if (!hasExposureMethodMetadata(origin, modelStatus)) return "neutral";
  if (modelStatus === "ineligible") return "error";
  const tier = exposureTier(origin, modelStatus);
  if (tier === "fundamental" || tier === "returns") return "projection";
  return "success";
}
