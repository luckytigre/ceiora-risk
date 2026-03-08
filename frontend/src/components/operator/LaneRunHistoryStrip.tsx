"use client";

import type { OperatorLaneLatestRun } from "@/lib/types";

function parseDurationMinutes(run: OperatorLaneLatestRun): number | null {
  if (!run.started_at || !run.finished_at) return null;
  const start = Date.parse(run.started_at);
  const end = Date.parse(run.finished_at);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return null;
  return (end - start) / 60000;
}

function barTone(status: string): string {
  const clean = String(status || "").toLowerCase();
  if (clean === "ok" || clean === "completed") return "rgba(107, 207, 154, 0.82)";
  if (clean === "running") return "rgba(224, 190, 92, 0.86)";
  if (clean === "missing" || clean === "unknown" || clean === "skipped") return "rgba(154, 171, 214, 0.48)";
  return "rgba(224, 87, 127, 0.86)";
}

export default function LaneRunHistoryStrip({ runs }: { runs: OperatorLaneLatestRun[] }) {
  if (!runs.length) {
    return <div style={{ color: "rgba(169,182,210,0.62)", fontSize: 12 }}>No recent runs yet.</div>;
  }
  const durations = runs.map(parseDurationMinutes).filter((v): v is number => typeof v === "number" && Number.isFinite(v));
  const maxDuration = Math.max(1, ...durations, 1);

  return (
    <div style={{ display: "flex", alignItems: "flex-end", gap: 6, minHeight: 58 }}>
      {runs.map((run, idx) => {
        const dur = parseDurationMinutes(run);
        const height = dur == null ? 12 : Math.max(12, Math.round((dur / maxDuration) * 46));
        const label = run.finished_at || run.updated_at || run.started_at || "n/a";
        return (
          <div
            key={`${run.run_id || "missing"}:${idx}`}
            title={`${run.status.toUpperCase()} | ${label}${dur == null ? "" : ` | ${dur.toFixed(1)} min`}`}
            style={{
              width: 14,
              height,
              borderRadius: 4,
              background: barTone(run.status),
              boxShadow: "0 0 0 1px rgba(255,255,255,0.06) inset",
              opacity: idx === 0 ? 1 : 0.88,
            }}
          />
        );
      })}
    </div>
  );
}
