"use client";

import dynamic from "next/dynamic";
import { useState } from "react";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ApiErrorState from "@/components/ApiErrorState";
import { useHealthDiagnostics, useOperatorStatus } from "@/hooks/useApi";
import OperatorStatusSection from "@/features/health/OperatorStatusSection";

const HealthDiagnosticsRoot = dynamic(() => import("@/features/health/HealthDiagnosticsRoot"), {
  ssr: false,
  loading: () => <AnalyticsLoadingViz message="Loading health diagnostics..." />,
});

export default function HealthPage() {
  const [loadDiagnostics, setLoadDiagnostics] = useState(false);
  const { data, isLoading, error } = useHealthDiagnostics(loadDiagnostics);
  const {
    data: operatorData,
    isLoading: operatorLoading,
    error: operatorError,
  } = useOperatorStatus();

  if (operatorLoading && !operatorData && !loadDiagnostics) {
    return <AnalyticsLoadingViz message="Loading operator health..." />;
  }
  if (isLoading && !operatorData) {
    return <AnalyticsLoadingViz message="Loading model health..." />;
  }

  const operatorSection = (
    <OperatorStatusSection data={operatorData} error={operatorError} isLoading={operatorLoading} />
  );

  if (!loadDiagnostics) {
    return (
      <div className="health-wrap">
        {operatorSection}
        <div className="chart-card">
          <h3 style={{ margin: "0 0 4px" }}>Model Health Diagnostics</h3>
          <div className="health-load-prompt">
            <div className="section-subtitle">
              This page runs the heaviest diagnostic study in the app. Sections mount lazily as you scroll so routine dashboard use stays fast.
            </div>
            <div className="section-subtitle" style={{ marginBottom: 0 }}>
              Operator Status above is the live control-room truth. Diagnostics below are a deeper local maintenance study and may lag the cloud-serving view.
            </div>
            <button className="health-load-btn" onClick={() => setLoadDiagnostics(true)}>
              Load Diagnostics
            </button>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="health-wrap">
        {operatorSection}
        <ApiErrorState title="Health Diagnostics Not Ready" error={error} />
      </div>
    );
  }

  if (!data || data.status !== "ok") {
    return (
      <div className="health-wrap">
        {operatorSection}
        <div className="chart-card">
          <h3>Health Diagnostics</h3>
          <div className="detail-history-empty">
            {isLoading
              ? "Health diagnostics are still loading."
              : "No diagnostics payload is available yet. Run refresh and reload this page."}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="health-wrap">
      {operatorSection}
      <HealthDiagnosticsRoot data={data} />
    </div>
  );
}
