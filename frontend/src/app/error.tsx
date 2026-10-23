"use client";

import { useEffect } from "react";
import RequestErrorState from "@/components/shared/RequestErrorState";

export default function AppError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error(error);
  }, [error]);

  return <RequestErrorState surface="this page" error={error} onRetry={reset} />;
}
