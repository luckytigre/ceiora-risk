"use client";

import { useEffect, useState } from "react";
import { AUTH_TOKENS_CHANGED_EVENT, hasStoredOperatorToken } from "@/lib/authTokens";

export function useOperatorTokenAvailable(): boolean {
  const [operatorTokenAvailable, setOperatorTokenAvailable] = useState(false);

  useEffect(() => {
    if (typeof window === "undefined") return undefined;

    const syncOperatorToken = () => {
      setOperatorTokenAvailable(hasStoredOperatorToken(window.localStorage));
    };

    syncOperatorToken();
    window.addEventListener(AUTH_TOKENS_CHANGED_EVENT, syncOperatorToken as EventListener);
    window.addEventListener("storage", syncOperatorToken);
    window.addEventListener("focus", syncOperatorToken);
    return () => {
      window.removeEventListener(AUTH_TOKENS_CHANGED_EVENT, syncOperatorToken as EventListener);
      window.removeEventListener("storage", syncOperatorToken);
      window.removeEventListener("focus", syncOperatorToken);
    };
  }, []);

  return operatorTokenAvailable;
}
