export const OPERATOR_TOKEN_STORAGE_KEY = "ceiora.operator-token";
export const EDITOR_TOKEN_STORAGE_KEY = "ceiora.editor-token";
export const AUTH_TOKENS_CHANGED_EVENT = "ceiora:auth-tokens-changed";

export interface StoredAuthTokens {
  operatorToken: string;
  editorToken: string;
}

function cleanToken(value: string | null | undefined): string {
  return String(value || "").trim();
}

function emitAuthTokensChanged(): void {
  if (typeof window === "undefined") return;
  window.dispatchEvent(new CustomEvent(AUTH_TOKENS_CHANGED_EVENT));
}

export function readStoredAuthTokens(storage?: Pick<Storage, "getItem"> | null): StoredAuthTokens {
  const source = storage ?? (typeof window !== "undefined" ? window.localStorage : null);
  if (!source) {
    return { operatorToken: "", editorToken: "" };
  }
  return {
    operatorToken: cleanToken(source.getItem(OPERATOR_TOKEN_STORAGE_KEY)),
    editorToken: cleanToken(source.getItem(EDITOR_TOKEN_STORAGE_KEY)),
  };
}

export function hasStoredOperatorToken(storage?: Pick<Storage, "getItem"> | null): boolean {
  return Boolean(readStoredAuthTokens(storage).operatorToken);
}

export function writeStoredAuthToken(key: typeof OPERATOR_TOKEN_STORAGE_KEY | typeof EDITOR_TOKEN_STORAGE_KEY, value: string): void {
  if (typeof window === "undefined") return;
  const clean = cleanToken(value);
  if (!clean) {
    window.localStorage.removeItem(key);
    emitAuthTokensChanged();
    return;
  }
  window.localStorage.setItem(key, clean);
  emitAuthTokensChanged();
}

export function clearStoredAuthTokens(): void {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem(OPERATOR_TOKEN_STORAGE_KEY);
  window.localStorage.removeItem(EDITOR_TOKEN_STORAGE_KEY);
  emitAuthTokensChanged();
}
