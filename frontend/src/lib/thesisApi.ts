import { thesisQuantityWire } from "@/lib/thesisQuantity";
import { apiFetch } from "@/lib/apiTransport";

export interface ThesisDto { id: string; name: string; color: string; created_at: string; }
export interface ThesisAllocationDto { thesis_id: string; account_id: string; ric: string; ticker: string; quantity: number | string; }
export interface ThesisStateDto { version: 1; workspace_id: string; revision: number; theses: ThesisDto[]; allocations: ThesisAllocationDto[]; manual_orders?: Record<string, string[]>; }
const JSON_HEADERS = { "content-type": "application/json" };

export function getThesisState(): Promise<ThesisStateDto> { return apiFetch("/api/portfolio/theses", { cache: "no-store" }); }
export function createThesisRequest(name: string, expectedRevision: number): Promise<ThesisStateDto> {
  return apiFetch("/api/portfolio/theses", { method: "POST", headers: JSON_HEADERS, body: JSON.stringify({ name, expected_revision: expectedRevision }) });
}
export function renameThesisRequest(id: string, name: string, expectedRevision: number): Promise<ThesisStateDto> {
  return apiFetch(`/api/portfolio/theses/${encodeURIComponent(id)}`, { method: "PATCH", headers: JSON_HEADERS, body: JSON.stringify({ name, expected_revision: expectedRevision }) });
}
export function deleteThesisRequest(id: string, expectedRevision: number): Promise<ThesisStateDto> {
  return apiFetch(`/api/portfolio/theses/${encodeURIComponent(id)}`, { method: "DELETE", headers: JSON_HEADERS, body: JSON.stringify({ expected_revision: expectedRevision }) });
}
export function saveManualOrderRequest(scope: string, keys: string[], expectedRevision: number): Promise<ThesisStateDto> {
  return apiFetch("/api/portfolio/manual-order", {
    method: "PUT", headers: JSON_HEADERS,
    body: JSON.stringify({ scope, keys, expected_revision: expectedRevision }),
  });
}
export function replacePositionAllocationsRequest(input: { accountId: string; ric: string; expectedRevision: number; allocations: Array<{ thesisId: string; quantity: number }> }): Promise<ThesisStateDto> {
  return apiFetch("/api/portfolio/thesis-allocations/position", {
    method: "PUT", headers: JSON_HEADERS,
    body: JSON.stringify({ account_id: input.accountId, ric: input.ric, expected_revision: input.expectedRevision, allocations: input.allocations.map((row) => ({ thesis_id: row.thesisId, quantity: thesisQuantityWire(row.quantity) })) }),
  });
}
