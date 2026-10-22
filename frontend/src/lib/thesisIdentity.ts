/** Aggregate model rows cannot identify a mutable holding. */
export function isConcreteThesisPosition(account: string, ric: string | null | undefined): boolean {
  const normalized = account.trim().toLowerCase();
  return /^[a-z0-9_-]{2,64}$/.test(normalized)
    && !["multi", "all", "all_accounts", "unallocated"].includes(normalized)
    && Boolean(ric?.trim());
}
