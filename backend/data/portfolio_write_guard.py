"""Optional thesis-schema guards for existing holdings writers (no schema creation)."""


class PortfolioAllocationConflict(ValueError):
    """Holding mutation would invalidate saved portfolio classifications."""


def lock_if_installed(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regprocedure('public.portfolio_thesis_lock()') IS NOT NULL")
        installed = bool(cur.fetchone()[0])
        if installed:
            cur.execute("SELECT portfolio_thesis_lock()")
        return installed


def prevent_allocated_rekey(conn, *, account_id: str, removed_rics: list[str]) -> None:
    if not removed_rics:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.portfolio_thesis_allocations') IS NOT NULL")
        if not cur.fetchone()[0]:
            return
        cur.execute("SELECT 1 FROM portfolio_thesis_allocations WHERE account_id = %s AND ric = ANY(%s) LIMIT 1", (account_id, removed_rics))
        if cur.fetchone():
            raise PortfolioAllocationConflict("Adjust thesis allocations before consolidating this ticker into a different RIC.")
