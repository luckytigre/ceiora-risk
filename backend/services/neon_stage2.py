"""Stage-2 Neon migration helpers (schema apply, sync, parity audit)."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from psycopg import sql

from backend.data.neon import connect, resolve_dsn


@dataclass(frozen=True)
class TableConfig:
    name: str
    pk_cols: tuple[str, ...]
    date_col: str | None = None
    overlap_days: int = 7
    sync_mode: str = "replace"  # replace | upsert


TABLE_CONFIGS: dict[str, TableConfig] = {
    "security_master": TableConfig(
        name="security_master",
        pk_cols=("ric",),
        sync_mode="upsert",
    ),
    "security_prices_eod": TableConfig(
        name="security_prices_eod",
        pk_cols=("ric", "date"),
        date_col="date",
        overlap_days=10,
    ),
    "security_fundamentals_pit": TableConfig(
        name="security_fundamentals_pit",
        pk_cols=("ric", "as_of_date", "stat_date"),
        date_col="as_of_date",
        overlap_days=62,
    ),
    "security_classification_pit": TableConfig(
        name="security_classification_pit",
        pk_cols=("ric", "as_of_date"),
        date_col="as_of_date",
        overlap_days=62,
    ),
    "barra_raw_cross_section_history": TableConfig(
        name="barra_raw_cross_section_history",
        pk_cols=("ric", "as_of_date"),
        date_col="as_of_date",
        overlap_days=14,
    ),
    "serving_payload_current": TableConfig(
        name="serving_payload_current",
        pk_cols=("payload_name",),
    ),
}


def canonical_tables() -> list[str]:
    return list(TABLE_CONFIGS.keys())


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(r[1]) for r in rows]


def _pg_columns(pg_conn, table: str) -> list[str]:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        return [str(r[0]) for r in cur.fetchall()]


def _table_exists_pg(pg_conn, table: str) -> bool:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
            LIMIT 1
            """,
            (table,),
        )
        return cur.fetchone() is not None


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    try:
        return date.fromisoformat(txt[:10])
    except ValueError:
        return None


def _format_iso_date(value: date | None, fallback: str | None = None) -> str | None:
    if value is None:
        return fallback
    return value.isoformat()


def _sqlite_count(conn: sqlite3.Connection, table: str, where_sql: str = "", params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {table} {where_sql}", params).fetchone()
    return int(row[0] or 0) if row else 0


def _sqlite_select_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[str],
    where_sql: str = "",
    params: tuple[Any, ...] = (),
    batch_size: int = 25_000,
):
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    cur = conn.execute(f"SELECT {cols_sql} FROM {table} {where_sql}", params)
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield tuple(row)


def _pg_max_date(pg_conn, *, table: str, date_col: str) -> str | None:
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT MAX({})::text FROM {}")
            .format(sql.Identifier(date_col), sql.Identifier(table))
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return str(row[0])


def _copy_into_postgres(
    pg_conn,
    *,
    table: str,
    columns: list[str],
    rows,
) -> int:
    copied = 0
    with pg_conn.cursor() as cur:
        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN")
        copy_sql = copy_sql.format(
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        )
        with cur.copy(copy_sql) as cp:
            for row in rows:
                cp.write_row(row)
                copied += 1
    return copied


def _upsert_security_master(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    *,
    table: str,
    columns: list[str],
    batch_size: int,
) -> int:
    if "ric" not in columns:
        raise ValueError("security_master upsert requires ric column")

    non_key = [c for c in columns if c != "ric"]
    insert_sql = sql.SQL(
        """
        INSERT INTO {} ({})
        VALUES ({})
        ON CONFLICT (ric) DO UPDATE SET {}
        """
    ).format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in non_key
        ),
    )

    src_rows = _sqlite_select_rows(
        sqlite_conn,
        table=table,
        columns=columns,
        batch_size=batch_size,
    )

    col_idx = {c: i for i, c in enumerate(columns)}

    def _to_flag(value: Any) -> int:
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, int):
            return 1 if value != 0 else 0
        if isinstance(value, float):
            return 1 if value != 0.0 else 0
        txt = str(value or "").strip().lower()
        if txt in {"1", "true", "yes", "y"}:
            return 1
        if txt in {"0", "false", "no", "n", ""}:
            return 0
        try:
            return 1 if float(txt) != 0.0 else 0
        except ValueError:
            return 0

    def _normalize_row(row: tuple[Any, ...]) -> tuple[Any, ...]:
        out = list(row)
        if "classification_ok" in col_idx:
            out[col_idx["classification_ok"]] = _to_flag(
                out[col_idx["classification_ok"]]
            )
        if "is_equity_eligible" in col_idx:
            out[col_idx["is_equity_eligible"]] = _to_flag(
                out[col_idx["is_equity_eligible"]]
            )
        if "updated_at" in col_idx:
            raw = out[col_idx["updated_at"]]
            txt = str(raw or "").strip()
            if txt:
                try:
                    datetime.fromisoformat(txt.replace("Z", "+00:00"))
                except ValueError:
                    txt = ""
            if not txt:
                txt = datetime.now(timezone.utc).isoformat()
            out[col_idx["updated_at"]] = txt
        return tuple(out)

    loaded = 0
    chunk: list[tuple[Any, ...]] = []
    with pg_conn.cursor() as cur:
        for row in src_rows:
            chunk.append(_normalize_row(row))
            if len(chunk) >= batch_size:
                cur.executemany(insert_sql, chunk)
                loaded += len(chunk)
                chunk = []
        if chunk:
            cur.executemany(insert_sql, chunk)
            loaded += len(chunk)
    return loaded


def sync_from_sqlite_to_neon(
    *,
    sqlite_path: Path,
    dsn: str | None = None,
    tables: list[str] | None = None,
    mode: str = "incremental",
    batch_size: int = 25_000,
) -> dict[str, Any]:
    db = Path(sqlite_path).expanduser().resolve()
    if not db.exists():
        raise FileNotFoundError(f"sqlite db not found: {db}")

    selected = tables or canonical_tables()
    unknown = [t for t in selected if t not in TABLE_CONFIGS]
    if unknown:
        raise ValueError(f"unknown table(s): {', '.join(sorted(unknown))}")

    selected_cfgs = [TABLE_CONFIGS[t] for t in selected]
    mode_norm = str(mode).strip().lower()
    if mode_norm not in {"full", "incremental"}:
        raise ValueError("mode must be one of: full, incremental")

    out: dict[str, Any] = {
        "status": "ok",
        "mode": mode_norm,
        "sqlite_path": str(db),
        "tables": {},
    }

    sqlite_conn = sqlite3.connect(str(db))
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=False)

    try:
        for cfg in selected_cfgs:
            table = cfg.name
            if not _table_exists_pg(pg_conn, table):
                raise RuntimeError(f"target table missing in Neon: {table}")

            src_cols = _sqlite_columns(sqlite_conn, table)
            tgt_cols = _pg_columns(pg_conn, table)
            if not src_cols:
                raise RuntimeError(f"source table missing in SQLite: {table}")
            if not tgt_cols:
                raise RuntimeError(f"target table has no columns in Neon: {table}")

            cols = [c for c in src_cols if c in set(tgt_cols)]
            missing_in_target = [c for c in src_cols if c not in set(tgt_cols)]
            if missing_in_target:
                raise RuntimeError(
                    f"target table {table} missing source columns: {', '.join(missing_in_target)}"
                )

            where_sql = ""
            params: tuple[Any, ...] = ()
            action = "replace"

            if cfg.sync_mode == "upsert":
                src_count = _sqlite_count(sqlite_conn, table)
                copied = _upsert_security_master(
                    sqlite_conn,
                    pg_conn,
                    table=table,
                    columns=cols,
                    batch_size=max(500, int(batch_size)),
                )
                pg_conn.commit()
                out["tables"][table] = {
                    "action": "upsert",
                    "source_rows": int(src_count),
                    "rows_loaded": int(copied),
                }
                continue

            if mode_norm == "full" or not cfg.date_col:
                with pg_conn.cursor() as cur:
                    cur.execute(sql.SQL("TRUNCATE TABLE {} ").format(sql.Identifier(table)))
                action = "truncate_and_reload"
            else:
                max_date = _pg_max_date(pg_conn, table=table, date_col=str(cfg.date_col))
                max_dt = _parse_iso_date(max_date)
                if max_dt is None:
                    with pg_conn.cursor() as cur:
                        cur.execute(sql.SQL("TRUNCATE TABLE {} ").format(sql.Identifier(table)))
                    action = "target_empty_truncate_and_reload"
                else:
                    cutoff = max_dt - timedelta(days=max(0, int(cfg.overlap_days)))
                    cutoff_txt = _format_iso_date(cutoff, fallback=max_date)
                    if cutoff_txt is None:
                        raise RuntimeError(f"unable to derive cutoff for table {table}")
                    where_sql = f"WHERE {cfg.date_col} >= ?"
                    params = (cutoff_txt,)
                    with pg_conn.cursor() as cur:
                        cur.execute(
                            sql.SQL("DELETE FROM {} WHERE {} >= %s").format(
                                sql.Identifier(table),
                                sql.Identifier(str(cfg.date_col)),
                            ),
                            (cutoff_txt,),
                        )
                    action = "incremental_overlap_reload"

            src_count = _sqlite_count(sqlite_conn, table, where_sql, params)
            copied = _copy_into_postgres(
                pg_conn,
                table=table,
                columns=cols,
                rows=_sqlite_select_rows(
                    sqlite_conn,
                    table=table,
                    columns=cols,
                    where_sql=where_sql,
                    params=params,
                    batch_size=max(1_000, int(batch_size)),
                ),
            )
            pg_conn.commit()

            out["tables"][table] = {
                "action": action,
                "source_rows": int(src_count),
                "rows_loaded": int(copied),
                "where_sql": where_sql or None,
                "where_params": list(params),
            }
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()

    return out


def _profile_sqlite_table(conn: sqlite3.Connection, cfg: TableConfig) -> dict[str, Any]:
    table = cfg.name
    out: dict[str, Any] = {
        "row_count": _sqlite_count(conn, table),
    }
    if cfg.date_col:
        row = conn.execute(
            f"SELECT MIN({cfg.date_col}), MAX({cfg.date_col}) FROM {table}"
        ).fetchone()
        out["min_date"] = (str(row[0]) if row and row[0] is not None else None)
        out["max_date"] = (str(row[1]) if row and row[1] is not None else None)
        max_date = out["max_date"]
        if max_date:
            if "ric" in _sqlite_columns(conn, table):
                drow = conn.execute(
                    f"SELECT COUNT(DISTINCT ric) FROM {table} WHERE {cfg.date_col} = ?",
                    (max_date,),
                ).fetchone()
                out["latest_distinct_ric"] = int(drow[0] or 0) if drow else 0
    return out


def _profile_pg_table(pg_conn, cfg: TableConfig) -> dict[str, Any]:
    table = cfg.name
    out: dict[str, Any] = {}
    with pg_conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
        out["row_count"] = int(cur.fetchone()[0] or 0)
        if cfg.date_col:
            cur.execute(
                sql.SQL("SELECT MIN({})::text, MAX({})::text FROM {}")
                .format(
                    sql.Identifier(cfg.date_col),
                    sql.Identifier(cfg.date_col),
                    sql.Identifier(table),
                )
            )
            row = cur.fetchone()
            out["min_date"] = (str(row[0]) if row and row[0] is not None else None)
            out["max_date"] = (str(row[1]) if row and row[1] is not None else None)
            if out["max_date"]:
                cur.execute(
                    sql.SQL("SELECT COUNT(DISTINCT ric) FROM {} WHERE {} = %s")
                    .format(sql.Identifier(table), sql.Identifier(cfg.date_col)),
                    (out["max_date"],),
                )
                out["latest_distinct_ric"] = int(cur.fetchone()[0] or 0)
    return out


def _duplicate_group_count_sqlite(conn: sqlite3.Connection, cfg: TableConfig) -> int:
    group_cols = ", ".join(cfg.pk_cols)
    row = conn.execute(
        f"SELECT COUNT(*) FROM (SELECT {group_cols}, COUNT(*) c FROM {cfg.name} GROUP BY {group_cols} HAVING c > 1)"
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _duplicate_group_count_pg(pg_conn, cfg: TableConfig) -> int:
    group_cols = sql.SQL(", ").join(sql.Identifier(c) for c in cfg.pk_cols)
    query = sql.SQL(
        "SELECT COUNT(*) FROM (SELECT {}, COUNT(*) c FROM {} GROUP BY {} HAVING COUNT(*) > 1) q"
    ).format(group_cols, sql.Identifier(cfg.name), group_cols)
    with pg_conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _orphan_ric_sqlite(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {table} x
        LEFT JOIN security_master sm
          ON sm.ric = x.ric
        WHERE sm.ric IS NULL
        """
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _orphan_ric_pg(pg_conn, table: str) -> int:
    with pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT COUNT(*)
                FROM {} x
                LEFT JOIN security_master sm
                  ON sm.ric = x.ric
                WHERE sm.ric IS NULL
                """
            ).format(sql.Identifier(table))
        )
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def run_parity_audit(
    *,
    sqlite_path: Path,
    dsn: str | None = None,
    tables: list[str] | None = None,
) -> dict[str, Any]:
    db = Path(sqlite_path).expanduser().resolve()
    if not db.exists():
        raise FileNotFoundError(f"sqlite db not found: {db}")

    selected = tables or canonical_tables()
    unknown = [t for t in selected if t not in TABLE_CONFIGS]
    if unknown:
        raise ValueError(f"unknown table(s): {', '.join(sorted(unknown))}")

    selected_cfgs = [TABLE_CONFIGS[t] for t in selected]
    out: dict[str, Any] = {
        "status": "ok",
        "sqlite_path": str(db),
        "tables": {},
        "issues": [],
    }

    sqlite_conn = sqlite3.connect(str(db))
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = connect(dsn=resolve_dsn(dsn), autocommit=False)

    try:
        for cfg in selected_cfgs:
            if not _table_exists_pg(pg_conn, cfg.name):
                out["issues"].append(f"missing_target_table:{cfg.name}")
                out["tables"][cfg.name] = {
                    "source": _profile_sqlite_table(sqlite_conn, cfg),
                    "target": None,
                    "duplicate_groups": {
                        "source": _duplicate_group_count_sqlite(sqlite_conn, cfg),
                        "target": None,
                    },
                }
                continue

            src = _profile_sqlite_table(sqlite_conn, cfg)
            tgt = _profile_pg_table(pg_conn, cfg)
            dup_src = _duplicate_group_count_sqlite(sqlite_conn, cfg)
            dup_tgt = _duplicate_group_count_pg(pg_conn, cfg)

            table_out: dict[str, Any] = {
                "source": src,
                "target": tgt,
                "duplicate_groups": {
                    "source": int(dup_src),
                    "target": int(dup_tgt),
                },
            }

            if cfg.name != "security_master":
                orphan_src = _orphan_ric_sqlite(sqlite_conn, cfg.name)
                orphan_tgt = _orphan_ric_pg(pg_conn, cfg.name)
                table_out["orphan_ric_rows"] = {
                    "source": int(orphan_src),
                    "target": int(orphan_tgt),
                }
                if orphan_src != orphan_tgt:
                    out["issues"].append(f"orphan_mismatch:{cfg.name}:{orphan_src}!={orphan_tgt}")

            if int(src.get("row_count") or 0) != int(tgt.get("row_count") or 0):
                out["issues"].append(
                    f"row_count_mismatch:{cfg.name}:{src.get('row_count')}!={tgt.get('row_count')}"
                )
            if cfg.date_col:
                for key in ("min_date", "max_date", "latest_distinct_ric"):
                    if src.get(key) != tgt.get(key):
                        out["issues"].append(
                            f"{key}_mismatch:{cfg.name}:{src.get(key)}!={tgt.get(key)}"
                        )
            if dup_src != dup_tgt:
                out["issues"].append(f"duplicate_group_mismatch:{cfg.name}:{dup_src}!={dup_tgt}")

            out["tables"][cfg.name] = table_out

        out["status"] = "ok" if not out["issues"] else "mismatch"
        return out
    finally:
        sqlite_conn.close()
        pg_conn.close()


def apply_sql_file(pg_conn, *, sql_path: Path) -> dict[str, Any]:
    path = Path(sql_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    script = path.read_text(encoding="utf-8")
    with pg_conn.cursor() as cur:
        cur.execute(script)
    pg_conn.commit()
    return {
        "status": "ok",
        "sql_path": str(path),
        "bytes": len(script.encode("utf-8")),
    }
