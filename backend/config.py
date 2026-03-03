"""Environment variable loading for Barra Dashboard backend."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


# AWS Postgres (read-only)
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "15432"))
PG_DB = os.getenv("PG_DB", "portfolio_cold_dev")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")

# Storage paths
BASE_DIR = Path(__file__).resolve().parent
APP_DATA_DIR = Path(os.getenv("APP_DATA_DIR", str(BASE_DIR))).expanduser()
if not APP_DATA_DIR.is_absolute():
    APP_DATA_DIR = (BASE_DIR / APP_DATA_DIR).resolve()
APP_DATA_DIR.mkdir(parents=True, exist_ok=True)


def _resolve_data_path(env_name: str, default_filename: str) -> str:
    raw = os.getenv(env_name, "").strip()
    if raw:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = (APP_DATA_DIR / p).resolve()
        return str(p)
    return str((APP_DATA_DIR / default_filename).resolve())


# SQLite/cache + analytics source DB
SQLITE_PATH = _resolve_data_path("SQLITE_CACHE_PATH", "cache.db")
DATA_DB_PATH = _resolve_data_path("DATA_DB_PATH", "data.db")

# Analytics
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "504"))  # ~2 years trading days
ANNUALIZATION_FACTOR = 252
RISK_RECOMPUTE_INTERVAL_DAYS = int(os.getenv("RISK_RECOMPUTE_INTERVAL_DAYS", "7"))
# Minimum calendar age of exposure snapshot used for cross-sectional regressions.
CROSS_SECTION_MIN_AGE_DAYS = int(os.getenv("CROSS_SECTION_MIN_AGE_DAYS", "7"))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


# cUSE4 foundation toggles (non-breaking additive path).
CUSE4_ENABLE_ESTU_AUDIT = _env_bool("CUSE4_ENABLE_ESTU_AUDIT", True)
CUSE4_AUTO_BOOTSTRAP = _env_bool("CUSE4_AUTO_BOOTSTRAP", True)


def pg_dsn() -> str:
    return f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
