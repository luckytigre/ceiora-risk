"""Environment variable loading for Barra Dashboard backend."""

import os
from pathlib import Path
from dotenv import dotenv_values

BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent

# Resolve env vars with precedence:
# shell/process env > backend/.env > project .env
root_env = dotenv_values(PROJECT_DIR / ".env")
backend_env = dotenv_values(BASE_DIR / ".env")
merged_env = {**root_env, **backend_env}
for key, value in merged_env.items():
    if key and value is not None:
        os.environ.setdefault(str(key), str(value))


# AWS Postgres (read-only)
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "15432"))
PG_DB = os.getenv("PG_DB", "portfolio_cold_dev")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")

# Storage paths
APP_DATA_DIR = Path(os.getenv("APP_DATA_DIR", str(BASE_DIR / "runtime"))).expanduser()
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
SQLITE_TIMEOUT_SECONDS = max(1.0, float(os.getenv("SQLITE_TIMEOUT_SECONDS", "30")))
SQLITE_BUSY_TIMEOUT_MS = max(1000, int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "5000")))
SQLITE_CACHE_RETRY_ATTEMPTS = max(1, int(os.getenv("SQLITE_CACHE_RETRY_ATTEMPTS", "4")))
SQLITE_CACHE_RETRY_DELAY_MS = max(10, int(os.getenv("SQLITE_CACHE_RETRY_DELAY_MS", "50")))
SQLITE_CACHE_SNAPSHOT_RETENTION = max(1, int(os.getenv("SQLITE_CACHE_SNAPSHOT_RETENTION", "3")))

# Data backend routing.
# Allowed values: "sqlite", "neon"
NEON_DATABASE_URL = str(os.getenv("NEON_DATABASE_URL", "")).strip()
_DEFAULT_DATA_BACKEND = "neon" if NEON_DATABASE_URL or os.getenv("DATABASE_URL", "").strip() else "sqlite"
DATA_BACKEND = str(os.getenv("DATA_BACKEND", _DEFAULT_DATA_BACKEND)).strip().lower()

# Analytics
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "504"))  # ~2 years trading days
ANNUALIZATION_FACTOR = 252
RISK_RECOMPUTE_INTERVAL_DAYS = int(os.getenv("RISK_RECOMPUTE_INTERVAL_DAYS", "7"))
# Minimum calendar age of exposure snapshot used for cross-sectional regressions.
CROSS_SECTION_MIN_AGE_DAYS = int(os.getenv("CROSS_SECTION_MIN_AGE_DAYS", "7"))
# Snapshot materialization policy for universe_cross_section_snapshot.
# "current": latest row per ticker (default); "full": historical rows by as_of_date.
CROSS_SECTION_SNAPSHOT_MODE = str(os.getenv("CROSS_SECTION_SNAPSHOT_MODE", "current")).strip().lower()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_csv(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if raw is None:
        return list(default)
    parts = [p.strip() for p in str(raw).split(",")]
    return [p for p in parts if p]


# Neon mirror + cutover controls.
NEON_AUTO_SYNC_ENABLED = _env_bool("NEON_AUTO_SYNC_ENABLED", bool(NEON_DATABASE_URL))
NEON_AUTO_SYNC_REQUIRED = _env_bool("NEON_AUTO_SYNC_REQUIRED", False)
NEON_AUTO_PARITY_ENABLED = _env_bool("NEON_AUTO_PARITY_ENABLED", bool(NEON_DATABASE_URL))
NEON_AUTO_PRUNE_ENABLED = _env_bool("NEON_AUTO_PRUNE_ENABLED", bool(NEON_DATABASE_URL))
NEON_AUTO_SYNC_MODE = str(os.getenv("NEON_AUTO_SYNC_MODE", "incremental")).strip().lower()
if NEON_AUTO_SYNC_MODE not in {"incremental", "full"}:
    NEON_AUTO_SYNC_MODE = "incremental"
NEON_AUTO_SYNC_TABLES = _env_csv("NEON_AUTO_SYNC_TABLES", [])
NEON_SOURCE_RETENTION_YEARS = max(1, int(os.getenv("NEON_SOURCE_RETENTION_YEARS", "10")))
NEON_ANALYTICS_RETENTION_YEARS = max(1, int(os.getenv("NEON_ANALYTICS_RETENTION_YEARS", "5")))
NEON_READ_SURFACES = {
    s.strip().lower()
    for s in _env_csv(
        "NEON_READ_SURFACES",
        (["core_reads", "factor_history", "price_history"] if NEON_DATABASE_URL else []),
    )
}


# cUSE4 foundation toggles (non-breaking additive path).
CUSE4_ENABLE_ESTU_AUDIT = _env_bool("CUSE4_ENABLE_ESTU_AUDIT", True)
CUSE4_AUTO_BOOTSTRAP = _env_bool("CUSE4_AUTO_BOOTSTRAP", False)

# Orchestrator ingest stage controls.
ORCHESTRATOR_ENABLE_INGEST = _env_bool("ORCHESTRATOR_ENABLE_INGEST", False)
ORCHESTRATOR_INGEST_SHARD_COUNT = max(1, int(os.getenv("ORCHESTRATOR_INGEST_SHARD_COUNT", "1")))

# CORS
CORS_ALLOW_ORIGINS = _env_csv(
    "CORS_ALLOW_ORIGINS",
    ["http://localhost:3000", "http://localhost:3001", "http://localhost:3002"],
)

# Optional protection for refresh endpoints.
REFRESH_API_TOKEN = str(os.getenv("REFRESH_API_TOKEN", "")).strip()


def pg_dsn() -> str:
    return f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"


def neon_dsn() -> str:
    return NEON_DATABASE_URL


def neon_surface_enabled(surface: str) -> bool:
    clean = str(surface or "").strip().lower()
    if not clean:
        return False
    if DATA_BACKEND == "neon":
        return True
    if "*" in NEON_READ_SURFACES:
        return True
    return clean in NEON_READ_SURFACES
