"""Environment variable loading for Barra Dashboard backend."""

import os
from dotenv import load_dotenv

load_dotenv()


# AWS Postgres (read-only)
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "15432"))
PG_DB = os.getenv("PG_DB", "portfolio_cold_dev")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")

# SQLite cache
SQLITE_PATH = os.getenv("SQLITE_CACHE_PATH", "cache.db")

# Analytics
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "504"))  # ~2 years trading days
ANNUALIZATION_FACTOR = 252
RISK_RECOMPUTE_INTERVAL_DAYS = int(os.getenv("RISK_RECOMPUTE_INTERVAL_DAYS", "7"))
# Minimum calendar age of exposure snapshot used for cross-sectional regressions.
CROSS_SECTION_MIN_AGE_DAYS = int(os.getenv("CROSS_SECTION_MIN_AGE_DAYS", "7"))


def pg_dsn() -> str:
    return f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
