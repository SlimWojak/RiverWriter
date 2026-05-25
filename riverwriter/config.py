"""Configuration constants for RiverWriter."""

from pathlib import Path

# ─── Currency Pairs & Point Values ───────────────────────────────────────────
PAIRS = {
    "EURUSD": {"point_value": 100_000},
    "GBPUSD": {"point_value": 100_000},
    "USDJPY": {"point_value": 1_000},
    "USDCAD": {"point_value": 100_000},
    "AUDUSD": {"point_value": 100_000},
    "USDCHF": {"point_value": 100_000},
}

# ─── Dukascopy Data Feed ─────────────────────────────────────────────────────
BASE_URL = "https://datafeed.dukascopy.com/datafeed"
USER_AGENT = "RiverWriter/1.0 (historical FX research)"

# ─── Paths ───────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PARQUET_DIR = DATA_DIR / "1m"
CATALOG_PATH = DATA_DIR / "catalog.parquet"
LOG_DIR = PROJECT_ROOT / "logs"
VALIDATION_REPORT_PATH = DATA_DIR / "validation_report.json"
GX_VALIDATION_REPORT_PATH = DATA_DIR / "gx_validation_report.json"
DUCKDB_CATALOG_PATH = DATA_DIR / "river.duckdb"

# ─── RIVER substrate (candidate_C) ───────────────────────────────────────────
# Bar schema contract — shared by writer, DuckDB views, and GX suite v1.
BAR_COLUMNS = [
    "timestamp", "open", "high", "low", "close",
    "volume", "source", "knowledge_time", "bar_hash",
]
GX_SUITE_NAME = "bar_suite_v1"
GX_SUITE_VERSION = "1.0.0"

# ─── Rate Limiting & Fetch Control ───────────────────────────────────────────
# Serial mode: ~4 req/s (proven stable on original backfill).
REQUEST_DELAY_SECONDS = 0.25

# Parallel mode: cap aggregate throughput across all workers.
# 6 workers × 1.5s delay ≈ 4 req/s total — same as serial, spread across pairs.
PARALLEL_WORKERS = len(PAIRS)
AGGREGATE_REQUESTS_PER_SECOND = 4.0

MAX_HOURS_PER_RUN = 1500
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
HTTP_TIMEOUT_SECONDS = 30
RATE_LIMIT_BACKOFF_SECONDS = 60


def request_delay_for_workers(num_workers: int, parallel: bool = False) -> float:
    """Per-worker delay that keeps aggregate throughput within bounds."""
    if not parallel or num_workers <= 1:
        return REQUEST_DELAY_SECONDS
    # e.g. 6 workers / 4 req/s → 1.5s per worker
    return num_workers / AGGREGATE_REQUESTS_PER_SECOND

# ─── Backfill Target ────────────────────────────────────────────────────────
TARGET_START_YEAR = 2021
TARGET_START_MONTH = 1
TARGET_START_DAY = 1

# ─── Binary Format ───────────────────────────────────────────────────────────
TICK_STRUCT_FORMAT = ">IIIff"
TICK_STRUCT_SIZE = 20  # bytes
