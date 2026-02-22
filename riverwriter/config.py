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

# ─── Rate Limiting & Fetch Control ───────────────────────────────────────────
REQUEST_DELAY_SECONDS = 0.25
MAX_HOURS_PER_RUN = 1500
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
HTTP_TIMEOUT_SECONDS = 30
RATE_LIMIT_BACKOFF_SECONDS = 60

# ─── Backfill Target ────────────────────────────────────────────────────────
TARGET_START_YEAR = 2021
TARGET_START_MONTH = 1
TARGET_START_DAY = 1

# ─── Binary Format ───────────────────────────────────────────────────────────
TICK_STRUCT_FORMAT = ">IIIff"
TICK_STRUCT_SIZE = 20  # bytes
