"""Downloads .bi5 tick files from Dukascopy's historical data feed."""

from __future__ import annotations

import logging
import time
from pathlib import Path

import requests

from . import config

logger = logging.getLogger(__name__)


def build_url(pair: str, year: int, month: int, day: int, hour: int) -> str:
    """Build Dukascopy download URL.

    Note: Dukascopy uses 0-indexed months (Jan=00, Dec=11).
    The `month` parameter here is 1-indexed (Jan=1, Dec=12).
    """
    month_0 = month - 1  # Convert to 0-indexed for URL
    return f"{config.BASE_URL}/{pair}/{year}/{month_0:02d}/{day:02d}/{hour:02d}h_ticks.bi5"


def raw_path(pair: str, year: int, month: int, day: int, hour: int) -> Path:
    """Return the local storage path for a raw .bi5 file."""
    return config.RAW_DIR / pair / str(year) / f"{month:02d}" / f"{day:02d}" / f"{hour:02d}h_ticks.bi5"


def fetch_hour(
    session: requests.Session,
    pair: str,
    year: int,
    month: int,
    day: int,
    hour: int,
) -> tuple[str, bytes | None]:
    """Fetch a single hourly .bi5 file from Dukascopy.

    Returns:
        (status, data) where status is one of:
        - "fetched": successful download with data
        - "no_data": 404 or empty response (weekends/holidays)
        - "error": failed after retries
    """
    url = build_url(pair, year, month, day, hour)
    label = f"{pair} {year}-{month:02d}-{day:02d} {hour:02d}h"

    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=config.HTTP_TIMEOUT_SECONDS)

            if resp.status_code == 404:
                logger.debug("%s | no_data (404)", label)
                return "no_data", None

            if resp.status_code == 429:
                wait = config.RATE_LIMIT_BACKOFF_SECONDS
                logger.warning("%s | rate limited (429), backing off %ds", label, wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()

            data = resp.content
            if len(data) == 0:
                logger.debug("%s | no_data (empty)", label)
                return "no_data", None

            # Save raw file
            dest = raw_path(pair, year, month, day, hour)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(data)

            logger.debug("%s | fetched %d bytes", label, len(data))
            return "fetched", data

        except requests.RequestException as exc:
            backoff = config.RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "%s | attempt %d/%d failed: %s — retrying in %ds",
                label, attempt, config.MAX_RETRIES, exc, backoff,
            )
            if attempt < config.MAX_RETRIES:
                time.sleep(backoff)

    logger.error("%s | failed after %d attempts", label, config.MAX_RETRIES)
    return "error", None


def create_session() -> requests.Session:
    """Create a requests session with connection pooling and proper headers."""
    session = requests.Session()
    session.headers.update({"User-Agent": config.USER_AGENT})
    return session
