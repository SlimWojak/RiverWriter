"""Rebuild fetch catalog from on-disk Parquet (and optional raw .bi5) files."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from . import config
from .catalog import Catalog, _is_weekend_closed

logger = logging.getLogger(__name__)


def rebuild_from_parquet(pairs: list[str] | None = None) -> dict:
    """Scan Parquet files and mark hours with bars as 'fetched' in the catalog.

    Returns a summary dict per pair.
    """
    pairs = pairs or list(config.PAIRS.keys())
    summary: dict[str, dict] = {}

    for pair in pairs:
        pair_dir = config.PARQUET_DIR / pair
        if not pair_dir.exists():
            summary[pair] = {"hours_marked": 0, "files": 0}
            continue

        files = sorted(pair_dir.glob(f"{pair}_*.parquet"))
        if not files:
            summary[pair] = {"hours_marked": 0, "files": 0}
            continue

        hours = _hours_from_parquet_files(files)
        raw_hours = _hours_from_raw_files(pair)
        hours |= raw_hours

        entries = _hours_to_catalog_df(pair, hours)
        Catalog.persist_pair(pair, entries)

        oldest = newest = None
        if hours:
            sorted_hours = sorted(hours)
            y, m, d, h = sorted_hours[0]
            oldest = f"{y}-{m:02d}-{d:02d}"
            y, m, d, h = sorted_hours[-1]
            newest = f"{y}-{m:02d}-{d:02d}"

        summary[pair] = {
            "hours_marked": len(hours),
            "from_parquet": len(hours - raw_hours),
            "from_raw": len(raw_hours),
            "files": len(files),
            "oldest": oldest,
            "newest": newest,
        }
        logger.info(
            "%s: marked %d hours (%d parquet files, %d raw)",
            pair, len(hours), len(files), len(raw_hours),
        )

    total = sum(s["hours_marked"] for s in summary.values())
    logger.info("Catalog rebuild complete: %d total hours marked", total)
    return summary


def _hours_from_parquet_files(files: list[Path]) -> set[tuple[int, int, int, int]]:
    """Extract unique (year, month, day, hour) slots that contain at least one bar."""
    hours: set[tuple[int, int, int, int]] = set()

    for path in files:
        table = pq.read_table(path, columns=["timestamp"])
        ts = table.column("timestamp").to_pandas()
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize("UTC")

        # Floor to hour and take unique slots (fast for millions of rows)
        hour_ts = ts.dt.floor("h")
        for t in hour_ts.unique():
            hours.add((t.year, t.month, t.day, t.hour))

    return hours


def _hours_from_raw_files(pair: str) -> set[tuple[int, int, int, int]]:
    """Mark hours with saved .bi5 files as fetched (data was downloaded)."""
    raw_pair_dir = config.RAW_DIR / pair
    if not raw_pair_dir.exists():
        return set()

    hours: set[tuple[int, int, int, int]] = set()
    for path in raw_pair_dir.rglob("*h_ticks.bi5"):
        # .../pair/YYYY/MM/DD/HHh_ticks.bi5
        try:
            hour = int(path.stem.split("h_")[0])
            day = int(path.parent.name)
            month = int(path.parent.parent.name)
            year = int(path.parent.parent.parent.name)
            hours.add((year, month, day, hour))
        except (ValueError, IndexError):
            logger.debug("Skipping unparseable raw path: %s", path)

    return hours


def _hours_to_catalog_df(
    pair: str,
    hours: set[tuple[int, int, int, int]],
) -> pd.DataFrame:
    """Build catalog rows for a set of fetched hours."""
    if not hours:
        return pd.DataFrame(
            columns=["pair", "year", "month", "day", "hour", "status", "fetched_at"]
        )

    now = pd.Timestamp.now(tz="UTC")
    rows = [
        {
            "pair": pair,
            "year": y,
            "month": m,
            "day": d,
            "hour": h,
            "status": "fetched",
            "fetched_at": now,
        }
        for y, m, d, h in sorted(hours)
    ]
    return pd.DataFrame(rows)


def count_pending_hours(pair: str, catalog: Catalog | None = None) -> int:
    """Count trading hours from target start through last complete hour not in catalog."""
    catalog = catalog or Catalog()
    batch = catalog.get_next_batch(pair, n=1_000_000)
    return len(batch)
