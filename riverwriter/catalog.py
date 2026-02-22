"""Tracks fetch state for every (pair, hour) slot. Identifies gaps and schedules work."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import config

logger = logging.getLogger(__name__)

CATALOG_SCHEMA = pa.schema([
    ("pair", pa.string()),
    ("year", pa.int16()),
    ("month", pa.int8()),
    ("day", pa.int8()),
    ("hour", pa.int8()),
    ("status", pa.string()),
    ("fetched_at", pa.timestamp("ns", tz="UTC")),
])


class Catalog:
    """Manages the fetch-state manifest for all (pair, hour) slots."""

    def __init__(self):
        self.df: pd.DataFrame = self._load()
        self._index: set[tuple] = set()
        self._rebuild_index()

    def _load(self) -> pd.DataFrame:
        """Load existing catalog from disk, or create empty."""
        if config.CATALOG_PATH.exists():
            df = pd.read_parquet(config.CATALOG_PATH)
            logger.info("Loaded catalog with %d entries", len(df))
            return df

        logger.info("No existing catalog found, starting fresh")
        return pd.DataFrame(
            columns=["pair", "year", "month", "day", "hour", "status", "fetched_at"]
        )

    def _rebuild_index(self):
        """Build a set of (pair, year, month, day, hour) for fast lookups."""
        if len(self.df) == 0:
            self._index = set()
            return
        self._index = set(
            zip(self.df["pair"], self.df["year"], self.df["month"], self.df["day"], self.df["hour"])
        )

    def has_entry(self, pair: str, year: int, month: int, day: int, hour: int) -> bool:
        """Check if we already have a catalog entry for this slot."""
        return (pair, year, month, day, hour) in self._index

    def get_status(self, pair: str, year: int, month: int, day: int, hour: int) -> str | None:
        """Get the status for a specific slot, or None if not in catalog."""
        if not self.has_entry(pair, year, month, day, hour):
            return None
        mask = (
            (self.df["pair"] == pair)
            & (self.df["year"] == year)
            & (self.df["month"] == month)
            & (self.df["day"] == day)
            & (self.df["hour"] == hour)
        )
        rows = self.df.loc[mask, "status"]
        if len(rows) == 0:
            return None
        return rows.iloc[0]

    def update(self, pair: str, year: int, month: int, day: int, hour: int, status: str):
        """Add or update a catalog entry."""
        now = pd.Timestamp.now(tz="UTC")
        key = (pair, year, month, day, hour)

        if key in self._index:
            mask = (
                (self.df["pair"] == pair)
                & (self.df["year"] == year)
                & (self.df["month"] == month)
                & (self.df["day"] == day)
                & (self.df["hour"] == hour)
            )
            self.df.loc[mask, "status"] = status
            self.df.loc[mask, "fetched_at"] = now
        else:
            new_row = pd.DataFrame([{
                "pair": pair,
                "year": year,
                "month": month,
                "day": day,
                "hour": hour,
                "status": status,
                "fetched_at": now,
            }])
            self.df = pd.concat([self.df, new_row], ignore_index=True)
            self._index.add(key)

    def save(self):
        """Persist catalog to disk."""
        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = config.CATALOG_PATH.with_suffix(".tmp")
        self.df.to_parquet(tmp, engine="pyarrow", compression="snappy", index=False)
        tmp.rename(config.CATALOG_PATH)
        logger.debug("Catalog saved with %d entries", len(self.df))

    def get_next_batch(self, pair: str, n: int) -> list[tuple[int, int, int, int]]:
        """Return up to `n` pending (year, month, day, hour) slots, working backward from now.

        Skips hours already in catalog (any status) and weekend non-trading hours.
        """
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        # Start from the most recent complete hour
        current = now - timedelta(hours=1)
        target_start = datetime(
            config.TARGET_START_YEAR, config.TARGET_START_MONTH, config.TARGET_START_DAY,
            tzinfo=timezone.utc,
        )

        batch = []
        while current >= target_start and len(batch) < n:
            y, m, d, h = current.year, current.month, current.day, current.hour

            if not _is_weekend_closed(current) and not self.has_entry(pair, y, m, d, h):
                batch.append((y, m, d, h))

            current -= timedelta(hours=1)

        return batch

    def get_error_batch(self, pair: str, n: int) -> list[tuple[int, int, int, int]]:
        """Return up to `n` previously errored slots for retry."""
        if len(self.df) == 0:
            return []
        mask = (self.df["pair"] == pair) & (self.df["status"] == "error")
        errors = self.df.loc[mask, ["year", "month", "day", "hour"]]
        errors = errors.sort_values(["year", "month", "day", "hour"], ascending=False)
        return [
            (int(r.year), int(r.month), int(r.day), int(r.hour))
            for _, r in errors.head(n).iterrows()
        ]

    def summary(self) -> dict:
        """Return a summary dict of catalog state per pair."""
        if len(self.df) == 0:
            return {}

        result = {}
        for pair in config.PAIRS:
            mask = self.df["pair"] == pair
            sub = self.df[mask]
            if len(sub) == 0:
                result[pair] = {
                    "total": 0, "fetched": 0, "no_data": 0, "error": 0,
                    "oldest": None, "newest": None,
                }
                continue

            counts = sub["status"].value_counts().to_dict()
            # Build date from components for oldest/newest
            fetched_mask = sub["status"] == "fetched"
            fetched = sub[fetched_mask]

            oldest = newest = None
            if len(fetched) > 0:
                sorted_f = fetched.sort_values(["year", "month", "day", "hour"])
                first = sorted_f.iloc[0]
                last = sorted_f.iloc[-1]
                oldest = f"{int(first.year)}-{int(first.month):02d}-{int(first.day):02d}"
                newest = f"{int(last.year)}-{int(last.month):02d}-{int(last.day):02d}"

            result[pair] = {
                "total": len(sub),
                "fetched": counts.get("fetched", 0),
                "no_data": counts.get("no_data", 0),
                "error": counts.get("error", 0),
                "oldest": oldest,
                "newest": newest,
            }

        return result


def _is_weekend_closed(dt: datetime) -> bool:
    """Check if a given UTC datetime falls in the forex weekend closure.

    Forex closes Friday ~22:00 UTC and reopens Sunday ~22:00 UTC.
    """
    wd = dt.weekday()  # Mon=0 .. Sun=6
    h = dt.hour

    # Saturday: all hours closed
    if wd == 5:
        return True
    # Sunday: closed before 22:00 UTC
    if wd == 6 and h < 22:
        return True
    # Friday: closed at 22:00+ UTC
    if wd == 4 and h >= 22:
        return True

    return False
