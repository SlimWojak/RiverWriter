"""Merges 1-minute bars into yearly Parquet files with dedup and validation."""

import logging
import tempfile
from pathlib import Path

import pandas as pd

from . import config

logger = logging.getLogger(__name__)


def validate_bars(df: pd.DataFrame) -> pd.DataFrame:
    """Validate OHLC integrity. Returns only clean rows, logs rejects."""
    if len(df) == 0:
        return df

    bad_mask = (
        (df["high"] < df[["open", "close"]].max(axis=1))
        | (df["low"] > df[["open", "close"]].min(axis=1))
        | (df["high"] < df["low"])
        | (df["volume"] < 0)
        | (df["tick_count"] < 1)
    )

    n_bad = bad_mask.sum()
    if n_bad > 0:
        logger.warning("Rejecting %d bars failing OHLC integrity checks", n_bad)
        bad_rows = df[bad_mask]
        for _, row in bad_rows.iterrows():
            logger.debug(
                "  BAD BAR: ts=%s O=%.6f H=%.6f L=%.6f C=%.6f V=%.2f tc=%d",
                row["timestamp"], row["open"], row["high"], row["low"],
                row["close"], row["volume"], row["tick_count"],
            )

    return df[~bad_mask].copy()


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate timestamps, keeping the row with higher tick_count."""
    if len(df) == 0:
        return df

    before = len(df)
    df = df.sort_values(["timestamp", "tick_count"], ascending=[True, False])
    df = df.drop_duplicates(subset=["timestamp"], keep="first")
    after = len(df)

    if before > after:
        logger.info("Deduplication removed %d duplicate bars", before - after)

    return df.sort_values("timestamp").reset_index(drop=True)


def write_bars(pair: str, new_bars: pd.DataFrame):
    """Merge new bars into yearly Parquet files for a given pair.

    - Loads existing yearly file (if any)
    - Concatenates with new bars
    - Deduplicates on timestamp (keeps higher tick_count)
    - Validates OHLC integrity
    - Writes atomically (temp file + rename)
    """
    if len(new_bars) == 0:
        return

    # Ensure timestamp is proper UTC
    if new_bars["timestamp"].dt.tz is None:
        new_bars["timestamp"] = new_bars["timestamp"].dt.tz_localize("UTC")

    pair_dir = config.PARQUET_DIR / pair
    pair_dir.mkdir(parents=True, exist_ok=True)

    # Group new bars by year
    new_bars["_year"] = new_bars["timestamp"].dt.year

    for year, year_bars in new_bars.groupby("_year"):
        year_bars = year_bars.drop(columns=["_year"])
        parquet_path = pair_dir / f"{pair}_{year}.parquet"

        # Load existing data
        if parquet_path.exists():
            existing = pd.read_parquet(parquet_path)
            combined = pd.concat([existing, year_bars], ignore_index=True)
        else:
            combined = year_bars.copy()

        # Deduplicate
        combined = deduplicate(combined)

        # Validate
        combined = validate_bars(combined)

        # Sort by timestamp
        combined = combined.sort_values("timestamp").reset_index(drop=True)

        # Atomic write: temp file then rename
        _atomic_write_parquet(combined, parquet_path)

        logger.info(
            "%s %d | wrote %d bars (total: %d)",
            pair, year, len(year_bars), len(combined),
        )


def _atomic_write_parquet(df: pd.DataFrame, path: Path):
    """Write DataFrame to Parquet atomically via temp file + rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        suffix=".parquet.tmp", dir=path.parent
    )
    tmp = Path(tmp_path)
    try:
        import os
        os.close(fd)
        df.to_parquet(tmp, engine="pyarrow", compression="snappy", index=False)
        tmp.rename(path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
