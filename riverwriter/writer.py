"""Merges 1-minute bars into yearly Parquet files with dedup and validation.

Output conforms to RAW_BAR_SCHEMA (9 columns):
    timestamp, open, high, low, close, volume, source, knowledge_time, bar_hash
"""

import hashlib
import logging
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa

from . import config

logger = logging.getLogger(__name__)

RAW_BAR_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ns", tz="UTC")),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
    ("source", pa.string()),
    ("knowledge_time", pa.timestamp("ns", tz="UTC")),
    ("bar_hash", pa.string()),
])


def compute_bar_hashes(df: pd.DataFrame) -> pd.Series:
    """Vectorized sha256(timestamp|open|high|low|close|volume|source).

    Matches core system hash computation exactly.
    """
    ts_str = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    payload = (
        ts_str
        + "|" + df["open"].map(repr)
        + "|" + df["high"].map(repr)
        + "|" + df["low"].map(repr)
        + "|" + df["close"].map(repr)
        + "|" + df["volume"].map(repr)
        + "|" + df["source"]
    )
    return payload.apply(lambda x: hashlib.sha256(x.encode()).hexdigest())


def validate_bars(df: pd.DataFrame) -> pd.DataFrame:
    """Validate OHLC integrity. Returns only clean rows, logs rejects."""
    if len(df) == 0:
        return df

    bad_mask = (
        (df["high"] < df[["open", "close"]].max(axis=1))
        | (df["low"] > df[["open", "close"]].min(axis=1))
        | (df["high"] < df["low"])
        | (df["volume"] < 0)
    )

    n_bad = bad_mask.sum()
    if n_bad > 0:
        logger.warning("Rejecting %d bars failing OHLC integrity checks", n_bad)
        bad_rows = df[bad_mask]
        for _, row in bad_rows.iterrows():
            logger.debug(
                "  BAD BAR: ts=%s O=%.6f H=%.6f L=%.6f C=%.6f V=%.2f",
                row["timestamp"], row["open"], row["high"], row["low"],
                row["close"], row["volume"],
            )

    return df[~bad_mask].copy()


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate timestamps, keeping the first occurrence."""
    if len(df) == 0:
        return df

    before = len(df)
    df = df.sort_values("timestamp")
    df = df.drop_duplicates(subset=["timestamp"], keep="first")
    after = len(df)

    if before > after:
        logger.info("Deduplication removed %d duplicate bars", before - after)

    return df.sort_values("timestamp").reset_index(drop=True)


def write_bars(pair: str, new_bars: pd.DataFrame):
    """Merge new bars into yearly Parquet files for a given pair.

    - Stamps knowledge_time and bar_hash onto incoming bars
    - Loads existing yearly file (if any)
    - Concatenates with new bars
    - Deduplicates on timestamp
    - Validates OHLC integrity
    - Writes atomically with explicit RAW_BAR_SCHEMA
    """
    if len(new_bars) == 0:
        return

    # Ensure timestamp is ns-precision UTC
    if new_bars["timestamp"].dt.tz is None:
        new_bars["timestamp"] = new_bars["timestamp"].dt.tz_localize("UTC")
    new_bars["timestamp"] = new_bars["timestamp"].astype("datetime64[ns, UTC]")

    # Stamp knowledge_time (when we fetched this data)
    new_bars["knowledge_time"] = pd.Timestamp.now(tz="UTC").as_unit("ns")

    # Compute integrity hashes
    new_bars["bar_hash"] = compute_bar_hashes(new_bars)

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

        # Atomic write with explicit schema
        _atomic_write_parquet(combined, parquet_path)

        logger.info(
            "%s %d | wrote %d bars (total: %d)",
            pair, year, len(year_bars), len(combined),
        )


def _atomic_write_parquet(df: pd.DataFrame, path: Path):
    """Write DataFrame to Parquet atomically via temp file + rename.

    Enforces RAW_BAR_SCHEMA column order and types.
    """
    import os

    # Enforce column order to match RAW_BAR_SCHEMA exactly
    col_order = [f.name for f in RAW_BAR_SCHEMA]
    df = df[col_order]

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        suffix=".parquet.tmp", dir=path.parent
    )
    tmp = Path(tmp_path)
    try:
        os.close(fd)
        df.to_parquet(
            tmp, engine="pyarrow", compression="snappy",
            index=False, schema=RAW_BAR_SCHEMA,
        )
        tmp.rename(path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
