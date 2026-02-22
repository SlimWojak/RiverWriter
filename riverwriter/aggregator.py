"""Aggregates raw ticks into 1-minute OHLCV bars.

Output matches RAW_BAR_SCHEMA from core system (minus knowledge_time and
bar_hash which are added at write time).
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Columns produced by aggregation (source added here, knowledge_time + bar_hash at write)
AGG_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume", "source"]


def aggregate_ticks(ticks: list[dict]) -> pd.DataFrame:
    """Aggregate a list of tick dicts into 1-minute OHLCV bars.

    Returns a DataFrame with columns: timestamp, open, high, low, close, volume, source.
    Returns an empty DataFrame if no ticks provided.
    """
    if not ticks:
        return _empty_bars()

    df = pd.DataFrame(ticks)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["total_volume"] = df["ask_volume"] + df["bid_volume"]

    # Floor to minute
    df["minute"] = df["timestamp"].dt.floor("min")

    grouped = df.groupby("minute", sort=True)

    bars = pd.DataFrame({
        "timestamp": grouped["minute"].first(),
        "open": grouped["mid"].first(),
        "high": grouped["mid"].max(),
        "low": grouped["mid"].min(),
        "close": grouped["mid"].last(),
        "volume": grouped["total_volume"].sum(),
    })

    bars = bars.reset_index(drop=True)
    # Ensure nanosecond-precision UTC timestamps to match RAW_BAR_SCHEMA
    bars["timestamp"] = bars["timestamp"].dt.tz_localize(None).dt.tz_localize("UTC").astype("datetime64[ns, UTC]")
    bars["source"] = "dukascopy"

    return bars


def _empty_bars() -> pd.DataFrame:
    """Return an empty DataFrame with the correct bar schema."""
    return pd.DataFrame(columns=AGG_COLUMNS).astype({
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "source": "object",
    })
