"""Aggregates raw ticks into 1-minute OHLCV bars."""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Schema for output bars
BAR_DTYPES = {
    "timestamp": "datetime64[ns, UTC]",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
    "tick_count": "int32",
    "spread_avg": "float64",
}


def aggregate_ticks(ticks: list[dict]) -> pd.DataFrame:
    """Aggregate a list of tick dicts into 1-minute OHLCV bars.

    Returns a DataFrame with columns matching BAR_DTYPES.
    Returns an empty DataFrame if no ticks provided.
    """
    if not ticks:
        return _empty_bars()

    df = pd.DataFrame(ticks)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["spread"] = df["ask"] - df["bid"]
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
        "tick_count": grouped["mid"].count().astype("int32"),
        "spread_avg": grouped["spread"].mean(),
    })

    bars = bars.reset_index(drop=True)
    bars["timestamp"] = bars["timestamp"].dt.tz_localize(None).dt.tz_localize("UTC")

    return bars


def _empty_bars() -> pd.DataFrame:
    """Return an empty DataFrame with the correct bar schema."""
    return pd.DataFrame(
        columns=["timestamp", "open", "high", "low", "close", "volume", "tick_count", "spread_avg"]
    ).astype({
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "tick_count": "int32",
        "spread_avg": "float64",
    })
