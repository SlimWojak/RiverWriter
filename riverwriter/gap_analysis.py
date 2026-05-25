"""Operational gap analysis — NOT part of GX (candidate_C scope discipline).

Gap detection with weekend/session awareness stays in RiverWriter because
it is operational telemetry, not a formal ingestion invariant. GX answers
"are the bars structurally valid?"; this module answers "are there holes?"
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pandas as pd

from . import config
from . import substrate
from .catalog import _is_weekend_closed

logger = logging.getLogger(__name__)


def analyze_gaps(pairs: list[str] | None = None) -> dict:
    """Find gaps > 5 minutes during trading hours per pair."""
    pairs = pairs or list(config.PAIRS.keys())
    conn = substrate.connect(readonly=True)
    report = {"pairs": {}}

    try:
        for pair in pairs:
            df = substrate.fetch_df(conn, pair)
            if len(df) == 0:
                report["pairs"][pair] = {"gap_count": 0, "examples": []}
                continue

            if df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")

            gaps = _find_gaps(df)
            report["pairs"][pair] = {
                "gap_count": len(gaps),
                "examples": gaps[:10],
            }
    finally:
        conn.close()

    return report


def _find_gaps(df: pd.DataFrame) -> list[dict]:
    """Find gaps > 5 minutes during expected trading hours."""
    if len(df) < 2:
        return []

    ts = df["timestamp"].sort_values().reset_index(drop=True)
    diffs = ts.diff()
    gap_mask = diffs > pd.Timedelta(minutes=5)
    gaps = []

    for idx in diffs[gap_mask].index:
        gap_start = ts.iloc[idx - 1]
        gap_end = ts.iloc[idx]
        if _spans_weekend(gap_start, gap_end):
            continue
        gaps.append({
            "from": gap_start.isoformat(),
            "to": gap_end.isoformat(),
            "minutes": int(diffs.iloc[idx].total_seconds() / 60),
        })

    return gaps


def _spans_weekend(start, end) -> bool:
    """Check if a gap spans a forex weekend closure."""
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    if start_ts.tz is None:
        start_ts = start_ts.tz_localize("UTC")
    if end_ts.tz is None:
        end_ts = end_ts.tz_localize("UTC")

    cursor = start_ts.floor("D") - pd.Timedelta(days=7)
    end_day = end_ts.floor("D") + pd.Timedelta(days=7)
    tolerance = pd.Timedelta(minutes=5)

    while cursor <= end_day:
        if cursor.weekday() == 4:  # Friday
            close = cursor + pd.Timedelta(hours=22)
            reopen = cursor + pd.Timedelta(days=2, hours=22)
            if start_ts >= close - tolerance and end_ts <= reopen + tolerance:
                return True
        cursor += pd.Timedelta(days=1)

    return False
