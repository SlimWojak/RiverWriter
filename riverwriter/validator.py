"""Standalone data quality checks for RiverWriter Parquet files."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from . import config
from .catalog import _is_weekend_closed

logger = logging.getLogger(__name__)


def validate_all() -> dict:
    """Run full QA suite across all pairs. Returns report dict."""
    report = {"generated_at": datetime.now(timezone.utc).isoformat(), "pairs": {}}

    for pair in config.PAIRS:
        pair_dir = config.PARQUET_DIR / pair
        if not pair_dir.exists():
            report["pairs"][pair] = {"status": "no_data"}
            continue

        files = sorted(pair_dir.glob(f"{pair}_*.parquet"))
        if not files:
            report["pairs"][pair] = {"status": "no_data"}
            continue

        # Load all years
        dfs = [pd.read_parquet(f) for f in files]
        df = pd.concat(dfs, ignore_index=True).sort_values("timestamp").reset_index(drop=True)

        report["pairs"][pair] = _validate_pair(pair, df)

    # Save report
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(config.VALIDATION_REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info("Validation report saved to %s", config.VALIDATION_REPORT_PATH)
    return report


def _validate_pair(pair: str, df: pd.DataFrame) -> dict:
    """Run all checks on a single pair's data."""
    result = {
        "total_bars": len(df),
        "date_range": None,
        "checks": {},
    }

    if len(df) == 0:
        return result

    # Ensure UTC
    if df["timestamp"].dt.tz is None:
        df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")

    result["date_range"] = {
        "start": df["timestamp"].min().isoformat(),
        "end": df["timestamp"].max().isoformat(),
    }

    # 1. Duplicates
    dupes = df["timestamp"].duplicated().sum()
    result["checks"]["duplicates"] = {"count": int(dupes), "pass": dupes == 0}

    # 2. OHLC integrity
    bad_ohlc = (
        (df["high"] < df[["open", "close"]].max(axis=1))
        | (df["low"] > df[["open", "close"]].min(axis=1))
        | (df["high"] < df["low"])
    ).sum()
    result["checks"]["ohlc_integrity"] = {
        "failures": int(bad_ohlc),
        "pass": bad_ohlc == 0,
    }

    # 3. Volume checks
    zero_vol = (df["volume"] <= 0).sum()
    result["checks"]["zero_volume"] = {"count": int(zero_vol)}

    # 4. Gaps (> 5 min during trading hours)
    gaps = _find_gaps(df)
    result["checks"]["gaps_over_5min"] = {
        "count": len(gaps),
        "examples": gaps[:10],  # First 10
    }

    # 5. Completeness estimate
    completeness = _estimate_completeness(df)
    result["checks"]["completeness"] = completeness

    # 6. Source provenance check
    if "source" in df.columns:
        sources = df["source"].unique().tolist()
        result["checks"]["sources"] = {"values": sources}

    # 7. Bar hash integrity
    if "bar_hash" in df.columns:
        null_hashes = df["bar_hash"].isna().sum()
        result["checks"]["bar_hash_nulls"] = {"count": int(null_hashes), "pass": null_hashes == 0}

    # 8. Price spike check (>5% jump within a day)
    price_spikes = _find_price_spikes(df)
    result["checks"]["price_spikes_5pct"] = {
        "count": len(price_spikes),
        "examples": price_spikes[:10],
    }

    # 9. Yearly row counts
    df["_year"] = df["timestamp"].dt.year
    yearly = df.groupby("_year").size().to_dict()
    result["checks"]["yearly_row_counts"] = {str(k): int(v) for k, v in yearly.items()}

    return result


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

        # Skip if the gap spans a weekend
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
    # Walk through the gap — if all hours in between are weekend, it's normal
    current = start + timedelta(hours=1)
    while current < end:
        if not _is_weekend_closed(current.to_pydatetime()):
            return False
        current += timedelta(hours=1)
    return True


def _estimate_completeness(df: pd.DataFrame) -> dict:
    """Estimate % of expected trading minutes covered."""
    if len(df) == 0:
        return {"percent": 0.0, "actual": 0, "expected": 0}

    start = df["timestamp"].min()
    end = df["timestamp"].max()

    # Count expected trading minutes
    expected = 0
    current = start.replace(second=0, microsecond=0)
    while current <= end:
        if not _is_weekend_closed(current.to_pydatetime()):
            expected += 1
        current += timedelta(minutes=1)

    actual = len(df)
    pct = (actual / expected * 100) if expected > 0 else 0.0

    return {
        "percent": round(pct, 2),
        "actual": actual,
        "expected": expected,
    }


def _find_price_spikes(df: pd.DataFrame) -> list[dict]:
    """Find bars where mid price jumps >5% from previous close, within same day."""
    if len(df) < 2:
        return []

    spikes = []
    sorted_df = df.sort_values("timestamp").reset_index(drop=True)

    prev_close = sorted_df["close"].shift(1)
    pct_change = ((sorted_df["open"] - prev_close) / prev_close).abs()

    # Only check within same day
    same_day = sorted_df["timestamp"].dt.date == sorted_df["timestamp"].shift(1).dt.date
    spike_mask = (pct_change > 0.05) & same_day

    for idx in sorted_df[spike_mask].index:
        spikes.append({
            "timestamp": sorted_df.loc[idx, "timestamp"].isoformat(),
            "prev_close": float(sorted_df.loc[idx - 1, "close"]),
            "open": float(sorted_df.loc[idx, "open"]),
            "pct_change": round(float(pct_change.iloc[idx]) * 100, 3),
        })

    return spikes


def print_report(report: dict):
    """Print a human-readable validation report to console."""
    print("\nRiverWriter Validation Report")
    print("=" * 60)
    print(f"Generated: {report['generated_at']}")
    print()

    for pair, data in report["pairs"].items():
        if data.get("status") == "no_data":
            print(f"  {pair}: No data")
            continue

        print(f"  {pair}")
        print(f"    Bars: {data['total_bars']:,}")
        if data["date_range"]:
            print(f"    Range: {data['date_range']['start'][:10]} → {data['date_range']['end'][:10]}")

        checks = data.get("checks", {})

        if "completeness" in checks:
            c = checks["completeness"]
            print(f"    Completeness: {c['percent']}% ({c['actual']:,} / {c['expected']:,})")

        if "duplicates" in checks:
            d = checks["duplicates"]
            status = "OK" if d["pass"] else f"FAIL ({d['count']})"
            print(f"    Duplicates: {status}")

        if "ohlc_integrity" in checks:
            o = checks["ohlc_integrity"]
            status = "OK" if o["pass"] else f"FAIL ({o['failures']})"
            print(f"    OHLC Integrity: {status}")

        if "gaps_over_5min" in checks:
            g = checks["gaps_over_5min"]
            print(f"    Gaps > 5min: {g['count']}")

        if "price_spikes_5pct" in checks:
            s = checks["price_spikes_5pct"]
            print(f"    Price Spikes > 5%: {s['count']}")

        if "yearly_row_counts" in checks:
            yrc = checks["yearly_row_counts"]
            for yr, count in sorted(yrc.items()):
                print(f"      {yr}: {count:,} bars")

        print()
