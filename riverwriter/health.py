"""Data timeliness and operational health reporting."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from . import config
from .catalog import Catalog, _is_weekend_closed, last_complete_hour
from .catalog_rebuild import count_pending_hours
from .gap_analysis import analyze_gaps
from .gx_validate import run_checkpoint, print_report as print_gx_report
from . import substrate

logger = logging.getLogger(__name__)


def check_timeliness(pairs: list[str] | None = None) -> dict:
    """Report how far each pair lags behind the last complete trading hour."""
    pairs = pairs or list(config.PAIRS.keys())
    target = last_complete_hour()
    report = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "target_hour": target.isoformat(),
        "pairs": {},
    }

    conn = substrate.connect(readonly=True)
    try:
        for pair in pairs:
            summary = substrate.pair_summary(conn, pair)

            if summary["total_bars"] == 0:
                report["pairs"][pair] = {
                    "status": "no_data",
                    "newest_bar": None,
                    "lag_hours": None,
                    "pending_hours": count_pending_hours(pair),
                }
                continue

            newest_ts = summary["newest"]
            lag_hours = _trading_hours_between(newest_ts, target) if newest_ts else None
            pending = count_pending_hours(pair)

            report["pairs"][pair] = {
                "status": "current" if pending == 0 else "behind",
                "newest_bar": newest_ts.isoformat() if newest_ts else None,
                "lag_hours": lag_hours,
                "pending_hours": pending,
                "total_bars": summary["total_bars"],
            }
    finally:
        conn.close()

    return report


def print_timeliness(report: dict):
    """Print human-readable timeliness report."""
    print()
    print("RiverWriter Timeliness Report")
    print("=" * 78)
    print(f"Checked:  {report['checked_at'][:19]}")
    print(f"Target:   {report['target_hour'][:19]} (last complete UTC hour)")
    print()
    print(
        f"{'Pair':<10} {'Status':<10} {'Newest Bar':<22} "
        f"{'Lag (hrs)':>10} {'Pending':>10} {'Bars':>12}"
    )
    print("-" * 78)

    for pair, data in report["pairs"].items():
        if data.get("status") == "no_data":
            print(
                f"{pair:<10} {'NO DATA':<10} {'—':<22} {'—':>10} "
                f"{data['pending_hours']:>10,} {'—':>12}"
            )
            continue

        newest = data["newest_bar"][:19] if data["newest_bar"] else "—"
        lag = f"{data['lag_hours']:,}" if data["lag_hours"] is not None else "—"
        bars = f"{data.get('total_bars', 0):,}"

        print(
            f"{pair:<10} {data['status']:<10} {newest:<22} "
            f"{lag:>10} {data['pending_hours']:>10,} {bars:>12}"
        )

    print()


def run_full_health_check(pairs: list[str] | None = None) -> dict:
    """Timeliness + GX checkpoint + gap analysis."""
    timeliness = check_timeliness(pairs)
    gx_report = run_checkpoint(pairs)
    gaps = analyze_gaps(pairs)

    return {
        "timeliness": timeliness,
        "gx": gx_report,
        "gaps": gaps,
    }


def print_full_health(report: dict):
    """Print timeliness, GX checkpoint, and gap reports."""
    print_timeliness(report["timeliness"])
    print_gx_report(report["gx"])

    gaps = report.get("gaps", {}).get("pairs", {})
    if gaps:
        print("Gap Analysis (operational — not GX)")
        print("-" * 40)
        for pair, data in gaps.items():
            print(f"  {pair}: {data['gap_count']} gaps > 5min (trading hours)")
        print()


def _trading_hours_between(start, end: datetime) -> int:
    """Count expected trading hours between start (exclusive) and end (inclusive)."""
    if hasattr(start, "to_pydatetime"):
        start = start.to_pydatetime()
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)

    current = start.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    end_dt = end.replace(minute=0, second=0, microsecond=0)

    count = 0
    while current <= end_dt:
        if not _is_weekend_closed(current):
            count += 1
        current += timedelta(hours=1)

    return count
