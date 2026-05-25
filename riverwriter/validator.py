"""Operational validation helpers.

Formal ingestion-boundary validation → gx_validate.py (GX authoritative).
Timeliness / lag → health.py.
Gap analysis → gap_analysis.py.

This module remains as a thin compatibility shim for --validate.
"""

from __future__ import annotations

import json
import logging

from . import config
from .gap_analysis import analyze_gaps
from .gx_validate import run_checkpoint, print_report as print_gx_report

logger = logging.getLogger(__name__)


def validate_all(pairs: list[str] | None = None) -> dict:
    """Run GX checkpoint + gap analysis. Returns combined report."""
    gx_report = run_checkpoint(pairs)
    gaps = analyze_gaps(pairs)

    report = {
        "generated_at": gx_report["generated_at"],
        "gx": gx_report,
        "gaps": gaps,
        "pass": gx_report["pass"],
    }

    # Legacy path for tools expecting validation_report.json
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(config.VALIDATION_REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2, default=str)

    return report


def print_report(report: dict):
    """Print GX checkpoint + gap summary."""
    if "gx" in report:
        print_gx_report(report["gx"])
    else:
        print_gx_report(report)

    gaps = report.get("gaps", {}).get("pairs", {})
    if gaps:
        print("Gap Analysis (operational — not GX)")
        print("-" * 40)
        for pair, data in gaps.items():
            print(f"  {pair}: {data['gap_count']} gaps > 5min (trading hours)")
        print()
