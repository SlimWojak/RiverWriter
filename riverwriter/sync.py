"""One-stop sync: rebuild catalog, health check, parallel catch-up, GX + substrate."""

from __future__ import annotations

import logging

from . import config
from .catalog_rebuild import rebuild_from_parquet
from .health import run_full_health_check, print_full_health
from .parallel_runner import run_parallel
from . import substrate
from .gx_validate import run_checkpoint, print_report as print_gx_report

logger = logging.getLogger(__name__)


def sync(
    pairs: list[str] | None = None,
    skip_fetch: bool = False,
    max_hours: int | None = None,
):
    """Full candidate_C pipeline:

    1. Rebuild catalog from Parquet
    2. Register DuckDB substrate views
    3. Timeliness + GX checkpoint + gap analysis
    4. Parallel catch-up (if behind)
    5. Re-register substrate + final GX checkpoint
    """
    pairs = pairs or list(config.PAIRS.keys())
    total_steps = 4 if skip_fetch else 5

    logger.info("Step 1/%d: Rebuilding catalog from Parquet files...", total_steps)
    summary = rebuild_from_parquet(pairs)
    for pair, s in summary.items():
        logger.info(
            "  %s: %d hours marked (newest: %s)",
            pair, s["hours_marked"], s.get("newest", "—"),
        )

    logger.info("Step 2/%d: Registering DuckDB substrate views...", total_steps)
    conn = substrate.connect(readonly=False, register=True)
    conn.close()
    substrate.print_substrate_status(pairs)

    logger.info("Step 3/%d: Timeliness + GX checkpoint + gap analysis...", total_steps)
    report = run_full_health_check(pairs)
    print_full_health(report)

    if skip_fetch:
        logger.info("Skipping fetch (--skip-fetch)")
        return report

    pending = sum(
        report["timeliness"]["pairs"].get(p, {}).get("pending_hours", 0)
        for p in pairs
    )

    if pending == 0:
        logger.info("All pairs are current — nothing to fetch")
        return report

    logger.info(
        "Step 4/%d: Parallel catch-up (%d pending hours across %d pairs)...",
        total_steps, pending, len(pairs),
    )

    run_parallel(
        pairs=pairs,
        max_hours=max_hours,
        catch_up=True,
    )

    logger.info("Step 5/%d: Re-register substrate + final GX checkpoint...", total_steps)
    conn = substrate.connect(readonly=False, register=True)
    conn.close()

    final = run_full_health_check(pairs)
    print_full_health(final)
    return final
