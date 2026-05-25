#!/usr/bin/env python3
"""RiverWriter — RIVER substrate pipeline (candidate_C)

Dukascopy fetch → Parquet → DuckDB views → GX checkpoint → ATOM-ready bars

Usage:
    python run.py --sync                # Full pipeline (catalog + substrate + GX + catch-up)
    python run.py --sync --skip-fetch   # Catalog + substrate + GX only
    python run.py --substrate           # DuckDB view status
    python run.py --gx-checkpoint       # GX ingestion-boundary validation
    python run.py --health              # Timeliness + GX + gap analysis
    python run.py --status              # Fetch catalog progress
    python run.py --parallel --catch-up # Parallel fetch until current
"""

import argparse
import logging
import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from riverwriter import config


def setup_logging(verbose: bool = False, pair: str | None = None):
    """Configure logging to both console and file."""
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    suffix = f"_{pair}" if pair else ""
    log_file = config.LOG_DIR / f"riverwriter{suffix}.log"

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    ))
    root.addHandler(console)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s %(name)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root.addHandler(file_handler)


def main():
    parser = argparse.ArgumentParser(
        description="RiverWriter — Dukascopy 1-minute OHLCV pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--pair",
        type=str,
        choices=list(config.PAIRS.keys()),
        help="Single pair only",
    )
    parser.add_argument(
        "--max-hours",
        type=int,
        default=None,
        help=f"Max hourly chunks per pair (default: {config.MAX_HOURS_PER_RUN})",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Legacy combined validation report (GX + gaps)",
    )
    parser.add_argument(
        "--gx-checkpoint",
        action="store_true",
        help="GX ingestion-boundary validation (authoritative for ATOM)",
    )
    parser.add_argument(
        "--substrate",
        action="store_true",
        help="Show DuckDB substrate status (Parquet views)",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show pipeline progress summary",
    )
    parser.add_argument(
        "--health",
        action="store_true",
        help="Timeliness and integrity report",
    )
    parser.add_argument(
        "--rebuild-catalog",
        action="store_true",
        help="Rebuild fetch catalog from existing Parquet files",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Full sync: rebuild catalog, health check, parallel catch-up",
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="With --sync: rebuild + health only, no download",
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Use parallel workers (one process per pair)",
    )
    parser.add_argument(
        "--catch-up",
        action="store_true",
        help="Fetch until current (use with --parallel)",
    )
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Retry previously errored hours",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose logging",
    )

    args = parser.parse_args()
    setup_logging(verbose=args.verbose, pair=args.pair)

    logger = logging.getLogger(__name__)
    logger.info("RiverWriter v1.0")

    pairs = [args.pair] if args.pair else None

    if args.status:
        from riverwriter.runner import show_status
        show_status()
        return

    if args.rebuild_catalog:
        from riverwriter.catalog_rebuild import rebuild_from_parquet
        summary = rebuild_from_parquet(pairs)
        print("\nCatalog Rebuild Summary")
        print("=" * 60)
        for pair, s in summary.items():
            print(
                f"  {pair}: {s['hours_marked']:,} hours | "
                f"{s['files']} files | {s.get('oldest', '—')} → {s.get('newest', '—')}"
            )
        print()
        return

    if args.substrate:
        from riverwriter import substrate
        conn = substrate.connect(readonly=False, register=True)
        conn.close()
        substrate.print_substrate_status(pairs)
        return

    if args.gx_checkpoint:
        from riverwriter.gx_validate import run_checkpoint, print_report
        gx_report = run_checkpoint(pairs)
        print_report(gx_report)
        if not gx_report["pass"]:
            sys.exit(1)
        return

    if args.validate:
        from riverwriter.validator import validate_all, print_report
        report = validate_all(pairs)
        print_report(report)
        if not report["pass"]:
            sys.exit(1)
        return

    if args.health:
        from riverwriter.health import run_full_health_check, print_full_health
        report = run_full_health_check(pairs)
        print_full_health(report)
        return

    if args.sync:
        from riverwriter.sync import sync
        sync(pairs=pairs, skip_fetch=args.skip_fetch, max_hours=args.max_hours)
        return

    # Fetch modes
    if args.parallel or args.catch_up:
        from riverwriter.parallel_runner import run_parallel
        delay = config.request_delay_for_workers(
            len(pairs) if pairs else config.PARALLEL_WORKERS,
            parallel=True,
        )
        logger.info(
            "Parallel mode: %.1fs delay/worker (~%.1f req/s aggregate)",
            delay,
            (len(pairs) if pairs else config.PARALLEL_WORKERS) / delay,
        )
        run_parallel(
            pairs=pairs,
            max_hours=args.max_hours,
            retry_errors=args.retry_errors,
            catch_up=args.catch_up,
        )
        return

    from riverwriter.runner import run

    logger.info(
        "Serial mode: %.1fs between requests (~%.1f req/s)",
        config.REQUEST_DELAY_SECONDS,
        1 / config.REQUEST_DELAY_SECONDS,
    )

    run(
        pairs=pairs,
        max_hours=args.max_hours,
        retry_errors=args.retry_errors,
    )


if __name__ == "__main__":
    main()
