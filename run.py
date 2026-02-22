#!/usr/bin/env python3
"""RiverWriter — Dukascopy 1-Minute OHLCV Data Pipeline

Usage:
    python run.py                    # Default backfill run (500 hours)
    python run.py --pair EURUSD      # Single pair only
    python run.py --max-hours 2000   # Override batch size
    python run.py --validate         # Run QA validator only
    python run.py --status           # Show progress summary
    python run.py --retry-errors     # Retry previously failed hours
"""

import argparse
import logging
import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from riverwriter import config


def setup_logging(verbose: bool = False):
    """Configure logging to both console and file."""
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = config.LOG_DIR / "riverwriter.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Console handler: INFO (or DEBUG if verbose)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    ))
    root.addHandler(console)

    # File handler: DEBUG (all details)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s %(name)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root.addHandler(file_handler)


def main():
    parser = argparse.ArgumentParser(
        description="RiverWriter — Dukascopy 1-minute OHLCV backfill pipeline",
    )
    parser.add_argument(
        "--pair",
        type=str,
        choices=list(config.PAIRS.keys()),
        help="Fetch a single pair only",
    )
    parser.add_argument(
        "--max-hours",
        type=int,
        default=None,
        help=f"Max hourly chunks to fetch (default: {config.MAX_HOURS_PER_RUN})",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run data quality validator only (no fetching)",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show pipeline progress summary",
    )
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Retry previously errored hours",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose logging (DEBUG level to console)",
    )

    args = parser.parse_args()
    setup_logging(verbose=args.verbose)

    logger = logging.getLogger(__name__)
    logger.info("RiverWriter v1.0")

    if args.status:
        from riverwriter.runner import show_status
        show_status()
        return

    if args.validate:
        from riverwriter.validator import validate_all, print_report
        report = validate_all()
        print_report(report)
        return

    # Run backfill
    from riverwriter.runner import run

    pairs = [args.pair] if args.pair else None

    logger.info(
        "Rate limit: %.1fs between requests | Max hours this run: %d",
        config.REQUEST_DELAY_SECONDS,
        args.max_hours or config.MAX_HOURS_PER_RUN,
    )

    run(
        pairs=pairs,
        max_hours=args.max_hours,
        retry_errors=args.retry_errors,
    )


if __name__ == "__main__":
    main()
