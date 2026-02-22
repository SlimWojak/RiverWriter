"""Orchestrator: progressive backfill with rate limiting and round-robin scheduling."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import pandas as pd

from . import config
from .catalog import Catalog
from .fetcher import create_session, fetch_hour
from .parser import parse_ticks
from .aggregator import aggregate_ticks
from .writer import write_bars

logger = logging.getLogger(__name__)


def run(
    pairs: list[str] | None = None,
    max_hours: int | None = None,
    retry_errors: bool = False,
):
    """Execute a backfill run.

    Args:
        pairs: List of pairs to process (default: all pairs)
        max_hours: Max hourly chunks to fetch this run (default: config value)
        retry_errors: If True, retry previously errored slots instead of new ones
    """
    pairs = pairs or list(config.PAIRS.keys())
    max_hours = max_hours or config.MAX_HOURS_PER_RUN

    catalog = Catalog()
    session = create_session()

    # Get batches for each pair
    hours_per_pair = max_hours // len(pairs)
    remainder = max_hours % len(pairs)

    pair_batches = {}
    for i, pair in enumerate(pairs):
        n = hours_per_pair + (1 if i < remainder else 0)
        if retry_errors:
            pair_batches[pair] = catalog.get_error_batch(pair, n)
        else:
            pair_batches[pair] = catalog.get_next_batch(pair, n)

    total_pending = sum(len(b) for b in pair_batches.values())
    if total_pending == 0:
        logger.info("Nothing to fetch — all slots processed or target reached")
        return

    logger.info(
        "Starting backfill: %d hours across %d pairs (rate: %.1fs/req)",
        total_pending, len(pairs), config.REQUEST_DELAY_SECONDS,
    )

    start_time = time.monotonic()
    total_fetched = 0
    total_bars_written = 0

    # Round-robin across pairs
    pair_iterators = {pair: iter(batch) for pair, batch in pair_batches.items()}
    pair_buffers: dict[str, list[pd.DataFrame]] = {pair: [] for pair in pairs}
    active_pairs = set(pairs)

    # Track batch boundaries for periodic writes
    fetches_since_write = {pair: 0 for pair in pairs}
    WRITE_EVERY = 50  # Write to Parquet every N fetches per pair

    while active_pairs:
        for pair in list(active_pairs):
            slot = next(pair_iterators[pair], None)
            if slot is None:
                active_pairs.discard(pair)
                continue

            year, month, day, hour = slot

            status, data = fetch_hour(session, pair, year, month, day, hour)
            catalog.update(pair, year, month, day, hour, status)
            total_fetched += 1

            if status == "fetched" and data:
                try:
                    ticks = parse_ticks(data, pair, year, month, day, hour)
                    bars = aggregate_ticks(ticks)

                    if len(bars) > 0:
                        pair_buffers[pair].append(bars)

                    tick_count = len(ticks)
                    bar_count = len(bars)
                    logger.info(
                        "[%s] %s %d-%02d-%02d %02dh | fetched | %d ticks → %d bars",
                        _elapsed(start_time), pair, year, month, day, hour,
                        tick_count, bar_count,
                    )
                except Exception as exc:
                    logger.error(
                        "[%s] %s %d-%02d-%02d %02dh | parse error: %s",
                        _elapsed(start_time), pair, year, month, day, hour, exc,
                    )
                    catalog.update(pair, year, month, day, hour, "error")
            elif status == "no_data":
                logger.info(
                    "[%s] %s %d-%02d-%02d %02dh | no_data",
                    _elapsed(start_time), pair, year, month, day, hour,
                )

            fetches_since_write[pair] += 1

            # Periodic flush: write bars and save catalog
            if fetches_since_write[pair] >= WRITE_EVERY:
                n_bars = _flush_pair(pair, pair_buffers)
                total_bars_written += n_bars
                fetches_since_write[pair] = 0
                catalog.save()

            # Rate limiting
            time.sleep(config.REQUEST_DELAY_SECONDS)

    # Final flush for all pairs
    for pair in pairs:
        n_bars = _flush_pair(pair, pair_buffers)
        total_bars_written += n_bars

    catalog.save()

    elapsed = time.monotonic() - start_time
    logger.info(
        "Run complete. Fetched %d hours in %s. Wrote %d new 1m bars across %d pairs.",
        total_fetched, _format_duration(elapsed), total_bars_written, len(pairs),
    )


def _flush_pair(pair: str, buffers: dict[str, list[pd.DataFrame]]) -> int:
    """Write buffered bars for a pair to Parquet. Returns bar count."""
    buf = buffers[pair]
    if not buf:
        return 0

    combined = pd.concat(buf, ignore_index=True)
    write_bars(pair, combined)
    n = len(combined)
    buffers[pair] = []
    return n


def show_status():
    """Print current pipeline status."""
    catalog = Catalog()
    summary = catalog.summary()

    print()
    print("RiverWriter Status Report")
    print("=" * 75)
    print(
        f"{'Pair':<10} {'Oldest Data':<14} {'Newest Data':<14} "
        f"{'Fetched':>9} {'No Data':>9} {'Errors':>8} {'Total':>8}"
    )
    print("-" * 75)

    total_fetched = 0
    total_no_data = 0
    total_error = 0
    total_entries = 0

    for pair in config.PAIRS:
        s = summary.get(pair, {})
        f = s.get("fetched", 0)
        nd = s.get("no_data", 0)
        e = s.get("error", 0)
        t = s.get("total", 0)
        oldest = s.get("oldest", "—")
        newest = s.get("newest", "—")

        print(
            f"{pair:<10} {oldest or '—':<14} {newest or '—':<14} "
            f"{f:>9,} {nd:>9,} {e:>8,} {t:>8,}"
        )

        total_fetched += f
        total_no_data += nd
        total_error += e
        total_entries += t

    print("-" * 75)
    print(
        f"{'TOTAL':<10} {'':<14} {'':<14} "
        f"{total_fetched:>9,} {total_no_data:>9,} {total_error:>8,} {total_entries:>8,}"
    )
    print()

    # Parquet file sizes
    if config.PARQUET_DIR.exists():
        total_size = sum(f.stat().st_size for f in config.PARQUET_DIR.rglob("*.parquet"))
        print(f"Parquet storage: {total_size / (1024*1024):.1f} MB")

    if config.RAW_DIR.exists():
        raw_size = sum(f.stat().st_size for f in config.RAW_DIR.rglob("*.bi5"))
        print(f"Raw .bi5 storage: {raw_size / (1024*1024):.1f} MB")

    print()


def _elapsed(start: float) -> str:
    """Format elapsed time since start."""
    return _format_duration(time.monotonic() - start)


def _format_duration(seconds: float) -> str:
    """Format seconds into Xm Ys."""
    m, s = divmod(int(seconds), 60)
    if m > 0:
        return f"{m}m {s:02d}s"
    return f"{s}s"
