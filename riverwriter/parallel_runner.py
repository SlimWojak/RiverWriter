"""Parallel backfill: one worker process per currency pair."""

from __future__ import annotations

import logging
import multiprocessing as mp
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

WRITE_EVERY = 50


def run_parallel(
    pairs: list[str] | None = None,
    max_hours: int | None = None,
    retry_errors: bool = False,
    catch_up: bool = False,
):
    """Run one worker process per pair, fetching in parallel."""
    pairs = pairs or list(config.PAIRS.keys())
    num_workers = len(pairs)
    delay = config.request_delay_for_workers(num_workers, parallel=True)

    if catch_up:
        max_hours = max_hours or 999_999
    else:
        max_hours = max_hours or config.MAX_HOURS_PER_RUN

    logger.info(
        "Parallel backfill: %d workers | %.1fs delay/worker | ~%.1f req/s aggregate | max %d hrs/pair",
        num_workers,
        delay,
        num_workers / delay,
        max_hours,
    )

    ctx = mp.get_context("spawn")
    processes = []
    for pair in pairs:
        p = ctx.Process(
            target=_pair_worker,
            args=(pair, max_hours, retry_errors, delay, catch_up),
            name=f"rw-{pair}",
            daemon=False,
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    logger.info("All %d parallel workers finished", len(processes))


def _pair_worker(
    pair: str,
    max_hours: int,
    retry_errors: bool,
    delay: float,
    catch_up: bool,
):
    """Fetch loop for a single pair (runs in its own process)."""
    _setup_worker_logging(pair)
    log = logging.getLogger(f"riverwriter.worker.{pair}")

    catalog = Catalog()
    session = create_session()
    pair_buffers: list[pd.DataFrame] = []
    fetches_since_write = 0
    total_fetched = 0
    total_bars = 0
    start = time.monotonic()

    remaining = max_hours

    while remaining > 0:
        batch_size = min(remaining, 500) if not catch_up else min(remaining, 1000)
        if retry_errors:
            batch = catalog.get_error_batch(pair, batch_size)
        else:
            batch = catalog.get_next_batch(pair, batch_size)

        if not batch:
            log.info("Nothing left to fetch for %s", pair)
            break

        for year, month, day, hour in batch:
            status, data = fetch_hour(session, pair, year, month, day, hour)
            catalog.update(pair, year, month, day, hour, status)
            total_fetched += 1
            remaining -= 1

            if status == "fetched" and data:
                try:
                    ticks = parse_ticks(data, pair, year, month, day, hour)
                    bars = aggregate_ticks(ticks)
                    if len(bars) > 0:
                        pair_buffers.append(bars)
                    log.info(
                        "%s %d-%02d-%02d %02dh | %d ticks → %d bars",
                        pair, year, month, day, hour, len(ticks), len(bars),
                    )
                except Exception as exc:
                    log.error(
                        "%s %d-%02d-%02d %02dh | parse error: %s",
                        pair, year, month, day, hour, exc,
                    )
                    catalog.update(pair, year, month, day, hour, "error")
            elif status == "no_data":
                log.debug("%s %d-%02d-%02d %02dh | no_data", pair, year, month, day, hour)

            fetches_since_write += 1
            if fetches_since_write >= WRITE_EVERY:
                total_bars += _flush_and_persist(pair, pair_buffers, catalog)
                fetches_since_write = 0

            time.sleep(delay)

            if remaining <= 0:
                break

        if catch_up and not batch:
            break

    total_bars += _flush_and_persist(pair, pair_buffers, catalog)

    elapsed = time.monotonic() - start
    log.info(
        "%s complete: %d hours fetched, %d bars written in %.0fs",
        pair, total_fetched, total_bars, elapsed,
    )


def _flush_and_persist(pair: str, buffers: list[pd.DataFrame], catalog: Catalog) -> int:
    """Write buffered bars and persist this pair's catalog slice."""
    if not buffers:
        Catalog.persist_pair(pair, catalog.df[catalog.df["pair"] == pair])
        return 0

    combined = pd.concat(buffers, ignore_index=True)
    write_bars(pair, combined)
    buffers.clear()

    Catalog.persist_pair(pair, catalog.df[catalog.df["pair"] == pair])
    return len(combined)


def _setup_worker_logging(pair: str):
    """Configure logging for a worker subprocess."""
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = config.LOG_DIR / f"riverwriter_{pair}.log"

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(
        f"%(asctime)s {pair} %(levelname)-5s %(message)s",
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
