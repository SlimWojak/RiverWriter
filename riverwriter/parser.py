"""Decompresses and parses Dukascopy .bi5 binary tick data."""

from __future__ import annotations

import lzma
import logging
import struct
from datetime import datetime, timedelta, timezone

from . import config

logger = logging.getLogger(__name__)


def decompress(data: bytes) -> bytes:
    """Decompress LZMA-compressed .bi5 data."""
    try:
        return lzma.decompress(data)
    except lzma.LZMAError as exc:
        logger.error("LZMA decompression failed: %s", exc)
        raise


def parse_ticks(
    raw_bytes: bytes,
    pair: str,
    year: int,
    month: int,
    day: int,
    hour: int,
) -> list[dict]:
    """Parse binary tick data into a list of tick dicts.

    Each .bi5 contains 20-byte structs: >IIIff
    (timestamp_ms, ask_price_int, bid_price_int, ask_volume, bid_volume)
    """
    decompressed = decompress(raw_bytes)
    n_ticks = len(decompressed) // config.TICK_STRUCT_SIZE

    if len(decompressed) % config.TICK_STRUCT_SIZE != 0:
        logger.warning(
            "%s %d-%02d-%02d %02dh | decompressed size %d not divisible by %d, truncating",
            pair, year, month, day, hour, len(decompressed), config.TICK_STRUCT_SIZE,
        )

    point_value = config.PAIRS[pair]["point_value"]
    hour_start = datetime(year, month, day, hour, tzinfo=timezone.utc)
    ticks = []

    for i in range(n_ticks):
        offset = i * config.TICK_STRUCT_SIZE
        chunk = decompressed[offset : offset + config.TICK_STRUCT_SIZE]
        ts_ms, ask_int, bid_int, ask_vol, bid_vol = struct.unpack(
            config.TICK_STRUCT_FORMAT, chunk
        )

        ask = ask_int / point_value
        bid = bid_int / point_value
        mid = (ask + bid) / 2.0

        ticks.append({
            "timestamp": hour_start + timedelta(milliseconds=ts_ms),
            "ask": ask,
            "bid": bid,
            "mid": mid,
            "ask_volume": float(ask_vol),
            "bid_volume": float(bid_vol),
        })

    return ticks
