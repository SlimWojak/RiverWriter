"""RIVER substrate — DuckDB views over Parquet (candidate_C).

DuckDB is the queryable layer; yearly Parquet partitions remain the durable
bar store and are updated only by writer.py's atomic merge path.
No interpretation logic — bars in, bars out.
"""

from __future__ import annotations

import logging
from typing import Any

import duckdb

from . import config

logger = logging.getLogger(__name__)

VIEW_PREFIX = "bars_"


def view_name(pair: str) -> str:
    """DuckDB view name for a pair (e.g. bars_eurusd)."""
    if pair not in config.PAIRS:
        raise ValueError(f"Unknown pair: {pair}")
    return f"{VIEW_PREFIX}{pair.lower()}"


def connect(readonly: bool = True, register: bool | None = None) -> duckdb.DuckDBPyConnection:
    """Open DuckDB connection. Views are registered only when register=True."""
    if register is None:
        register = not readonly

    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(config.DUCKDB_CATALOG_PATH), read_only=readonly)
    if register:
        register_views(conn)
    return conn


def register_views(conn: duckdb.DuckDBPyConnection, pairs: list[str] | None = None):
    """Create or replace views that union yearly Parquet files per pair."""
    pairs = pairs or list(config.PAIRS.keys())

    for pair in pairs:
        glob = str(config.PARQUET_DIR / pair / f"{pair}_*.parquet")
        vname = view_name(pair)
        pair_dir = config.PARQUET_DIR / pair

        if not pair_dir.exists() or not list(pair_dir.glob("*.parquet")):
            conn.execute(f"""
                CREATE OR REPLACE VIEW {vname} AS
                SELECT
                    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS timestamp,
                    CAST(NULL AS DOUBLE) AS open,
                    CAST(NULL AS DOUBLE) AS high,
                    CAST(NULL AS DOUBLE) AS low,
                    CAST(NULL AS DOUBLE) AS close,
                    CAST(NULL AS DOUBLE) AS volume,
                    CAST(NULL AS VARCHAR) AS source,
                    CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS knowledge_time,
                    CAST(NULL AS VARCHAR) AS bar_hash
                WHERE FALSE
            """)
            logger.debug("Registered empty view %s (no parquet yet)", vname)
            continue

        conn.execute(f"""
            CREATE OR REPLACE VIEW {vname} AS
            SELECT * FROM read_parquet('{glob}', union_by_name=true)
            ORDER BY timestamp
        """)
        logger.debug("Registered view %s ← %s", vname, glob)


def query(conn: duckdb.DuckDBPyConnection, sql: str) -> duckdb.DuckDBPyRelation:
    """Run SQL against the substrate."""
    return conn.execute(sql)


def fetch_df(conn: duckdb.DuckDBPyConnection, pair: str):
    """Load all bars for a pair as a pandas DataFrame."""
    return conn.execute(f"SELECT * FROM {view_name(pair)}").df()


def pair_summary(conn: duckdb.DuckDBPyConnection, pair: str) -> dict[str, Any]:
    """Summary stats for a pair via DuckDB."""
    vname = view_name(pair)
    row = conn.execute(f"""
        SELECT
            COUNT(*) AS total_bars,
            MIN(timestamp) AS oldest,
            MAX(timestamp) AS newest
        FROM {vname}
    """).fetchone()

    if row is None or row[0] == 0:
        return {"pair": pair, "total_bars": 0, "oldest": None, "newest": None}

    return {
        "pair": pair,
        "total_bars": int(row[0]),
        "oldest": row[1],
        "newest": row[2],
    }


def all_summaries(pairs: list[str] | None = None) -> list[dict[str, Any]]:
    """Summaries for all pairs."""
    pairs = pairs or list(config.PAIRS.keys())
    conn = connect(readonly=True)
    try:
        return [pair_summary(conn, p) for p in pairs]
    finally:
        conn.close()


def print_substrate_status(pairs: list[str] | None = None):
    """Print DuckDB substrate status."""
    summaries = all_summaries(pairs)

    print()
    print("RIVER Substrate (DuckDB → Parquet views)")
    print("=" * 70)
    print(f"Catalog: {config.DUCKDB_CATALOG_PATH}")
    print()
    print(f"{'Pair':<10} {'Bars':>12} {'Oldest':<22} {'Newest':<22}")
    print("-" * 70)

    for s in summaries:
        oldest = str(s["oldest"])[:19] if s["oldest"] else "—"
        newest = str(s["newest"])[:19] if s["newest"] else "—"
        bars = f"{s['total_bars']:,}" if s["total_bars"] else "—"
        print(f"{s['pair']:<10} {bars:>12} {oldest:<22} {newest:<22}")

    print()
