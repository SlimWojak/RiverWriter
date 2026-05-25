"""Passive RIVER status surface for local operators and future MCP inspect."""

from __future__ import annotations

import json
from typing import Any

from . import config, substrate


def inspect_river(pairs: list[str] | None = None) -> dict[str, Any]:
    """Return local substrate state without fetching, validating, or mutating data."""
    pairs = pairs or list(config.PAIRS.keys())
    return {
        "paths": {
            "data_dir": str(config.DATA_DIR),
            "parquet_dir": str(config.PARQUET_DIR),
            "duckdb_catalog": str(config.DUCKDB_CATALOG_PATH),
            "gx_report": str(config.GX_VALIDATION_REPORT_PATH),
        },
        "runtime_files": {
            "data_dir_exists": config.DATA_DIR.exists(),
            "duckdb_exists": config.DUCKDB_CATALOG_PATH.exists(),
            "catalog_exists": config.CATALOG_PATH.exists(),
            "gx_report_exists": config.GX_VALIDATION_REPORT_PATH.exists(),
        },
        "substrate": _substrate_summaries(pairs),
        "gx": _read_gx_summary(),
    }


def _substrate_summaries(pairs: list[str]) -> list[dict[str, Any]]:
    if not config.DUCKDB_CATALOG_PATH.exists():
        return []

    conn = substrate.connect(readonly=True)
    try:
        summaries = []
        for pair in pairs:
            try:
                summaries.append(substrate.pair_summary(conn, pair))
            except Exception as exc:
                summaries.append({"pair": pair, "error": str(exc)})
        return summaries
    finally:
        conn.close()


def _read_gx_summary() -> dict[str, Any] | None:
    if not config.GX_VALIDATION_REPORT_PATH.exists():
        return None

    with open(config.GX_VALIDATION_REPORT_PATH) as f:
        report = json.load(f)
    return {
        "generated_at": report.get("generated_at"),
        "suite": report.get("suite"),
        "suite_version": report.get("suite_version"),
        "pass": report.get("pass"),
    }
