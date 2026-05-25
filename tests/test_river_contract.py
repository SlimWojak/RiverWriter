"""Focused RIVER contract tests for the candidate_C substrate."""

from __future__ import annotations

import contextlib
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from expectations.bar_suite_v1 import build_suite
from riverwriter import config, substrate
from riverwriter.gap_analysis import _find_gaps
from riverwriter.inspect import inspect_river
from riverwriter.writer import RAW_BAR_SCHEMA, compute_bar_hashes, write_bars


@contextlib.contextmanager
def isolated_runtime():
    originals = {
        "DATA_DIR": config.DATA_DIR,
        "RAW_DIR": config.RAW_DIR,
        "PARQUET_DIR": config.PARQUET_DIR,
        "CATALOG_PATH": config.CATALOG_PATH,
        "VALIDATION_REPORT_PATH": config.VALIDATION_REPORT_PATH,
        "GX_VALIDATION_REPORT_PATH": config.GX_VALIDATION_REPORT_PATH,
        "DUCKDB_CATALOG_PATH": config.DUCKDB_CATALOG_PATH,
        "LOG_DIR": config.LOG_DIR,
    }
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        config.DATA_DIR = root / "data"
        config.RAW_DIR = config.DATA_DIR / "raw"
        config.PARQUET_DIR = config.DATA_DIR / "1m"
        config.CATALOG_PATH = config.DATA_DIR / "catalog.parquet"
        config.VALIDATION_REPORT_PATH = config.DATA_DIR / "validation_report.json"
        config.GX_VALIDATION_REPORT_PATH = config.DATA_DIR / "gx_validation_report.json"
        config.DUCKDB_CATALOG_PATH = config.DATA_DIR / "river.duckdb"
        config.LOG_DIR = root / "logs"
        yield root
    for name, value in originals.items():
        setattr(config, name, value)


class RiverContractTests(unittest.TestCase):
    def test_writer_merges_dedupes_and_enforces_schema(self):
        with isolated_runtime():
            bars = pd.DataFrame(
                [
                    {
                        "timestamp": pd.Timestamp("2026-01-05 12:00", tz="UTC"),
                        "open": 1.1,
                        "high": 1.2,
                        "low": 1.0,
                        "close": 1.15,
                        "volume": 10.0,
                        "source": "dukascopy",
                    },
                    {
                        "timestamp": pd.Timestamp("2026-01-05 12:00", tz="UTC"),
                        "open": 1.1,
                        "high": 1.2,
                        "low": 1.0,
                        "close": 1.16,
                        "volume": 11.0,
                        "source": "dukascopy",
                    },
                    {
                        "timestamp": pd.Timestamp("2026-01-05 12:01", tz="UTC"),
                        "open": 1.2,
                        "high": 1.1,
                        "low": 1.0,
                        "close": 1.15,
                        "volume": 10.0,
                        "source": "dukascopy",
                    },
                ]
            )

            write_bars("EURUSD", bars)

            out = pd.read_parquet(config.PARQUET_DIR / "EURUSD" / "EURUSD_2026.parquet")
            self.assertEqual(list(out.columns), [field.name for field in RAW_BAR_SCHEMA])
            self.assertEqual(len(out), 1)
            self.assertEqual(out.iloc[0]["close"], 1.15)
            recomputed = compute_bar_hashes(out).iloc[0]
            self.assertEqual(out.iloc[0]["bar_hash"], recomputed)

    def test_substrate_registers_empty_typed_views(self):
        with isolated_runtime():
            conn = substrate.connect(readonly=False, register=True)
            try:
                summary = substrate.pair_summary(conn, "EURUSD")
            finally:
                conn.close()

            self.assertEqual(summary["total_bars"], 0)
            self.assertIsNone(summary["oldest"])
            self.assertIsNone(summary["newest"])

    def test_gap_analysis_ignores_weekend_gap_only(self):
        weekend_df = pd.DataFrame(
            {
                "timestamp": [
                    pd.Timestamp("2026-01-02 21:59", tz="UTC"),
                    pd.Timestamp("2026-01-04 22:01", tz="UTC"),
                ]
            }
        )
        weekday_df = pd.DataFrame(
            {
                "timestamp": [
                    pd.Timestamp("2026-01-05 00:00", tz="UTC"),
                    pd.Timestamp("2026-01-05 00:10", tz="UTC"),
                ]
            }
        )

        self.assertEqual(_find_gaps(weekend_df), [])
        gaps = _find_gaps(weekday_df)
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]["minutes"], 10)

    def test_gx_suite_stays_at_bar_ingestion_boundary(self):
        suite = build_suite()
        expectation_types = {type(exp).__name__ for exp in suite.expectations}

        self.assertIn("ExpectTableColumnsToMatchOrderedList", expectation_types)
        self.assertIn("ExpectColumnValuesToBeUnique", expectation_types)
        self.assertIn("ExpectColumnValuesToBeInSet", expectation_types)
        self.assertNotIn("ExpectColumnValuesToMatchRegex", expectation_types)

    def test_inspect_river_is_passive_without_runtime_files(self):
        with isolated_runtime():
            status = inspect_river(["EURUSD"])

        self.assertFalse(status["runtime_files"]["duckdb_exists"])
        self.assertEqual(status["substrate"], [])
        self.assertIsNone(status["gx"])


if __name__ == "__main__":
    unittest.main()
