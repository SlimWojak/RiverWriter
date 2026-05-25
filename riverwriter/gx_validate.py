"""GX checkpoint — authoritative ingestion-boundary validation (candidate_C).

Uses ephemeral GX context (no Data Docs bloat). DuckDB SQL handles OHLC
integrity checks that GX's UnexpectedRowsExpectation handles poorly on
pandas batches.

Authoritative for: "can ATOM consume this data?"
Operational only (not GX): timeliness, gap analysis → health.py
Construction-time fast gate: writer.py
"""

from __future__ import annotations

import json
import logging
import warnings
from datetime import datetime, timezone

import great_expectations as gx
from expectations.bar_suite_v1 import build_suite

from . import config
from . import substrate

logger = logging.getLogger(__name__)

# OHLC integrity via DuckDB — single source of truth for this check.
OHLC_INTEGRITY_SQL = """
SELECT COUNT(*) FROM {view}
WHERE high < low
   OR high < open OR high < close
   OR low > open OR low > close
"""


def run_checkpoint(pairs: list[str] | None = None) -> dict:
    """Run GX suite + substrate SQL checks for each pair. Returns report dict."""
    pairs = pairs or list(config.PAIRS.keys())

    # Ensure DuckDB views are current
    conn = substrate.connect(readonly=False, register=True)

    suite = _load_suite()
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "suite": config.GX_SUITE_NAME,
        "suite_version": config.GX_SUITE_VERSION,
        "scope": "ingestion_boundary_only",
        "pairs": {},
        "pass": True,
    }

    for pair in pairs:
        pair_report = _validate_pair(conn, pair, suite)
        report["pairs"][pair] = pair_report
        if not pair_report["pass"]:
            report["pass"] = False

    conn.close()

    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(config.GX_VALIDATION_REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(
        "GX checkpoint %s → %s",
        "PASS" if report["pass"] else "FAIL",
        config.GX_VALIDATION_REPORT_PATH,
    )
    return report


def _load_suite() -> gx.ExpectationSuite:
    """Load expectation suite from expectations/ module."""
    # Suppress GX registration noise on import
    logging.getLogger("great_expectations").setLevel(logging.WARNING)
    return build_suite()


def _validate_pair(
    conn,
    pair: str,
    suite: gx.ExpectationSuite,
) -> dict:
    """Validate one pair: GX expectations + DuckDB OHLC check."""
    vname = substrate.view_name(pair)
    summary = substrate.pair_summary(conn, pair)

    if summary["total_bars"] == 0:
        return {"status": "no_data", "pass": False, "total_bars": 0}

    df = conn.execute(f"SELECT * FROM {vname}").df()

    gx_result = _run_gx_batch(df, suite)
    ohlc_count = conn.execute(
        OHLC_INTEGRITY_SQL.format(view=vname)
    ).fetchone()[0]

    ohlc_pass = int(ohlc_count) == 0
    gx_pass = gx_result["success"]

    return {
        "status": "pass" if (gx_pass and ohlc_pass) else "fail",
        "pass": gx_pass and ohlc_pass,
        "total_bars": summary["total_bars"],
        "date_range": {
            "start": str(summary["oldest"]),
            "end": str(summary["newest"]),
        },
        "gx": gx_result,
        "substrate_checks": {
            "ohlc_integrity": {
                "violations": int(ohlc_count),
                "pass": ohlc_pass,
            },
        },
    }


def _run_gx_batch(df, suite: gx.ExpectationSuite) -> dict:
    """Run GX expectation suite on a pandas batch."""
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    logging.getLogger("great_expectations").setLevel(logging.ERROR)

    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas(name="river")
    asset = ds.add_dataframe_asset(name="bars")
    batch_def = asset.add_batch_definition_whole_dataframe(name="full")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    validator = context.get_validator(batch=batch, expectation_suite=suite)
    result = validator.validate()

    expectations = []
    for r in result.results:
        exp_type = r.expectation_config.type
        expectations.append({
            "expectation": exp_type,
            "pass": r.success,
        })

    return {
        "success": result.success,
        "evaluated": len(expectations),
        "expectations": expectations,
    }


def print_report(report: dict):
    """Print human-readable GX checkpoint report."""
    print()
    print("RIVER GX Checkpoint (ingestion boundary)")
    print("=" * 60)
    print(f"Suite:     {report['suite']} v{report['suite_version']}")
    print(f"Generated: {report['generated_at'][:19]}")
    print(f"Overall:   {'PASS' if report['pass'] else 'FAIL'}")
    print()

    for pair, data in report["pairs"].items():
        if data.get("status") == "no_data":
            print(f"  {pair}: NO DATA — FAIL")
            continue

        status = "PASS" if data["pass"] else "FAIL"
        print(f"  {pair}: {status} ({data['total_bars']:,} bars)")
        if data.get("date_range"):
            print(f"    Range: {data['date_range']['start'][:10]} → {data['date_range']['end'][:10]}")

        gx = data.get("gx", {})
        failed = [e for e in gx.get("expectations", []) if not e["pass"]]
        if failed:
            print(f"    GX failures: {len(failed)}")
            for e in failed:
                print(f"      ✗ {e['expectation']}")

        ohlc = data.get("substrate_checks", {}).get("ohlc_integrity", {})
        if not ohlc.get("pass"):
            print(f"    OHLC violations: {ohlc.get('violations', '?')}")

    print()
