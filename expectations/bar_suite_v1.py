"""GX expectation suite v1 — ingestion boundary only (candidate_C).

Scope: raw bar invariants at the RIVER substrate layer.
Out of scope: ATOM primitives, gap analysis (see health.py), timeliness.
"""

from __future__ import annotations

import great_expectations as gx

from riverwriter import config

SUITE_NAME = config.GX_SUITE_NAME
SUITE_VERSION = config.GX_SUITE_VERSION


def build_suite() -> gx.ExpectationSuite:
    """Return the canonical bar ingestion expectation suite."""
    context = gx.get_context(mode="ephemeral")
    suite = context.suites.add(gx.ExpectationSuite(name=SUITE_NAME))

    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=list(config.BAR_COLUMNS),
        )
    )

    for col in ("timestamp", "open", "high", "low", "close", "volume", "source", "bar_hash"):
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col),
        )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="timestamp"),
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="source",
            value_set=["dukascopy"],
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="volume",
            min_value=0,
            strict_min=False,
        )
    )

    return suite
