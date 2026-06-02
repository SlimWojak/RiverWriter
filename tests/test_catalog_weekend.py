"""Forex weekend-closure boundary tests for catalog._is_weekend_closed.

Forex week is anchored at 17:00 America/New_York (DST-aware), matching
IBKR-live and ATOM canon. dt is a UTC datetime.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from riverwriter.catalog import _is_weekend_closed


def _utc(y, m, d, hh, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=timezone.utc)


class WeekendBoundaryTests(unittest.TestCase):
    # --- Summer (EDT, UTC-4): 17:00 NY = 21:00 UTC ---

    def test_summer_sunday_open_band_is_open(self):
        # 2026-06-07 is a Sunday. 21:30 UTC = 17:30 EDT -> week is OPEN.
        # (was wrongly CLOSED under the old fixed 22:00 UTC rule)
        self.assertFalse(_is_weekend_closed(_utc(2026, 6, 7, 21, 30)))

    def test_summer_sunday_before_open_is_closed(self):
        # 20:30 UTC = 16:30 EDT, before the 17:00 NY open -> CLOSED.
        self.assertTrue(_is_weekend_closed(_utc(2026, 6, 7, 20, 30)))

    def test_summer_friday_after_close_is_closed(self):
        # 2026-06-05 is a Friday. 21:30 UTC = 17:30 EDT, at/after close -> CLOSED.
        self.assertTrue(_is_weekend_closed(_utc(2026, 6, 5, 21, 30)))

    def test_summer_friday_before_close_is_open(self):
        # 20:30 UTC = 16:30 EDT, before the 17:00 NY close -> OPEN.
        self.assertFalse(_is_weekend_closed(_utc(2026, 6, 5, 20, 30)))

    # --- Winter (EST, UTC-5): 17:00 NY = 22:00 UTC ---

    def test_winter_sunday_open_band_is_open(self):
        # 2026-01-04 is a Sunday. 22:30 UTC = 17:30 EST -> OPEN.
        self.assertFalse(_is_weekend_closed(_utc(2026, 1, 4, 22, 30)))

    def test_winter_sunday_before_open_is_closed(self):
        # 21:30 UTC = 16:30 EST, before the 17:00 NY open -> CLOSED.
        self.assertTrue(_is_weekend_closed(_utc(2026, 1, 4, 21, 30)))

    def test_winter_friday_after_close_is_closed(self):
        # 2026-01-02 is a Friday. 22:30 UTC = 17:30 EST, at/after close -> CLOSED.
        self.assertTrue(_is_weekend_closed(_utc(2026, 1, 2, 22, 30)))

    def test_winter_friday_before_close_is_open(self):
        # 21:30 UTC = 16:30 EST, before the 17:00 NY close -> OPEN.
        self.assertFalse(_is_weekend_closed(_utc(2026, 1, 2, 21, 30)))

    # --- Saturday always closed (both seasons) ---

    def test_saturday_summer_always_closed(self):
        # 2026-06-06 Saturday.
        self.assertTrue(_is_weekend_closed(_utc(2026, 6, 6, 0, 0)))
        self.assertTrue(_is_weekend_closed(_utc(2026, 6, 6, 23, 59)))

    def test_saturday_winter_always_closed(self):
        # 2026-01-03 Saturday.
        self.assertTrue(_is_weekend_closed(_utc(2026, 1, 3, 0, 0)))
        self.assertTrue(_is_weekend_closed(_utc(2026, 1, 3, 23, 59)))

    # --- Midweek sanity ---

    def test_midweek_is_open(self):
        # 2026-06-03 Wednesday, 2026-01-07 Wednesday.
        self.assertFalse(_is_weekend_closed(_utc(2026, 6, 3, 12, 0)))
        self.assertFalse(_is_weekend_closed(_utc(2026, 1, 7, 12, 0)))


if __name__ == "__main__":
    unittest.main()
