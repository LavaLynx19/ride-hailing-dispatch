import os
import tempfile
import unittest

import duckdb

from prepare_supply import UTILISATION, build_supply


class BuildSupplyTest(unittest.TestCase):
    def supply(self, trips):
        con = duckdb.connect()
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "demand.parquet")
            values = ", ".join(
                f"(TIMESTAMP '{requested}', TIMESTAMP '{dropoff}', {zone})" for requested, dropoff, zone in trips
            )
            con.execute(
                f"COPY (SELECT * FROM (VALUES {values}) AS t(requested_at, dropoff_at, pickup_zone)) TO '{path}' (FORMAT parquet)"
            )
            build_supply(con, path)
        return con.execute("SELECT zone, hour_of_week, online_drivers FROM supply ORDER BY 1, 2").fetchall()

    def test_one_busy_hour_needs_one_driver_over_utilisation(self):
        rows = self.supply([("2026-09-07 08:00:00", "2026-09-07 09:00:00", 5)])
        self.assertEqual(rows[0][:2], (5, 8))
        self.assertAlmostEqual(rows[0][2], 1 / UTILISATION)

    def test_averages_over_each_occurrence_of_the_hour(self):
        rows = self.supply(
            [
                ("2026-09-07 08:00:00", "2026-09-07 09:00:00", 5),
                ("2026-09-14 08:00:00", "2026-09-14 08:00:00", 6),
            ]
        )
        self.assertAlmostEqual(dict((r[0], r[2]) for r in rows)[5], 0.5 / UTILISATION)


if __name__ == "__main__":
    unittest.main()
