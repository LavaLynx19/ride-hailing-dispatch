import os
import tempfile
import unittest

import duckdb

from prepare_demand import prepare

COLUMNS = """
    request_datetime, on_scene_datetime, pickup_datetime, dropoff_datetime,
    PULocationID, DOLocationID, trip_miles, trip_time, base_passenger_fare
"""


def row(request, pickup_zone=1, dropoff_zone=2, miles=1.0, seconds=60, pickup="2026-09-10 12:05:00"):
    return (
        f"(TIMESTAMP '{request}', TIMESTAMP '{pickup}', TIMESTAMP '{pickup}', "
        f"TIMESTAMP '{pickup}' + INTERVAL 10 MINUTE, {pickup_zone}, {dropoff_zone}, {miles}, {seconds}, 10.0)"
    )


class PrepareTest(unittest.TestCase):
    def run_prepare(self, rows):
        con = duckdb.connect()
        with tempfile.TemporaryDirectory() as directory:
            source = os.path.join(directory, "source.parquet")
            output = os.path.join(directory, "output.parquet")
            con.execute(
                f"COPY (SELECT * FROM (VALUES {', '.join(rows)}) AS t({COLUMNS})) TO '{source}' (FORMAT parquet)"
            )
            prepare(con, source, output, "2026-09")
            return [r[0] for r in con.execute(f"SELECT requested_at FROM '{output}'").fetchall()]

    def test_keeps_valid_rows_sorted_by_request_time(self):
        kept = self.run_prepare([row("2026-09-10 12:02:00"), row("2026-09-10 12:01:00")])
        self.assertEqual([k.minute for k in kept], [1, 2])

    def test_drops_requests_outside_the_month(self):
        kept = self.run_prepare(
            [
                row("2026-08-31 23:59:59", pickup="2026-09-01 00:05:00"),
                row("2026-09-30 23:59:59", pickup="2026-10-01 00:05:00"),
                row("2026-10-01 00:00:00", pickup="2026-10-01 00:05:00"),
            ]
        )
        self.assertEqual([k.day for k in kept], [30])

    def test_drops_unplaceable_pickups_and_impossible_trips(self):
        kept = self.run_prepare(
            [
                row("2026-09-10 12:00:00", pickup_zone=264),
                row("2026-09-10 12:00:00", pickup_zone=265),
                row("2026-09-10 12:00:00", miles=0),
                row("2026-09-10 12:00:00", seconds=0),
                row("2026-09-10 12:10:00"),
            ]
        )
        self.assertEqual(kept, [])

    def test_keeps_trips_leaving_the_city(self):
        kept = self.run_prepare([row("2026-09-10 12:00:00", dropoff_zone=265)])
        self.assertEqual(len(kept), 1)


if __name__ == "__main__":
    unittest.main()
