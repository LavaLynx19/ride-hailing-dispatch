import os
import tempfile
import unittest

import duckdb

from export_zone_demand import export


class ExportZoneDemandTest(unittest.TestCase):
    def test_counts_requests_per_pickup_zone(self):
        con = duckdb.connect()
        with tempfile.TemporaryDirectory() as directory:
            demand = os.path.join(directory, "demand.parquet")
            output = os.path.join(directory, "zones.csv")
            con.execute(f"COPY (SELECT * FROM (VALUES (7), (3), (7)) AS t(pickup_zone)) TO '{demand}' (FORMAT parquet)")
            export(con, demand, output)
            with open(output) as f:
                self.assertEqual(f.read().split(), ["zone,requests", "3,1", "7,2"])


if __name__ == "__main__":
    unittest.main()
