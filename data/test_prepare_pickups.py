import unittest

import duckdb

from prepare_pickups import GPS_HALF_TRUST_PICKUPS, build_pickup_points


def square(west, south, size=0.01):
    east, north = west + size, south + size
    return f"POLYGON(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))"


class BuildPickupPointsTest(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()
        self.con.execute("INSTALL spatial; LOAD spatial;")
        self.con.execute("CREATE TABLE zones (zone INT, geom GEOMETRY)")
        self.con.execute("CREATE TABLE osm_cells (lat DOUBLE, lng DOUBLE, score DOUBLE)")
        self.con.execute("CREATE TABLE gps_cells (lat DOUBLE, lng DOUBLE, pickups DOUBLE)")

    def add_zone(self, zone, west, south):
        self.con.execute("INSERT INTO zones VALUES (?, ST_GeomFromText(?))", [zone, square(west, south)])

    def weights(self, zone):
        build_pickup_points(self.con)
        rows = self.con.execute(
            "SELECT lat, weight FROM pickup_points WHERE zone = ? ORDER BY lat", [zone]
        ).fetchall()
        return rows

    def test_zone_without_gps_follows_osm(self):
        self.add_zone(1, -74.0, 40.0)
        self.con.execute("INSERT INTO osm_cells VALUES (40.002, -73.995, 1), (40.004, -73.995, 3)")

        self.assertEqual([w for _, w in self.weights(1)], [0.25, 0.75])

    def test_gps_rich_zone_mostly_follows_gps(self):
        self.add_zone(1, -74.0, 40.0)
        self.con.execute("INSERT INTO osm_cells VALUES (40.002, -73.995, 9), (40.004, -73.995, 1)")
        pickups = GPS_HALF_TRUST_PICKUPS * 99
        self.con.execute("INSERT INTO gps_cells VALUES (40.004, -73.995, ?)", [pickups])

        weights = self.weights(1)

        self.assertLess(weights[0][1], 0.01)
        self.assertGreater(weights[1][1], 0.99)

    def test_weights_sum_to_one_in_each_zone(self):
        self.add_zone(1, -74.0, 40.0)
        self.add_zone(2, -74.1, 40.0)
        self.con.execute("INSERT INTO osm_cells VALUES (40.002, -73.995, 2), (40.004, -74.095, 5)")
        self.con.execute("INSERT INTO gps_cells VALUES (40.006, -73.995, 7)")

        build_pickup_points(self.con)
        totals = self.con.execute("SELECT sum(weight) FROM pickup_points GROUP BY zone").fetchall()

        for (total,) in totals:
            self.assertAlmostEqual(total, 1.0)

    def test_empty_zone_gets_one_point_inside_it(self):
        self.add_zone(1, -74.0, 40.0)

        weights = self.weights(1)

        self.assertEqual(len(weights), 1)
        self.assertEqual(weights[0][1], 1.0)
        self.assertTrue(40.0 < weights[0][0] < 40.01)

    def test_cells_outside_every_zone_are_ignored(self):
        self.add_zone(1, -74.0, 40.0)
        self.con.execute("INSERT INTO osm_cells VALUES (41.0, -73.995, 5), (40.002, -73.995, 1)")

        self.assertEqual(len(self.weights(1)), 1)


if __name__ == "__main__":
    unittest.main()
