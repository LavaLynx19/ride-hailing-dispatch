"""Build weighted pickup points per taxi zone from OSM and 2016 taxi GPS pickups.

Each zone is divided into grid cells of about 100 m. A cell's weight blends its
share of observed GPS pickups with its share of OSM streets and venues. The more
GPS pickups a zone has, the more the blend trusts them, since yellow taxi data is
rich in Manhattan and sparse elsewhere.
"""

import argparse

import duckdb

GRID_DECIMALS = 3
NYC_BOUNDS = "lat BETWEEN 40.45 AND 40.95 AND lon BETWEEN -74.30 AND -73.65"
# A venue attracts more pickups than one street node. Assumed, not measured.
VENUE_WEIGHT = 5.0
# GPS pickups at which a zone's blend is half GPS and half OSM. Assumed, not measured.
GPS_HALF_TRUST_PICKUPS = 100.0
DRIVABLE_ROADS = (
    "motorway_link", "trunk", "trunk_link", "primary", "primary_link", "secondary",
    "secondary_link", "tertiary", "tertiary_link", "residential", "unclassified",
    "living_street", "service",
)


def load_zones(con, shapefile):
    con.execute(
        """
        CREATE TABLE zones AS
        SELECT LocationID AS zone,
               ST_Union_Agg(ST_Transform(geom, 'EPSG:2263', 'EPSG:4326', always_xy := true)) AS geom
        FROM st_read(?) GROUP BY LocationID
        """,
        [shapefile],
    )


def load_osm_counts(con, pbf):
    con.execute(
        f"""
        CREATE TEMP TABLE osm AS
        SELECT kind, id, lat, lon, refs, tags FROM st_readosm('{pbf}')
        WHERE kind IN ('node', 'way')
        """
    )
    con.execute(
        f"""
        CREATE TABLE osm_cells AS
        WITH nodes AS (SELECT id, lat, lon, tags FROM osm WHERE kind = 'node' AND {NYC_BOUNDS}),
        road_refs AS (
            SELECT DISTINCT unnest(refs) AS id FROM osm
            WHERE kind = 'way' AND map_extract_value(tags, 'highway') IN {DRIVABLE_ROADS}
        ),
        street AS (SELECT n.lat, n.lon, 1.0 AS score FROM nodes n JOIN road_refs r USING (id)),
        venue AS (
            SELECT lat, lon, {VENUE_WEIGHT} AS score FROM nodes
            WHERE map_contains(tags, 'amenity') OR map_contains(tags, 'shop')
               OR map_contains(tags, 'tourism') OR map_contains(tags, 'office')
               OR map_contains(tags, 'public_transport') OR map_contains(tags, 'aeroway')
        ),
        scored AS (SELECT * FROM street UNION ALL SELECT * FROM venue)
        SELECT round(lat, {GRID_DECIMALS}) AS lat, round(lon, {GRID_DECIMALS}) AS lng, sum(score) AS score
        FROM scored GROUP BY 1, 2
        """
    )
    con.execute("DROP TABLE osm")


def load_gps_counts(con, csv):
    con.execute(
        f"""
        CREATE TABLE gps_cells AS
        SELECT round(lat, {GRID_DECIMALS}) AS lat, round(lng, {GRID_DECIMALS}) AS lng, count(*)::DOUBLE AS pickups
        FROM read_csv(?, header = true)
        WHERE lat BETWEEN 40.45 AND 40.95 AND lng BETWEEN -74.30 AND -73.65
        GROUP BY 1, 2
        """,
        [csv],
    )


def build_pickup_points(con):
    """Create pickup_points(zone, lat, lng, weight) from zones, osm_cells and gps_cells."""
    con.execute(
        f"""
        CREATE TABLE pickup_points AS
        WITH cells AS (
            SELECT lat, lng, sum(score) AS score, sum(pickups) AS pickups FROM (
                SELECT lat, lng, score, 0.0 AS pickups FROM osm_cells
                UNION ALL SELECT lat, lng, 0.0, pickups FROM gps_cells
            ) GROUP BY 1, 2
        ),
        located AS (
            SELECT z.zone, c.lat, c.lng, c.score, c.pickups
            FROM cells c JOIN zones z ON ST_Within(ST_Point(c.lng, c.lat), z.geom)
        ),
        fallback AS (
            SELECT z.zone, ST_Y(ST_PointOnSurface(z.geom)) AS lat, ST_X(ST_PointOnSurface(z.geom)) AS lng,
                   1.0 AS score, 0.0 AS pickups
            FROM zones z WHERE z.zone NOT IN (SELECT zone FROM located)
        ),
        all_cells AS (SELECT * FROM located UNION ALL SELECT * FROM fallback),
        shares AS (
            SELECT zone, lat, lng,
                   coalesce(score / nullif(sum(score) OVER w, 0), 0) AS osm_share,
                   coalesce(pickups / nullif(sum(pickups) OVER w, 0), 0) AS gps_share,
                   sum(score) OVER w AS zone_score,
                   sum(pickups) OVER w AS zone_pickups
            FROM all_cells WINDOW w AS (PARTITION BY zone)
        ),
        blended AS (
            SELECT zone, lat, lng,
                   CASE
                       WHEN zone_score = 0 THEN gps_share
                       ELSE gps_share * zone_pickups / (zone_pickups + {GPS_HALF_TRUST_PICKUPS})
                          + osm_share * {GPS_HALF_TRUST_PICKUPS} / (zone_pickups + {GPS_HALF_TRUST_PICKUPS})
                   END AS weight
            FROM shares
        )
        SELECT zone, lat, lng, weight FROM blended WHERE weight > 0 ORDER BY zone, lat, lng
        """
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zones", required=True)
    parser.add_argument("--osm", required=True)
    parser.add_argument("--gps", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    load_zones(con, args.zones)
    load_osm_counts(con, args.osm)
    load_gps_counts(con, args.gps)
    build_pickup_points(con)
    con.execute(f"COPY pickup_points TO '{args.output}' (FORMAT parquet)")

    zones, points, min_points, median_points = con.execute(
        """
        SELECT count(*), sum(n), min(n), median(n)
        FROM (SELECT count(*) AS n FROM pickup_points GROUP BY zone)
        """
    ).fetchone()
    print(f"zones: {zones}, points: {points}, per zone min {min_points}, median {median_points}")


if __name__ == "__main__":
    main()
