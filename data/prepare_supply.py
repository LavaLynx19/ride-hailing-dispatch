"""Derive driver supply per zone and hour of week, and each zone's city exit point."""

import argparse

import duckdb

# Share of an online driver's time spent on a trip. Assumed, not measured.
UTILISATION = 0.6


def build_supply(con, demand):
    """Busy time is attributed to the request's hour and pickup zone."""
    con.execute(
        f"""
        CREATE TABLE supply AS
        WITH trips AS (
            SELECT pickup_zone AS zone,
                   (isodow(requested_at) - 1) * 24 + hour(requested_at) AS hour_of_week,
                   date_trunc('hour', requested_at) AS hour_start,
                   date_diff('second', requested_at, dropoff_at) AS busy_seconds
            FROM read_parquet('{demand}')
        ),
        occurrences AS (
            SELECT hour_of_week, count(DISTINCT hour_start) AS hours FROM trips GROUP BY 1
        )
        SELECT t.zone, t.hour_of_week,
               sum(t.busy_seconds) / (3600.0 * o.hours) / {UTILISATION} AS online_drivers
        FROM trips t JOIN occurrences o USING (hour_of_week)
        GROUP BY t.zone, t.hour_of_week, o.hours
        ORDER BY 1, 2
        """
    )


def build_zone_exits(con, shapefile):
    con.execute(
        f"""
        CREATE TABLE zone_exits AS
        WITH zones AS (
            SELECT LocationID AS zone,
                   ST_Union_Agg(ST_Transform(geom, 'EPSG:2263', 'EPSG:4326', always_xy := true)) AS geom
            FROM st_read('{shapefile}') GROUP BY LocationID
        ),
        city AS (SELECT ST_Boundary(ST_Union_Agg(geom)) AS edge FROM zones),
        nearest AS (
            SELECT z.zone, ST_EndPoint(ST_ShortestLine(ST_PointOnSurface(z.geom), c.edge)) AS point
            FROM zones z, city c
        )
        SELECT zone, ST_Y(point) AS lat, ST_X(point) AS lng FROM nearest ORDER BY zone
        """
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demand", required=True)
    parser.add_argument("--zones", required=True)
    parser.add_argument("--pickup-points", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    build_supply(con, args.demand)
    build_zone_exits(con, args.zones)
    con.execute(f"COPY supply TO '{args.output_dir}/supply.csv' (HEADER)")
    con.execute(f"COPY zone_exits TO '{args.output_dir}/zone_exits.csv' (HEADER)")
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{args.pickup_points}')) TO '{args.output_dir}/pickup_points.csv' (HEADER)"
    )

    lowest, median, highest = con.execute(
        """
        SELECT min(total), median(total), max(total)
        FROM (SELECT sum(online_drivers) AS total FROM supply GROUP BY hour_of_week)
        """
    ).fetchone()
    print(f"online drivers citywide: lowest {lowest:.0f}, median {median:.0f}, highest {highest:.0f}")


if __name__ == "__main__":
    main()
