"""Prepare one month of Ride Requests from TLC HVFHV trip records."""

import argparse
import datetime

import duckdb

# TLC reserves zones 264 and 265 for unknown and outside-NYC locations. Only the
# pickup has to be placeable: a trip leaving the city is still a real Ride Request.
UNKNOWN_ZONES = (264, 265)


def month_bounds(month):
    start = datetime.datetime.strptime(month, "%Y-%m")
    end = (start + datetime.timedelta(days=32)).replace(day=1)
    return start, end


def prepare(con, source, output, month):
    start, end = month_bounds(month)
    con.execute(
        f"""
        COPY (
            SELECT
                request_datetime AS requested_at,
                on_scene_datetime AS on_scene_at,
                pickup_datetime AS pickup_at,
                dropoff_datetime AS dropoff_at,
                PULocationID AS pickup_zone,
                DOLocationID AS dropoff_zone,
                trip_miles,
                trip_time AS trip_seconds,
                base_passenger_fare
            FROM read_parquet(?)
            WHERE request_datetime >= ? AND request_datetime < ?
                AND pickup_datetime >= request_datetime
                AND dropoff_datetime > pickup_datetime
                AND PULocationID NOT IN {UNKNOWN_ZONES}
                AND trip_miles > 0
                AND trip_time > 0
            ORDER BY requested_at
        ) TO '{output}' (FORMAT parquet)
        """,
        [source, start, end],
    )


def summarise(con, source, output):
    total = con.execute("SELECT count(*) FROM read_parquet(?)", [source]).fetchone()[0]
    kept, first, last = con.execute(
        "SELECT count(*), min(requested_at), max(requested_at) FROM read_parquet(?)", [output]
    ).fetchone()
    print(f"source rows: {total}")
    print(f"kept rows:   {kept} ({kept / total:.2%})")
    print(f"requests:    {first} to {last}")
    print(f"mean/s:      {kept / (last - first).total_seconds():.1f}")
    for label, truncate in (("second", "second"), ("minute", "minute")):
        peak = con.execute(
            f"""
            SELECT count(*) AS n FROM read_parquet(?)
            GROUP BY date_trunc('{truncate}', requested_at) ORDER BY n DESC LIMIT 1
            """,
            [output],
        ).fetchone()[0]
        print(f"peak/{label}: {peak}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()

    con = duckdb.connect()
    prepare(con, args.source, args.output, args.month)
    summarise(con, args.source, args.output)


if __name__ == "__main__":
    main()
