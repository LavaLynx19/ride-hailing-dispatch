"""Export one window of prepared demand as CSV for the simulator."""

import argparse

import duckdb


def export(con, demand, start, end, output):
    con.execute(
        f"""
        COPY (
            SELECT epoch(requested_at)::BIGINT AS requested_at, pickup_zone, dropoff_zone, trip_seconds
            FROM read_parquet('{demand}')
            WHERE requested_at >= TIMESTAMP '{start}' AND requested_at < TIMESTAMP '{end}'
            ORDER BY requested_at
        ) TO '{output}' (HEADER)
        """
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demand", required=True)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    export(duckdb.connect(), args.demand, args.start, args.end, args.output)


if __name__ == "__main__":
    main()
