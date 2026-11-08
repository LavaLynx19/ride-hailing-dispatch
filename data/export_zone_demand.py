"""Export Ride Requests per pickup zone over the prepared month, for drawing the shard map."""

import argparse

import duckdb


def export(con, demand, output):
    con.execute(
        f"""
        COPY (
            SELECT pickup_zone AS zone, count(*) AS requests
            FROM read_parquet('{demand}')
            GROUP BY pickup_zone
            ORDER BY pickup_zone
        ) TO '{output}' (HEADER)
        """
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demand", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    export(duckdb.connect(), args.demand, args.output)


if __name__ == "__main__":
    main()
