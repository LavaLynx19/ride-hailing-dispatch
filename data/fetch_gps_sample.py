"""Fetch 2016 yellow taxi pickups: ten minutes from every hour of one week."""

import csv
import datetime
import json
import sys
import urllib.parse
import urllib.request

URL = "https://data.cityofnewyork.us/resource/uacg-pexx.json"
WEEK_START = datetime.datetime(2016, 3, 7)
SLICE = datetime.timedelta(minutes=10)


def fetch_slice(start):
    end = start + SLICE
    query = urllib.parse.urlencode(
        {
            "$select": "tpep_pickup_datetime,pickup_latitude,pickup_longitude",
            "$where": f"tpep_pickup_datetime >= '{start.isoformat()}' AND tpep_pickup_datetime < '{end.isoformat()}'",
            "$limit": 50000,
        }
    )
    with urllib.request.urlopen(f"{URL}?{query}", timeout=300) as response:
        return json.load(response)


def main(output):
    with open(output, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["pickup_at", "lat", "lng"])
        for hour in range(7 * 24):
            rows = fetch_slice(WEEK_START + datetime.timedelta(hours=hour))
            for row in rows:
                writer.writerow(
                    [row["tpep_pickup_datetime"], row["pickup_latitude"], row["pickup_longitude"]]
                )
            print(f"hour {hour}: {len(rows)} rows", flush=True)


if __name__ == "__main__":
    main(sys.argv[1])
