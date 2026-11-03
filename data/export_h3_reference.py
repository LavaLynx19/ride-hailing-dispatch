"""Writes H3 values from the reference library, for checking h3o against them.

Needs the `h3` package, which nothing else uses, so it isn't in requirements.txt.
Usage: export_h3_reference.py OUTPUT_CSV
"""
import random
import sys

import h3

RESOLUTIONS = [7, 8, 9, 10]
POINTS_NYC = 200
# Far from NYC: high latitude, the antimeridian, near a pentagon, and the southern hemisphere.
POINTS_ELSEWHERE = [(78.2, 15.6), (-17.7, 179.9), (64.7, 10.5), (-33.9, 151.2), (0.0, 0.0)]


def main():
    rng = random.Random(26)
    points = [(rng.uniform(40.49, 40.92), rng.uniform(-74.27, -73.68)) for _ in range(POINTS_NYC)]
    points += POINTS_ELSEWHERE
    with open(sys.argv[1], "w") as out:
        out.write(f"# h3 {h3.__version__}\n")
        out.write("lat,lng,resolution,cell,sorted_neighbours\n")
        for lat, lng in points:
            for resolution in RESOLUTIONS:
                cell = h3.latlng_to_cell(lat, lng, resolution)
                neighbours = sorted(set(h3.grid_disk(cell, 1)) - {cell})
                out.write(f"{lat!r},{lng!r},{resolution},{cell},{' '.join(neighbours)}\n")


if __name__ == "__main__":
    main()
