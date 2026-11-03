//! Spot-checks h3o against values from the reference H3 library. Regenerate the file with
//! data/export_h3_reference.py.

use std::str::FromStr;

use h3o::{CellIndex, LatLng, Resolution};

#[test]
fn h3o_matches_the_reference_library() {
    let mut checked = 0;
    for line in include_str!("h3_reference.csv").lines().skip(2) {
        let fields: Vec<&str> = line.split(',').collect();
        let [lat, lng, resolution, cell, neighbours] = fields[..] else { panic!("bad line: {line}") };
        let point = LatLng::new(lat.parse().unwrap(), lng.parse().unwrap()).unwrap();
        let resolution = Resolution::try_from(resolution.parse::<u8>().unwrap()).unwrap();
        let expected = CellIndex::from_str(cell).unwrap();
        assert_eq!(point.to_cell(resolution), expected, "cell for {line}");

        let mut found: Vec<String> = expected
            .grid_disk::<Vec<_>>(1)
            .into_iter()
            .filter(|neighbour| *neighbour != expected)
            .map(|neighbour| neighbour.to_string())
            .collect();
        found.sort();
        assert_eq!(found.join(" "), neighbours, "neighbours for {line}");
        checked += 1;
    }
    assert_eq!(checked, 205 * 4);
}
