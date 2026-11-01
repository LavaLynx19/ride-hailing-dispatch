use std::collections::HashMap;

/// Online drivers by zone and hour of week, derived from trip data.
#[derive(Debug, Default)]
pub struct SupplyTable {
    online: HashMap<(u16, u16), f64>,
}

impl SupplyTable {
    /// Parses `zone,hour_of_week,online_drivers` rows, with a header.
    pub fn parse(csv: &str) -> Result<Self, String> {
        let mut online = HashMap::new();
        for (index, line) in csv.lines().enumerate().skip(1) {
            let bad = || format!("bad supply row {}: {line}", index + 1);
            let mut fields = line.split(',');
            let mut next = || fields.next().ok_or_else(bad);
            let zone = next()?.parse().map_err(|_| bad())?;
            let hour = next()?.parse().map_err(|_| bad())?;
            let drivers = next()?.parse().map_err(|_| bad())?;
            online.insert((zone, hour), drivers);
        }
        Ok(Self { online })
    }

    /// `supply_scale` creates shortages or surpluses independently of demand.
    pub fn target(&self, zone: u16, hour_of_week: u16, supply_scale: f64) -> f64 {
        self.online.get(&(zone, hour_of_week)).copied().unwrap_or(0.0) * supply_scale
    }

    pub fn zones(&self) -> impl Iterator<Item = u16> + '_ {
        let unique: std::collections::BTreeSet<u16> = self.online.keys().map(|(zone, _)| *zone).collect();
        unique.into_iter()
    }

    pub fn citywide(&self, hour_of_week: u16, supply_scale: f64) -> f64 {
        self.online
            .iter()
            .filter(|((_, hour), _)| *hour == hour_of_week)
            .map(|(_, drivers)| drivers)
            .sum::<f64>()
            * supply_scale
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CSV: &str = "zone,hour_of_week,online_drivers\n1,8,10.5\n2,8,4.5\n1,9,3\n";

    #[test]
    fn target_applies_the_scale() {
        let table = SupplyTable::parse(CSV).unwrap();
        assert_eq!(table.target(1, 8, 2.0), 21.0);
        assert_eq!(table.target(99, 8, 2.0), 0.0);
    }

    #[test]
    fn citywide_sums_zones_in_that_hour() {
        assert_eq!(SupplyTable::parse(CSV).unwrap().citywide(8, 1.0), 15.0);
    }

    #[test]
    fn malformed_row_names_its_line() {
        let error = SupplyTable::parse("header\n1,x,3\n").unwrap_err();
        assert!(error.contains("row 2"));
    }
}
