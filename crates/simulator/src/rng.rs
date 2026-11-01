/// SplitMix64: seeded, reproducible and free of dependencies.
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// An independent stream, so one driver's draws don't shift another's.
    pub fn stream(seed: u64, id: u64) -> Self {
        let mut mixer = Self::new(seed ^ id.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        Self::new(mixer.next_u64())
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in [0, 1).
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_gives_same_sequence() {
        let (mut a, mut b) = (Rng::new(7), Rng::new(7));
        assert_eq!(a.next_u64(), b.next_u64());
        assert_eq!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn streams_differ_by_id() {
        assert_ne!(Rng::stream(7, 1).next_u64(), Rng::stream(7, 2).next_u64());
    }

    #[test]
    fn floats_stay_in_unit_range() {
        let mut rng = Rng::new(1);
        assert!((0..10_000).map(|_| rng.next_f64()).all(|x| (0.0..1.0).contains(&x)));
    }
}
