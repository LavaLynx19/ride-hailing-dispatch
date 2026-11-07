//! Auction solver for one Batch Window: assigns Ride Requests (bidders) to idle drivers
//! (objects) to match as many as possible, then minimise total pickup cost.
//!
//! Each ride may also stay unassigned, at a value of zero. Its value for a driver is
//! `UNMATCHED_PENALTY - cost`, so matching more rides always beats a cheaper pickup. Prices
//! start at zero and only rise, so the result is within `bidders * epsilon` of the optimum
//! over the candidates given. There is no epsilon scaling: with rides able to stay
//! unassigned, later phases would start from prices that break that bound.

use std::time::Instant;

/// Larger than any pickup cost, in the same unit as costs.
const UNMATCHED_PENALTY: f64 = 1.0e6;
/// How many bids run between deadline checks.
const BIDS_PER_CHECK: usize = 64;

#[derive(Debug, Clone, PartialEq)]
pub struct Solution {
    /// The object each bidder got, if any.
    pub assigned: Vec<Option<usize>>,
    /// False when the deadline stopped bidding early. What is assigned is still valid.
    pub complete: bool,
}

/// `candidates[bidder]` lists `(object, cost)` pairs, with objects below `objects`.
pub fn solve(objects: usize, candidates: &[Vec<(usize, f64)>], epsilon: f64, deadline: Option<Instant>) -> Solution {
    let mut prices = vec![0.0_f64; objects];
    let mut owner: Vec<Option<usize>> = vec![None; objects];
    let mut assigned: Vec<Option<usize>> = vec![None; candidates.len()];
    let mut unassigned: Vec<usize> = (0..candidates.len()).rev().collect();
    let mut bids = 0_usize;

    while let Some(bidder) = unassigned.pop() {
        bids += 1;
        if bids.is_multiple_of(BIDS_PER_CHECK) && deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Solution { assigned, complete: false };
        }
        // Staying unassigned is always on offer, at zero.
        let mut best: Option<(usize, f64)> = None;
        let mut second = 0.0_f64;
        for &(object, cost) in &candidates[bidder] {
            let value = UNMATCHED_PENALTY - cost - prices[object];
            match best {
                Some((_, best_value)) if value <= best_value => second = second.max(value),
                _ => {
                    if let Some((_, best_value)) = best {
                        second = second.max(best_value);
                    }
                    best = Some((object, value));
                }
            }
        }
        let Some((object, value)) = best.filter(|&(_, value)| value >= 0.0) else { continue };
        prices[object] += value - second + epsilon;
        if let Some(previous) = owner[object].replace(bidder) {
            assigned[previous] = None;
            unassigned.push(previous);
        }
        assigned[bidder] = Some(object);
    }
    Solution { assigned, complete: true }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn total(candidates: &[Vec<(usize, f64)>], assigned: &[Option<usize>]) -> (usize, f64) {
        let mut matched = 0;
        let mut cost = 0.0;
        for (bidder, object) in assigned.iter().enumerate() {
            if let Some(object) = object {
                matched += 1;
                cost += candidates[bidder].iter().find(|(o, _)| o == object).expect("assigned a candidate").1;
            }
        }
        (matched, cost)
    }

    fn assert_valid(objects: usize, candidates: &[Vec<(usize, f64)>], solution: &Solution) {
        let mut used = vec![false; objects];
        for (bidder, object) in solution.assigned.iter().enumerate() {
            if let Some(object) = *object {
                assert!(candidates[bidder].iter().any(|(o, _)| *o == object), "bidder {bidder} got a non-candidate");
                assert!(!used[object], "object {object} assigned twice");
                used[object] = true;
            }
        }
    }

    /// The best (most matched, then cheapest) assignment by trying every one.
    fn brute_force(objects: usize, candidates: &[Vec<(usize, f64)>]) -> (usize, f64) {
        fn go(bidder: usize, used: &mut [bool], candidates: &[Vec<(usize, f64)>], matched: usize, cost: f64, best: &mut (usize, f64)) {
            if bidder == candidates.len() {
                if matched > best.0 || (matched == best.0 && cost < best.1) {
                    *best = (matched, cost);
                }
                return;
            }
            go(bidder + 1, used, candidates, matched, cost, best);
            for &(object, c) in &candidates[bidder] {
                if !used[object] {
                    used[object] = true;
                    go(bidder + 1, used, candidates, matched + 1, cost + c, best);
                    used[object] = false;
                }
            }
        }
        let mut best = (0, 0.0);
        go(0, &mut vec![false; objects], candidates, 0, 0.0, &mut best);
        best
    }

    #[test]
    fn swaps_where_greedy_would_not() {
        // r1: d1 1 min, d2 4 min. r2: d1 3 min, d2 15 min. Greedy in arrival order gives 1 + 15.
        let candidates = vec![vec![(0, 1.0), (1, 4.0)], vec![(0, 3.0), (1, 15.0)]];
        let solution = solve(2, &candidates, 0.001, None);
        assert_eq!(solution.assigned, [Some(1), Some(0)]);
        assert_eq!(total(&candidates, &solution.assigned), (2, 7.0));
    }

    #[test]
    fn more_rides_than_drivers_matches_the_cheapest_and_ends() {
        let candidates = vec![vec![(0, 5.0)], vec![(0, 2.0)], vec![(0, 9.0)]];
        let solution = solve(1, &candidates, 0.001, None);
        assert!(solution.complete);
        assert_eq!(solution.assigned, [None, Some(0), None]);
    }

    #[test]
    fn matching_more_rides_beats_a_cheaper_total() {
        // r1 alone on d1 costs 1; but then r2, who can only use d1, is left out.
        let candidates = vec![vec![(0, 1.0), (1, 50.0)], vec![(0, 2.0)]];
        let solution = solve(2, &candidates, 0.001, None);
        assert_eq!(solution.assigned, [Some(1), Some(0)]);
    }

    #[test]
    fn a_ride_without_candidates_stays_unassigned() {
        let solution = solve(1, &[vec![], vec![(0, 1.0)]], 0.001, None);
        assert_eq!(solution.assigned, [None, Some(0)]);
    }

    #[test]
    fn matches_brute_force_on_small_random_cases() {
        let mut state = 0x2545_F491_4F6C_DD1D_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let epsilon = 0.001;
        for case in 0..500 {
            let bidders = 1 + (next() % 6) as usize;
            let objects = 1 + (next() % 6) as usize;
            let mut candidates: Vec<Vec<(usize, f64)>> = vec![Vec::new(); bidders];
            for list in &mut candidates {
                for object in 0..objects {
                    if next() % 3 != 0 {
                        // Whole minutes give ties, which is where auctions go wrong.
                        list.push((object, (next() % 20) as f64));
                    }
                }
            }
            let solution = solve(objects, &candidates, epsilon, None);
            assert!(solution.complete);
            assert_valid(objects, &candidates, &solution);
            let (matched, cost) = total(&candidates, &solution.assigned);
            let (best_matched, best_cost) = brute_force(objects, &candidates);
            assert_eq!(matched, best_matched, "case {case}: {candidates:?}");
            assert!(cost <= best_cost + bidders as f64 * epsilon + 1e-9, "case {case}: {cost} vs {best_cost} for {candidates:?}");
        }
    }

    #[test]
    fn a_passed_deadline_returns_a_valid_partial_answer() {
        let candidates: Vec<Vec<(usize, f64)>> = (0..500).map(|_| (0..50).map(|o| (o, 1.0)).collect()).collect();
        let solution = solve(50, &candidates, 0.001, Some(Instant::now()));
        assert!(!solution.complete);
        assert_valid(50, &candidates, &solution);
    }
}
