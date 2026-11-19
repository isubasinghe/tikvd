use std::collections::{BTreeMap, HashSet};

use tikv_client::RegionRoute;
use tikv_client::proto::metapb::RegionEpoch;

pub type KeyRange = (Vec<u8>, Vec<u8>);

#[derive(Debug, Clone, PartialEq)]
pub struct FreshRegion {
    pub id: u64,
    pub range: KeyRange,
    pub epoch: RegionEpoch,
    pub store_address: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Coverage {
    pub range: KeyRange,
    pub epoch: RegionEpoch,
    pub checkpoint: u64,
    pub live: bool,
}

#[derive(Debug, PartialEq)]
pub struct Replacement {
    pub dropped: Vec<u64>,
    pub registered: Vec<(u64, u64)>,
}

pub fn clamp_to(range: &KeyRange, region_start: &[u8], region_end: &[u8]) -> Option<KeyRange> {
    let start = range.0.as_slice().max(region_start);
    let end = match region_end {
        [] => range.1.as_slice(),
        _ => range.1.as_slice().min(region_end),
    };
    (start < end).then(|| (start.to_vec(), end.to_vec()))
}

pub fn fresh_region(watched: &KeyRange, route: RegionRoute) -> Option<FreshRegion> {
    let range = clamp_to(watched, &route.region.start_key, &route.region.end_key)?;
    Some(FreshRegion {
        id: route.region.id,
        range,
        epoch: route.region.region_epoch.unwrap_or_default(),
        store_address: route.store_address,
    })
}

pub fn plan_replacement(
    tracked: &BTreeMap<u64, Coverage>,
    fresh: &[FreshRegion],
    fallback_checkpoint: u64,
) -> Replacement {
    let registered = fresh
        .iter()
        .filter(|region| !is_live_at(tracked.get(&region.id), &region.epoch))
        .map(|region| {
            let checkpoint = lowest_overlapping_checkpoint(tracked, &region.range);
            (region.id, checkpoint.unwrap_or(fallback_checkpoint))
        })
        .collect();
    let fresh_ids: HashSet<u64> = fresh.iter().map(|region| region.id).collect();
    let dropped = tracked
        .iter()
        .filter(|(id, region)| !fresh_ids.contains(id) && is_covered(&region.range, fresh))
        .map(|(id, _)| *id)
        .collect();
    Replacement {
        dropped,
        registered,
    }
}

fn is_live_at(region: Option<&Coverage>, epoch: &RegionEpoch) -> bool {
    region.is_some_and(|region| region.live && region.epoch == *epoch)
}

fn lowest_overlapping_checkpoint(
    tracked: &BTreeMap<u64, Coverage>,
    range: &KeyRange,
) -> Option<u64> {
    tracked
        .values()
        .filter(|region| overlaps(&region.range, range))
        .map(|region| region.checkpoint)
        .min()
}

fn overlaps(a: &KeyRange, b: &KeyRange) -> bool {
    a.0 < b.1 && b.0 < a.1
}

pub fn is_covered(range: &KeyRange, fresh: &[FreshRegion]) -> bool {
    let mut ranges: Vec<&KeyRange> = fresh.iter().map(|region| &region.range).collect();
    ranges.sort();
    let mut covered_to = range.0.as_slice();
    for (start, end) in ranges {
        if start.as_slice() <= covered_to && end.as_slice() > covered_to {
            covered_to = end;
        }
    }
    covered_to >= range.1.as_slice()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn watched() -> KeyRange {
        (b"k/".to_vec(), b"k0".to_vec())
    }

    #[test]
    fn region_covering_everything_is_clamped_to_the_watched_range() {
        assert_eq!(clamp_to(&watched(), b"", b""), Some(watched()));
    }

    #[test]
    fn region_inside_the_watched_range_keeps_its_bounds() {
        assert_eq!(
            clamp_to(&watched(), b"k/a", b"k/m"),
            Some((b"k/a".to_vec(), b"k/m".to_vec()))
        );
    }

    #[test]
    fn region_straddling_an_edge_is_cut_at_it() {
        assert_eq!(
            clamp_to(&watched(), b"j", b"k/m"),
            Some((b"k/".to_vec(), b"k/m".to_vec()))
        );
        assert_eq!(
            clamp_to(&watched(), b"k/m", b""),
            Some((b"k/m".to_vec(), b"k0".to_vec()))
        );
    }

    #[test]
    fn region_outside_the_watched_range_is_none() {
        assert_eq!(clamp_to(&watched(), b"", b"k/"), None);
        assert_eq!(clamp_to(&watched(), b"k0", b""), None);
        assert_eq!(clamp_to(&watched(), b"x", b"y"), None);
    }

    fn epoch(version: u64) -> RegionEpoch {
        RegionEpoch {
            conf_ver: 1,
            version,
        }
    }

    fn range(start: &str, end: &str) -> KeyRange {
        (start.as_bytes().to_vec(), end.as_bytes().to_vec())
    }

    fn tracked(start: &str, end: &str, version: u64, checkpoint: u64, live: bool) -> Coverage {
        Coverage {
            range: range(start, end),
            epoch: epoch(version),
            checkpoint,
            live,
        }
    }

    fn fresh(id: u64, start: &str, end: &str, version: u64) -> FreshRegion {
        FreshRegion {
            id,
            range: range(start, end),
            epoch: epoch(version),
            store_address: "tikv:20160".to_owned(),
        }
    }

    #[test]
    fn initial_lookup_registers_everything_at_the_start_timestamp() {
        let plan = plan_replacement(
            &BTreeMap::new(),
            &[fresh(1, "a", "m", 1), fresh(2, "m", "z", 1)],
            100,
        );
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![],
                registered: vec![(1, 100), (2, 100)],
            }
        );
    }

    #[test]
    fn leader_change_re_registers_the_same_region_from_its_checkpoint() {
        let regions = BTreeMap::from([
            (1, tracked("a", "m", 1, 300, false)),
            (2, tracked("m", "z", 1, 500, true)),
        ]);
        let plan = plan_replacement(&regions, &[fresh(1, "a", "m", 1)], 0);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![],
                registered: vec![(1, 300)],
            }
        );
    }

    #[test]
    fn split_registers_both_halves_from_the_old_checkpoint() {
        let regions = BTreeMap::from([
            (1, tracked("a", "m", 1, 300, false)),
            (2, tracked("m", "z", 1, 500, true)),
        ]);
        let plan = plan_replacement(&regions, &[fresh(1, "a", "f", 2), fresh(3, "f", "m", 2)], 0);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![],
                registered: vec![(1, 300), (3, 300)],
            }
        );
    }

    #[test]
    fn split_where_the_old_id_goes_away_drops_it() {
        let regions = BTreeMap::from([(1, tracked("a", "m", 1, 300, false))]);
        let plan = plan_replacement(&regions, &[fresh(4, "a", "f", 2), fresh(3, "f", "m", 2)], 0);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![1],
                registered: vec![(4, 300), (3, 300)],
            }
        );
    }

    #[test]
    fn merge_inherits_the_lowest_checkpoint_of_what_it_covers() {
        let regions = BTreeMap::from([
            (1, tracked("a", "m", 1, 300, false)),
            (2, tracked("m", "z", 1, 500, true)),
        ]);
        let plan = plan_replacement(&regions, &[fresh(2, "a", "z", 2)], 0);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![1],
                registered: vec![(2, 300)],
            }
        );
    }

    #[test]
    fn a_live_region_with_the_same_epoch_is_left_alone() {
        let regions = BTreeMap::from([
            (1, tracked("a", "m", 1, 300, false)),
            (2, tracked("m", "z", 1, 500, true)),
        ]);
        let plan = plan_replacement(&regions, &[fresh(1, "a", "m", 1), fresh(2, "m", "z", 1)], 0);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![],
                registered: vec![(1, 300)],
            }
        );
    }

    #[test]
    fn an_old_region_only_partly_covered_is_kept() {
        let regions = BTreeMap::from([
            (1, tracked("a", "m", 1, 300, false)),
            (2, tracked("m", "z", 1, 500, true)),
        ]);
        let plan = plan_replacement(&regions, &[fresh(5, "a", "q", 3)], 0);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![1],
                registered: vec![(5, 300)],
            }
        );
    }

    #[test]
    fn a_fresh_region_overlapping_nothing_tracked_uses_the_fallback() {
        let regions = BTreeMap::from([(1, tracked("a", "m", 1, 300, false))]);
        let plan = plan_replacement(&regions, &[fresh(6, "m", "z", 1)], 42);
        assert_eq!(
            plan,
            Replacement {
                dropped: vec![],
                registered: vec![(6, 42)],
            }
        );
    }

    #[test]
    fn coverage_is_checked_across_adjacent_fresh_regions() {
        let fresh_regions = [fresh(1, "a", "f", 1), fresh(2, "f", "m", 1)];
        assert!(is_covered(&range("a", "m"), &fresh_regions));
        assert!(is_covered(&range("b", "g"), &fresh_regions));
        assert!(!is_covered(&range("a", "n"), &fresh_regions));
        assert!(!is_covered(
            &range("a", "m"),
            &[fresh(1, "a", "f", 1), fresh(2, "g", "m", 1)]
        ));
    }
}
