const DATA_PREFIX: &[u8] = b"k/";
const DATA_PREFIX_END: &[u8] = b"k0";
const TTL_PREFIX: &[u8] = b"l/";
const TTL_PREFIX_END: &[u8] = b"l0";
const EXPIRY_WIDTH: usize = 8;

pub fn data_key(etcd_key: &[u8]) -> Vec<u8> {
    [DATA_PREFIX, etcd_key].concat()
}

pub fn etcd_key(data_key: &[u8]) -> Option<&[u8]> {
    data_key.strip_prefix(DATA_PREFIX)
}

pub fn data_range(key: &[u8], range_end: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let start = data_key(key);
    let end = match range_end {
        [] => [start.as_slice(), &[0]].concat(),
        [0] => DATA_PREFIX_END.to_vec(),
        _ => data_key(range_end),
    };
    (start, end)
}

pub fn ttl_key(expiry_unix_secs: u64, etcd_key: &[u8]) -> Vec<u8> {
    [TTL_PREFIX, &expiry_unix_secs.to_be_bytes(), etcd_key].concat()
}

pub fn parse_ttl_key(ttl_key: &[u8]) -> Option<(u64, &[u8])> {
    let rest = ttl_key.strip_prefix(TTL_PREFIX)?;
    let (expiry, key) = rest.split_at_checked(EXPIRY_WIDTH)?;
    Some((u64::from_be_bytes(expiry.try_into().ok()?), key))
}

pub fn ttl_range_until(now_unix_secs: u64) -> (Vec<u8>, Vec<u8>) {
    let end = match now_unix_secs.checked_add(1) {
        Some(next) => ttl_key(next, b""),
        None => TTL_PREFIX_END.to_vec(),
    };
    (TTL_PREFIX.to_vec(), end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contains(range: &(Vec<u8>, Vec<u8>), key: &[u8]) -> bool {
        range.0.as_slice() <= key && key < range.1.as_slice()
    }

    #[test]
    fn empty_range_end_selects_single_key() {
        let range = data_range(b"a", b"");
        assert!(contains(&range, &data_key(b"a")));
        assert!(!contains(&range, &data_key(b"a\0")));
    }

    #[test]
    fn zero_range_end_extends_to_end_of_data_prefix() {
        assert_eq!(data_range(b"a", b"\0").1, b"k0");
    }

    #[test]
    fn explicit_range_end_is_half_open() {
        let range = data_range(b"a\xff", b"b");
        assert!(contains(&range, &data_key(b"a\xff\xff")));
        assert!(!contains(&range, &data_key(b"b")));
    }

    #[test]
    fn etcd_key_round_trips_and_rejects_other_prefixes() {
        assert_eq!(etcd_key(&data_key(b"foo")), Some(&b"foo"[..]));
        assert_eq!(etcd_key(b"l/x"), None);
    }

    #[test]
    fn ttl_key_round_trips() {
        assert_eq!(parse_ttl_key(&ttl_key(7, b"a/b")), Some((7, &b"a/b"[..])));
        assert_eq!(parse_ttl_key(b"k/x"), None);
        assert_eq!(parse_ttl_key(b"l/short"), None);
    }

    #[test]
    fn ttl_keys_sort_by_expiry() {
        assert!(ttl_key(1, b"z") < ttl_key(2, b"a"));
    }

    #[test]
    fn ttl_range_until_is_inclusive_of_now() {
        let range = ttl_range_until(5);
        assert!(contains(&range, &ttl_key(5, b"x")));
        assert!(!contains(&range, &ttl_key(6, b"")));
    }

    #[test]
    fn ttl_range_until_max_does_not_overflow() {
        let range = ttl_range_until(u64::MAX);
        assert!(contains(&range, &ttl_key(u64::MAX, b"x")));
    }
}
