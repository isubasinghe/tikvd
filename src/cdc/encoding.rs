const GROUP: usize = 8;
const GROUP_MARKER: u8 = 0xFF;

pub fn memcomparable_key(raw: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity((raw.len() / GROUP + 1) * (GROUP + 1));
    let mut groups = raw.chunks_exact(GROUP);
    for group in groups.by_ref() {
        encoded.extend_from_slice(group);
        encoded.push(GROUP_MARKER);
    }
    let tail = groups.remainder();
    let padding = GROUP - tail.len();
    encoded.extend_from_slice(tail);
    encoded.resize(encoded.len() + padding, 0);
    encoded.push(GROUP_MARKER - padding as u8);
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02X}")).collect()
    }

    #[test]
    fn encodes_the_observed_data_range() {
        assert_eq!(hex(&memcomparable_key(b"k/")), "6B2F000000000000F9");
        assert_eq!(hex(&memcomparable_key(b"k0")), "6B30000000000000F9");
    }

    #[test]
    fn eight_byte_key_gets_a_trailing_empty_group() {
        assert_eq!(
            hex(&memcomparable_key(b"abcdefgh")),
            "6162636465666768FF0000000000000000F7"
        );
    }

    #[test]
    fn longer_key_spans_groups() {
        assert_eq!(
            hex(&memcomparable_key(b"abcdefghij")),
            "6162636465666768FF696A000000000000F9"
        );
    }

    #[test]
    fn empty_key_is_one_padded_group() {
        assert_eq!(hex(&memcomparable_key(b"")), "0000000000000000F7");
    }

    #[test]
    fn encoding_preserves_order() {
        let keys: [&[u8]; 6] = [b"", b"k", b"k/", b"k/\0", b"k/abcdefgh", b"k/abcdefgh\0"];
        for pair in keys.windows(2) {
            assert!(
                memcomparable_key(pair[0]) < memcomparable_key(pair[1]),
                "{pair:?}"
            );
        }
    }
}
