const HEADER_WIDTH: usize = 24;

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub version: i64,
    pub created: u64,
    pub lease_expiry: u64,
    pub value: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub struct CorruptRecord;

impl Record {
    pub fn encode(&self) -> Vec<u8> {
        [
            &self.version.to_be_bytes()[..],
            &self.created.to_be_bytes(),
            &self.lease_expiry.to_be_bytes(),
            &self.value,
        ]
        .concat()
    }

    pub fn decode(bytes: &[u8]) -> Result<Record, CorruptRecord> {
        let (header, value) = bytes.split_at_checked(HEADER_WIDTH).ok_or(CorruptRecord)?;
        Ok(Record {
            version: i64::from_be_bytes(header[0..8].try_into().unwrap()),
            created: u64::from_be_bytes(header[8..16].try_into().unwrap()),
            lease_expiry: u64::from_be_bytes(header[16..24].try_into().unwrap()),
            value: value.to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_with(value: Vec<u8>) -> Record {
        Record {
            version: -3,
            created: 42,
            lease_expiry: 99,
            value,
        }
    }

    #[test]
    fn round_trips_empty_value() {
        let record = record_with(Vec::new());
        assert_eq!(Record::decode(&record.encode()), Ok(record));
    }

    #[test]
    fn round_trips_one_mebibyte_value() {
        let record = record_with(vec![0xab; 1 << 20]);
        assert_eq!(Record::decode(&record.encode()), Ok(record));
    }

    #[test]
    fn short_input_is_corrupt() {
        assert_eq!(Record::decode(&[1, 2, 3]), Err(CorruptRecord));
    }
}
