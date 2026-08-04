//! Prometheus XOR chunk encoding used by streamed Remote Read.
//!
//! The wire algorithm follows Prometheus's Apache-2.0 `tsdb/chunkenc/xor.go`,
//! which in turn credits Damian Gryski's BSD-licensed `go-tsz` implementation.

use crate::{TelemetryError, TelemetryResult};

const CHUNK_HEADER_BYTES: usize = 2;

#[derive(Debug)]
struct BitWriter {
    bytes: Vec<u8>,
    available: u8,
}

impl BitWriter {
    fn new() -> Self {
        Self {
            bytes: vec![0; CHUNK_HEADER_BYTES],
            available: 0,
        }
    }

    fn write_bit(&mut self, value: bool) {
        if self.available == 0 {
            self.bytes.push(0);
            self.available = 8;
        }
        if value {
            let last = self.bytes.len() - 1;
            self.bytes[last] |= 1 << (self.available - 1);
        }
        self.available -= 1;
    }

    fn write_byte(&mut self, value: u8) {
        if self.available == 0 {
            self.bytes.push(value);
            return;
        }
        let last = self.bytes.len() - 1;
        self.bytes[last] |= value >> (8 - self.available);
        self.bytes.push(value << self.available);
    }

    fn write_bits(&mut self, value: u64, bits: u8) {
        for shift in (0..bits).rev() {
            self.write_bit(value & (1_u64 << shift) != 0);
        }
    }

    fn write_uvarint(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.write_byte((value as u8) | 0x80);
            value >>= 7;
        }
        self.write_byte(value as u8);
    }

    fn write_varint(&mut self, value: i64) {
        let mut encoded = (value as u64) << 1;
        if value < 0 {
            encoded = !encoded;
        }
        self.write_uvarint(encoded);
    }
}

/// Encodes timestamp/value samples into Prometheus's standard XOR chunk bytes.
pub(crate) fn encode_xor_chunk(samples: &[(i64, f64)]) -> TelemetryResult<Vec<u8>> {
    if samples.is_empty() || samples.len() > usize::from(u16::MAX) {
        return Err(TelemetryError::InvalidMetricSample(
            "Prometheus XOR chunks require 1..=65535 samples".into(),
        ));
    }
    if samples.windows(2).any(|pair| pair[0].0 > pair[1].0) {
        return Err(TelemetryError::InvalidMetricSample(
            "Prometheus XOR chunk timestamps must be sorted".into(),
        ));
    }

    let mut writer = BitWriter::new();
    let mut timestamp = i64::MIN;
    let mut timestamp_delta = 0_u64;
    let mut value = 0_f64;
    let mut leading = u8::MAX;
    let mut trailing = 0_u8;

    for (ordinal, &(next_timestamp, next_value)) in samples.iter().enumerate() {
        match ordinal {
            0 => {
                writer.write_varint(next_timestamp);
                writer.write_bits(next_value.to_bits(), 64);
            }
            1 => {
                timestamp_delta = next_timestamp
                    .checked_sub(timestamp)
                    .and_then(|delta| u64::try_from(delta).ok())
                    .ok_or_else(|| {
                        TelemetryError::InvalidMetricSample(
                            "Prometheus XOR timestamp delta is negative".into(),
                        )
                    })?;
                writer.write_uvarint(timestamp_delta);
                write_value_delta(&mut writer, next_value, value, &mut leading, &mut trailing);
            }
            _ => {
                let next_delta = next_timestamp
                    .checked_sub(timestamp)
                    .and_then(|delta| u64::try_from(delta).ok())
                    .ok_or_else(|| {
                        TelemetryError::InvalidMetricSample(
                            "Prometheus XOR timestamp delta is negative".into(),
                        )
                    })?;
                let delta_of_delta = next_delta.wrapping_sub(timestamp_delta) as i64;
                write_timestamp_delta(&mut writer, delta_of_delta);
                write_value_delta(&mut writer, next_value, value, &mut leading, &mut trailing);
                timestamp_delta = next_delta;
            }
        }
        timestamp = next_timestamp;
        value = next_value;
    }
    writer.bytes[..2].copy_from_slice(&(samples.len() as u16).to_be_bytes());
    Ok(writer.bytes)
}

fn write_timestamp_delta(writer: &mut BitWriter, delta_of_delta: i64) {
    if delta_of_delta == 0 {
        writer.write_bit(false);
    } else if fits_prometheus_signed_range(delta_of_delta, 14) {
        writer.write_bits(0b10, 2);
        writer.write_bits(delta_of_delta as u64, 14);
    } else if fits_prometheus_signed_range(delta_of_delta, 17) {
        writer.write_bits(0b110, 3);
        writer.write_bits(delta_of_delta as u64, 17);
    } else if fits_prometheus_signed_range(delta_of_delta, 20) {
        writer.write_bits(0b1110, 4);
        writer.write_bits(delta_of_delta as u64, 20);
    } else {
        writer.write_bits(0b1111, 4);
        writer.write_bits(delta_of_delta as u64, 64);
    }
}

fn fits_prometheus_signed_range(value: i64, bits: u8) -> bool {
    let half = 1_i128 << (bits - 1);
    i128::from(value) >= -(half - 1) && i128::from(value) <= half
}

fn write_value_delta(
    writer: &mut BitWriter,
    next: f64,
    current: f64,
    leading: &mut u8,
    trailing: &mut u8,
) {
    let delta = next.to_bits() ^ current.to_bits();
    if delta == 0 {
        writer.write_bit(false);
        return;
    }
    writer.write_bit(true);
    let next_leading = (delta.leading_zeros() as u8).min(31);
    let next_trailing = delta.trailing_zeros() as u8;
    if *leading != u8::MAX && next_leading >= *leading && next_trailing >= *trailing {
        writer.write_bit(false);
        writer.write_bits(delta >> *trailing, 64 - *leading - *trailing);
        return;
    }
    *leading = next_leading;
    *trailing = next_trailing;
    writer.write_bit(true);
    writer.write_bits(u64::from(next_leading), 5);
    let significant = 64 - next_leading - next_trailing;
    writer.write_bits(u64::from(significant), 6);
    writer.write_bits(delta >> next_trailing, significant);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct BitReader<'a> {
        bytes: &'a [u8],
        bit: usize,
    }

    impl<'a> BitReader<'a> {
        fn new(bytes: &'a [u8]) -> Self {
            Self { bytes, bit: 0 }
        }

        fn read_bit(&mut self) -> bool {
            let value = self.bytes[self.bit / 8] & (1 << (7 - self.bit % 8)) != 0;
            self.bit += 1;
            value
        }

        fn read_bits(&mut self, count: u8) -> u64 {
            (0..count).fold(0, |value, _| (value << 1) | u64::from(self.read_bit()))
        }

        fn read_uvarint(&mut self) -> u64 {
            let mut value = 0;
            for shift in (0..64).step_by(7) {
                let byte = self.read_bits(8) as u8;
                value |= u64::from(byte & 0x7f) << shift;
                if byte & 0x80 == 0 {
                    return value;
                }
            }
            panic!("test varint overflow")
        }

        fn read_varint(&mut self) -> i64 {
            let encoded = self.read_uvarint();
            let value = (encoded >> 1) as i64;
            if encoded & 1 == 0 { value } else { !value }
        }
    }

    #[test]
    fn xor_chunk_round_trips_prometheus_timestamp_and_value_controls() {
        let samples = vec![
            (-1, 1.0),
            (999, 1.0),
            (1_999, 1.5),
            (3_000, 1.75),
            (100_000, f64::from_bits(0x7ff0_0000_0000_0002)),
        ];
        let encoded = encode_xor_chunk(&samples).unwrap();
        assert_eq!(u16::from_be_bytes(encoded[..2].try_into().unwrap()), 5);
        let decoded = decode_for_test(&encoded);
        assert_eq!(
            decoded
                .iter()
                .map(|(timestamp, value)| (*timestamp, value.to_bits()))
                .collect::<Vec<_>>(),
            samples
                .iter()
                .map(|(timestamp, value)| (*timestamp, value.to_bits()))
                .collect::<Vec<_>>()
        );
    }

    fn decode_for_test(encoded: &[u8]) -> Vec<(i64, f64)> {
        let count = u16::from_be_bytes(encoded[..2].try_into().unwrap());
        let mut reader = BitReader::new(&encoded[2..]);
        let mut output = Vec::with_capacity(usize::from(count));
        let mut timestamp = reader.read_varint();
        let mut value = f64::from_bits(reader.read_bits(64));
        output.push((timestamp, value));
        if count == 1 {
            return output;
        }
        let mut timestamp_delta = reader.read_uvarint();
        timestamp += timestamp_delta as i64;
        let mut leading = 0_u8;
        let mut trailing = 0_u8;
        value = read_value_delta(&mut reader, value, &mut leading, &mut trailing);
        output.push((timestamp, value));
        for _ in 2..count {
            let mut control = 0_u8;
            for _ in 0..4 {
                control <<= 1;
                if !reader.read_bit() {
                    break;
                }
                control |= 1;
            }
            let bits = match control {
                0 => 0,
                0b10 => 14,
                0b110 => 17,
                0b1110 => 20,
                0b1111 => 64,
                _ => unreachable!(),
            };
            let mut delta_of_delta = if bits == 0 {
                0
            } else {
                reader.read_bits(bits) as i64
            };
            if bits != 0 && bits != 64 && delta_of_delta as u64 > (1_u64 << (bits - 1)) {
                delta_of_delta -= 1_i64 << bits;
            }
            timestamp_delta = (timestamp_delta as i64 + delta_of_delta) as u64;
            timestamp += timestamp_delta as i64;
            value = read_value_delta(&mut reader, value, &mut leading, &mut trailing);
            output.push((timestamp, value));
        }
        output
    }

    fn read_value_delta(
        reader: &mut BitReader<'_>,
        current: f64,
        leading: &mut u8,
        trailing: &mut u8,
    ) -> f64 {
        if !reader.read_bit() {
            return current;
        }
        if reader.read_bit() {
            *leading = reader.read_bits(5) as u8;
            let significant = match reader.read_bits(6) as u8 {
                0 => 64,
                value => value,
            };
            *trailing = 64 - *leading - significant;
        }
        let significant = 64 - *leading - *trailing;
        let delta = reader.read_bits(significant) << *trailing;
        f64::from_bits(current.to_bits() ^ delta)
    }
}
