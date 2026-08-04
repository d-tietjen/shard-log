use std::sync::Arc;

use crate::{TelemetryError, TelemetryResult, TelemetrySignal};

const ENVELOPE_MAGIC: [u8; 4] = *b"STEL";
const ENVELOPE_VERSION: u8 = 1;
const ENVELOPE_HEADER_BYTES: usize = 64;
const CHECKSUM_OFFSET: usize = 32;

/// Maximum accepted durable envelope, matching the default OTLP request limit.
pub const MAX_TELEMETRY_ENVELOPE_BYTES: usize = 64 * 1024 * 1024;

/// One checksummed durable telemetry append payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryEnvelope {
    /// Signal-specific payload kind.
    pub signal: TelemetrySignal,
    /// Authenticated single-tenant identity.
    pub tenant: Arc<str>,
    /// Number of offsets represented by this envelope.
    pub item_count: u32,
    /// Signal-specific deterministic routing metadata.
    pub routing_metadata: Arc<[u8]>,
    /// Signal-specific encoded records.
    pub payload: Arc<[u8]>,
}

impl TelemetryEnvelope {
    /// Returns whether bytes begin with the current STEL envelope magic.
    #[must_use]
    pub fn is_encoded(encoded: &[u8]) -> bool {
        encoded.starts_with(&ENVELOPE_MAGIC)
    }

    /// Creates an envelope after enforcing all durable size/count invariants.
    pub fn new(
        signal: TelemetrySignal,
        tenant: impl Into<Arc<str>>,
        item_count: u32,
        routing_metadata: impl Into<Arc<[u8]>>,
        payload: impl Into<Arc<[u8]>>,
    ) -> TelemetryResult<Self> {
        let envelope = Self {
            signal,
            tenant: tenant.into(),
            item_count,
            routing_metadata: routing_metadata.into(),
            payload: payload.into(),
        };
        envelope.validate()?;
        Ok(envelope)
    }

    /// Encodes the single pre-release `STEL` wire format.
    pub fn encode(&self) -> TelemetryResult<Vec<u8>> {
        self.validate()?;
        let total_len = ENVELOPE_HEADER_BYTES
            .checked_add(self.tenant.len())
            .and_then(|value| value.checked_add(self.routing_metadata.len()))
            .and_then(|value| value.checked_add(self.payload.len()))
            .ok_or(TelemetryError::TelemetryEnvelopeTooLarge)?;
        if total_len > MAX_TELEMETRY_ENVELOPE_BYTES {
            return Err(TelemetryError::TelemetryEnvelopeTooLarge);
        }
        let mut encoded = vec![0; ENVELOPE_HEADER_BYTES];
        encoded[..4].copy_from_slice(&ENVELOPE_MAGIC);
        encoded[4] = ENVELOPE_VERSION;
        encoded[5] = self.signal as u8;
        encoded[8..12].copy_from_slice(&self.item_count.to_le_bytes());
        encoded[12..16].copy_from_slice(
            &u32::try_from(self.tenant.len())
                .map_err(|_| TelemetryError::TelemetryEnvelopeTooLarge)?
                .to_le_bytes(),
        );
        encoded[16..20].copy_from_slice(
            &u32::try_from(self.routing_metadata.len())
                .map_err(|_| TelemetryError::TelemetryEnvelopeTooLarge)?
                .to_le_bytes(),
        );
        encoded[20..28].copy_from_slice(
            &u64::try_from(self.payload.len())
                .map_err(|_| TelemetryError::TelemetryEnvelopeTooLarge)?
                .to_le_bytes(),
        );
        encoded.extend_from_slice(self.tenant.as_bytes());
        encoded.extend_from_slice(&self.routing_metadata);
        encoded.extend_from_slice(&self.payload);
        let checksum = checksum(&encoded);
        encoded[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 32].copy_from_slice(checksum.as_bytes());
        Ok(encoded)
    }

    /// Decodes and verifies an `STEL` envelope before exposing any payload bytes.
    pub fn decode(encoded: &[u8]) -> TelemetryResult<Self> {
        if encoded.len() < ENVELOPE_HEADER_BYTES || encoded.len() > MAX_TELEMETRY_ENVELOPE_BYTES {
            return Err(TelemetryError::InvalidTelemetryEnvelope(
                "envelope length is outside the supported range",
            ));
        }
        if encoded[..4] != ENVELOPE_MAGIC {
            return Err(TelemetryError::InvalidTelemetryEnvelope(
                "missing STEL envelope magic",
            ));
        }
        if encoded[4] != ENVELOPE_VERSION {
            return Err(TelemetryError::InvalidTelemetryEnvelope(
                "unsupported STEL envelope version",
            ));
        }
        if encoded[6..8] != [0, 0] || encoded[28..32] != [0, 0, 0, 0] {
            return Err(TelemetryError::InvalidTelemetryEnvelope(
                "nonzero reserved envelope bits",
            ));
        }
        let expected_checksum: [u8; 32] = encoded[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 32]
            .try_into()
            .expect("fixed range");
        if checksum(encoded).as_bytes() != &expected_checksum {
            return Err(TelemetryError::InvalidTelemetryEnvelope(
                "envelope checksum mismatch",
            ));
        }
        let signal = TelemetrySignal::from_wire(encoded[5])?;
        let item_count = u32::from_le_bytes(encoded[8..12].try_into().expect("fixed range"));
        let tenant_len =
            u32::from_le_bytes(encoded[12..16].try_into().expect("fixed range")) as usize;
        let routing_len =
            u32::from_le_bytes(encoded[16..20].try_into().expect("fixed range")) as usize;
        let payload_len = usize::try_from(u64::from_le_bytes(
            encoded[20..28].try_into().expect("fixed range"),
        ))
        .map_err(|_| TelemetryError::TelemetryEnvelopeTooLarge)?;
        let expected_len = ENVELOPE_HEADER_BYTES
            .checked_add(tenant_len)
            .and_then(|value| value.checked_add(routing_len))
            .and_then(|value| value.checked_add(payload_len))
            .ok_or(TelemetryError::TelemetryEnvelopeTooLarge)?;
        if expected_len != encoded.len() {
            return Err(TelemetryError::InvalidTelemetryEnvelope(
                "envelope sections do not match its length",
            ));
        }
        let tenant_start = ENVELOPE_HEADER_BYTES;
        let routing_start = tenant_start + tenant_len;
        let payload_start = routing_start + routing_len;
        let tenant = std::str::from_utf8(&encoded[tenant_start..routing_start])
            .map_err(|_| TelemetryError::InvalidTelemetryEnvelope("tenant is not UTF-8"))?;
        Self::new(
            signal,
            tenant,
            item_count,
            &encoded[routing_start..payload_start],
            &encoded[payload_start..],
        )
    }

    fn validate(&self) -> TelemetryResult<()> {
        if self.tenant.is_empty() {
            return Err(TelemetryError::InvalidTelemetryEnvelope(
                "tenant must not be empty",
            ));
        }
        if self.item_count == 0 {
            return Err(TelemetryError::InvalidTelemetryEnvelope(
                "item count must not be zero",
            ));
        }
        if self.payload.is_empty() {
            return Err(TelemetryError::InvalidTelemetryEnvelope(
                "payload must not be empty",
            ));
        }
        Ok(())
    }
}

fn checksum(encoded: &[u8]) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&encoded[..CHECKSUM_OFFSET]);
    hasher.update(&[0; 32]);
    hasher.update(&encoded[CHECKSUM_OFFSET + 32..]);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trips_all_sections() {
        let envelope = TelemetryEnvelope::new(
            TelemetrySignal::Traces,
            "tenant-a",
            7,
            &b"partition-key"[..],
            &b"signal-payload"[..],
        )
        .unwrap();
        let encoded = envelope.encode().unwrap();
        assert_eq!(TelemetryEnvelope::decode(&encoded).unwrap(), envelope);
    }

    #[test]
    fn checksum_covers_header_and_every_section() {
        let envelope = TelemetryEnvelope::new(
            TelemetrySignal::Metrics,
            "tenant-a",
            1,
            &b"route"[..],
            &b"payload"[..],
        )
        .unwrap();
        let mut encoded = envelope.encode().unwrap();
        *encoded.last_mut().unwrap() ^= 1;
        assert!(matches!(
            TelemetryEnvelope::decode(&encoded),
            Err(TelemetryError::InvalidTelemetryEnvelope(
                "envelope checksum mismatch"
            ))
        ));
    }
}
