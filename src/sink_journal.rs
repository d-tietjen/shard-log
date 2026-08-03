use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use bytes::Bytes;
use shard_stream_core::{
    LogicalOffset, LogicalPartitionId, PlacementSequence, ShardId, TopicId, TopicPartition,
};
use shard_stream_engine::{DurableAppend, DurableSinkCheckpoint};

use crate::{LogDbError, LogDbResult};

const MAGIC: &[u8; 8] = b"SLOGSNK1";
const CHECKSUM_BYTES: usize = 32;
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct RecoveredAppend {
    pub(crate) topic_partition: TopicPartition,
    pub(crate) first_offset: LogicalOffset,
    pub(crate) payload: Bytes,
}

#[derive(Debug)]
pub(crate) struct RecoveredTransaction {
    pub(crate) expected: DurableSinkCheckpoint,
    pub(crate) next: DurableSinkCheckpoint,
    pub(crate) appends: Vec<RecoveredAppend>,
}

#[derive(Debug)]
struct JournalState {
    file: File,
    bytes: u64,
    checkpoints: HashMap<TopicPartition, DurableSinkCheckpoint>,
}

/// Checksummed transaction journal for one physical sink stripe.
#[derive(Debug)]
pub(crate) struct SinkJournal {
    max_bytes: u64,
    state: Mutex<JournalState>,
}

impl SinkJournal {
    pub(crate) fn open(
        directory: &Path,
        shard_id: ShardId,
        max_bytes: u64,
    ) -> LogDbResult<(Self, Vec<RecoveredTransaction>)> {
        if max_bytes < MAGIC.len() as u64 {
            return Err(LogDbError::InvalidConfig(
                "sink journal max bytes must fit its header",
            ));
        }
        fs::create_dir_all(directory).map_err(|error| journal_io("create directory", error))?;
        let path = directory.join(format!("shard-{}.journal", shard_id.get()));
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| journal_io("open", error))?;
        if file
            .metadata()
            .map_err(|error| journal_io("inspect", error))?
            .len()
            == 0
        {
            file.write_all(MAGIC)
                .and_then(|()| file.sync_data())
                .map_err(|error| journal_io("initialize", error))?;
        }
        let transactions = recover(&mut file, shard_id, max_bytes)?;
        let checkpoints = transactions
            .iter()
            .map(|transaction| (transaction.next.topic_partition, transaction.next))
            .collect();
        let bytes = file
            .metadata()
            .map_err(|error| journal_io("inspect recovered", error))?
            .len();
        file.seek(SeekFrom::End(0))
            .map_err(|error| journal_io("seek append", error))?;
        Ok((
            Self {
                max_bytes,
                state: Mutex::new(JournalState {
                    file,
                    bytes,
                    checkpoints,
                }),
            },
            transactions,
        ))
    }

    pub(crate) fn append(
        &self,
        expected: DurableSinkCheckpoint,
        appends: &[DurableAppend],
        next: DurableSinkCheckpoint,
    ) -> LogDbResult<()> {
        let payload = encode_transaction(expected, appends, next)?;
        let frame_bytes = 4_usize
            .checked_add(payload.len())
            .and_then(|value| value.checked_add(CHECKSUM_BYTES))
            .ok_or(LogDbError::RecordTooLarge)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| LogDbError::StorageIo("sink journal lock is poisoned".into()))?;
        let actual = state
            .checkpoints
            .get(&expected.topic_partition)
            .copied()
            .unwrap_or_else(|| DurableSinkCheckpoint::initial(expected.topic_partition));
        if checkpoint_covers(actual, next) {
            return Ok(());
        }
        if !checkpoint_allows_lane_gap(actual, expected) {
            return Err(LogDbError::CorruptSinkJournal(
                "journal append does not continue its durable checkpoint chain".into(),
            ));
        }
        let next_bytes = state
            .bytes
            .checked_add(u64::try_from(frame_bytes).map_err(|_| LogDbError::RecordTooLarge)?)
            .ok_or(LogDbError::RecordTooLarge)?;
        if next_bytes > self.max_bytes {
            return Err(LogDbError::SinkJournalFull {
                bytes: next_bytes,
                capacity: self.max_bytes,
            });
        }
        let length = u32::try_from(payload.len())
            .map_err(|_| LogDbError::RecordTooLarge)?
            .to_le_bytes();
        let checksum = blake3::hash(&payload);
        state
            .file
            .write_all(&length)
            .and_then(|()| state.file.write_all(&payload))
            .and_then(|()| state.file.write_all(checksum.as_bytes()))
            .and_then(|()| state.file.sync_data())
            .map_err(|error| journal_io("append transaction", error))?;
        state.bytes = next_bytes;
        state.checkpoints.insert(next.topic_partition, next);
        Ok(())
    }
}

fn recover(
    file: &mut File,
    shard_id: ShardId,
    max_bytes: u64,
) -> LogDbResult<Vec<RecoveredTransaction>> {
    let file_bytes = file
        .metadata()
        .map_err(|error| journal_io("inspect", error))?
        .len();
    if file_bytes > max_bytes {
        return Err(LogDbError::SinkJournalFull {
            bytes: file_bytes,
            capacity: max_bytes,
        });
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| journal_io("seek start", error))?;
    let mut magic = [0_u8; MAGIC.len()];
    file.read_exact(&mut magic)
        .map_err(|error| journal_io("read header", error))?;
    if &magic != MAGIC {
        return Err(LogDbError::CorruptSinkJournal(
            "journal magic is invalid".into(),
        ));
    }

    let mut transactions = Vec::new();
    let mut checkpoints = HashMap::<TopicPartition, DurableSinkCheckpoint>::new();
    let mut position = MAGIC.len() as u64;
    loop {
        let frame_start = position;
        let mut length_bytes = [0_u8; 4];
        match file.read_exact(&mut length_bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                file.set_len(frame_start)
                    .map_err(|error| journal_io("repair partial header", error))?;
                break;
            }
            Err(error) => return Err(journal_io("read frame header", error)),
        }
        let length = u32::from_le_bytes(length_bytes) as usize;
        if length == 0 || length > MAX_FRAME_BYTES {
            return Err(LogDbError::CorruptSinkJournal(
                "journal frame length is invalid".into(),
            ));
        }
        let mut payload = vec![0_u8; length];
        let mut checksum = [0_u8; CHECKSUM_BYTES];
        if let Err(error) = file
            .read_exact(&mut payload)
            .and_then(|()| file.read_exact(&mut checksum))
        {
            if error.kind() == ErrorKind::UnexpectedEof {
                file.set_len(frame_start)
                    .map_err(|error| journal_io("repair partial frame", error))?;
                break;
            }
            return Err(journal_io("read frame", error));
        }
        if blake3::hash(&payload).as_bytes() != &checksum {
            return Err(LogDbError::CorruptSinkJournal(
                "journal frame checksum is invalid".into(),
            ));
        }
        let transaction = decode_transaction(&payload, shard_id)?;
        if let Some(actual) = checkpoints
            .get(&transaction.expected.topic_partition)
            .copied()
            && (actual.next_placement_sequence > transaction.expected.next_placement_sequence
                || actual.next_offset > transaction.expected.next_offset)
        {
            return Err(LogDbError::CorruptSinkJournal(
                "journal checkpoint sequence or offset regressed".into(),
            ));
        }
        checkpoints.insert(transaction.next.topic_partition, transaction.next);
        transactions.push(transaction);
        position = frame_start
            .checked_add(4)
            .and_then(|value| value.checked_add(length as u64))
            .and_then(|value| value.checked_add(CHECKSUM_BYTES as u64))
            .ok_or(LogDbError::RecordTooLarge)?;
    }
    file.seek(SeekFrom::End(0))
        .map_err(|error| journal_io("seek recovered end", error))?;
    Ok(transactions)
}

pub(crate) fn checkpoint_allows_lane_gap(
    actual: DurableSinkCheckpoint,
    expected: DurableSinkCheckpoint,
) -> bool {
    actual.topic_partition == expected.topic_partition
        && actual.next_placement_sequence == expected.next_placement_sequence
        && actual.next_offset <= expected.next_offset
}

fn checkpoint_covers(checkpoint: DurableSinkCheckpoint, candidate: DurableSinkCheckpoint) -> bool {
    checkpoint.topic_partition == candidate.topic_partition
        && checkpoint.next_placement_sequence >= candidate.next_placement_sequence
        && checkpoint.next_offset >= candidate.next_offset
}

fn encode_transaction(
    expected: DurableSinkCheckpoint,
    appends: &[DurableAppend],
    next: DurableSinkCheckpoint,
) -> LogDbResult<Vec<u8>> {
    if expected.topic_partition != next.topic_partition {
        return Err(LogDbError::CorruptSinkJournal(
            "transaction checkpoints refer to different partitions".into(),
        ));
    }
    let mut encoded = Vec::new();
    encode_checkpoint(&mut encoded, expected);
    encode_checkpoint(&mut encoded, next);
    encoded.extend_from_slice(
        &u32::try_from(appends.len())
            .map_err(|_| LogDbError::RecordTooLarge)?
            .to_le_bytes(),
    );
    for append in appends {
        if append.topic_partition() != expected.topic_partition {
            return Err(LogDbError::CorruptSinkJournal(
                "transaction append belongs to another partition".into(),
            ));
        }
        encoded.extend_from_slice(&append.physical_shard_id.get().to_le_bytes());
        encoded.extend_from_slice(&append.reservation.first_offset.get().to_le_bytes());
        encoded.extend_from_slice(
            &u32::try_from(append.payload.len())
                .map_err(|_| LogDbError::RecordTooLarge)?
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&append.payload);
    }
    if encoded.len() > MAX_FRAME_BYTES {
        return Err(LogDbError::RecordTooLarge);
    }
    Ok(encoded)
}

fn encode_checkpoint(encoded: &mut Vec<u8>, checkpoint: DurableSinkCheckpoint) {
    encoded.extend_from_slice(&checkpoint.topic_partition.topic_id.get().to_le_bytes());
    encoded.extend_from_slice(&checkpoint.topic_partition.partition_id.get().to_le_bytes());
    encoded.extend_from_slice(&checkpoint.next_placement_sequence.get().to_le_bytes());
    encoded.extend_from_slice(&checkpoint.next_offset.get().to_le_bytes());
}

fn decode_transaction(bytes: &[u8], shard_id: ShardId) -> LogDbResult<RecoveredTransaction> {
    let mut cursor = 0;
    let expected = decode_checkpoint(bytes, &mut cursor)?;
    let next = decode_checkpoint(bytes, &mut cursor)?;
    if expected.topic_partition != next.topic_partition {
        return Err(LogDbError::CorruptSinkJournal(
            "transaction checkpoints refer to different partitions".into(),
        ));
    }
    let count = read_u32(bytes, &mut cursor)? as usize;
    let mut appends = Vec::with_capacity(count);
    for _ in 0..count {
        let observed_shard = ShardId::new(read_u32(bytes, &mut cursor)?);
        if observed_shard != shard_id {
            return Err(LogDbError::CorruptSinkJournal(
                "journal frame belongs to another physical shard".into(),
            ));
        }
        let first_offset = LogicalOffset::new(read_u64(bytes, &mut cursor)?);
        let payload_bytes = read_u32(bytes, &mut cursor)? as usize;
        let end = cursor
            .checked_add(payload_bytes)
            .ok_or(LogDbError::RecordTooLarge)?;
        let payload = bytes
            .get(cursor..end)
            .ok_or_else(|| LogDbError::CorruptSinkJournal("append payload is truncated".into()))?;
        cursor = end;
        appends.push(RecoveredAppend {
            topic_partition: expected.topic_partition,
            first_offset,
            payload: Bytes::copy_from_slice(payload),
        });
    }
    if cursor != bytes.len() {
        return Err(LogDbError::CorruptSinkJournal(
            "journal frame has trailing bytes".into(),
        ));
    }
    Ok(RecoveredTransaction {
        expected,
        next,
        appends,
    })
}

fn decode_checkpoint(bytes: &[u8], cursor: &mut usize) -> LogDbResult<DurableSinkCheckpoint> {
    let partition = TopicPartition::new(
        TopicId::new(read_u128(bytes, cursor)?),
        LogicalPartitionId::new(read_u32(bytes, cursor)?),
    );
    Ok(DurableSinkCheckpoint {
        topic_partition: partition,
        next_placement_sequence: PlacementSequence::new(read_u64(bytes, cursor)?),
        next_offset: LogicalOffset::new(read_u64(bytes, cursor)?),
    })
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> LogDbResult<u32> {
    Ok(u32::from_le_bytes(read_array(bytes, cursor)?))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> LogDbResult<u64> {
    Ok(u64::from_le_bytes(read_array(bytes, cursor)?))
}

fn read_u128(bytes: &[u8], cursor: &mut usize) -> LogDbResult<u128> {
    Ok(u128::from_le_bytes(read_array(bytes, cursor)?))
}

fn read_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> LogDbResult<[u8; N]> {
    let end = cursor.checked_add(N).ok_or(LogDbError::RecordTooLarge)?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| LogDbError::CorruptSinkJournal("journal frame is truncated".into()))?
        .try_into()
        .expect("slice length is exact");
    *cursor = end;
    Ok(value)
}

fn journal_io(operation: &str, error: std::io::Error) -> LogDbError {
    LogDbError::StorageIo(format!("{operation} sink journal: {error}"))
}
