use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};
use std::sync::Arc;

use arrow_array::builder::{
    MapBuilder, StringBuilder, TimestampNanosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use axum::body::{Body, Bytes};
use axum::http::{HeaderName, HeaderValue, header};
use axum::response::Response;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::loki_api::LokiApiError;
use crate::query::message_has_term;
use crate::{LokiStore, MetadataField};

/// Pinned ClickHouse release whose evaluator defines ShardLog SQL semantics.
pub const CLICKHOUSE_COMPATIBILITY_TARGET: &str = "26.3.17.56-lts";

/// Version of the typed columnar boundary exposed to analytical engines.
pub const ANALYTICS_SCHEMA_VERSION: u16 = 1;

const DEFAULT_SCAN_BATCH_ROWS: usize = 8_192;
const STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// One stable column available from ShardLog's analytical scan boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AnalyticsColumn {
    /// Loki tenant owning the record.
    Tenant,
    /// Event timestamp as a nanosecond Arrow timestamp in UTC.
    Timestamp,
    /// Logical shard-stream partition number.
    Partition,
    /// Durable logical offset inside the partition.
    Offset,
    /// Original log message.
    Message,
    /// Loki stream labels encoded as a native string map.
    Labels,
    /// Structured metadata encoded as a native string map.
    Metadata,
}

impl AnalyticsColumn {
    /// All columns in stable wire order.
    pub const ALL: [Self; 7] = [
        Self::Tenant,
        Self::Timestamp,
        Self::Partition,
        Self::Offset,
        Self::Message,
        Self::Labels,
        Self::Metadata,
    ];

    /// Stable wire name used by Arrow and ClickHouse.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Tenant => "tenant",
            Self::Timestamp => "timestamp",
            Self::Partition => "partition",
            Self::Offset => "offset",
            Self::Message => "message",
            Self::Labels => "labels",
            Self::Metadata => "metadata",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "tenant" => Some(Self::Tenant),
            "timestamp" => Some(Self::Timestamp),
            "partition" => Some(Self::Partition),
            "offset" => Some(Self::Offset),
            "message" => Some(Self::Message),
            "labels" => Some(Self::Labels),
            "metadata" => Some(Self::Metadata),
            _ => None,
        }
    }

    fn field(self) -> Field {
        let data_type = match self {
            Self::Tenant | Self::Message => DataType::Utf8,
            Self::Labels | Self::Metadata => string_map_data_type(),
            Self::Timestamp => {
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::<str>::from("UTC")))
            }
            Self::Partition => DataType::UInt32,
            Self::Offset => DataType::UInt64,
        };
        Field::new(self.name(), data_type, false)
    }
}

/// Bounded storage-level scan requested by an analytical query engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyticsScanRequest {
    /// Tenant to scan.
    pub tenant: Arc<str>,
    /// Inclusive event timestamp lower bound.
    pub start_timestamp_unix_nanos: Option<u64>,
    /// Exclusive event timestamp upper bound.
    pub end_timestamp_unix_nanos: Option<u64>,
    /// Case-insensitive indexed message terms combined with AND semantics.
    pub terms: Vec<Arc<str>>,
    /// Exact Loki stream-label constraints combined with AND semantics.
    pub labels: Vec<MetadataField>,
    /// Exact structured-metadata constraints combined with AND semantics.
    pub metadata: Vec<MetadataField>,
    /// Columns emitted in stable request order.
    pub projection: Vec<AnalyticsColumn>,
    /// Optional global row limit.
    pub limit: Option<usize>,
}

impl AnalyticsScanRequest {
    /// Creates an unbounded, full-column scan for one tenant.
    #[must_use]
    pub fn new(tenant: impl Into<Arc<str>>) -> Self {
        Self {
            tenant: tenant.into(),
            start_timestamp_unix_nanos: None,
            end_timestamp_unix_nanos: None,
            terms: Vec::new(),
            labels: Vec::new(),
            metadata: Vec::new(),
            projection: AnalyticsColumn::ALL.to_vec(),
            limit: None,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), LokiApiError> {
        if self.tenant.is_empty() {
            return Err(LokiApiError::bad_request(
                "analytics tenant must not be empty",
            ));
        }
        if self.projection.is_empty() {
            return Err(LokiApiError::bad_request(
                "analytics projection must contain at least one column",
            ));
        }
        let mut columns = BTreeSet::new();
        if self
            .projection
            .iter()
            .any(|column| !columns.insert(*column))
        {
            return Err(LokiApiError::bad_request(
                "analytics projection contains a duplicate column",
            ));
        }
        if self
            .start_timestamp_unix_nanos
            .zip(self.end_timestamp_unix_nanos)
            .is_some_and(|(start, end)| start >= end)
        {
            return Err(LokiApiError::bad_request(
                "analytics timestamp range must be non-empty",
            ));
        }
        Ok(())
    }
}

/// One normalized row emitted by the analytical scan boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyticsLogRow {
    /// Tenant owning the record.
    pub tenant: Arc<str>,
    /// Event timestamp in Unix nanoseconds.
    pub timestamp_unix_nanos: i64,
    /// Logical partition number.
    pub partition: u32,
    /// Durable logical offset.
    pub offset: u64,
    /// Original message.
    pub message: Arc<str>,
    /// Loki stream labels.
    pub labels: BTreeMap<String, String>,
    /// Structured metadata.
    pub metadata: BTreeMap<String, String>,
}

pub(crate) fn parse_scan_request(
    tenant: String,
    raw_query: Option<&str>,
) -> Result<AnalyticsScanRequest, LokiApiError> {
    let mut request = AnalyticsScanRequest::new(tenant);
    let mut projection_seen = false;
    for (key, value) in form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes()) {
        match key.as_ref() {
            "start_ns" => {
                request.start_timestamp_unix_nanos = Some(parse_u64("start_ns", &value)?);
            }
            "end_ns" => {
                request.end_timestamp_unix_nanos = Some(parse_u64("end_ns", &value)?);
            }
            "limit" => {
                request.limit = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| LokiApiError::bad_request("limit is not a usize"))?,
                );
            }
            "term" => request.terms.push(Arc::from(value.as_ref())),
            "columns" => {
                if projection_seen {
                    return Err(LokiApiError::bad_request(
                        "columns may be specified only once",
                    ));
                }
                projection_seen = true;
                request.projection = value
                    .split(',')
                    .map(|column| {
                        AnalyticsColumn::parse(column).ok_or_else(|| {
                            LokiApiError::bad_request(format!(
                                "unknown analytics column {column:?}"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
            }
            key if key.starts_with("label.") => {
                let name = &key["label.".len()..];
                if name.is_empty() {
                    return Err(LokiApiError::bad_request("label name must not be empty"));
                }
                request
                    .labels
                    .push(MetadataField::new(name, value.as_ref()));
            }
            key if key.starts_with("metadata.") => {
                let name = &key["metadata.".len()..];
                if name.is_empty() {
                    return Err(LokiApiError::bad_request("metadata name must not be empty"));
                }
                request
                    .metadata
                    .push(MetadataField::new(name, value.as_ref()));
            }
            unknown => {
                return Err(LokiApiError::bad_request(format!(
                    "unknown analytics parameter {unknown:?}"
                )));
            }
        }
    }
    request.validate()?;
    Ok(request)
}

fn parse_u64(name: &str, value: &str) -> Result<u64, LokiApiError> {
    value
        .parse::<u64>()
        .map_err(|_| LokiApiError::bad_request(format!("{name} is not a u64")))
}

pub(crate) fn scan_entries(
    entries: Vec<crate::LokiEntry>,
    request: &AnalyticsScanRequest,
    emit: &mut dyn FnMut(&[AnalyticsLogRow]) -> Result<(), LokiApiError>,
) -> Result<(), LokiApiError> {
    request.validate()?;
    let limit = request.limit.unwrap_or(usize::MAX);
    if limit == 0 {
        return Ok(());
    }
    let mut rows = Vec::with_capacity(DEFAULT_SCAN_BATCH_ROWS.min(limit));
    let mut emitted = 0usize;
    for (ordinal, entry) in entries.into_iter().enumerate() {
        if emitted == limit {
            break;
        }
        if !entry_matches(&entry, request) {
            continue;
        }
        rows.push(AnalyticsLogRow {
            tenant: Arc::clone(&request.tenant),
            timestamp_unix_nanos: entry.timestamp_unix_nanos,
            partition: 0,
            offset: u64::try_from(ordinal).unwrap_or(u64::MAX),
            message: Arc::from(entry.line),
            labels: entry.labels,
            metadata: entry.structured_metadata,
        });
        emitted += 1;
        if rows.len() == DEFAULT_SCAN_BATCH_ROWS {
            emit(&rows)?;
            rows.clear();
        }
    }
    if !rows.is_empty() {
        emit(&rows)?;
    }
    Ok(())
}

fn entry_matches(entry: &crate::LokiEntry, request: &AnalyticsScanRequest) -> bool {
    let timestamp = u64::try_from(entry.timestamp_unix_nanos).ok();
    if request
        .start_timestamp_unix_nanos
        .is_some_and(|start| timestamp.is_none_or(|observed| observed < start))
        || request
            .end_timestamp_unix_nanos
            .is_some_and(|end| timestamp.is_none_or(|observed| observed >= end))
    {
        return false;
    }
    if request.labels.iter().any(|field| {
        entry.labels.get(field.key.as_ref()).map(String::as_str) != Some(field.value.as_ref())
    }) || request.metadata.iter().any(|field| {
        entry
            .structured_metadata
            .get(field.key.as_ref())
            .map(String::as_str)
            != Some(field.value.as_ref())
    }) {
        return false;
    }
    request
        .terms
        .iter()
        .all(|expected| message_has_term(&entry.line, expected))
}

pub(crate) fn arrow_stream_response(
    store: Arc<dyn LokiStore>,
    request: AnalyticsScanRequest,
) -> Response {
    let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    tokio::task::spawn_blocking(move || {
        if let Err(error) = write_arrow_stream(store, &request, sender.clone()) {
            let _ = sender.blocking_send(Err(io::Error::other(error.to_string())));
        }
    });
    let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.apache.arrow.stream"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardlog-schema-version"),
        HeaderValue::from_static("1"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardlog-clickhouse-target"),
        HeaderValue::from_static(CLICKHOUSE_COMPATIBILITY_TARGET),
    );
    response
}

fn write_arrow_stream(
    store: Arc<dyn LokiStore>,
    request: &AnalyticsScanRequest,
    sender: mpsc::Sender<Result<Bytes, io::Error>>,
) -> Result<(), LokiApiError> {
    let schema = projection_schema(&request.projection);
    let mut sink = ChannelWriter::new(sender, STREAM_CHUNK_BYTES);
    {
        let mut writer = StreamWriter::try_new(&mut sink, &schema)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        store.scan_analytics(request, &mut |rows| {
            let batch = record_batch(rows, &request.projection, Arc::clone(&schema))?;
            writer
                .write(&batch)
                .map_err(|error| LokiApiError::internal(error.to_string()))
        })?;
        writer
            .finish()
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
    }
    sink.finish()
        .map_err(|error| LokiApiError::internal(error.to_string()))
}

fn projection_schema(projection: &[AnalyticsColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .copied()
            .map(AnalyticsColumn::field)
            .collect::<Vec<_>>(),
    ))
}

fn record_batch(
    rows: &[AnalyticsLogRow],
    projection: &[AnalyticsColumn],
    schema: SchemaRef,
) -> Result<RecordBatch, LokiApiError> {
    let mut arrays = Vec::<ArrayRef>::with_capacity(projection.len());
    for column in projection {
        let array: ArrayRef = match column {
            AnalyticsColumn::Tenant => {
                let mut builder = StringBuilder::new();
                for row in rows {
                    builder.append_value(&row.tenant);
                }
                Arc::new(builder.finish())
            }
            AnalyticsColumn::Timestamp => {
                let mut builder = TimestampNanosecondBuilder::with_capacity(rows.len());
                for row in rows {
                    builder.append_value(row.timestamp_unix_nanos);
                }
                Arc::new(builder.finish().with_timezone("UTC"))
            }
            AnalyticsColumn::Partition => {
                let mut builder = UInt32Builder::with_capacity(rows.len());
                for row in rows {
                    builder.append_value(row.partition);
                }
                Arc::new(builder.finish())
            }
            AnalyticsColumn::Offset => {
                let mut builder = UInt64Builder::with_capacity(rows.len());
                for row in rows {
                    builder.append_value(row.offset);
                }
                Arc::new(builder.finish())
            }
            AnalyticsColumn::Message => {
                let mut builder = StringBuilder::new();
                for row in rows {
                    builder.append_value(&row.message);
                }
                Arc::new(builder.finish())
            }
            AnalyticsColumn::Labels => {
                Arc::new(string_map_array(rows.iter().map(|row| &row.labels))?)
            }
            AnalyticsColumn::Metadata => {
                Arc::new(string_map_array(rows.iter().map(|row| &row.metadata))?)
            }
        };
        arrays.push(array);
    }
    RecordBatch::try_new(schema, arrays).map_err(|error| LokiApiError::internal(error.to_string()))
}

fn string_map_data_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

fn string_map_array<'a>(
    rows: impl Iterator<Item = &'a BTreeMap<String, String>>,
) -> Result<arrow_array::MapArray, LokiApiError> {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for values in rows {
        for (key, value) in values {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder
            .append(true)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
    }
    Ok(builder.finish())
}

struct ChannelWriter {
    sender: mpsc::Sender<Result<Bytes, io::Error>>,
    bytes: Vec<u8>,
    chunk_bytes: usize,
}

impl ChannelWriter {
    fn new(sender: mpsc::Sender<Result<Bytes, io::Error>>, chunk_bytes: usize) -> Self {
        Self {
            sender,
            bytes: Vec::with_capacity(chunk_bytes),
            chunk_bytes,
        }
    }

    fn emit(&mut self) -> io::Result<()> {
        if self.bytes.is_empty() {
            return Ok(());
        }
        let bytes = Bytes::from(std::mem::take(&mut self.bytes));
        self.bytes = Vec::with_capacity(self.chunk_bytes);
        self.sender
            .blocking_send(Ok(bytes))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "analytics client disconnected"))
    }

    fn finish(&mut self) -> io::Result<()> {
        self.emit()
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(buffer);
        if self.bytes.len() >= self.chunk_bytes {
            self.emit()?;
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.emit()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow_array::{MapArray, StringArray, TimestampNanosecondArray};
    use arrow_ipc::reader::StreamReader;

    use super::*;
    use crate::LokiEntry;

    #[test]
    fn scan_query_parses_projection_and_pushdown_constraints() {
        let request = parse_scan_request(
            "tenant-a".to_owned(),
            Some("start_ns=10&end_ns=20&term=error&label.app=api&metadata.code=500&columns=timestamp,message&limit=7"),
        )
        .expect("valid scan");
        assert_eq!(request.tenant.as_ref(), "tenant-a");
        assert_eq!(request.start_timestamp_unix_nanos, Some(10));
        assert_eq!(request.end_timestamp_unix_nanos, Some(20));
        assert_eq!(request.terms[0].as_ref(), "error");
        assert_eq!(request.labels[0], MetadataField::new("app", "api"));
        assert_eq!(request.metadata[0], MetadataField::new("code", "500"));
        assert_eq!(
            request.projection,
            vec![AnalyticsColumn::Timestamp, AnalyticsColumn::Message]
        );
        assert_eq!(request.limit, Some(7));
    }

    #[test]
    fn projected_arrow_batch_preserves_timestamp_message_and_maps() {
        let rows = vec![AnalyticsLogRow {
            tenant: Arc::from("tenant-a"),
            timestamp_unix_nanos: 123,
            partition: 4,
            offset: 9,
            message: Arc::from("request failed"),
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            metadata: BTreeMap::from([("code".to_owned(), "500".to_owned())]),
        }];
        let projection = AnalyticsColumn::ALL.to_vec();
        let schema = projection_schema(&projection);
        let batch = record_batch(&rows, &projection, Arc::clone(&schema)).expect("batch");
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &schema).expect("writer");
            writer.write(&batch).expect("write");
            writer.finish().expect("finish");
        }
        let mut reader = StreamReader::try_new(Cursor::new(bytes), None).expect("reader");
        let decoded = reader.next().expect("one batch").expect("valid batch");
        let timestamp = decoded
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("timestamp");
        let message = decoded
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("message");
        let labels = decoded
            .column(5)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("labels");
        assert_eq!(timestamp.value(0), 123);
        assert_eq!(message.value(0), "request failed");
        assert_eq!(labels.value_length(0), 1);
    }

    #[test]
    fn in_memory_scan_applies_all_storage_constraints() {
        let entries = vec![
            LokiEntry {
                timestamp_unix_nanos: 11,
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                line: "request completed".to_owned(),
                structured_metadata: BTreeMap::from([("code".to_owned(), "200".to_owned())]),
            },
            LokiEntry {
                timestamp_unix_nanos: 12,
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                line: "request ERROR".to_owned(),
                structured_metadata: BTreeMap::from([("code".to_owned(), "500".to_owned())]),
            },
        ];
        let mut request = AnalyticsScanRequest::new("tenant-a");
        request.start_timestamp_unix_nanos = Some(10);
        request.end_timestamp_unix_nanos = Some(20);
        request.terms.push(Arc::from("error"));
        request.labels.push(MetadataField::new("app", "api"));
        request.metadata.push(MetadataField::new("code", "500"));
        let mut observed = Vec::new();
        scan_entries(entries, &request, &mut |rows| {
            observed.extend_from_slice(rows);
            Ok(())
        })
        .expect("scan");
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].message.as_ref(), "request ERROR");
    }
}
