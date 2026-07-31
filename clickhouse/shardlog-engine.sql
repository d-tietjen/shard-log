-- StorageShardLog requires a ClickHouse binary built with clickhouse/adapter.
-- Replace the endpoint and inject a short-lived token from a protected source.
CREATE TABLE shardlog_logs
(
    tenant String,
    timestamp DateTime64(9, 'UTC'),
    partition UInt32,
    offset UInt64,
    message String,
    labels Map(String, String),
    metadata Map(String, String)
)
ENGINE = ShardLog(
    'http://127.0.0.1:3100/shardlog/api/v1/clickhouse/scan',
    'ArrowStream',
    headers(
        'Authorization' = 'Bearer REPLACE_FROM_SECRET_STORE',
        'X-Scope-OrgID' = 'fake'
    )
);
