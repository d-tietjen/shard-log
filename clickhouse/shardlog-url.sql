-- Replace both placeholders from a secret-aware deployment template.
CREATE DATABASE IF NOT EXISTS shardlog;

CREATE TABLE IF NOT EXISTS shardlog.logs
(
    tenant String,
    timestamp DateTime64(9, 'UTC'),
    partition UInt32,
    offset UInt64,
    message String,
    labels Map(String, String),
    metadata Map(String, String)
)
ENGINE = URL(
    'http://127.0.0.1:3100/shardlog/api/v1/clickhouse/scan',
    ArrowStream,
    headers(
        'Authorization' = 'Bearer REPLACE_FROM_SECRET_STORE',
        'X-Scope-OrgID' = 'fake'
    )
);
