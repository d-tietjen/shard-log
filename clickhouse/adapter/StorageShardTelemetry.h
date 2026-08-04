#pragma once

#include <Storages/StorageURL.h>

namespace DB
{

/// ClickHouse storage adapter for ShardTelemetry's authenticated Arrow scan API.
///
/// The adapter deliberately inherits StorageURL so ClickHouse remains
/// responsible for Arrow decoding and all residual query evaluation. The only
/// specialization is translating proven-safe parts of the analyzed filter DAG
/// into ShardTelemetry scan parameters.
class StorageShardTelemetry final : public StorageURL
{
public:
    using StorageURL::StorageURL;

    String getName() const override { return "ShardTelemetry"; }

protected:
    std::vector<std::pair<std::string, std::string>> getReadURIParams(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        const SelectQueryInfo & query_info,
        const ContextPtr & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const override;
};

void registerStorageShardTelemetry(StorageFactory & factory);

}
