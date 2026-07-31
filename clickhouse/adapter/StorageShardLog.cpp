#include <Storages/StorageShardLog.h>

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>

#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <unordered_set>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

using URIParams = std::vector<std::pair<std::string, std::string>>;
using InputColumns = std::unordered_map<std::string, ColumnWithTypeAndName>;

struct TimestampBounds
{
    std::optional<UInt64> start;
    std::optional<UInt64> end;
    bool impossible = false;

    void addStart(UInt64 value)
    {
        start = std::max(start.value_or(value), value);
        checkRange();
    }

    void addEnd(UInt64 value)
    {
        end = std::min(end.value_or(value), value);
        checkRange();
    }

    void checkRange()
    {
        impossible |= start && end && *start >= *end;
    }
};

struct Pushdown
{
    TimestampBounds timestamps;
    URIParams equalities;
    bool fully_supported = true;
};

const ActionsDAG::Node * unwrapAlias(const ActionsDAG::Node * node)
{
    while (node && node->type == ActionsDAG::ActionType::ALIAS && node->children.size() == 1)
        node = node->children.front();
    return node;
}

std::optional<String> constantString(const ActionsDAG::Node * node)
{
    node = unwrapAlias(node);
    if (!node || node->type != ActionsDAG::ActionType::COLUMN || !node->column || node->column->empty())
        return std::nullopt;

    Field value = (*node->column)[0];
    if (value.getType() != Field::Types::String)
        return std::nullopt;
    return value.safeGet<String>();
}

std::optional<String> inputName(const ActionsDAG::Node * node, const InputColumns & inputs)
{
    node = unwrapAlias(node);
    if (!node || node->type != ActionsDAG::ActionType::INPUT)
        return std::nullopt;

    if (auto mapped = inputs.find(node->result_name); mapped != inputs.end())
        return mapped->second.name;
    return node->result_name;
}

std::optional<UInt64> dateTime64Nanos(const ActionsDAG::Node * node)
{
    node = unwrapAlias(node);
    if (!node || node->type != ActionsDAG::ActionType::COLUMN || !node->column || node->column->empty())
        return std::nullopt;

    const auto * type = typeid_cast<const DataTypeDateTime64 *>(node->result_type.get());
    if (!type)
        return std::nullopt;

    const Int64 raw = (*node->column)[0].safeGet<DateTime64>().getValue();
    if (raw < 0 || type->getScale() > 9)
        return std::nullopt;

    static constexpr std::array<UInt64, 10> powers_of_ten{
        1ULL,
        10ULL,
        100ULL,
        1'000ULL,
        10'000ULL,
        100'000ULL,
        1'000'000ULL,
        10'000'000ULL,
        100'000'000ULL,
        1'000'000'000ULL,
    };
    const UInt64 multiplier = powers_of_ten[9 - type->getScale()];
    const UInt64 value = static_cast<UInt64>(raw);
    if (value > std::numeric_limits<UInt64>::max() / multiplier)
        return std::nullopt;
    return value * multiplier;
}

std::optional<std::pair<String, String>> mapElement(
    const ActionsDAG::Node * node,
    const InputColumns & inputs)
{
    node = unwrapAlias(node);
    if (!node || node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base
        || node->function_base->getName() != "arrayElement" || node->children.size() != 2)
        return std::nullopt;

    auto map_name = inputName(node->children[0], inputs);
    auto key = constantString(node->children[1]);
    if (!map_name || !key || key->empty() || (*map_name != "labels" && *map_name != "metadata"))
        return std::nullopt;
    return std::pair{std::move(*map_name), std::move(*key)};
}

std::optional<std::pair<String, String>> mapSubcolumn(
    const ActionsDAG::Node * node,
    const InputColumns & inputs)
{
    auto name = inputName(node, inputs);
    if (!name)
        return std::nullopt;

    String map_name;
    std::string_view serialized_key;
    if (name->starts_with("labels.key_"))
    {
        map_name = "labels";
        serialized_key = std::string_view(*name).substr(std::string_view("labels.key_").size());
    }
    else if (name->starts_with("metadata.key_"))
    {
        map_name = "metadata";
        serialized_key = std::string_view(*name).substr(std::string_view("metadata.key_").size());
    }
    else
        return std::nullopt;

    if (serialized_key.empty())
        return std::nullopt;

    try
    {
        DataTypeString key_type;
        auto key_column = key_type.createColumn();
        ReadBufferFromString buffer(serialized_key);
        key_type.getDefaultSerialization()->deserializeWholeText(*key_column, buffer, FormatSettings{});
        if (key_column->size() != 1)
            return std::nullopt;
        Field key = (*key_column)[0];
        if (key.getType() != Field::Types::String || key.safeGet<String>().empty())
            return std::nullopt;
        return std::pair{std::move(map_name), key.safeGet<String>()};
    }
    catch (...)
    {
        /// An unfamiliar future subcolumn encoding only disables pushdown.
        return std::nullopt;
    }
}

std::optional<std::pair<String, String>> mapLookup(
    const ActionsDAG::Node * node,
    const InputColumns & inputs)
{
    if (auto element = mapElement(node, inputs))
        return element;
    return mapSubcolumn(node, inputs);
}

String reversedComparison(const String & function)
{
    if (function == "less")
        return "greater";
    if (function == "lessOrEquals")
        return "greaterOrEquals";
    if (function == "greater")
        return "less";
    if (function == "greaterOrEquals")
        return "lessOrEquals";
    return function;
}

bool addTimestampComparison(
    const String & function,
    const ActionsDAG::Node * left,
    const ActionsDAG::Node * right,
    const InputColumns & inputs,
    TimestampBounds & bounds)
{
    auto name = inputName(left, inputs);
    auto nanos = dateTime64Nanos(right);
    String normalized = function;
    if (!name || *name != "timestamp" || !nanos)
    {
        name = inputName(right, inputs);
        nanos = dateTime64Nanos(left);
        normalized = reversedComparison(function);
    }
    if (!name || *name != "timestamp" || !nanos)
        return false;

    if (normalized == "greaterOrEquals")
        bounds.addStart(*nanos);
    else if (normalized == "greater")
    {
        if (*nanos == std::numeric_limits<UInt64>::max())
            bounds.impossible = true;
        else
            bounds.addStart(*nanos + 1);
    }
    else if (normalized == "less")
        bounds.addEnd(*nanos);
    else if (normalized == "lessOrEquals")
    {
        if (*nanos != std::numeric_limits<UInt64>::max())
            bounds.addEnd(*nanos + 1);
    }
    else if (normalized == "equals")
    {
        bounds.addStart(*nanos);
        if (*nanos == std::numeric_limits<UInt64>::max())
            bounds.impossible = true;
        else
            bounds.addEnd(*nanos + 1);
    }
    else
        return false;
    return true;
}

bool addMapEquality(
    const ActionsDAG::Node * left,
    const ActionsDAG::Node * right,
    const InputColumns & inputs,
    URIParams & equalities)
{
    auto element = mapLookup(left, inputs);
    auto value = constantString(right);
    if (!element || !value)
    {
        element = mapLookup(right, inputs);
        value = constantString(left);
    }
    /// ClickHouse returns the String default ("") for a missing Map key.
    /// ShardLog's exact-field index distinguishes missing from stored empty,
    /// so empty equality must remain a residual predicate.
    if (!element || !value || value->empty())
        return false;

    equalities.emplace_back(element->first + "." + element->second, std::move(*value));
    return true;
}

void collectPushdown(const ActionsDAG::Node * node, const InputColumns & inputs, Pushdown & pushdown)
{
    node = unwrapAlias(node);
    if (!node || node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
    {
        pushdown.fully_supported = false;
        return;
    }

    const String function = node->function_base->getName();
    if (function == "and")
    {
        for (const auto * child : node->children)
            collectPushdown(child, inputs, pushdown);
        return;
    }

    if (node->children.size() == 2
        && addTimestampComparison(function, node->children[0], node->children[1], inputs, pushdown.timestamps))
        return;

    if (function == "equals" && node->children.size() == 2
        && addMapEquality(node->children[0], node->children[1], inputs, pushdown.equalities))
        return;

    /// OR, NOT, LIKE, regexes, message functions, casts, and expressions over
    /// dynamic values stay in ClickHouse. Never guess at semantic equivalence.
    pushdown.fully_supported = false;
}

bool isShardLogColumn(const String & name)
{
    return name == "tenant" || name == "timestamp" || name == "partition" || name == "offset"
        || name == "message" || name == "labels" || name == "metadata";
}

std::optional<String> physicalShardLogColumn(const String & name)
{
    if (isShardLogColumn(name))
        return name;
    if (name.starts_with("labels."))
        return "labels";
    if (name.starts_with("metadata."))
        return "metadata";
    return std::nullopt;
}

std::optional<String> projection(const Names & column_names)
{
    if (column_names.empty())
        return std::nullopt;

    std::unordered_set<String> seen;
    String result;
    for (const auto & name : column_names)
    {
        auto physical_name = physicalShardLogColumn(name);
        if (!physical_name)
            return std::nullopt;
        if (!seen.emplace(*physical_name).second)
            continue;
        if (!result.empty())
            result += ',';
        result += *physical_name;
    }
    if (result.empty())
        return std::nullopt;
    return result;
}

}

std::vector<std::pair<std::string, std::string>> StorageShardLog::getReadURIParams(
    const Names & column_names,
    const StorageSnapshotPtr &,
    const SelectQueryInfo & query_info,
    const ContextPtr &,
    QueryProcessingStage::Enum &,
    size_t) const
{
    URIParams params;
    if (auto columns = projection(column_names))
        params.emplace_back("columns", std::move(*columns));

    Pushdown pushdown;
    if (query_info.filter_actions_dag && !query_info.filter_actions_dag->getOutputs().empty())
    {
        const auto inputs = query_info.buildNodeNameToInputNodeColumn();
        collectPushdown(query_info.filter_actions_dag->getOutputs().front(), inputs, pushdown);
    }

    if (pushdown.timestamps.impossible)
        params.emplace_back("limit", "0");
    else
    {
        if (pushdown.timestamps.start)
            params.emplace_back("start_ns", std::to_string(*pushdown.timestamps.start));
        if (pushdown.timestamps.end)
            params.emplace_back("end_ns", std::to_string(*pushdown.timestamps.end));
        params.insert(params.end(), pushdown.equalities.begin(), pushdown.equalities.end());
        if (query_info.trivial_limit && query_info.filter_actions_dag && pushdown.fully_supported)
            params.emplace_back("limit", std::to_string(query_info.trivial_limit));
    }
    return params;
}

void registerStorageShardLog(StorageFactory & factory)
{
    factory.registerStorage(
        "ShardLog",
        [](const StorageFactory::Arguments & args)
        {
            ASTs & engine_args = args.engine_args;
            auto configuration = StorageURL::getConfiguration(engine_args, args.getLocalContext(), &args.table_id);
            if (configuration.format != "ArrowStream")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "ShardLog storage requires the ArrowStream format");

            auto context = args.getLocalContext();
            return std::make_shared<StorageShardLog>(
                configuration.url,
                args.table_id,
                configuration.format,
                StorageURL::getFormatSettingsFromArgs(args),
                args.columns,
                args.constraints,
                args.comment,
                context,
                configuration.compression_method,
                configuration.headers,
                configuration.http_method);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = false,
            .source_access_type = AccessTypeObjects::Source::URL,
            .has_builtin_setting_fn = Settings::hasBuiltin,
        });
}

}
