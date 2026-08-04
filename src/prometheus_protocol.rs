//! Apache-2.0 Prometheus protobuf wire contracts used for clean interoperability.
//!
//! Field numbers follow the Prometheus 3.13 `prompb` schemas. The compact local
//! definitions deliberately omit Go-specific gogoproto options, which do not
//! affect the protobuf wire format.

pub(crate) mod v1 {
    use prost::{Enumeration, Message};

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct WriteRequest {
        #[prost(message, repeated, tag = "1")]
        pub timeseries: Vec<TimeSeries>,
        #[prost(message, repeated, tag = "3")]
        pub metadata: Vec<MetricMetadata>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct TimeSeries {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(message, repeated, tag = "3")]
        pub exemplars: Vec<Exemplar>,
        #[prost(message, repeated, tag = "4")]
        pub histograms: Vec<Histogram>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct Label {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub value: String,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct Sample {
        #[prost(double, tag = "1")]
        pub value: f64,
        #[prost(int64, tag = "2")]
        pub timestamp: i64,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct Exemplar {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(double, tag = "2")]
        pub value: f64,
        #[prost(int64, tag = "3")]
        pub timestamp: i64,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct MetricMetadata {
        #[prost(enumeration = "MetricType", tag = "1")]
        pub r#type: i32,
        #[prost(string, tag = "2")]
        pub metric_family_name: String,
        #[prost(string, tag = "4")]
        pub help: String,
        #[prost(string, tag = "5")]
        pub unit: String,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
    #[repr(i32)]
    pub(crate) enum MetricType {
        Unknown = 0,
        Counter = 1,
        Gauge = 2,
        Histogram = 3,
        GaugeHistogram = 4,
        Summary = 5,
        Info = 6,
        StateSet = 7,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct Histogram {
        #[prost(oneof = "histogram::Count", tags = "1, 2")]
        pub count: Option<histogram::Count>,
        #[prost(double, tag = "3")]
        pub sum: f64,
        #[prost(sint32, tag = "4")]
        pub schema: i32,
        #[prost(double, tag = "5")]
        pub zero_threshold: f64,
        #[prost(oneof = "histogram::ZeroCount", tags = "6, 7")]
        pub zero_count: Option<histogram::ZeroCount>,
        #[prost(message, repeated, tag = "8")]
        pub negative_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, tag = "9")]
        pub negative_deltas: Vec<i64>,
        #[prost(double, repeated, tag = "10")]
        pub negative_counts: Vec<f64>,
        #[prost(message, repeated, tag = "11")]
        pub positive_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, tag = "12")]
        pub positive_deltas: Vec<i64>,
        #[prost(double, repeated, tag = "13")]
        pub positive_counts: Vec<f64>,
        #[prost(enumeration = "histogram::ResetHint", tag = "14")]
        pub reset_hint: i32,
        #[prost(int64, tag = "15")]
        pub timestamp: i64,
        #[prost(double, repeated, tag = "16")]
        pub custom_values: Vec<f64>,
        #[prost(int64, tag = "17")]
        pub start_timestamp: i64,
    }

    pub(crate) mod histogram {
        use prost::{Enumeration, Oneof};

        #[derive(Clone, Copy, PartialEq, Oneof)]
        pub(crate) enum Count {
            #[prost(uint64, tag = "1")]
            Int(u64),
            #[prost(double, tag = "2")]
            Float(f64),
        }

        #[derive(Clone, Copy, PartialEq, Oneof)]
        pub(crate) enum ZeroCount {
            #[prost(uint64, tag = "6")]
            Int(u64),
            #[prost(double, tag = "7")]
            Float(f64),
        }

        #[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
        #[repr(i32)]
        pub(crate) enum ResetHint {
            Unknown = 0,
            Yes = 1,
            No = 2,
            Gauge = 3,
        }
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct BucketSpan {
        #[prost(sint32, tag = "1")]
        pub offset: i32,
        #[prost(uint32, tag = "2")]
        pub length: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct ReadRequest {
        #[prost(message, repeated, tag = "1")]
        pub queries: Vec<Query>,
        #[prost(enumeration = "ReadRequestResponseType", repeated, tag = "2")]
        pub accepted_response_types: Vec<i32>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct Query {
        #[prost(int64, tag = "1")]
        pub start_timestamp_ms: i64,
        #[prost(int64, tag = "2")]
        pub end_timestamp_ms: i64,
        #[prost(message, repeated, tag = "3")]
        pub matchers: Vec<LabelMatcher>,
        #[prost(message, optional, tag = "4")]
        pub hints: Option<ReadHints>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct LabelMatcher {
        #[prost(enumeration = "LabelMatcherType", tag = "1")]
        pub r#type: i32,
        #[prost(string, tag = "2")]
        pub name: String,
        #[prost(string, tag = "3")]
        pub value: String,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
    #[repr(i32)]
    pub(crate) enum LabelMatcherType {
        Equal = 0,
        NotEqual = 1,
        RegexMatch = 2,
        RegexNoMatch = 3,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct ReadHints {
        #[prost(int64, tag = "1")]
        pub step_ms: i64,
        #[prost(string, tag = "2")]
        pub func: String,
        #[prost(int64, tag = "3")]
        pub start_ms: i64,
        #[prost(int64, tag = "4")]
        pub end_ms: i64,
        #[prost(string, repeated, tag = "5")]
        pub grouping: Vec<String>,
        #[prost(bool, tag = "6")]
        pub by: bool,
        #[prost(int64, tag = "7")]
        pub range_ms: i64,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
    #[repr(i32)]
    pub(crate) enum ReadRequestResponseType {
        Samples = 0,
        StreamedXorChunks = 1,
    }

    #[allow(dead_code)]
    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct ReadResponse {
        #[prost(message, repeated, tag = "1")]
        pub results: Vec<QueryResult>,
    }

    #[allow(dead_code)]
    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct QueryResult {
        #[prost(message, repeated, tag = "1")]
        pub timeseries: Vec<TimeSeries>,
    }
}

pub(crate) mod v2 {
    use prost::{Enumeration, Message};

    use super::v1::BucketSpan;

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct Request {
        #[prost(string, repeated, tag = "4")]
        pub symbols: Vec<String>,
        #[prost(message, repeated, tag = "5")]
        pub timeseries: Vec<TimeSeries>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct TimeSeries {
        #[prost(uint32, repeated, tag = "1")]
        pub labels_refs: Vec<u32>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(message, repeated, tag = "3")]
        pub histograms: Vec<Histogram>,
        #[prost(message, repeated, tag = "4")]
        pub exemplars: Vec<Exemplar>,
        #[prost(message, optional, tag = "5")]
        pub metadata: Option<Metadata>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct Sample {
        #[prost(double, tag = "1")]
        pub value: f64,
        #[prost(int64, tag = "2")]
        pub timestamp: i64,
        #[prost(int64, tag = "3")]
        pub start_timestamp: i64,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct Exemplar {
        #[prost(uint32, repeated, tag = "1")]
        pub labels_refs: Vec<u32>,
        #[prost(double, tag = "2")]
        pub value: f64,
        #[prost(int64, tag = "3")]
        pub timestamp: i64,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct Metadata {
        #[prost(enumeration = "MetricType", tag = "1")]
        pub r#type: i32,
        #[prost(uint32, tag = "3")]
        pub help_ref: u32,
        #[prost(uint32, tag = "4")]
        pub unit_ref: u32,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
    #[repr(i32)]
    pub(crate) enum MetricType {
        Unspecified = 0,
        Counter = 1,
        Gauge = 2,
        Histogram = 3,
        GaugeHistogram = 4,
        Summary = 5,
        Info = 6,
        StateSet = 7,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct Histogram {
        #[prost(oneof = "histogram::Count", tags = "1, 2")]
        pub count: Option<histogram::Count>,
        #[prost(double, tag = "3")]
        pub sum: f64,
        #[prost(sint32, tag = "4")]
        pub schema: i32,
        #[prost(double, tag = "5")]
        pub zero_threshold: f64,
        #[prost(oneof = "histogram::ZeroCount", tags = "6, 7")]
        pub zero_count: Option<histogram::ZeroCount>,
        #[prost(message, repeated, tag = "8")]
        pub negative_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, tag = "9")]
        pub negative_deltas: Vec<i64>,
        #[prost(double, repeated, tag = "10")]
        pub negative_counts: Vec<f64>,
        #[prost(message, repeated, tag = "11")]
        pub positive_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, tag = "12")]
        pub positive_deltas: Vec<i64>,
        #[prost(double, repeated, tag = "13")]
        pub positive_counts: Vec<f64>,
        #[prost(enumeration = "histogram::ResetHint", tag = "14")]
        pub reset_hint: i32,
        #[prost(int64, tag = "15")]
        pub timestamp: i64,
        #[prost(double, repeated, tag = "16")]
        pub custom_values: Vec<f64>,
        #[prost(int64, tag = "17")]
        pub start_timestamp: i64,
    }

    pub(crate) mod histogram {
        use prost::{Enumeration, Oneof};

        #[derive(Clone, Copy, PartialEq, Oneof)]
        pub(crate) enum Count {
            #[prost(uint64, tag = "1")]
            Int(u64),
            #[prost(double, tag = "2")]
            Float(f64),
        }

        #[derive(Clone, Copy, PartialEq, Oneof)]
        pub(crate) enum ZeroCount {
            #[prost(uint64, tag = "6")]
            Int(u64),
            #[prost(double, tag = "7")]
            Float(f64),
        }

        #[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
        #[repr(i32)]
        pub(crate) enum ResetHint {
            Unspecified = 0,
            Yes = 1,
            No = 2,
            Gauge = 3,
        }
    }
}
