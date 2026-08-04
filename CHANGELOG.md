# Changelog

All notable changes to ShardTelemetry will be documented here. The project follows
Semantic Versioning after `1.0.0`; pre-1.0 releases may change unstable storage
and protocol interfaces when called out in release notes.

## [Unreleased]

No changes yet.

## [0.1.0] - 2026-08-03

### Added

- Apache License 2.0 distribution files and public contribution policies.
- Automated formatting, lint, test, supply-chain, package, and release checks.
- Append-aligned immutable payload/query-index publication, cold range reads,
  bounded SSD caching, and catalog-checkpoint recovery.
- Loki POST form compatibility, parser/formatting pipelines, unwrapped range
  functions, vector aggregation, binary operators, and vector matching.
- Required third-party notices, SPDX SBOM generation, provenance attestation,
  and immutable GitHub Actions dependencies.

### Removed

- GPL/AGPL codec dependencies from the public build and codec benchmark.

- Initial public release of the single-tenant ShardTelemetry storage engine.
