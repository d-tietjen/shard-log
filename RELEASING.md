# Releasing ShardTelemetry

Releases are built from annotated `vMAJOR.MINOR.PATCH` tags by GitHub Actions.
ShardTelemetry is distributed as a GitHub source archive and attested server binary;
`publish = false` prevents an unusable crates.io package while shard-stream is
consumed from pinned Git revisions.

1. Update `CHANGELOG.md`, `Cargo.toml`, and `Cargo.lock` with the release date
   and version.
2. Run `bash scripts/release-gate.sh` on Linux and retain its output with the
   release evidence.
3. Confirm required CI and supply-chain checks pass on the release commit.
4. Create and push an annotated version tag.
5. Verify the GitHub release contains the Linux binary archive, source archive,
   Apache and third-party notices, SHA-256 checksums, SPDX SBOM, and
   build-provenance attestation.
6. Install the archive on a clean Linux host and run `shard-telemetry-server --help`
   before publishing the release notes.

Never retag or replace a published release. Issue a new patch version instead.
