# Contributing to ShardLog

Thank you for helping improve ShardLog.

## Development

ShardLog uses the Rust toolchain pinned in `rust-toolchain.toml`. A change is
ready for review when these commands pass:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo build --workspace --all-targets --release
```

Add regression tests for behavioral changes. Storage-format, recovery, query,
and protocol changes require malformed-input and restart-boundary coverage.
Benchmark claims must include the command, corpus identity, machine, CPU
allocation, build profile, and verification mode.

## Pull requests

Keep changes focused and explain compatibility or operational consequences.
Do not commit credentials, production log content, benchmark corpora, build
artifacts, or generated secrets. By submitting a contribution, you agree that
it is licensed under the Apache License 2.0.

Use GitHub's private vulnerability-reporting flow for security issues; see
`SECURITY.md`.
