#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

test -s LICENSE
test -s NOTICE
test -s THIRD_PARTY_NOTICES
grep -q '^                                 Apache License$' LICENSE || {
  echo "LICENSE is not the Apache License 2.0 distribution text" >&2
  exit 1
}
grep -q '^license = "Apache-2.0"$' Cargo.toml

cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked
cargo build --workspace --all-targets --all-features --release --locked
cargo audit --deny warnings
cargo deny check
