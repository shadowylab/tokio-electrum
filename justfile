#!/usr/bin/env just --justfile

fmt:
    cargo fmt --all

check:
    cargo check --all

clippy:
    cargo clippy --all

test:
    cargo test --all

precommit: fmt check clippy test
