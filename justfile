check:
    cargo check --workspace
    cargo clippy --workspace -- -D warnings
    bash scripts/check-no-tokio.sh

install:
    cargo install --path crates/reckon-cli

test:
    cargo test --workspace
