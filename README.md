# rust-storage-streamer

A Cargo workspace for bounded, framed HTTP storage streaming.

## Crates

- [`frame-streamer`](crates/frame-streamer/README.md) — core library: async
  streaming primitives that turn a lazy sequence of framed objects into one
  ordered stream. Transport and cryptography left to adapters.
- [`file-router`](crates/file-router/README.md) — Axum router component with
  SQLite catalog, segmented upload/download, and AES-256-GCM encryption.
- [`discord`](crates/discord/README.md) — Discord-webhook storage backend.
- [`discord-host`](crates/discord-host/README.md) — production HTTP server that
  assembles `file-router` + `discord`.
- [`cli`](crates/cli/README.md) — minimal example binary (`frame-streamer-cli`)
  that drives a `StreamSession` over real HTTP with `reqwest`.

## Usage

```sh
# Test the core library
cargo test -p frame-streamer

# Run the server (needs a webhooks file: <id>:<token> per line)
DH_WEBHOOKS_FILE=webhooks.txt cargo run -p discord-host

# Run the CLI client
cargo run -p cli
```
