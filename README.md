# rust-storage-streamer

A Cargo workspace for bounded, framed HTTP storage streaming.

## Crates

- [`frame-streamer`](crates/frame-streamer/README.md) — core library: async
  streaming primitives that turn a lazy sequence of framed objects into one
  ordered stream. Transport and cryptography left to adapters.
- [`server`](crates/server/README.md) — Axum-based HTTP server with SQLite
  catalog, segmented upload/download, and AES-256-GCM encryption.
- [`cli`](crates/cli/README.md) — minimal example binary (`frame-streamer-cli`)
  that drives a `StreamSession` over real HTTP with `reqwest`.

## Usage

```sh
# Test the core library
cargo test -p frame-streamer

# Run the server
cargo run -p server

# Run the CLI client
cargo run -p cli
```
