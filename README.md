# s-streamer workspace

A Cargo workspace around bounded, framed HTTP streaming.

## Crates

- [`crates/frame-streamer`](crates/frame-streamer/README.md) — the core library
  (`frame_streamer`): async streaming primitives that turn a lazy sequence of
  framed objects into one ordered stream. Network transport and cryptography are
  left to adapters.
- [`crates/cli`](crates/cli) — a minimal example binary (`frame-streamer-cli`)
  that drives a `StreamSession` over real HTTP with async `reqwest` and a
  hardcoded object catalog.

## Usage

```sh
# Test the core library
cargo test -p frame-streamer

# Run the example application
cargo run -p cli
```
