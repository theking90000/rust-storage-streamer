# rust-storage-streamer

**Turn Discord webhooks into a high-throughput, encrypted file store.**

Discord lets any webhook post message attachments for free. This project treats
that as a block device: every file you upload is split into chunks, encrypted,
and parked inside webhook messages — then streamed back out, in order, fast
enough to feel like a normal download (the streaming engine targets ~60 MB/s and
speaks HTTP `Range`, so media seeks and resumable downloads just work).

It started as an excuse to build a _bounded_ streaming engine in Rust — one that
reassembles a lazy sequence of remote, encrypted frames into a single ordered
byte stream without unbounded buffering, detached tasks, or head-of-line
surprises. Discord is just the backend that made it fun.

## How it works

**Upload** is segmented and parallel. The client (the bundled web UI, or any
HTTP client) cuts the file into frame-aligned segments and `PUT`s them
concurrently. Each segment is encrypted with its own random AES-256-GCM key,
framed as `tag || ciphertext`, checksummed with BLAKE3, and pushed to a Discord
webhook. A SQLite catalog records the file, its segments, and the message URLs.

**Download** is sequential streaming. The server resolves the stored objects,
authenticates and decrypts frames on the fly, and pipes them to the client
behind a global memory budget. A logical byte `Range` is mapped down to physical
frames and clipped at the edges, so partial requests cost only what they ask
for. Responses carry `Content-Type`, `Content-Disposition`, `Accept-Ranges`, and
`Content-Range`.

The engine never holds the whole file in memory: a frame leaves its buffer only
once the output sink is ready for it, and the prefetch window grows or shrinks
live with the available memory budget and consumer speed.

## Architecture

A small Cargo workspace, each crate with one job:

| Crate                                               | Role                                                                                                                                                                                                            |
| --------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`frame-streamer`](crates/frame-streamer/README.md) | The core. Async primitives that turn a lazy sequence of framed, encrypted objects into one ordered `ByteStream`, with a global `FrameBudget` and a transfer model. Transport and crypto adapters are pluggable. |
| [`files-gateway`](crates/files-gateway/README.md) | Files HTTP gateway: SQLite catalog, segmented upload/download endpoints, AES-256-GCM, and range requests. Store-agnostic. |
| [`s3-gateway`](crates/s3-gateway/README.md) | Private S3-compatible gateway built on `s3s`, with SigV4, buckets, metadata, ranges, multipart uploads, and server-side copies. |
| [`discord-store`](crates/discord-store) | Physical store using Discord webhooks, with rate-limit handling. |
| [`streamer-files-discord`](crates/streamer-files-discord/README.md) | Flavored server assembling `files-gateway` with `discord-store`. |
| [`streamer-s3-discord`](crates/streamer-s3-discord/README.md) | Flavored server assembling `s3-gateway` with `discord-store`. |
| [`files-cli`](crates/files-cli/README.md) | `streamer-files-cli` uploads files or piped streams to the files gateway. |

## Quick start

```sh
# 1. Create a webhooks file — one `<id>:<token>` per line
echo '123456789:your-webhook-token' > webhooks.txt

# 2. Run the server (defaults to 127.0.0.1:8080)
cargo run -p streamer-files-discord -- --webhooks-file webhooks.txt

# Optional WireProxy / HTTP(S) endpoint used by Discord API client
cargo run -p streamer-files-discord -- \
  --webhooks-file webhooks.txt \
  --proxy-url socks5h://127.0.0.1:25344

# 3. Open http://localhost:8080 and drop a file in
```

The web UI shows live throughput and per-chunk progress. Downloads are linkable
straight from the result.

### From the command line

`streamer-files-cli` does the same upload pipeline as the web UI, from a file or
a pipe, with a live progress bar and parallel segments:

```sh
# A file (progress bar: %, MB/s, ETA, blocks)
cargo run -p files-cli -- ./video.mp4 --content-type video/mp4

# A pipe / stdin (spinner: MB/s, blocks sent), 8-way parallel
cat ./video.mp4 | cargo run -p files-cli -- --name video.mp4 -p 8
```

It prints the download URL to stdout. The backend defaults to
`https://wd40.theking90000.be` and can be overridden with `-b/--backend` or the
`WD40_BACKEND` env var — see the [`cli` README](crates/cli/README.md).

### HTTP API

| Method         | Path                           |                                                         |
| -------------- | ------------------------------ | ------------------------------------------------------- |
| `POST`         | `/files`                       | create a file (`name`, `content_type`, `expected_size`) |
| `PUT`          | `/files/{id}/segments/{index}` | upload one segment                                      |
| `POST`         | `/files/{id}/complete`         | finalize                                                |
| `GET` / `HEAD` | `/files/{id}`                  | download (supports `Range`)                             |
| `GET`          | `/`                            | web upload UI                                           |

Configuration (bind address, frame size, rate calibration, file-size limits)
is layered **env > CLI > TOML** — see the
[`streamer-files-discord` README](crates/streamer-files-discord/README.md).

### S3 / rclone

The separate `streamer-s3-discord` binary exposes only an authenticated S3 API. Create a
credential, start it, then configure rclone with `provider = Other`, path-style
access, endpoint `http://localhost:8080`, and region `us-east-1`:

```sh
cargo run -p streamer-s3-discord -- credential create --can-create-buckets
cargo run -p streamer-s3-discord -- serve --webhooks-file webhooks.txt
```

See the [`streamer-s3-discord` README](crates/streamer-s3-discord/README.md) for the rclone config and
credential grant commands.

## Build with Nix

A flake is provided; the package builds the whole workspace with no system
dependencies (vendored SQLite, rustls).

```sh
nix build              # -> ./result/bin/streamer-files-discord
nix run .#streamer-files-discord
nix run .#streamer-s3-discord
nix run .#streamer-files-cli
nix develop            # dev shell with rust-analyzer, clippy, rustfmt
```

## Development

```sh
cargo test             # whole workspace; the live Discord test is #[ignore]d
cargo test -p frame-streamer   # just the core engine
```
