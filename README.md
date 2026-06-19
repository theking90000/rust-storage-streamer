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

Use it through whichever interface fits the job:

- **S3-compatible API** for rclone and other S3 clients.
- **Native HTTP API** for direct uploads, downloads, ranges, and application integration.
- **Network disk** by mounting the S3-compatible endpoint with `rclone mount`.

## How it works

**Upload** is segmented and parallel. The client (the bundled web UI, or any
HTTP client) cuts the file into frame-aligned segments and `PUT`s them
concurrently. AES-256-GCM is applied **per frame, not per segment**: each
segment carries its own random key, but the file is sealed in fixed-size frames
of 2^16 bytes — a 16-byte authentication tag plus 65,520 bytes of ciphertext,
laid out as `tag || ciphertext` — with the frame's index as its nonce. The whole
segment is then checksummed with BLAKE3 and pushed to a Discord webhook. A SQLite
catalog records the file, its segments, and the message URLs.

Because every frame is an independent, self-authenticating GCM unit, the store
supports fast random-access seeking: a read maps directly to the frames that
cover it and decrypts only those, instead of having to authenticate an entire
segment to reach an offset inside it.

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

## Install prebuilt binaries

Release archives are available for macOS ARM64, Linux ARM64, and Linux
x86-64-v3. The following snippet detects the platform, downloads the latest
matching archive, and installs all three executables in `/usr/local/bin`:

```sh
case "$(uname -s):$(uname -m)" in
  Darwin:arm64)  target=aarch64-apple-darwin ;;
  Linux:aarch64) target=aarch64-unknown-linux-gnu ;;
  Linux:x86_64)  target=x86_64-unknown-linux-gnu-v3 ;;
  *) echo "Unsupported platform: $(uname -s) $(uname -m)" >&2; exit 1 ;;
esac

version=$(curl -fsSLI -o /dev/null -w '%{url_effective}' \
  https://github.com/theking90000/rust-storage-streamer/releases/latest)
version=${version##*/}
package="rust-storage-streamer-${version}-${target}"

curl -fLo "${package}.tar.gz" \
  "https://github.com/theking90000/rust-storage-streamer/releases/latest/download/${package}.tar.gz"
tar -xzf "${package}.tar.gz"
sudo install -m755 \
  "${package}/streamer-files-discord" \
  "${package}/streamer-s3-discord" \
  "${package}/streamer-files-cli" \
  /usr/local/bin/
```

The installed executables are:

- `streamer-files-discord`: native HTTP files gateway backed by Discord.
- `streamer-s3-discord`: S3-compatible gateway backed by Discord.
- `streamer-files-cli`: command-line uploader for the HTTP files gateway.

The Linux x86_64 build targets the x86-64-v3 instruction set and requires a
compatible CPU (AVX2, BMI2, and FMA support).

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
`WD40_BACKEND` env var — see the [`files-cli` README](crates/files-cli/README.md).

### HTTP API

`streamer-files-discord` exposes the project-native HTTP interface. It is the
smallest option for a web UI, custom application, or direct streaming client:
uploads are split into independently encrypted segments, while downloads are a
normal HTTP resource with `HEAD` and byte-range support.

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

### S3-compatible API

The separate `streamer-s3-discord` binary exposes an authenticated,
S3-compatible API backed by Discord webhooks. It supports SigV4 authentication,
private buckets, metadata, ranges, multipart uploads, and server-side copies.
It is intended for clients such as rclone; it is not an AWS service.

Create a credential and start the gateway:

```sh
cargo run -p streamer-s3-discord -- credential create --can-create-buckets
cargo run -p streamer-s3-discord -- serve --webhooks-file webhooks.txt
```

Configure rclone with the access and secret keys printed by the first command:

```ini
[streamer]
type = s3
provider = Other
access_key_id = ACCESS_KEY
secret_access_key = SECRET_KEY
endpoint = http://localhost:8080
region = us-east-1
force_path_style = true
```

Normal rclone operations such as `mkdir`, `copy`, `sync`, `check`, `ls`,
`moveto`, `delete`, and `purge` work against this remote.

### Mount Discord webhook storage as a network disk via rclone

Once the `streamer` remote above is configured, create a bucket and mount it:

```sh
rclone mkdir streamer:storage
mkdir -p ~/mnt/discord-storage
rclone mount streamer:storage ~/mnt/discord-storage --vfs-cache-mode writes
```

Files written under `~/mnt/discord-storage` are encrypted and stored as Discord
webhook attachments. Keep `rclone mount` running while the disk is mounted.
FUSE support is required (`macFUSE` on macOS or FUSE on Linux); Windows can use
WinFsp. `--vfs-cache-mode writes` lets applications use ordinary file operations
even though the underlying storage is object-based.

See the [`streamer-s3-discord` README](crates/streamer-s3-discord/README.md) for
credential grants, proxy configuration, and the complete list of tested rclone
operations.

## Build with Nix

A flake is provided with one package per executable flavor. Each package builds
only its selected binary and has no system SQLite/OpenSSL dependency (vendored
SQLite, rustls).

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
