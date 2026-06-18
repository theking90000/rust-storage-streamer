# s-streamer

Async streaming primitives for turning a lazy sequence of framed objects into
one ordered stream.

## Implemented core

The crate currently provides:

- a byte-granular `ByteStream` facade that maps logical byte ranges and rates
  to the frame-native scheduler, then clips the first and last plaintext frame;
- an `EncryptedBytesDownloadBackend` boundary receiving physical byte ranges, plus a
  `StreamDownloadBackend` adapter that assembles chunks and authenticates AES-256-GCM
  frames formatted as `tag || ciphertext`;
- a frame-native `StreamRequest` containing a global `Range<u64>` and its
  allocated average frame rate;
- HTTP chunk-to-frame assembly with incomplete-body detection;
- a cryptographic decoder boundary;
- a global `FrameBudget` semaphore;
- a transfer model converting target rates and backend latency into frame counts;
- a single-owner `StreamSession` that polls its lazy object source, URL tickets,
  sequential object downloads, and per-object frame buffers;
- progressive live resizing from the allocated rate, consumer rate and globally
  available frame budget. Growth extends download rights immediately; shrinkage
  stops new authorization and releases memory as committed frames drain.

`StreamSession::pipe_into(sink)` returns a `StreamDriver` future owning both the
session and its output. It keeps polling every URL ticket and authorized
download while the sink is backpressured, until the frame window is full. A
frame leaves the front object's buffer only after `Sink::poll_ready` succeeds.
This keeps head-of-line ordering explicit without channels, detached tasks,
global frame slots, or a separate action queue.

The scheduler, sizing policy, request and budget know only frames. `ByteStream`
is the only conversion boundary: output rates use plaintext payload bytes,
backend throughput uses encrypted physical bytes, and memory remains accounted
exactly through `FrameBudget`.

The effective target rate is the minimum of the allocated stream rate, the
consumer rate, and the memory-safe rate derived from the granted frame capacity.
It sizes buffering and prefetch; it does not pace output. A sink may implement
its own rate limiting, and its backpressure only pauses frame emission—not the
bounded background filling of Ready + Data.

```rust
session.pipe_into(output_sink).await?;
```

`TransferModel` currently receives fixed object throughput, TTFB, URL latency
and object frame count. A caller may update it later as measurements improve.

Network transport and signed-URL coordination remain caller-provided. Object
keys must be unique: each frame nonce is the local frame index encoded as four
zero bytes followed by a big-endian `u64`.

Run the test suite with:

```sh
cargo test
```
