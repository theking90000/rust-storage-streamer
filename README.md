# s-streamer

Async streaming primitives for turning a lazy sequence of framed objects into
one ordered stream.

## Implemented core

The crate currently provides:

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

`StreamSession` implements `Stream<Item = Result<Bytes, BoxError>>` directly.
It polls an object's URL ticket only after at least one of its frames is
authorized, and polls each sequential download only up to its authorization.
Output pops exclusively from the front object's buffer, keeping head-of-line
ordering explicit without channels, detached tasks, global frame slots, or a
separate action queue.

The scheduler, sizing policy, request and budget know only frames. A
higher HTTP adapter is responsible for converting local frame ranges into
physical byte ranges and for clipping decrypted first/last frames when a public
API exposes byte-granular ranges.

The effective target rate is the minimum of the allocated stream rate, the
consumer rate, and the memory-safe rate derived from the granted frame capacity.
It sizes buffering and prefetch; it does not pace output. Ready frames are
returned immediately and transport backpressure remains outside the core.

`TransferModel` currently receives fixed object throughput, TTFB, URL latency
and object frame count. A caller may update it later as measurements improve.

Network transport, signed-URL coordination, and concrete cryptography are not
implemented yet. Their external contracts need to be specified before they can
be integrated safely.

Run the test suite with:

```sh
cargo test
```
