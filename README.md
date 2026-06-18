# s-streamer

Async streaming primitives for turning a lazy sequence of framed objects into
one ordered stream.

## Implemented core

The crate currently provides:

- a frame-native `StreamRequest` containing only a global `Range<u64>`;
- HTTP chunk-to-frame assembly with incomplete-body detection;
- a cryptographic decoder boundary;
- a global `FrameBudget` semaphore;
- a frame-per-second token-bucket pacer;
- a sizing policy converting target rates and measured latencies into frame counts;
- a single-owner `StreamSession` that polls its lazy object source, URL tickets,
  sequential object downloads, and per-object frame buffers;
- progressive live resizing: growth immediately extends download rights, while
  shrinkage drains already committed slots before releasing them.

`StreamSession` implements `Stream<Item = Result<Bytes, BoxError>>` directly.
It polls an object's URL ticket only after at least one of its frames is
authorized, and polls each sequential download only up to its authorization.
Output pops exclusively from the front object's buffer, keeping head-of-line
ordering explicit without channels, detached tasks, global frame slots, or a
separate action queue.

The scheduler, sizing policy, pacer, request and budget know only frames. A
higher HTTP adapter is responsible for converting local frame ranges into
physical byte ranges and for clipping decrypted first/last frames when a public
API exposes byte-granular ranges.

Network transport, signed-URL coordination, and concrete cryptography are not
implemented yet. Their external contracts need to be specified before they can
be integrated safely.

Run the test suite with:

```sh
cargo test
```
