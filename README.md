# s-streamer

Async streaming primitives for mapping a logical byte range onto framed HTTP
objects. The implementation is being built from the pure, testable core
outward: planning first, then framing, budgets, HTTP, URL coordination, and the
streaming pipeline.

## Implemented core

The crate currently provides:

- a validated request model with explicit final-object frame counts;
- a planner mapping logical ranges to complete, object-local encrypted frames;
- HTTP chunk-to-frame assembly with incomplete-body detection;
- a cryptographic decoder boundary and zero-copy logical clipping;
- a coarse global memory budget that never exceeds its configured hard limit;
- a per-stream token-bucket pacer that also handles writes larger than its burst;
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

Network transport, signed-URL coordination, and concrete cryptography are not
implemented yet. Their external contracts need to be specified before they can
be integrated safely.

Run the test suite with:

```sh
cargo test
```
