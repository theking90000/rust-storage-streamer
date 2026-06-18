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
- a lazy sliding-window controller with one sequential frame buffer per object;
- progressive live resizing: growth immediately extends download rights, while
  shrinkage drains already committed slots before releasing them.

The window controller consumes a `Stream<Item = Result<ObjectMeta, E>>` only as
far as its URL-prefetch horizon. Its methods return transport-neutral actions:

```text
FetchUrl
OpenDownload { full_local_range, authorized_local_end }
AdvanceDownload { authorized_local_end }
```

An HTTP integration can stop polling an object's sequential response whenever
`authorized_local_end` is reached. Objects are never copied between Ready,
Data, and Prefetch; those zones are calculated as frame-range intersections
over one `ObjectPlan`. Output simply pops the first buffered frame from the
front object, so head-of-line blocking remains explicit.

Network transport, signed-URL coordination, and concrete cryptography are not
implemented yet. Their external contracts need to be specified before they can
be integrated safely.

Run the test suite with:

```sh
cargo test
```
