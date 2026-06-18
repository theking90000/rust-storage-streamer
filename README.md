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
- a per-stream token-bucket pacer that also handles writes larger than its burst.

Network transport, signed-URL coordination, and concrete cryptography are not
implemented yet. Their external contracts need to be specified before they can
be integrated safely.

Run the test suite with:

```sh
cargo test
```
