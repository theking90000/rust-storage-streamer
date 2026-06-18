# s-streamer

Async streaming primitives for mapping a logical byte range onto framed HTTP
objects. The implementation is being built from the pure, testable core
outward: planning first, then framing, budgets, HTTP, URL coordination, and the
streaming pipeline.

## Current scope

The crate currently provides a synchronous read planner. It:

- maps a logical `[offset, offset + size)` request to object-local HTTP ranges;
- always requests complete encrypted frames;
- clips requests at the real logical EOF;
- supports a shorter final object through per-object frame counts;
- rejects malformed object layouts and arithmetic overflow.

Run the test suite with:

```sh
cargo test
```

