# files-gateway

Axum router component for `frame-streamer` objects — exposes `router(state)` and
`AppState`, with a SQLite-backed catalog, segmented file upload/download, and
AES-256-GCM encrypted storage. It does **not** bind a socket; an executable
(e.g. [`streamer-files-discord`](../streamer-files-discord/README.md)) assembles it with a storage
backend and serves it.
