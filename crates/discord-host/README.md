# discord-host

Production HTTP server assembling the [`discord`](../discord/README.md) storage
backend with the [`file-router`](../file-router/README.md) Axum component.

## Run

```sh
# webhooks.txt: one `<id>:<token>` per line (blank lines and `#` comments ignored)
DH_WEBHOOKS_FILE=webhooks.txt cargo run -p discord-host
```

## Configuration

Three layers, precedence **env > CLI > TOML**:

- env: `DH_BIND`, `DH_DATABASE_URL`, `DH_WEBHOOKS_FILE`, `DH_FRAME_SIZE`,
  `DH_MAX_FILE_SIZE`, `DH_TARGET_RATE`, `DH_OBJECT_RATE`, `DH_DATA_TTFB_MS`,
  `DH_URL_LATENCY_MS`, `DH_FRAME_BUDGET`, `DH_CONFIG` (TOML path).
- CLI: matching `--bind`, `--webhooks-file`, … flags plus `--config <file>`.
- TOML: same field names as keys (see `DH_CONFIG` / `--config`).

`webhooks_file` is the only required field. `frame_size` MUST match the value
used by clients; the streaming-rate fields are calibration knobs for real
Discord throughput.
