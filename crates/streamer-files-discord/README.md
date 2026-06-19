# streamer-files-discord

Production HTTP server assembling the [`discord-store`](../discord-store) backend
with the [`files-gateway`](../files-gateway/README.md) Axum component.

## Run

```sh
# webhooks.txt: one `<id>:<token>` per line (blank lines and `#` comments ignored)
cargo run -p streamer-files-discord -- --webhooks-file webhooks.txt

# Optional: route Discord API traffic through one proxy
cargo run -p streamer-files-discord -- \
  --webhooks-file webhooks.txt \
  --proxy-url socks5h://127.0.0.1:25344
```

## Configuration

Three layers, precedence **env > CLI > TOML**:

- env: `STREAMER_BIND`, `STREAMER_DATABASE_URL`, `DISCORD_WEBHOOKS_FILE`,
  `DISCORD_PROXY_URL`, `STREAMER_FRAME_SIZE`, `FILES_MAX_FILE_SIZE`,
  `STREAMER_TARGET_RATE`, `STREAMER_OBJECT_RATE`, `STREAMER_DATA_TTFB_MS`,
  `STREAMER_URL_LATENCY_MS`, `STREAMER_FRAME_BUDGET`, `FILES_CONFIG` (TOML path).
- CLI: matching `--bind`, `--webhooks-file`, … flags plus `--config <file>`.
- TOML: same field names as keys (see `FILES_CONFIG` / `--config`).

`webhooks_file` is the only required field. `proxy_url` accepts `http://`,
`https://`, `socks5://`, and `socks5h://`; `socks5h` resolves DNS through the
proxy. `frame_size` MUST match the value
used by clients; the streaming-rate fields are calibration knobs for real
Discord throughput.
