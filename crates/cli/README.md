# cli

`frame-streamer-cli` — upload a file or a piped stream to a
rust-storage-streamer HTTP backend.

It mirrors the web UI pipeline: `POST /files` →
parallel `PUT /files/{id}/segments/{index}` → `POST /files/{id}/complete`, then
prints the download URL to stdout. Segments are sent as plaintext; the server
encrypts. A live progress bar (file) or spinner (pipe) shows throughput and
blocks sent.

## Usage

```sh
# Upload a file (progress bar with %, MB/s, ETA, blocks)
frame-streamer-cli ./video.mp4 --content-type video/mp4

# Upload from a pipe / stdin (spinner with MB/s, blocks sent)
cat ./video.mp4 | frame-streamer-cli --name video.mp4
frame-streamer-cli --name dump.sql < dump.sql

# Tune concurrency and target a different backend
frame-streamer-cli ./big.iso -p 8 -b http://localhost:8080
```

## Options

| Flag | Default | |
|------|---------|--|
| `[INPUT]` | stdin | file to upload; omit to read a pipe/stdin |
| `-b, --backend <URL>` | `https://wd40.theking90000.be` | backend base URL (env `WD40_BACKEND`) |
| `-p, --parallel <N>` | `4` | concurrent segment uploads |
| `--name <STR>` | input file name, or `stream` | name recorded server-side |
| `--content-type <STR>` | `application/octet-stream` | Content-Type recorded server-side |
| `--frame-size <N>` | `65536` | MUST match the server's `frame_size` |
| `--expected-size <BYTES>` | `8 GiB` | allocation ceiling for stdin uploads (ignored for files) |

For a file the exact size is sent as the allocation; for a pipe the size is
unknown up front, so `--expected-size` is used as an upper bound.
