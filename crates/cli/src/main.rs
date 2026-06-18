//! `frame-streamer-cli` — upload a file or a piped stream to a
//! rust-storage-streamer HTTP backend.
//!
//! Mirrors the web UI pipeline: `POST /files` → parallel
//! `PUT /files/{id}/segments/{index}` → `POST /files/{id}/complete`, then prints
//! the download URL. The client sends plaintext segments; the server encrypts.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use clap::Parser;
use frame_streamer::{BoxError, TAG_SIZE};
use futures_util::{Stream, StreamExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Discord caps each object at 150 frames, so one segment holds at most 150.
const FRAMES_PER_SEGMENT: usize = 150;
/// Per-segment upload attempts before giving up (1 try + retries).
const MAX_RETRIES: u32 = 3;

#[derive(Parser)]
#[command(about = "Upload a file or stream to a rust-storage-streamer backend")]
struct Cli {
    /// File to upload. Omit to read from stdin (e.g. `cli < file` or a pipe).
    input: Option<PathBuf>,
    /// Backend base URL.
    #[arg(short, long, env = "WD40_BACKEND", default_value = "https://wd40.theking90000.be")]
    backend: String,
    /// Number of segments uploaded concurrently.
    #[arg(short, long, default_value_t = 4)]
    parallel: usize,
    /// File name recorded server-side (defaults to the input file name, or `stream`).
    #[arg(long)]
    name: Option<String>,
    /// Content-Type recorded server-side.
    #[arg(long, default_value = "application/octet-stream")]
    content_type: String,
    /// Frame size in bytes. MUST match the server's `frame_size`.
    #[arg(long, default_value_t = 1 << 16)]
    frame_size: usize,
    /// Allocation ceiling for stdin uploads (ignored when a file is given).
    #[arg(long, default_value_t = 8 * 1024 * 1024 * 1024)]
    expected_size: u64,
}

#[derive(Serialize)]
struct CreateFile<'a> {
    name: &'a str,
    content_type: &'a str,
    expected_size: u64,
}

#[derive(Deserialize)]
struct FileCreated {
    id: String,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let base = cli.backend.trim_end_matches('/').to_owned();
    if cli.frame_size <= TAG_SIZE {
        return Err(format!("frame-size must be greater than {TAG_SIZE}").into());
    }
    let segment_size = (cli.frame_size - TAG_SIZE) * FRAMES_PER_SEGMENT;

    // Resolve input into an async reader plus a known total size when possible.
    let (reader, total): (Box<dyn AsyncRead + Unpin + Send>, Option<u64>) = match &cli.input {
        Some(path) => {
            let file = tokio::fs::File::open(path).await?;
            let len = file.metadata().await?.len();
            (Box::new(file), Some(len))
        }
        None => (Box::new(tokio::io::stdin()), None),
    };
    let name = cli.name.clone().unwrap_or_else(|| match &cli.input {
        Some(path) => path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "stream".to_owned()),
        None => "stream".to_owned(),
    });

    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(120))
        .pool_max_idle_per_host(64)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .tcp_nodelay(true)
        .build()?;

    // Create the file: for a real file the exact length, otherwise the ceiling.
    let created: FileCreated = {
        let response = client
            .post(format!("{base}/files"))
            .json(&CreateFile {
                name: &name,
                content_type: &cli.content_type,
                expected_size: total.unwrap_or(cli.expected_size),
            })
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(format!("create failed: {}", response.text().await.unwrap_or_default()).into());
        }
        response.json().await?
    };
    let id = created.id;

    let total_blocks = total.map(|t| t.div_ceil(segment_size as u64));
    let uploaded = Arc::new(AtomicU64::new(0));
    let blocks = Arc::new(AtomicU64::new(0));
    let progress = make_progress(total);

    // Repaint position + block count on a timer so MB/s stays live even though
    // each segment only resolves every ~10 MB.
    let ticker = {
        let (progress, uploaded, blocks) = (progress.clone(), uploaded.clone(), blocks.clone());
        tokio::spawn(async move {
            loop {
                paint(&progress, &uploaded, &blocks, total_blocks);
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        })
    };

    // Read segments sequentially; `try_buffer_unordered` keeps `parallel` of them
    // in flight, which is what bounds read-ahead (and memory) to ~parallel blocks.
    let segments = segment_stream(reader, segment_size);
    let uploads = segments
        .map_ok(|(index, bytes)| {
            let url = format!("{base}/files/{id}/segments/{index}");
            upload_segment(client.clone(), url, bytes, cli.frame_size, uploaded.clone(), blocks.clone())
        })
        .try_buffer_unordered(cli.parallel);
    tokio::pin!(uploads);
    while let Some(result) = uploads.next().await {
        result?;
    }

    ticker.abort();
    paint(&progress, &uploaded, &blocks, total_blocks);

    let response = client.post(format!("{base}/files/{id}/complete")).send().await?;
    if !response.status().is_success() {
        return Err(format!("complete failed: {}", response.text().await.unwrap_or_default()).into());
    }
    progress.finish();
    println!("{base}/files/{id}");
    Ok(())
}

/// Reads `size`-byte segments from `reader` until EOF, yielding `(index, bytes)`.
fn segment_stream(
    mut reader: Box<dyn AsyncRead + Unpin + Send>,
    size: usize,
) -> impl Stream<Item = Result<(u32, Bytes), BoxError>> {
    async_stream::try_stream! {
        let mut index: u32 = 0;
        while let Some(bytes) = read_segment(&mut reader, size).await? {
            yield (index, bytes);
            index += 1;
        }
    }
}

/// Reads up to `size` bytes, returning `None` only at immediate EOF.
async fn read_segment<R: AsyncRead + Unpin>(reader: &mut R, size: usize) -> std::io::Result<Option<Bytes>> {
    let mut buf = BytesMut::with_capacity(size);
    while buf.len() < size {
        if reader.read_buf(&mut buf).await? == 0 {
            break;
        }
    }
    Ok((!buf.is_empty()).then(|| buf.freeze()))
}

/// Uploads one segment, retrying with exponential backoff. Feeds `uploaded` live
/// as the body streams; a failed attempt rolls its bytes back so the count stays
/// honest across retries.
async fn upload_segment(
    client: reqwest::Client,
    url: String,
    bytes: Bytes,
    frame_size: usize,
    uploaded: Arc<AtomicU64>,
    blocks: Arc<AtomicU64>,
) -> Result<(), BoxError> {
    let size = bytes.len() as u64;
    let mut last_error = String::new();
    for attempt in 0..=MAX_RETRIES {
        let counted = Arc::new(AtomicU64::new(0));
        let body = body_stream(bytes.clone(), frame_size, uploaded.clone(), counted.clone());
        let result = client.put(&url).body(reqwest::Body::wrap_stream(body)).send().await;
        match result {
            Ok(response) if response.status().is_success() => {
                // Account for any tail bytes the chunker didn't emit before send returned.
                uploaded.fetch_add(size.saturating_sub(counted.load(Ordering::Relaxed)), Ordering::Relaxed);
                blocks.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            Ok(response) => {
                uploaded.fetch_sub(counted.load(Ordering::Relaxed), Ordering::Relaxed);
                let status = response.status();
                last_error = format!("HTTP {status}: {}", response.text().await.unwrap_or_default());
            }
            Err(error) => {
                uploaded.fetch_sub(counted.load(Ordering::Relaxed), Ordering::Relaxed);
                last_error = error.to_string();
            }
        }
        if attempt < MAX_RETRIES {
            tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
        }
    }
    Err(format!("{url}: failed after {} attempts: {last_error}", MAX_RETRIES + 1).into())
}

/// Splits a segment into `frame_size` chunks, tallying bytes as reqwest drains them.
fn body_stream(
    bytes: Bytes,
    frame_size: usize,
    uploaded: Arc<AtomicU64>,
    counted: Arc<AtomicU64>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> {
    async_stream::stream! {
        let mut offset = 0;
        while offset < bytes.len() {
            let end = (offset + frame_size).min(bytes.len());
            let chunk = bytes.slice(offset..end);
            let len = chunk.len() as u64;
            uploaded.fetch_add(len, Ordering::Relaxed);
            counted.fetch_add(len, Ordering::Relaxed);
            offset = end;
            yield Ok(chunk);
        }
    }
}

fn make_progress(total: Option<u64>) -> ProgressBar {
    match total {
        Some(total) => {
            let bar = ProgressBar::new(total);
            bar.set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] \
                     {bytes}/{total_bytes} ({bytes_per_sec}, eta {eta}) {msg}",
                )
                .unwrap()
                .progress_chars("#>-"),
            );
            bar
        }
        None => {
            let bar = ProgressBar::new_spinner();
            bar.set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} [{elapsed_precise}] {bytes} ({bytes_per_sec}) {msg}",
                )
                .unwrap(),
            );
            bar
        }
    }
}

fn paint(progress: &ProgressBar, uploaded: &AtomicU64, blocks: &AtomicU64, total_blocks: Option<u64>) {
    progress.set_position(uploaded.load(Ordering::Relaxed));
    let done = blocks.load(Ordering::Relaxed);
    progress.set_message(match total_blocks {
        Some(total) => format!("{done}/{total} blocks"),
        None => format!("{done} blocks"),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reads_full_blocks_then_partial_then_eof() {
        let mut reader = std::io::Cursor::new(vec![7u8; 2500]);
        assert_eq!(read_segment(&mut reader, 1000).await.unwrap().unwrap().len(), 1000);
        assert_eq!(read_segment(&mut reader, 1000).await.unwrap().unwrap().len(), 1000);
        assert_eq!(read_segment(&mut reader, 1000).await.unwrap().unwrap().len(), 500);
        assert!(read_segment(&mut reader, 1000).await.unwrap().is_none());
    }
}
