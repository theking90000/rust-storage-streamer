use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_stream::try_stream;
use bytes::Bytes;
use frame_streamer::{
    BoxError, ByteRate, ByteRequest, ByteStream, ByteStreamConfig, ByteTransferModel, DecryptKey,
    EncryptedByteStream, EncryptedBytesBackend, FrameBudget, ObjectId, ObjectMeta, SignedUrl,
    UrlTicket,
};
use futures_util::{Sink, StreamExt, stream};
use reqwest::header::RANGE;
use tokio_into_sink::IntoSinkExt;

const FRAME_SIZE: usize = 1 << 16;
const TOTAL_BYTES: u64 = 27_153_749;
const OBJECT_RATE: f64 = 500_000.0;
const TARGET_RATE: f64 = OBJECT_RATE * 3.0;

struct OneFrameOutput(Option<Bytes>);

impl Sink<Bytes> for OneFrameOutput {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.0.is_none() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn start_send(mut self: Pin<&mut Self>, bytes: Bytes) -> Result<(), Self::Error> {
        println!("output accepted {} bytes, then blocked", bytes.len());
        self.0 = Some(bytes);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }
}

struct HttpBackend {
    client: reqwest::Client,
}

impl EncryptedBytesBackend for HttpBackend {
    fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket {
        let url = object.uri.clone();
        Box::pin(async move { Ok(url) })
    }

    fn download(
        &self,
        object: &ObjectMeta,
        url: SignedUrl,
        physical_bytes: Range<u64>,
    ) -> EncryptedByteStream {
        let client = self.client.clone();
        let id = object.id.as_str().to_owned();

        Box::pin(try_stream! {
            let expected = physical_bytes.end - physical_bytes.start;
            let range = format!("bytes={}-{}", physical_bytes.start, physical_bytes.end - 1);
            println!("GET {id} {range}");
            let response = client
                .get(url)
                .header(RANGE, range)
                .send()
                .await?
                .error_for_status()?;
            let full_object = response.status() == reqwest::StatusCode::OK
                && physical_bytes.start == 0
                && response.content_length() == Some(expected);
            if response.status() != reqwest::StatusCode::PARTIAL_CONTENT && !full_object {
                Err(format!("{id}: server ignored the Range request"))?;
            }
            let mut body = response.bytes_stream();
            while let Some(chunk) = body.next().await {
                yield chunk?;
            }
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let catalog = vec![
        object(
            "flux.1",
            150,
            "e14c08356134cb311e1ab933c6d3fc421fb43e5fbc97997470cecb4ca5e5a3e3",
        ),
        object(
            "flux.2",
            150,
            "45b481e3033561e4f4bd884ce9e875cc932b7d5d94c0a7f22a94bc9305ef7d97",
        ),
        object(
            "flux.3",
            115,
            "688172a63bc45555f6b7565d64f814cb2a95fcdd34c61c3b134b29209926d1b2",
        ),
    ];
    let objects = stream::iter(catalog.into_iter().map(Ok::<_, BoxError>));
    let backend = Arc::new(HttpBackend {
        client: reqwest::Client::new(),
    });
    let config = ByteStreamConfig::new(
        FRAME_SIZE,
        ByteRate::new(TARGET_RATE)?,
        ByteTransferModel {
            object_rate: ByteRate::new(OBJECT_RATE)?,
            data_ttfb: Duration::from_millis(500),
            url_latency: Duration::ZERO,
            frames_per_object: 150,
        },
    )?;
    let stream = ByteStream::new(
        objects,
        backend,
        ByteRequest::new(0..TOTAL_BYTES, ByteRate::new(TARGET_RATE)?)?,
        FrameBudget::new(415)?,
        config,
    )?;
    println!(
        "target={:.3} Mbit/s, window={} frames",
        TARGET_RATE * 8.0 / 1_000_000.0,
        stream.capacity_frames()
    );
    let start = std::time::Instant::now();

    if std::env::args().any(|arg| arg == "--blocked") {
        match tokio::time::timeout(
            Duration::from_secs(10),
            stream.pipe_into(OneFrameOutput(None)),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                println!("demo complete: output stayed blocked while the frame window filled")
            }
        }
        return Ok(());
    }

    stream
        .pipe_into(tokio::fs::File::create("output.bin").await?.into_sink())
        .await?;
    println!(
        "wrote {TOTAL_BYTES} plaintext bytes in {:.2}s ({:.3} Mbit/s)",
        start.elapsed().as_secs_f64(),
        TOTAL_BYTES as f64 * 8.0 / start.elapsed().as_secs_f64() / 1_000_000.0,
    );
    Ok(())
}

fn object(id: &str, frame_count: u32, key: &str) -> ObjectMeta {
    ObjectMeta {
        id: ObjectId::new(id),
        uri: format!("http://192.168.129.87:8080/{id}"),
        frame_count,
        decrypt_key: hex_key(key),
    }
}

fn hex_key(hex: &str) -> DecryptKey {
    let mut key = [0; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).unwrap();
    }
    DecryptKey::new(key)
}
