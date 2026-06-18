use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use frame_streamer::{
    BoxError, ByteRate, ByteRequest, ByteStream, ByteStreamConfig, ByteTransferModel, DecryptKey,
    FrameBudget, ObjectId, ObjectMeta,
};
use futures_util::{Sink, stream};
use tokio_into_sink::IntoSinkExt;

mod measure;
use measure::MeasureBackend;

const FRAME_SIZE: usize = 1 << 16;
const TOTAL_BYTES: u64 = 27_153_749;
const OBJECT_RATE: f64 = 60_000_000.0;
const TARGET_RATE: f64 = OBJECT_RATE * 10.0; // OBJECT_RATE * 3.0;

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

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let catalog = vec![
        object(
            "https://cdn.discordapp.com/attachments/1517138836453589052/1517202414241976441/flux.1?ex=6a356c5b&is=6a341adb&hm=0c586e1001d572690535fffad9298aaba7c6cf09d4b42ae85e290f4158f9e444&",
            150,
            "e14c08356134cb311e1ab933c6d3fc421fb43e5fbc97997470cecb4ca5e5a3e3",
        ),
        object(
            "https://cdn.discordapp.com/attachments/1517138836453589052/1517202424052449550/flux.2?ex=6a356c5e&is=6a341ade&hm=09cb534967a77db375bc9ee36eabcd276bfc752d01a26b50c3257bceb0179d76&",
            150,
            "45b481e3033561e4f4bd884ce9e875cc932b7d5d94c0a7f22a94bc9305ef7d97",
        ),
        object(
            "https://cdn.discordapp.com/attachments/1517138836453589052/1517202435184005181/flux.3?ex=6a356c60&is=6a341ae0&hm=4b6a0c8c75df92f4598c9edb7bb8c6536dc3d5950c9b80e1daec2a75b38372cf&",
            115,
            "688172a63bc45555f6b7565d64f814cb2a95fcdd34c61c3b134b29209926d1b2",
        ),
    ];
    let objects = stream::iter(catalog.into_iter().map(Ok::<_, BoxError>));
    let backend = Arc::new(MeasureBackend::new(
        reqwest::Client::new(),
        "metrics",
        FRAME_SIZE,
        TARGET_RATE,
    ));
    let config = ByteStreamConfig::new(
        FRAME_SIZE,
        ByteRate::new(TARGET_RATE)?,
        ByteTransferModel {
            object_rate: ByteRate::new(OBJECT_RATE)?,
            data_ttfb: Duration::from_millis(100),
            url_latency: Duration::ZERO,
            frames_per_object: 150,
        },
    )?;
    let stream = ByteStream::new(
        objects,
        backend.clone(),
        ByteRequest::new(0..TOTAL_BYTES, ByteRate::new(TARGET_RATE)?)?,
        FrameBudget::new(415)?,
        config,
    )?;
    backend.set_window_frames(stream.capacity_frames());
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
    println!("dropped metric samples: {}", backend.dropped_samples());
    Ok(())
}

fn object(id: &str, frame_count: u32, key: &str) -> ObjectMeta {
    ObjectMeta {
        id: ObjectId::new(
            reqwest::Url::parse(id)
                .ok()
                .and_then(|url| url.path_segments()?.next_back().map(str::to_owned))
                .unwrap_or_else(|| id.to_owned()),
        ),
        uri: id.to_owned(),
        //uri: format!("http://192.168.129.87:8080/{id}"),
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
