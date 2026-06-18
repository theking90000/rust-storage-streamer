//! Minimal example application driving `frame-streamer` over real HTTP.
//!
//! It feeds the session a hardcoded list of objects, resolves each "signed URL"
//! to the object's own URI, and downloads the body with async `reqwest`,
//! assembling physical frames from the streamed response chunk by chunk. It is
//! intentionally simple: a real adapter would also decrypt each frame.

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_stream::try_stream;
use bytes::Bytes;
use frame_streamer::{
    BoxError, DecryptKey, FrameAssembler, FrameBackend, FrameBudget, FrameRate, FrameStream,
    ObjectId, ObjectMeta, SignedUrl, StreamConfig, StreamRequest, StreamSession, TransferModel,
    UrlTicket,
};
use futures_util::{Sink, StreamExt, stream};
use tokio_into_sink::IntoSinkExt;

/// Simulates a transport whose userspace buffer accepts one frame and then
/// remains blocked forever. The driver must keep filling its bounded window.
struct OneFrameOutput {
    buffered: Option<Bytes>,
}

impl Sink<Bytes> for OneFrameOutput {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.buffered.is_none() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn start_send(mut self: Pin<&mut Self>, frame: Bytes) -> Result<(), Self::Error> {
        println!("output accepted one frame, then blocked");
        self.buffered = Some(frame);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }
}

/// Backend that resolves URLs trivially and downloads bodies over HTTP.
struct HttpBackend {
    client: reqwest::Client,
}

impl FrameBackend for HttpBackend {
    fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket {
        // No URL coordinator here: the object URI is already fetchable.
        let url = object.uri.clone();
        Box::pin(async move {
            println!("resolving URL for object : {url}");
            tokio::time::sleep(Duration::from_millis(1000)).await; // simulate latency
            println!("resolved URL for object : {url}");
            Ok(url)
        })
    }

    fn download(&self, object: &ObjectMeta, url: SignedUrl, frames: Range<u32>) -> FrameStream {
        let client = self.client.clone();
        let frame_count = object.frame_count;
        let id = object.id.as_str().to_owned();

        Box::pin(try_stream! {
            let response = client.get(&url).send().await?.error_for_status()?;
            let total = response
                .content_length()
                .ok_or("response is missing a Content-Length")?;
            if total % u64::from(frame_count) != 0 {
                /*Err(format!(
                    "object {id} body of {total} bytes does not split into {frame_count} frames"
                ))?;*/
            }
            let frame_size = 65536;

            let mut assembler = FrameAssembler::new(frame_size)?;
            let mut body = response.bytes_stream();
            let mut index: u32 = 0;

            // Pull the body lazily and emit each physical frame the moment it is
            // complete. The session only polls this stream up to the frames it has
            // authorized; once it stops, the future suspends mid-body and the TCP
            // read backpressures the server, so we never buffer the whole object.
            while index < frames.end {
                let Some(chunk) = body.next().await else { break };
                assembler.push(chunk?);
                while let Some(frame) = assembler.next_frame() {
                    if index >= frames.start {
                        println!("downloaded frame {index:>3} of object {id}");
                        yield frame;
                    }
                    index += 1;
                    if index >= frames.end {
                        break;
                    }
                }
            }
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // Hardcoded objects. httpbin returns N random bytes, split into frames below.
    let catalog = vec![
        object("flux.0", "https://proof.ovh.net/files/10Mb.dat", 150),
        object("flux.1", "https://proof.ovh.net/files/10Mb.dat", 150),
        object("flux.2", "https://proof.ovh.net/files/10Mb.dat", 150),
        object("flux.3", "https://proof.ovh.net/files/10Mb.dat", 150),
        object("flux.4", "https://proof.ovh.net/files/10Mb.dat", 150),
        // object("flux.1", "http://192.168.129.87:8080/flux.1", 150),
        // object("flux.2", "http://192.168.129.87:8080/flux.2", 150),
        // object("flux.3", "http://192.168.129.87:8080/flux.3", 150),
    ];
    let total_frames: u64 = catalog
        .iter()
        .map(|object| u64::from(object.frame_count))
        .sum();
    let objects = stream::iter(catalog.into_iter().map(Ok::<_, BoxError>));

    let backend = Arc::new(HttpBackend {
        client: reqwest::Client::new(),
    });
    let request = StreamRequest::new(0..total_frames, FrameRate::new(120.0)?)?;
    let budget = FrameBudget::new(150 * 10)?; // 100MB of memory budget
    let config = StreamConfig::new(
        FrameRate::new(12.0)?, // 1200fps
        TransferModel {
            object_rate: FrameRate::new(7.6317)?,
            data_ttfb: Duration::from_millis(500),
            url_latency: Duration::from_millis(1000),
            frames_per_object: 150,
        },
    )?;

    let x = TransferModel {
        object_rate: FrameRate::new(7.6317)?,
        data_ttfb: Duration::from_millis(500),
        url_latency: Duration::from_millis(1000),
        frames_per_object: 150,
    }
    .window_for(FrameRate::new(120.0)?)?;

    println!("window: {:?}", x);

    let session = StreamSession::new(objects, backend, request, budget, config)?;
    let output = tokio::fs::File::create("output.bin").await?.into_sink();
    let start = std::time::Instant::now();

    /*match tokio::time::timeout(Duration::from_secs(10), session.pipe_into(output)).await {
        Ok(result) => result?,
        Err(_) => println!("demo complete: output stayed blocked while the frame window filled"),
    }*/
    session.pipe_into(output).await?;
    println!(
        "demo complete: downloaded {} frames in {:.2}s",
        total_frames,
        start.elapsed().as_secs_f64()
    );
    Ok(())
}

fn object(id: &str, uri: &str, frame_count: u32) -> ObjectMeta {
    ObjectMeta {
        id: ObjectId::new(id),
        uri: uri.to_owned(),
        frame_count,
        decrypt_key: DecryptKey::new([0; 32]),
    }
}
