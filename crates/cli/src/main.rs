//! Minimal example application driving `frame-streamer` over real HTTP.
//!
//! It feeds the session a hardcoded list of objects, resolves each "signed URL"
//! to the object's own URI, and downloads the body with async `reqwest`,
//! splitting it into the object's frames. It is intentionally simple: a real
//! adapter would assemble fixed-size physical frames and decrypt them.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use frame_streamer::{
    BoxError, FrameBudget, FrameRate, FrameStream, ObjectId, ObjectMeta, SignedUrl, StreamBackend,
    StreamConfig, StreamRequest, StreamSession, TransferModel, UrlTicket,
};
use futures_util::{StreamExt, TryStreamExt, stream};

/// Backend that resolves URLs trivially and downloads bodies over HTTP.
struct HttpBackend {
    client: reqwest::Client,
}

impl StreamBackend for HttpBackend {
    fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket {
        // No URL coordinator here: the object URI is already fetchable.
        let url = object.uri.clone();
        Box::pin(async move { Ok(url) })
    }

    fn download(&self, object: &ObjectMeta, url: SignedUrl, frames: Range<u32>) -> FrameStream {
        let client = self.client.clone();
        let frame_count = object.frame_count;

        let body = async move {
            let bytes = client
                .get(&url)
                .send()
                .await?
                .error_for_status()?
                .bytes()
                .await?;
            Ok::<_, BoxError>(split_frames(&bytes, frame_count, frames))
        };

        // Turn the single download future into a stream of frame results.
        stream::once(body)
            .map(|result| match result {
                Ok(parts) => stream::iter(parts.into_iter().map(Ok)).boxed(),
                Err(error) => stream::once(async move { Err(error) }).boxed(),
            })
            .flatten()
            .boxed()
    }
}

/// Splits a body into `count` near-equal frames and keeps the `wanted` subrange.
fn split_frames(body: &Bytes, count: u32, wanted: Range<u32>) -> Vec<Bytes> {
    let count = count as usize;
    let len = body.len();
    (wanted.start as usize..wanted.end as usize)
        .map(|index| {
            let start = index * len / count;
            let end = (index + 1) * len / count;
            body.slice(start..end)
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // Hardcoded objects. httpbin returns N random bytes, split into frames below.
    let catalog = vec![
        object("clip-0", "http://192.168.129.87:8080/flux.1", 150),
        object("clip-0", "http://192.168.129.87:8080/flux.2", 150),
        object("clip-0", "http://192.168.129.87:8080/flux.3", 150),
    ];
    let total_frames: u64 = catalog.iter().map(|object| u64::from(object.frame_count)).sum();
    let objects = stream::iter(catalog.into_iter().map(Ok::<_, BoxError>));

    let backend = Arc::new(HttpBackend {
        client: reqwest::Client::new(),
    });
    let request = StreamRequest::new(0..total_frames, FrameRate::new(120.0)?)?;
    let budget = FrameBudget::new(64)?;
    let config = StreamConfig::new(
        FrameRate::new(120.0)?,
        TransferModel {
            object_rate: FrameRate::new(1_000.0)?,
            data_ttfb: Duration::from_millis(200),
            url_latency: Duration::from_millis(100),
            frames_per_object: 4,
        },
    )?;

    let mut session = StreamSession::new(objects, backend, request, budget, config)?;

    let mut index = 0u64;
    while let Some(frame) = session.try_next().await? {
        println!("frame {index:>3}: {} bytes", frame.len());
        index += 1;
    }
    println!("done: streamed {index} frames");
    Ok(())
}

fn object(id: &str, uri: &str, frame_count: u32) -> ObjectMeta {
    ObjectMeta {
        id: ObjectId::new(id),
        uri: uri.to_owned(),
        frame_count,
    }
}
