use std::collections::VecDeque;
use std::error::Error;
use std::future::Future;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::SystemTime;

use async_stream::try_stream;
use bytes::Bytes;
use futures_util::stream::{Fuse, FusedStream};
use futures_util::task::noop_waker_ref;
use futures_util::{Sink, Stream, StreamExt};

use crate::{
    FrameAssembler, FrameBudget, FrameBudgetError, FrameDecoder, FrameError, FramePermit,
    FrameRate, ObjectMeta, StreamRequest, TAG_SIZE,
};
use crate::{TransferModel, WindowSizing};

pub type BoxError = Box<dyn Error + Send + Sync>;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedUrl {
    value: String,
    expires_at: Option<SystemTime>,
}

impl SignedUrl {
    pub fn new(value: impl Into<String>, expires_at: Option<SystemTime>) -> Self {
        Self {
            value: value.into(),
            expires_at,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub const fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }
}
pub type UrlTicket = Pin<Box<dyn Future<Output = Result<SignedUrl, BoxError>> + Send>>;
pub type FrameStream = Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send>>;
pub type EncryptedByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send>>;
type ObjectStream = Pin<Box<dyn Stream<Item = Result<ObjectMeta, BoxError>> + Send>>;
type BudgetTicket = Pin<Box<dyn Future<Output = Result<FramePermit, FrameBudgetError>> + Send>>;

/// The URL coordinator and HTTP/crypto pipeline seen by a stream session.
pub trait DownloadBackend: Send + Sync {
    /// Creates an owned URL-resolution future. The session polls it while the
    /// object is inside its prefetch window.
    fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket;

    /// Opens one sequential stream for the useful local frame range.
    fn download(&self, object: &ObjectMeta, url: SignedUrl, frames: Range<u32>) -> FrameStream;
}

pub trait EncryptedBytesDownloadBackend: Send + Sync {
    fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket;

    fn download(
        &self,
        object: &ObjectMeta,
        url: SignedUrl,
        physical_bytes: Range<u64>,
    ) -> EncryptedByteStream;
}

/// Turns encrypted HTTP chunks into authenticated plaintext frames.
pub struct StreamDownloadBackend {
    encrypted: Arc<dyn EncryptedBytesDownloadBackend>,
    frame_size: usize,
}

impl StreamDownloadBackend {
    pub fn new(
        encrypted: Arc<dyn EncryptedBytesDownloadBackend>,
        frame_size: usize,
    ) -> Result<Self, BoxError> {
        if frame_size <= TAG_SIZE {
            return Err(Box::new(FrameError::FrameTooSmall));
        }
        Ok(Self {
            encrypted,
            frame_size,
        })
    }
}

impl DownloadBackend for StreamDownloadBackend {
    fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket {
        self.encrypted.resolve_url(object)
    }

    fn download(&self, object: &ObjectMeta, url: SignedUrl, frames: Range<u32>) -> FrameStream {
        let frame_size = self.frame_size;
        let start = u64::from(frames.start);
        let end = u64::from(frames.end);
        let Some(physical_start) = start.checked_mul(frame_size as u64) else {
            return Box::pin(futures_util::stream::once(async {
                Err(message("physical range overflow"))
            }));
        };
        let Some(physical_end) = end.checked_mul(frame_size as u64) else {
            return Box::pin(futures_util::stream::once(async {
                Err(message("physical range overflow"))
            }));
        };
        let physical_bytes = physical_start..physical_end;
        let mut input = self.encrypted.download(object, url, physical_bytes);
        let key = object.decrypt_key.clone();

        Box::pin(try_stream! {
            let decoder = FrameDecoder::new(frame_size, &key)?;
            let mut assembler = FrameAssembler::new(frame_size)?;
            let mut index = start;

            while let Some(chunk) = input.next().await {
                assembler.push(chunk?);
                while let Some(frame) = assembler.next_frame_mut() {
                    if index >= end {
                        Err(message("backend returned more bytes than requested"))?;
                    }
                    yield decoder.decode_frame(frame, index)?;
                    index += 1;
                }
            }

            assembler.finish()?;
            if index != end {
                Err(message("backend ended before its last requested frame"))?;
            }
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct StreamConfig {
    pub consumer_rate: FrameRate,
    pub transfer: TransferModel,
}

impl StreamConfig {
    pub fn new(consumer_rate: FrameRate, transfer: TransferModel) -> Result<Self, BoxError> {
        transfer.window_for(consumer_rate)?;
        Ok(Self {
            consumer_rate,
            transfer,
        })
    }
}

/// Runtime state for one object. HTTP frames are sequential, so one deque is
/// enough; later objects may fill theirs while the front object blocks output.
pub struct ObjectPlan {
    object: ObjectMeta,
    local_frames: Range<u32>,
    ticket: Option<UrlTicket>,
    url: Option<SignedUrl>,
    download: Option<FrameStream>,
    authorized: u32,
    received: u32,
    emitted: u32,
    finished: bool,
    buffer: VecDeque<Bytes>,
}

impl ObjectPlan {
    pub fn object(&self) -> &ObjectMeta {
        &self.object
    }

    pub const fn authorized_frames(&self) -> u32 {
        self.authorized - self.local_frames.start
    }

    pub fn buffered_frames(&self) -> usize {
        self.buffer.len()
    }

    pub fn local_frames(&self) -> Range<u32> {
        self.local_frames.clone()
    }

    fn remaining(&self) -> usize {
        (self.local_frames.end - self.emitted) as usize
    }

    fn committed(&self) -> usize {
        (self.authorized - self.emitted) as usize
    }
}

/// A single-owner state machine. It polls every authorized object download,
/// buffers frames per object, and only emits from the front object.
pub struct StreamSession {
    objects: Fuse<ObjectStream>,
    backend: Arc<dyn DownloadBackend>,
    plans: VecDeque<ObjectPlan>,
    request: StreamRequest,
    budget: FrameBudget,
    allocation: FramePermit,
    budget_ticket: Option<BudgetTicket>,
    consumer_rate: FrameRate,
    transfer: TransferModel,
    source_cursor: u64,
}

impl StreamSession {
    pub fn new(
        objects: impl Stream<Item = Result<ObjectMeta, BoxError>> + Send + 'static,
        backend: Arc<dyn DownloadBackend>,
        request: StreamRequest,
        budget: FrameBudget,
        config: StreamConfig,
    ) -> Result<Self, BoxError> {
        let objects: ObjectStream = Box::pin(objects);
        let allocation = budget.try_reserve(1)?;
        let mut session = Self {
            objects: objects.fuse(),
            backend,
            plans: VecDeque::new(),
            request,
            budget,
            allocation,
            budget_ticket: None,
            consumer_rate: config.consumer_rate,
            transfer: config.transfer,
            source_cursor: 0,
        };
        let desired = session.desired_window()?.ready_data_frames;
        session.grow_available(desired)?;
        Ok(session)
    }

    pub fn plans(&self) -> &VecDeque<ObjectPlan> {
        &self.plans
    }

    pub fn pipe_into<S>(self, output: S) -> StreamDriver<S> {
        StreamDriver {
            session: self,
            output: Box::pin(output),
        }
    }

    pub fn set_consumer_rate(&mut self, consumer_rate: FrameRate) {
        self.consumer_rate = consumer_rate;
    }

    pub fn set_transfer_model(&mut self, transfer: TransferModel) -> Result<(), BoxError> {
        transfer.window_for(self.consumer_rate)?;
        self.transfer = transfer;
        Ok(())
    }

    pub fn buffered_frames(&self) -> usize {
        self.plans.iter().map(ObjectPlan::buffered_frames).sum()
    }

    pub fn capacity_frames(&self) -> usize {
        self.desired_window()
            .map(|window| window.ready_data_frames.min(self.allocation.frames()))
            .unwrap_or(1)
    }

    pub fn target_rate(&self) -> Result<FrameRate, BoxError> {
        let requested = self.request.allocated_rate().min(self.consumer_rate);
        Ok(requested.min(self.transfer.max_rate_for(self.allocation.frames())?))
    }

    fn desired_window(&self) -> Result<WindowSizing, BoxError> {
        Ok(self
            .transfer
            .window_for(self.request.allocated_rate().min(self.consumer_rate))?)
    }

    fn grow_available(&mut self, desired: usize) -> Result<(), BoxError> {
        let missing = desired
            .saturating_sub(self.allocation.frames())
            .min(self.budget.available());
        if missing == 0 {
            return Ok(());
        }
        match self.budget.try_reserve(missing) {
            Ok(extra) => self.allocation.merge(extra),
            Err(FrameBudgetError::Unavailable) => {}
            Err(error) => return Err(Box::new(error)),
        }
        Ok(())
    }

    fn refresh_allocation(&mut self, cx: &mut Context<'_>) -> Result<WindowSizing, BoxError> {
        let desired = self.desired_window()?;
        if desired.ready_data_frames <= self.allocation.frames() {
            self.budget_ticket = None;
        } else if let Some(ticket) = &mut self.budget_ticket {
            match ticket.as_mut().poll(cx) {
                Poll::Ready(Ok(extra)) => {
                    self.allocation.merge(extra);
                    self.budget_ticket = None;
                }
                Poll::Ready(Err(error)) => return Err(Box::new(error)),
                Poll::Pending => {}
            }
        }

        self.grow_available(desired.ready_data_frames)?;
        if desired.ready_data_frames > self.allocation.frames() && self.budget_ticket.is_none() {
            let budget = self.budget.clone();
            let mut ticket: BudgetTicket = Box::pin(async move { budget.reserve(1).await });
            match ticket.as_mut().poll(cx) {
                Poll::Ready(Ok(extra)) => self.allocation.merge(extra),
                Poll::Ready(Err(error)) => return Err(Box::new(error)),
                Poll::Pending => self.budget_ticket = Some(ticket),
            }
        }

        let capacity = desired.ready_data_frames.min(self.allocation.frames());
        let committed: usize = self.plans.iter().map(ObjectPlan::committed).sum();
        self.allocation.shrink_to(capacity.max(committed));

        Ok(WindowSizing {
            ready_data_frames: capacity,
            url_prefetch_frames: self
                .transfer
                .window_for(self.target_rate()?)?
                .url_prefetch_frames,
        })
    }

    fn poll_objects(
        &mut self,
        cx: &mut Context<'_>,
        window: WindowSizing,
    ) -> Poll<Result<(), BoxError>> {
        let target = window.ready_data_frames + window.url_prefetch_frames;
        let mut planned: usize = self.plans.iter().map(ObjectPlan::remaining).sum();

        while planned < target
            && self.source_cursor < self.request.frames().end
            && !self.objects.is_terminated()
        {
            let object = match self.objects.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(object))) => object,
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                Poll::Ready(None) | Poll::Pending => break,
            };
            if object.frame_count == 0 {
                return Poll::Ready(Err(message("object contains no frames")));
            }
            let object_start = self.source_cursor;
            let object_end = object_start
                .checked_add(u64::from(object.frame_count))
                .ok_or_else(|| message("global frame index overflow"))?;
            self.source_cursor = object_end;

            let requested = self.request.frames();
            let useful_start = object_start.max(requested.start);
            let useful_end = object_end.min(requested.end);
            if useful_start >= useful_end {
                continue;
            }
            let local_frames =
                (useful_start - object_start) as u32..(useful_end - object_start) as u32;
            planned += local_frames.len();
            let ticket = self.backend.resolve_url(&object);
            let first_frame = local_frames.start;
            self.plans.push_back(ObjectPlan {
                object,
                local_frames,
                ticket: Some(ticket),
                url: None,
                download: None,
                authorized: first_frame,
                received: first_frame,
                emitted: first_frame,
                finished: false,
                buffer: VecDeque::new(),
            });
        }
        Poll::Ready(Ok(()))
    }

    fn authorize(&mut self, capacity_frames: usize) {
        let committed: usize = self.plans.iter().map(ObjectPlan::committed).sum();
        let mut available = capacity_frames.saturating_sub(committed);

        for plan in &mut self.plans {
            if available == 0 {
                break;
            }
            let missing = (plan.local_frames.end - plan.authorized) as usize;
            let added = missing.min(available);
            plan.authorized += added as u32;
            available -= added;
        }
    }

    fn poll_urls(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        let mut speculative_cx = Context::from_waker(noop_waker_ref());
        let mut limiting_ticket_seen = false;

        for plan in &mut self.plans {
            let Some(ticket) = &mut plan.ticket else {
                continue;
            };

            let is_limiting =
                !limiting_ticket_seen && plan.authorized > plan.emitted && plan.download.is_none();
            limiting_ticket_seen |= is_limiting;
            let ticket_cx = if is_limiting {
                &mut *cx
            } else {
                &mut speculative_cx
            };

            match ticket.as_mut().poll(ticket_cx) {
                Poll::Ready(Ok(url)) => {
                    plan.url = Some(url);
                    plan.ticket = None;
                }
                Poll::Ready(Err(error)) => {
                    plan.ticket = None;
                    return Poll::Ready(Err(error));
                }
                Poll::Pending => {}
            }
        }

        for plan in &mut self.plans {
            if plan.authorized == plan.emitted || plan.download.is_some() {
                continue;
            }
            if let Some(url) = plan.url.take() {
                plan.download = Some(self.backend.download(
                    &plan.object,
                    url,
                    plan.local_frames.clone(),
                ));
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_downloads(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool, BoxError>> {
        let mut progressed = false;

        for plan in &mut self.plans {
            if plan.finished
                || (plan.received == plan.authorized && plan.received < plan.local_frames.end)
            {
                continue;
            }
            let Some(download) = &mut plan.download else {
                continue;
            };
            match download.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    if plan.received == plan.local_frames.end {
                        return Poll::Ready(Err(message(
                            "object returned more frames than requested",
                        )));
                    }
                    plan.buffer.push_back(frame);
                    plan.received += 1;
                    progressed = true;
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                Poll::Ready(None) => {
                    if plan.received != plan.local_frames.end {
                        return Poll::Ready(Err(message("object ended before its last frame")));
                    }
                    plan.finished = true;
                    progressed = true;
                }
                Poll::Pending => {}
            }
        }
        Poll::Ready(Ok(progressed))
    }

    fn pop_ready(&mut self) -> Option<Bytes> {
        let plan = self.plans.front_mut()?;
        let frame = plan.buffer.pop_front()?;
        plan.emitted += 1;
        if plan.emitted == plan.local_frames.end && plan.finished {
            self.plans.pop_front();
        }
        let committed: usize = self.plans.iter().map(ObjectPlan::committed).sum();
        self.allocation
            .shrink_to(self.capacity_frames().max(committed));
        Some(frame)
    }

    fn discard_finished(&mut self) {
        while self
            .plans
            .front()
            .is_some_and(|plan| plan.finished && plan.emitted == plan.local_frames.end)
        {
            self.plans.pop_front();
        }
    }

    fn poll_fill(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        let window = self.refresh_allocation(cx)?;
        if let Poll::Ready(Err(error)) = self.poll_objects(cx, window) {
            return Poll::Ready(Err(error));
        }
        self.authorize(window.ready_data_frames);
        if let Poll::Ready(Err(error)) = self.poll_urls(cx) {
            return Poll::Ready(Err(error));
        }
        let progressed = match self.poll_downloads(cx) {
            Poll::Ready(Ok(progressed)) => progressed,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => false,
        };
        self.discard_finished();

        if self.plans.is_empty()
            && (self.objects.is_terminated() || self.source_cursor >= self.request.frames().end)
        {
            return Poll::Ready(Ok(()));
        }
        if progressed {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

/// Owns both halves of the pipeline, so input keeps progressing while output
/// is backpressured. No task or intermediate channel is required.
pub struct StreamDriver<S> {
    session: StreamSession,
    output: Pin<Box<S>>,
}

impl<S> Future for StreamDriver<S>
where
    S: Sink<Bytes>,
    S::Error: Error + Send + Sync + 'static,
{
    type Output = Result<(), BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let done = match this.session.poll_fill(cx) {
            Poll::Ready(Ok(())) => true,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => false,
        };

        if !done && !this.session.plans.is_empty() {
            match this.output.as_mut().poll_ready(cx) {
                Poll::Ready(Ok(())) => {
                    if let Some(frame) = this.session.pop_ready() {
                        if let Err(error) = this.output.as_mut().start_send(frame) {
                            return Poll::Ready(Err(Box::new(error)));
                        }
                        cx.waker().wake_by_ref();
                    }
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(Box::new(error))),
                Poll::Pending => {}
            }
        }

        if done {
            return this
                .output
                .as_mut()
                .poll_close(cx)
                .map_err(|error| Box::new(error) as BoxError);
        }
        match this.output.as_mut().poll_flush(cx) {
            Poll::Ready(Ok(())) | Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(Box::new(error))),
        }
    }
}

fn message(text: &str) -> BoxError {
    Box::new(io::Error::other(text))
}

#[cfg(test)]
mod tests {
    use aes_gcm::aead::{AeadInPlace, KeyInit};
    use aes_gcm::{Aes256Gcm, Nonce};
    use std::io;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Context;
    use std::time::Duration;

    use futures_util::task::noop_waker_ref;
    use futures_util::{Sink, TryStreamExt, sink, stream};

    use super::*;
    use crate::ObjectId;

    #[derive(Default)]
    struct TestBackend {
        block_first: bool,
        polled_urls: Arc<AtomicUsize>,
    }

    impl DownloadBackend for TestBackend {
        fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket {
            let url = object.uri.clone();
            let polled_urls = self.polled_urls.clone();
            Box::pin(async move {
                polled_urls.fetch_add(1, Ordering::Relaxed);
                Ok(SignedUrl::new(url, None))
            })
        }

        fn download(
            &self,
            object: &ObjectMeta,
            _url: SignedUrl,
            frames: Range<u32>,
        ) -> FrameStream {
            if self.block_first && object.id.as_str() == "object-0" {
                return Box::pin(stream::pending());
            }
            let id = object.id.as_str().to_owned();
            let output = frames.map(move |frame| Ok(Bytes::from(format!("{id}:{frame}"))));
            Box::pin(stream::iter(output))
        }
    }

    fn object(index: usize, frames: u32) -> ObjectMeta {
        ObjectMeta {
            id: ObjectId::new(format!("object-{index}")),
            uri: format!("objects/{index}"),
            frame_count: frames,
            decrypt_key: crate::DecryptKey::new([index as u8; 32]),
        }
    }

    fn objects(counts: &[u32]) -> ObjectStream {
        Box::pin(stream::iter(
            counts
                .iter()
                .copied()
                .enumerate()
                .map(|(index, count)| Ok(object(index, count)))
                .collect::<Vec<_>>(),
        ))
    }

    struct RawBackend {
        body: Bytes,
        requested: Mutex<Option<Range<u64>>>,
    }

    impl EncryptedBytesDownloadBackend for RawBackend {
        fn resolve_url(&self, _object: &ObjectMeta) -> UrlTicket {
            Box::pin(async { Ok(SignedUrl::new("url", None)) })
        }

        fn download(
            &self,
            _object: &ObjectMeta,
            _url: SignedUrl,
            physical_bytes: Range<u64>,
        ) -> EncryptedByteStream {
            *self.requested.lock().unwrap() = Some(physical_bytes.clone());
            let selected = self.body.slice(
                physical_bytes.start as usize..(physical_bytes.end as usize).min(self.body.len()),
            );
            let chunks = selected
                .chunks(5)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                .collect::<Vec<_>>();
            Box::pin(stream::iter(chunks))
        }
    }

    fn encrypted_frame(key: &crate::DecryptKey, index: u64, payload: &[u8]) -> Bytes {
        let cipher = Aes256Gcm::new(key.as_bytes().into());
        let mut ciphertext = payload.to_vec();
        let mut nonce = [0; 12];
        nonce[4..].copy_from_slice(&index.to_be_bytes());
        let tag = cipher
            .encrypt_in_place_detached(Nonce::from_slice(&nonce), b"", &mut ciphertext)
            .unwrap();
        let mut frame = Vec::from(tag.as_slice());
        frame.extend(ciphertext);
        Bytes::from(frame)
    }

    fn request(frames: Range<u64>) -> StreamRequest {
        StreamRequest::new(frames, FrameRate::new(1_000.0).unwrap()).unwrap()
    }

    fn config(capacity: usize, prefetch: usize) -> StreamConfig {
        let rate = FrameRate::new(capacity as f64).unwrap();
        StreamConfig::new(
            rate,
            TransferModel {
                object_rate: FrameRate::new(1_000_000.0).unwrap(),
                data_ttfb: Duration::from_secs(1),
                url_latency: Duration::from_secs_f64(prefetch as f64 / capacity as f64),
                frames_per_object: 1_000,
            },
        )
        .unwrap()
    }

    fn collecting_output() -> (Arc<Mutex<Vec<Bytes>>>, impl Sink<Bytes, Error = io::Error>) {
        let frames = Arc::new(Mutex::new(Vec::new()));
        let output = sink::unfold(frames.clone(), |frames, frame| async move {
            frames.lock().unwrap().push(frame);
            Ok(frames)
        });
        (frames, output)
    }

    struct BlockedOutput;

    impl Sink<Bytes> for BlockedOutput {
        type Error = io::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _item: Bytes) -> Result<(), Self::Error> {
            unreachable!()
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn emits_frames_in_object_order() {
        let backend = Arc::new(TestBackend {
            block_first: false,
            ..Default::default()
        });
        let session = StreamSession::new(
            objects(&[2, 2]),
            backend,
            request(0..4),
            FrameBudget::new(10).unwrap(),
            config(3, 3),
        )
        .unwrap();
        let (frames, output) = collecting_output();

        session.pipe_into(output).await.unwrap();
        let frames = frames.lock().unwrap();
        let text: Vec<_> = frames.iter().map(|frame| &frame[..]).collect();
        assert_eq!(
            text,
            [b"object-0:0", b"object-0:1", b"object-1:0", b"object-1:1"]
        );
    }

    #[tokio::test]
    async fn decrypting_backend_maps_ranges_and_assembles_chunks() {
        let object = object(7, 2);
        let mut body = Vec::new();
        body.extend(encrypted_frame(&object.decrypt_key, 0, b"frame-0!"));
        body.extend(encrypted_frame(&object.decrypt_key, 1, b"frame-1!"));
        let raw = Arc::new(RawBackend {
            body: Bytes::from(body),
            requested: Mutex::new(None),
        });
        let backend = StreamDownloadBackend::new(raw.clone(), 24).unwrap();

        let frames = DownloadBackend::download(
            &backend,
            &object,
            SignedUrl::new("url", None),
            1..2,
        )
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(*raw.requested.lock().unwrap(), Some(24..48));
        assert_eq!(frames, [Bytes::from_static(b"frame-1!")]);
    }

    #[tokio::test]
    async fn decrypting_backend_rejects_an_incomplete_response() {
        let object = object(7, 2);
        let mut body = Vec::new();
        body.extend(encrypted_frame(&object.decrypt_key, 0, b"frame-0!"));
        body.extend(encrypted_frame(&object.decrypt_key, 1, b"frame-1!"));
        body.pop();
        let raw = Arc::new(RawBackend {
            body: Bytes::from(body),
            requested: Mutex::new(None),
        });
        let backend = StreamDownloadBackend::new(raw, 24).unwrap();

        let error = DownloadBackend::download(
            &backend,
            &object,
            SignedUrl::new("url", None),
            1..2,
        )
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();

        assert!(error.to_string().contains("incomplete frame"));
    }

    #[test]
    fn fills_window_while_output_is_backpressured() {
        let backend = Arc::new(TestBackend {
            block_first: false,
            ..Default::default()
        });
        let session = StreamSession::new(
            objects(&[2, 2]),
            backend,
            request(0..4),
            FrameBudget::new(4).unwrap(),
            config(4, 0),
        )
        .unwrap();
        let mut driver = session.pipe_into(BlockedOutput);
        let mut cx = Context::from_waker(noop_waker_ref());

        assert!(Pin::new(&mut driver).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut driver).poll(&mut cx).is_pending());
        assert_eq!(driver.session.plans()[0].buffered_frames(), 2);
        assert_eq!(driver.session.plans()[1].buffered_frames(), 2);
    }

    #[test]
    fn polls_prefetched_url_without_opening_its_download() {
        let backend = Arc::new(TestBackend {
            block_first: true,
            ..Default::default()
        });
        let mut session = StreamSession::new(
            objects(&[2, 2]),
            backend.clone(),
            request(0..4),
            FrameBudget::new(4).unwrap(),
            config(1, 3),
        )
        .unwrap();
        let mut cx = Context::from_waker(noop_waker_ref());

        assert!(session.poll_fill(&mut cx).is_pending());
        assert_eq!(session.plans().len(), 2);
        assert_eq!(backend.polled_urls.load(Ordering::Relaxed), 2);
        assert_eq!(session.plans()[1].authorized_frames(), 0);
        assert!(session.plans()[1].download.is_none());
    }

    #[test]
    fn capacity_can_change_while_streaming() {
        let backend = Arc::new(TestBackend {
            block_first: false,
            ..Default::default()
        });
        let mut session = StreamSession::new(
            objects(&[3]),
            backend,
            request(0..3),
            FrameBudget::new(3).unwrap(),
            config(1, 0),
        )
        .unwrap();

        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(session.poll_fill(&mut cx).is_pending());
        assert_eq!(session.pop_ready().unwrap(), "object-0:0");
        session.set_consumer_rate(FrameRate::new(3.0).unwrap());
        assert!(session.poll_fill(&mut cx).is_pending());
        assert_eq!(session.pop_ready().unwrap(), "object-0:1");
        assert_eq!(session.plans()[0].authorized_frames(), 3);
    }

    #[test]
    fn shrinking_waits_for_committed_frames_to_drain() {
        let backend = Arc::new(TestBackend {
            block_first: false,
            ..Default::default()
        });
        let budget = FrameBudget::new(3).unwrap();
        let mut session = StreamSession::new(
            objects(&[3]),
            backend,
            request(0..3),
            budget.clone(),
            config(3, 0),
        )
        .unwrap();

        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(session.poll_fill(&mut cx).is_pending());
        assert_eq!(session.pop_ready().unwrap(), "object-0:0");
        session.set_consumer_rate(FrameRate::new(1.0).unwrap());
        assert!(session.poll_fill(&mut cx).is_pending());
        assert_eq!(session.pop_ready().unwrap(), "object-0:1");

        assert_eq!(session.capacity_frames(), 1);
        assert_eq!(budget.available(), 2);
    }

    #[tokio::test]
    async fn requests_only_intersecting_frames() {
        let backend = Arc::new(TestBackend {
            block_first: false,
            ..Default::default()
        });
        let session = StreamSession::new(
            objects(&[3, 3]),
            backend,
            request(2..5),
            FrameBudget::new(3).unwrap(),
            config(3, 0),
        )
        .unwrap();
        let (frames, output) = collecting_output();

        session.pipe_into(output).await.unwrap();
        let frames = frames.lock().unwrap();
        let text: Vec<_> = frames.iter().map(|frame| &frame[..]).collect();
        assert_eq!(text, [b"object-0:2", b"object-1:0", b"object-1:1"]);
    }

    #[test]
    fn global_budget_caps_the_window_and_target_rate() {
        let backend = Arc::new(TestBackend {
            block_first: true,
            ..Default::default()
        });
        let session = StreamSession::new(
            objects(&[10]),
            backend,
            request(0..10),
            FrameBudget::new(2).unwrap(),
            config(10, 0),
        )
        .unwrap();

        assert_eq!(session.capacity_frames(), 2);
        assert!(session.target_rate().unwrap().frames_per_second() < 10.0);
    }

    #[test]
    fn window_grows_when_another_session_releases_memory() {
        let budget = FrameBudget::new(3).unwrap();
        let backend = || {
            Arc::new(TestBackend {
                block_first: true,
                ..Default::default()
            })
        };
        let holder = StreamSession::new(
            objects(&[2]),
            backend(),
            request(0..2),
            budget.clone(),
            config(2, 0),
        )
        .unwrap();
        let mut waiting = StreamSession::new(
            objects(&[2]),
            backend(),
            request(0..2),
            budget,
            config(2, 0),
        )
        .unwrap();
        let mut cx = Context::from_waker(noop_waker_ref());

        assert_eq!(waiting.capacity_frames(), 1);
        assert!(waiting.poll_fill(&mut cx).is_pending());
        drop(holder);
        assert!(waiting.poll_fill(&mut cx).is_pending());
        assert_eq!(waiting.capacity_frames(), 2);
    }
}
