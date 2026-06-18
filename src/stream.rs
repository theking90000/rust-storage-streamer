use std::collections::VecDeque;
use std::error::Error;
use std::future::Future;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::stream::{Fuse, FusedStream};
use futures_util::{Stream, StreamExt};

use crate::{FrameBudget, FrameBudgetError, FramePermit, FrameRate, ObjectMeta, StreamRequest};
use crate::{TransferModel, WindowSizing};

pub type BoxError = Box<dyn Error + Send + Sync>;
pub type SignedUrl = String;
pub type UrlTicket = Pin<Box<dyn Future<Output = Result<SignedUrl, BoxError>> + Send>>;
pub type FrameStream = Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send>>;
type ObjectStream = Pin<Box<dyn Stream<Item = Result<ObjectMeta, BoxError>> + Send>>;
type BudgetTicket = Pin<Box<dyn Future<Output = Result<FramePermit, FrameBudgetError>> + Send>>;

/// The URL coordinator and HTTP/crypto pipeline seen by a stream session.
pub trait StreamBackend: Send + Sync {
    /// Starts or joins URL resolution immediately. The returned ticket may be
    /// polled later; resolution itself must not wait for that first poll.
    fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket;

    /// Opens one sequential stream for the useful local frame range.
    fn download(&self, object: &ObjectMeta, url: SignedUrl, frames: Range<u32>) -> FrameStream;
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
    download: Option<FrameStream>,
    authorized: u32,
    received: u32,
    emitted: u32,
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
    backend: Arc<dyn StreamBackend>,
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
        backend: Arc<dyn StreamBackend>,
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
                download: None,
                authorized: first_frame,
                received: first_frame,
                emitted: first_frame,
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
        for plan in &mut self.plans {
            if plan.authorized == plan.emitted || plan.download.is_some() {
                continue;
            }
            let Some(ticket) = &mut plan.ticket else {
                continue;
            };
            match ticket.as_mut().poll(cx) {
                Poll::Ready(Ok(url)) => {
                    plan.download = Some(self.backend.download(
                        &plan.object,
                        url,
                        plan.local_frames.clone(),
                    ));
                    plan.ticket = None;
                }
                Poll::Ready(Err(error)) => {
                    plan.ticket = None;
                    return Poll::Ready(Err(error));
                }
                Poll::Pending => {}
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_downloads(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool, BoxError>> {
        let mut progressed = false;

        for plan in &mut self.plans {
            if plan.received == plan.authorized {
                continue;
            }
            let Some(download) = &mut plan.download else {
                continue;
            };
            match download.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    plan.buffer.push_back(frame);
                    plan.received += 1;
                    progressed = true;
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                Poll::Ready(None) => {
                    return Poll::Ready(Err(message("object ended before its last frame")));
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
        if plan.emitted == plan.local_frames.end {
            self.plans.pop_front();
        }
        Some(frame)
    }
}

impl Stream for StreamSession {
    type Item = Result<Bytes, BoxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        let window = match this.refresh_allocation(cx) {
            Ok(window) => window,
            Err(error) => return Poll::Ready(Some(Err(error))),
        };
        if let Poll::Ready(Err(error)) = this.poll_objects(cx, window) {
            return Poll::Ready(Some(Err(error)));
        }
        this.authorize(window.ready_data_frames);
        if let Poll::Ready(Err(error)) = this.poll_urls(cx) {
            return Poll::Ready(Some(Err(error)));
        }
        let progressed = match this.poll_downloads(cx) {
            Poll::Ready(Ok(progressed)) => progressed,
            Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
            Poll::Pending => false,
        };

        if let Some(frame) = this.pop_ready() {
            let committed: usize = this.plans.iter().map(ObjectPlan::committed).sum();
            this.allocation
                .shrink_to(window.ready_data_frames.max(committed));
            return Poll::Ready(Some(Ok(frame)));
        }
        if this.plans.is_empty()
            && (this.objects.is_terminated() || this.source_cursor >= this.request.frames().end)
        {
            return Poll::Ready(None);
        }
        if progressed {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

fn message(text: &str) -> BoxError {
    Box::new(io::Error::other(text))
}

#[cfg(test)]
mod tests {
    use std::task::Context;
    use std::time::Duration;

    use futures_util::future;
    use futures_util::stream;
    use futures_util::task::noop_waker_ref;
    use futures_util::{StreamExt, TryStreamExt};

    use super::*;
    use crate::ObjectId;

    struct TestBackend {
        block_first: bool,
        panic_second_url: bool,
    }

    impl StreamBackend for TestBackend {
        fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket {
            if self.panic_second_url && object.id.as_str() == "object-1" {
                return Box::pin(async { panic!("prefetched URL ticket was polled") });
            }
            Box::pin(future::ready(Ok(object.uri.clone())))
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

    #[tokio::test]
    async fn emits_frames_in_object_order() {
        let backend = Arc::new(TestBackend {
            block_first: false,
            panic_second_url: false,
        });
        let session = StreamSession::new(
            objects(&[2, 2]),
            backend,
            request(0..4),
            FrameBudget::new(10).unwrap(),
            config(3, 3),
        )
        .unwrap();

        let frames: Vec<_> = session.try_collect().await.unwrap();
        let text: Vec<_> = frames.iter().map(|frame| &frame[..]).collect();
        assert_eq!(
            text,
            [b"object-0:0", b"object-0:1", b"object-1:0", b"object-1:1"]
        );
    }

    #[test]
    fn buffers_future_objects_without_bypassing_head_of_line() {
        let backend = Arc::new(TestBackend {
            block_first: true,
            panic_second_url: false,
        });
        let mut session = StreamSession::new(
            objects(&[2, 2]),
            backend,
            request(0..4),
            FrameBudget::new(4).unwrap(),
            config(4, 0),
        )
        .unwrap();
        let mut cx = Context::from_waker(noop_waker_ref());

        assert!(Pin::new(&mut session).poll_next(&mut cx).is_pending());
        assert!(Pin::new(&mut session).poll_next(&mut cx).is_pending());
        assert_eq!(session.plans()[0].buffered_frames(), 0);
        assert_eq!(session.plans()[1].buffered_frames(), 2);
    }

    #[test]
    fn does_not_poll_prefetched_url_without_data_capacity() {
        let backend = Arc::new(TestBackend {
            block_first: true,
            panic_second_url: true,
        });
        let mut session = StreamSession::new(
            objects(&[2, 2]),
            backend,
            request(0..4),
            FrameBudget::new(4).unwrap(),
            config(1, 3),
        )
        .unwrap();
        let mut cx = Context::from_waker(noop_waker_ref());

        assert!(Pin::new(&mut session).poll_next(&mut cx).is_pending());
        assert_eq!(session.plans().len(), 2);
        assert_eq!(session.plans()[1].authorized_frames(), 0);
    }

    #[tokio::test]
    async fn capacity_can_change_while_streaming() {
        let backend = Arc::new(TestBackend {
            block_first: false,
            panic_second_url: false,
        });
        let mut session = StreamSession::new(
            objects(&[3]),
            backend,
            request(0..3),
            FrameBudget::new(3).unwrap(),
            config(1, 0),
        )
        .unwrap();

        assert_eq!(session.next().await.unwrap().unwrap(), "object-0:0");
        session.set_consumer_rate(FrameRate::new(3.0).unwrap());
        assert_eq!(session.next().await.unwrap().unwrap(), "object-0:1");
        assert_eq!(session.plans()[0].authorized_frames(), 3);
    }

    #[tokio::test]
    async fn shrinking_waits_for_committed_frames_to_drain() {
        let backend = Arc::new(TestBackend {
            block_first: false,
            panic_second_url: false,
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

        assert_eq!(session.next().await.unwrap().unwrap(), "object-0:0");
        session.set_consumer_rate(FrameRate::new(1.0).unwrap());
        assert_eq!(session.next().await.unwrap().unwrap(), "object-0:1");

        assert_eq!(session.capacity_frames(), 1);
        assert_eq!(budget.available(), 2);
    }

    #[tokio::test]
    async fn requests_only_intersecting_frames() {
        let backend = Arc::new(TestBackend {
            block_first: false,
            panic_second_url: false,
        });
        let session = StreamSession::new(
            objects(&[3, 3]),
            backend,
            request(2..5),
            FrameBudget::new(3).unwrap(),
            config(3, 0),
        )
        .unwrap();

        let frames: Vec<_> = session.try_collect().await.unwrap();
        let text: Vec<_> = frames.iter().map(|frame| &frame[..]).collect();
        assert_eq!(text, [b"object-0:2", b"object-1:0", b"object-1:1"]);
    }

    #[test]
    fn global_budget_caps_the_window_and_target_rate() {
        let backend = Arc::new(TestBackend {
            block_first: true,
            panic_second_url: false,
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
                panic_second_url: false,
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
        assert!(Pin::new(&mut waiting).poll_next(&mut cx).is_pending());
        drop(holder);
        assert!(Pin::new(&mut waiting).poll_next(&mut cx).is_pending());
        assert_eq!(waiting.capacity_frames(), 2);
    }
}
