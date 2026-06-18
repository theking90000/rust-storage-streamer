use std::collections::VecDeque;
use std::error::Error;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::stream::{Fuse, FusedStream};
use futures_util::{Stream, StreamExt};

use crate::ObjectMeta;

pub type BoxError = Box<dyn Error + Send + Sync>;
pub type SignedUrl = String;
pub type UrlTicket = Pin<Box<dyn Future<Output = Result<SignedUrl, BoxError>> + Send>>;
pub type FrameStream = Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send>>;
type ObjectStream = Pin<Box<dyn Stream<Item = Result<ObjectMeta, BoxError>> + Send>>;

/// The URL coordinator and HTTP/crypto pipeline seen by a stream session.
pub trait StreamBackend: Send + Sync {
    /// Starts or joins URL resolution immediately. The returned ticket may be
    /// polled later; resolution itself must not wait for that first poll.
    fn resolve_url(&self, object: &ObjectMeta) -> UrlTicket;

    /// Opens one sequential stream of decrypted frames for the whole object.
    fn download(&self, object: &ObjectMeta, url: SignedUrl) -> FrameStream;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamConfig {
    pub capacity_frames: usize,
    pub prefetch_frames: usize,
}

impl StreamConfig {
    pub fn new(capacity_frames: usize, prefetch_frames: usize) -> Result<Self, BoxError> {
        if capacity_frames == 0 {
            return Err(message("Ready + Data must contain at least one frame"));
        }
        Ok(Self {
            capacity_frames,
            prefetch_frames,
        })
    }
}

/// Runtime state for one object. HTTP frames are sequential, so one deque is
/// enough; later objects may fill theirs while the front object blocks output.
pub struct ObjectPlan {
    object: ObjectMeta,
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
        self.authorized
    }

    pub fn buffered_frames(&self) -> usize {
        self.buffer.len()
    }

    fn remaining(&self) -> usize {
        (self.object.frame_count - self.emitted) as usize
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
    capacity_frames: usize,
    prefetch_frames: usize,
}

impl StreamSession {
    pub fn new(
        objects: impl Stream<Item = Result<ObjectMeta, BoxError>> + Send + 'static,
        backend: Arc<dyn StreamBackend>,
        config: StreamConfig,
    ) -> Self {
        let objects: ObjectStream = Box::pin(objects);
        Self {
            objects: objects.fuse(),
            backend,
            plans: VecDeque::new(),
            capacity_frames: config.capacity_frames,
            prefetch_frames: config.prefetch_frames,
        }
    }

    pub fn plans(&self) -> &VecDeque<ObjectPlan> {
        &self.plans
    }

    pub fn set_capacity_frames(&mut self, capacity_frames: usize) -> Result<(), BoxError> {
        if capacity_frames == 0 {
            return Err(message("Ready + Data must contain at least one frame"));
        }
        self.capacity_frames = capacity_frames;
        Ok(())
    }

    pub fn set_prefetch_frames(&mut self, prefetch_frames: usize) {
        self.prefetch_frames = prefetch_frames;
    }

    pub fn buffered_frames(&self) -> usize {
        self.plans.iter().map(ObjectPlan::buffered_frames).sum()
    }

    fn poll_objects(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        let target = self.capacity_frames + self.prefetch_frames;
        let mut planned: usize = self.plans.iter().map(ObjectPlan::remaining).sum();

        while planned < target && !self.objects.is_terminated() {
            let object = match self.objects.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(object))) => object,
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                Poll::Ready(None) | Poll::Pending => break,
            };
            if object.frame_count == 0 {
                return Poll::Ready(Err(message("object contains no frames")));
            }
            planned += object.frame_count as usize;
            let ticket = self.backend.resolve_url(&object);
            self.plans.push_back(ObjectPlan {
                object,
                ticket: Some(ticket),
                download: None,
                authorized: 0,
                received: 0,
                emitted: 0,
                buffer: VecDeque::new(),
            });
        }
        Poll::Ready(Ok(()))
    }

    fn authorize(&mut self) {
        let committed: usize = self.plans.iter().map(ObjectPlan::committed).sum();
        let mut available = self.capacity_frames.saturating_sub(committed);

        for plan in &mut self.plans {
            if available == 0 {
                break;
            }
            let missing = (plan.object.frame_count - plan.authorized) as usize;
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
                    plan.download = Some(self.backend.download(&plan.object, url));
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
        if plan.emitted == plan.object.frame_count {
            self.plans.pop_front();
        }
        Some(frame)
    }
}

impl Stream for StreamSession {
    type Item = Result<Bytes, BoxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        if let Poll::Ready(Err(error)) = this.poll_objects(cx) {
            return Poll::Ready(Some(Err(error)));
        }
        this.authorize();
        if let Poll::Ready(Err(error)) = this.poll_urls(cx) {
            return Poll::Ready(Some(Err(error)));
        }
        let progressed = match this.poll_downloads(cx) {
            Poll::Ready(Ok(progressed)) => progressed,
            Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
            Poll::Pending => false,
        };

        if let Some(frame) = this.pop_ready() {
            return Poll::Ready(Some(Ok(frame)));
        }
        if this.objects.is_terminated() && this.plans.is_empty() {
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

        fn download(&self, object: &ObjectMeta, _url: SignedUrl) -> FrameStream {
            if self.block_first && object.id.as_str() == "object-0" {
                return Box::pin(stream::pending());
            }
            let id = object.id.as_str().to_owned();
            let frames =
                (0..object.frame_count).map(move |frame| Ok(Bytes::from(format!("{id}:{frame}"))));
            Box::pin(stream::iter(frames))
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

    #[tokio::test]
    async fn emits_frames_in_object_order() {
        let backend = Arc::new(TestBackend {
            block_first: false,
            panic_second_url: false,
        });
        let config = StreamConfig::new(3, 3).unwrap();
        let session = StreamSession::new(objects(&[2, 2]), backend, config);

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
        let config = StreamConfig::new(4, 0).unwrap();
        let mut session = StreamSession::new(objects(&[2, 2]), backend, config);
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
        let config = StreamConfig::new(1, 3).unwrap();
        let mut session = StreamSession::new(objects(&[2, 2]), backend, config);
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
        let config = StreamConfig::new(1, 0).unwrap();
        let mut session = StreamSession::new(objects(&[3]), backend, config);

        assert_eq!(session.next().await.unwrap().unwrap(), "object-0:0");
        session.set_capacity_frames(3).unwrap();
        assert_eq!(session.next().await.unwrap().unwrap(), "object-0:1");
        assert_eq!(session.plans()[0].authorized_frames(), 3);
    }
}
