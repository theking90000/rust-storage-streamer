use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::ops::Range;

use bytes::Bytes;
use futures_util::stream::{Fuse, FusedStream};
use futures_util::{Stream, StreamExt};

use crate::{ObjectId, ObjectMeta};

pub type FrameIndex = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UrlState {
    Requested,
    Ready,
    Failed,
}

/// One object, one sequential buffer. Zone membership is derived from the
/// global window boundaries; the object is never copied between zones.
#[derive(Debug)]
pub struct ObjectPlan {
    object: ObjectMeta,
    start: FrameIndex,
    url: UrlState,
    download_started: bool,
    authorized_end: u32,
    received: u32,
    emitted: u32,
    buffer: VecDeque<Bytes>,
}

impl ObjectPlan {
    pub fn object(&self) -> &ObjectMeta {
        &self.object
    }

    pub fn global_frames(&self) -> Range<FrameIndex> {
        self.start..self.end()
    }

    pub fn url_state(&self) -> &UrlState {
        &self.url
    }

    pub const fn authorized_local_end(&self) -> u32 {
        self.authorized_end
    }

    pub const fn next_received_local(&self) -> u32 {
        self.received
    }

    pub const fn next_emitted_local(&self) -> u32 {
        self.emitted
    }

    pub fn buffered_frames(&self) -> usize {
        self.buffer.len()
    }

    fn end(&self) -> FrameIndex {
        self.start + u64::from(self.object.frame_count)
    }

    fn local_intersection(&self, range: Range<FrameIndex>) -> Option<Range<u32>> {
        let start = self.start.max(range.start);
        let end = self.end().min(range.end);
        (start < end).then(|| (start - self.start) as u32..(end - self.start) as u32)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanZones {
    pub ready: Option<Range<u32>>,
    pub data: Option<Range<u32>>,
    pub prefetch: Option<Range<u32>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowBoundaries {
    pub sent: FrameIndex,
    pub ready_until: FrameIndex,
    pub data_end: FrameIndex,
    pub prefetch_end: FrameIndex,
    pub planned_until: FrameIndex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowAction {
    FetchUrl {
        object_id: ObjectId,
    },
    OpenDownload {
        object_id: ObjectId,
        authorized_local_end: u32,
    },
    AdvanceDownload {
        object_id: ObjectId,
        authorized_local_end: u32,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowConfig {
    pub capacity_frames: usize,
    pub prefetch_frames: FrameIndex,
}

impl WindowConfig {
    pub fn new(
        capacity_frames: usize,
        prefetch_frames: FrameIndex,
    ) -> Result<Self, WindowError<()>> {
        if capacity_frames == 0 {
            return Err(WindowError::ZeroCapacity);
        }
        Ok(Self {
            capacity_frames,
            prefetch_frames,
        })
    }
}

/// Sliding window over a lazy object stream.
///
/// `capacity_frames` is the desired Ready + Data width. Existing download
/// grants are never revoked, so shrinking drains naturally as frames leave.
pub struct WindowController<S> {
    source: Fuse<S>,
    plans: VecDeque<ObjectPlan>,
    capacity_frames: usize,
    prefetch_frames: FrameIndex,
    sent: FrameIndex,
    planned_until: FrameIndex,
}

impl<S> WindowController<S> {
    pub fn plans(&self) -> &VecDeque<ObjectPlan> {
        &self.plans
    }

    pub fn capacity_frames(&self) -> usize {
        self.capacity_frames
    }

    pub fn boundaries(&self) -> WindowBoundaries
    where
        S: Stream,
    {
        let ready_until = self.ready_until();
        let target_data_end = self.sent.saturating_add(self.capacity_frames as u64);
        let committed_end = self
            .plans
            .iter()
            .filter(|plan| plan.download_started)
            .map(|plan| plan.start + u64::from(plan.authorized_end))
            .max()
            .unwrap_or(self.sent);
        let mut data_end = target_data_end.max(committed_end);
        if self.source.is_terminated() {
            data_end = data_end.min(self.planned_until);
        }
        let mut prefetch_end = data_end.saturating_add(self.prefetch_frames);
        if self.source.is_terminated() {
            prefetch_end = prefetch_end.min(self.planned_until);
        }
        WindowBoundaries {
            sent: self.sent,
            ready_until,
            data_end,
            prefetch_end,
            planned_until: self.planned_until,
        }
    }

    pub fn ready_frames(&self) -> FrameIndex {
        self.ready_until() - self.sent
    }

    pub fn buffered_frames(&self) -> usize {
        self.plans.iter().map(ObjectPlan::buffered_frames).sum()
    }

    pub fn plan_zones(&self, object_id: &ObjectId) -> Option<PlanZones>
    where
        S: Stream,
    {
        let plan = self.plan(object_id)?;
        let window = self.boundaries();
        Some(PlanZones {
            ready: plan.local_intersection(window.sent..window.ready_until),
            data: plan.local_intersection(window.ready_until..window.data_end),
            prefetch: plan.local_intersection(window.data_end..window.prefetch_end),
        })
    }

    fn ready_until(&self) -> FrameIndex {
        let mut ready = self.sent;
        for plan in &self.plans {
            ready += plan.buffer.len() as u64;
            if plan.received < plan.object.frame_count {
                break;
            }
        }
        ready
    }

    fn plan(&self, object_id: &ObjectId) -> Option<&ObjectPlan> {
        self.plans.iter().find(|plan| plan.object.id == *object_id)
    }

    fn plan_mut(&mut self, object_id: &ObjectId) -> Option<&mut ObjectPlan> {
        // ponytail: linear scan is fine for a short prefetch deque; add a map
        // only if measurements show thousands of simultaneously planned objects.
        self.plans
            .iter_mut()
            .find(|plan| plan.object.id == *object_id)
    }

    fn schedule_downloads(&mut self) -> Vec<WindowAction>
    where
        S: Stream,
    {
        let data_end = self.boundaries().data_end;
        let mut actions = Vec::new();
        for plan in &mut self.plans {
            if plan.url != UrlState::Ready || plan.start >= data_end {
                continue;
            }
            let authorized_end = (data_end.min(plan.end()) - plan.start) as u32;
            if !plan.download_started {
                plan.download_started = true;
                plan.authorized_end = authorized_end;
                actions.push(WindowAction::OpenDownload {
                    object_id: plan.object.id.clone(),
                    authorized_local_end: authorized_end,
                });
            } else if authorized_end > plan.authorized_end {
                plan.authorized_end = authorized_end;
                actions.push(WindowAction::AdvanceDownload {
                    object_id: plan.object.id.clone(),
                    authorized_local_end: authorized_end,
                });
            }
        }
        actions
    }
}

impl<S, E> WindowController<S>
where
    S: Stream<Item = Result<ObjectMeta, E>> + Unpin,
{
    pub async fn new(
        source: S,
        config: WindowConfig,
    ) -> Result<(Self, Vec<WindowAction>), WindowError<E>> {
        if config.capacity_frames == 0 {
            return Err(WindowError::ZeroCapacity);
        }
        let mut window = Self {
            source: source.fuse(),
            plans: VecDeque::new(),
            capacity_frames: config.capacity_frames,
            prefetch_frames: config.prefetch_frames,
            sent: 0,
            planned_until: 0,
        };
        let actions = window.refill().await?;
        Ok((window, actions))
    }

    pub async fn set_capacity_frames(
        &mut self,
        capacity_frames: usize,
    ) -> Result<Vec<WindowAction>, WindowError<E>> {
        if capacity_frames == 0 {
            return Err(WindowError::ZeroCapacity);
        }
        self.capacity_frames = capacity_frames;
        self.refill().await
    }

    pub async fn set_prefetch_frames(
        &mut self,
        prefetch_frames: FrameIndex,
    ) -> Result<Vec<WindowAction>, WindowError<E>> {
        self.prefetch_frames = prefetch_frames;
        self.refill().await
    }

    pub fn url_ready(&mut self, object_id: &ObjectId) -> Result<Vec<WindowAction>, WindowError<E>> {
        self.plan_mut(object_id)
            .ok_or_else(|| WindowError::UnknownObject(object_id.clone()))?
            .url = UrlState::Ready;
        Ok(self.schedule_downloads())
    }

    pub fn url_failed(&mut self, object_id: &ObjectId) -> Result<(), WindowError<E>> {
        self.plan_mut(object_id)
            .ok_or_else(|| WindowError::UnknownObject(object_id.clone()))?
            .url = UrlState::Failed;
        Ok(())
    }

    /// Pushes the next sequential frame produced by an object's decoder.
    pub fn frame_ready(
        &mut self,
        object_id: &ObjectId,
        payload: Bytes,
    ) -> Result<(), WindowError<E>> {
        let plan = self
            .plan_mut(object_id)
            .ok_or_else(|| WindowError::UnknownObject(object_id.clone()))?;
        if !plan.download_started {
            return Err(WindowError::DownloadNotOpen(object_id.clone()));
        }
        if plan.received >= plan.authorized_end {
            return Err(WindowError::FrameNotAuthorized {
                object_id: object_id.clone(),
                local_frame: plan.received,
                authorized_local_end: plan.authorized_end,
            });
        }
        plan.buffer.push_back(payload);
        plan.received += 1;
        Ok(())
    }

    /// Returns the next globally ordered frame, then slides and refills the
    /// window. Later objects may be buffered, but never bypass the front plan.
    pub async fn pop_ready(
        &mut self,
    ) -> Result<(Option<Bytes>, Vec<WindowAction>), WindowError<E>> {
        let Some(plan) = self.plans.front_mut() else {
            return Ok((None, Vec::new()));
        };
        let Some(frame) = plan.buffer.pop_front() else {
            return Ok((None, Vec::new()));
        };
        plan.emitted += 1;
        self.sent += 1;
        if plan.emitted == plan.object.frame_count {
            self.plans.pop_front();
        }
        let actions = self.refill().await?;
        Ok((Some(frame), actions))
    }

    async fn refill(&mut self) -> Result<Vec<WindowAction>, WindowError<E>> {
        let mut actions = Vec::new();
        let target = self.boundaries().prefetch_end;
        while self.planned_until < target && !self.source.is_terminated() {
            let Some(object) = self
                .source
                .next()
                .await
                .transpose()
                .map_err(WindowError::Source)?
            else {
                break;
            };
            if object.frame_count == 0 {
                return Err(WindowError::EmptyObject(object.id));
            }
            if self.plans.iter().any(|plan| plan.object.id == object.id) {
                return Err(WindowError::DuplicateObject(object.id));
            }
            let start = self.planned_until;
            self.planned_until = self
                .planned_until
                .checked_add(u64::from(object.frame_count))
                .ok_or(WindowError::FrameIndexOverflow)?;
            actions.push(WindowAction::FetchUrl {
                object_id: object.id.clone(),
            });
            self.plans.push_back(ObjectPlan {
                object,
                start,
                url: UrlState::Requested,
                download_started: false,
                authorized_end: 0,
                received: 0,
                emitted: 0,
                buffer: VecDeque::new(),
            });
        }
        actions.extend(self.schedule_downloads());
        Ok(actions)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum WindowError<E> {
    Source(E),
    ZeroCapacity,
    EmptyObject(ObjectId),
    DuplicateObject(ObjectId),
    UnknownObject(ObjectId),
    DownloadNotOpen(ObjectId),
    FrameNotAuthorized {
        object_id: ObjectId,
        local_frame: u32,
        authorized_local_end: u32,
    },
    FrameIndexOverflow,
}

impl<E: fmt::Display> fmt::Display for WindowError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(error) => write!(f, "object source failed: {error}"),
            Self::ZeroCapacity => f.write_str("Ready + Data must contain at least one frame"),
            Self::EmptyObject(id) => write!(f, "object {} contains no frames", id.as_str()),
            Self::DuplicateObject(id) => write!(f, "duplicate object {}", id.as_str()),
            Self::UnknownObject(id) => write!(f, "unknown object {}", id.as_str()),
            Self::DownloadNotOpen(id) => write!(f, "download {} is not open", id.as_str()),
            Self::FrameNotAuthorized {
                object_id,
                local_frame,
                authorized_local_end,
            } => write!(
                f,
                "object {} frame {local_frame} is beyond authorized end {authorized_local_end}",
                object_id.as_str()
            ),
            Self::FrameIndexOverflow => f.write_str("global frame index overflow"),
        }
    }
}

impl<E> Error for WindowError<E> where E: Error + 'static {}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use futures_util::stream;

    use super::*;

    fn object(index: usize, frames: u32) -> ObjectMeta {
        ObjectMeta {
            id: ObjectId::new(format!("object-{index}")),
            uri: format!("objects/{index}"),
            frame_count: frames,
        }
    }

    fn source(
        counts: &[u32],
    ) -> impl Stream<Item = Result<ObjectMeta, Infallible>> + Unpin + use<> {
        stream::iter(
            counts
                .iter()
                .copied()
                .enumerate()
                .map(|(index, count)| Ok(object(index, count)))
                .collect::<Vec<_>>(),
        )
    }

    #[tokio::test]
    async fn objects_own_their_buffers_and_head_of_line_stays_ordered() {
        let config = WindowConfig::new(6, 0).unwrap();
        let (mut window, _) = WindowController::new(source(&[3, 3]), config)
            .await
            .unwrap();
        let first = window.plans()[0].object.id.clone();
        let second = window.plans()[1].object.id.clone();
        window.url_ready(&first).unwrap();
        window.url_ready(&second).unwrap();

        window
            .frame_ready(&second, Bytes::from_static(b"future"))
            .unwrap();
        assert_eq!(window.ready_frames(), 0);
        assert_eq!(window.plans()[1].buffered_frames(), 1);

        window
            .frame_ready(&first, Bytes::from_static(b"current"))
            .unwrap();
        assert_eq!(window.ready_frames(), 1);
        assert_eq!(window.pop_ready().await.unwrap().0.unwrap(), "current");
    }

    #[tokio::test]
    async fn one_object_is_intersected_instead_of_copied_between_zones() {
        let config = WindowConfig::new(3, 3).unwrap();
        let (mut window, _) = WindowController::new(source(&[3, 3]), config)
            .await
            .unwrap();
        let first = window.plans()[0].object.id.clone();
        window.url_ready(&first).unwrap();
        window
            .frame_ready(&first, Bytes::from_static(b"0"))
            .unwrap();
        window
            .frame_ready(&first, Bytes::from_static(b"1"))
            .unwrap();

        assert_eq!(
            window.plan_zones(&first),
            Some(PlanZones {
                ready: Some(0..2),
                data: Some(2..3),
                prefetch: None,
            })
        );
    }

    #[tokio::test]
    async fn sliding_authorizes_the_next_object_and_resize_drains() {
        let config = WindowConfig::new(3, 3).unwrap();
        let (mut window, _) = WindowController::new(source(&[3, 3]), config)
            .await
            .unwrap();
        let first = window.plans()[0].object.id.clone();
        let second = window.plans()[1].object.id.clone();
        window.url_ready(&first).unwrap();
        window.url_ready(&second).unwrap();
        for value in 0..3 {
            window
                .frame_ready(&first, Bytes::from(vec![value]))
                .unwrap();
        }

        let (_, actions) = window.pop_ready().await.unwrap();
        assert!(matches!(
            actions.as_slice(),
            [WindowAction::OpenDownload {
                object_id,
                authorized_local_end: 1,
            }] if *object_id == second
        ));

        window.set_capacity_frames(1).await.unwrap();
        assert_eq!(window.boundaries().data_end, 4);
        window.pop_ready().await.unwrap();
        window.pop_ready().await.unwrap();
        assert_eq!(window.boundaries().data_end, 4);

        window
            .frame_ready(&second, Bytes::from_static(b"next"))
            .unwrap();
        let (_, actions) = window.pop_ready().await.unwrap();
        assert_eq!(window.boundaries().data_end, 5);
        assert!(matches!(
            actions.as_slice(),
            [WindowAction::AdvanceDownload {
                object_id,
                authorized_local_end: 2,
            }] if *object_id == second
        ));
    }

    #[tokio::test]
    async fn source_is_only_consumed_to_the_prefetch_horizon() {
        let config = WindowConfig::new(3, 3).unwrap();
        let (window, actions) = WindowController::new(source(&[3, 3, 3]), config)
            .await
            .unwrap();

        assert_eq!(window.plans().len(), 2);
        assert_eq!(actions.len(), 2);
        assert_eq!(window.boundaries().planned_until, 6);
    }
}
