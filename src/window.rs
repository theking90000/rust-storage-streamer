use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::ops::Range;

use bytes::Bytes;
use futures_util::{Stream, StreamExt};

use crate::{ObjectId, ObjectMeta};

pub type FrameIndex = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PlanId(u64);

impl PlanId {
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UrlState {
    Requested,
    Ready,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DownloadState {
    NotStarted,
    Open {
        authorized_local_end: u32,
        next_local_frame: u32,
    },
    Complete,
}

/// A single materialization of an object. Zone membership is calculated by
/// intersecting this plan with the controller's global frame boundaries.
#[derive(Clone, Debug)]
pub struct ObjectPlan {
    id: PlanId,
    object: ObjectMeta,
    global_frames: Range<FrameIndex>,
    useful_frames: Range<FrameIndex>,
    url_state: UrlState,
    download_state: DownloadState,
}

impl ObjectPlan {
    pub const fn id(&self) -> PlanId {
        self.id
    }

    pub fn object(&self) -> &ObjectMeta {
        &self.object
    }

    pub fn global_frames(&self) -> Range<FrameIndex> {
        self.global_frames.clone()
    }

    pub fn useful_frames(&self) -> Range<FrameIndex> {
        self.useful_frames.clone()
    }

    pub fn url_state(&self) -> &UrlState {
        &self.url_state
    }

    pub fn download_state(&self) -> &DownloadState {
        &self.download_state
    }

    pub fn local_useful_frames(&self) -> Range<u32> {
        self.to_local(self.useful_frames.clone())
            .expect("useful frames are inside the object")
    }

    fn to_local(&self, frames: Range<FrameIndex>) -> Option<Range<u32>> {
        let intersection = intersect(&self.useful_frames, &frames)?;
        Some(
            (intersection.start - self.global_frames.start) as u32
                ..(intersection.end - self.global_frames.start) as u32,
        )
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
    pub prefetch_target: FrameIndex,
    pub planned_until: FrameIndex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowAction {
    FetchUrl {
        plan_id: PlanId,
        object_id: ObjectId,
    },
    OpenDownload {
        plan_id: PlanId,
        object_id: ObjectId,
        full_local_range: Range<u32>,
        authorized_local_end: u32,
    },
    AdvanceDownload {
        plan_id: PlanId,
        authorized_local_end: u32,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameSlotState {
    Unplanned,
    WaitingForUrl,
    Downloadable,
    Ready,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameSlotSnapshot {
    pub global_frame: FrameIndex,
    pub plan_id: Option<PlanId>,
    pub state: FrameSlotState,
}

#[derive(Debug)]
struct FrameSlot {
    global_frame: FrameIndex,
    plan_id: Option<PlanId>,
    payload: Option<Bytes>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowConfig {
    start_frame: FrameIndex,
    end_frame_exclusive: Option<FrameIndex>,
    capacity_frames: usize,
    prefetch_lead_frames: FrameIndex,
}

impl WindowConfig {
    pub fn new(
        start_frame: FrameIndex,
        end_frame_exclusive: Option<FrameIndex>,
        capacity_frames: usize,
        prefetch_lead_frames: FrameIndex,
    ) -> Result<Self, WindowConfigError> {
        if capacity_frames == 0 {
            return Err(WindowConfigError::ZeroCapacity);
        }
        if end_frame_exclusive.is_some_and(|end| end <= start_frame) {
            return Err(WindowConfigError::InvalidFrameRange);
        }
        Ok(Self {
            start_frame,
            end_frame_exclusive,
            capacity_frames,
            prefetch_lead_frames,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowConfigError {
    ZeroCapacity,
    InvalidFrameRange,
}

impl fmt::Display for WindowConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => f.write_str("Ready + Data must contain at least one frame"),
            Self::InvalidFrameRange => f.write_str("stream frame range is empty or reversed"),
        }
    }
}

impl Error for WindowConfigError {}

/// Sliding, frame-addressed window backed by a lazy stream of object metadata.
///
/// `S` is consumed only far enough to cover the URL prefetch horizon. Heavy
/// payload storage is bounded by `desired_capacity_frames` and represented by
/// the ring-like `slots` deque.
pub struct WindowController<S> {
    source: S,
    requested_start: FrameIndex,
    requested_end: Option<FrameIndex>,
    desired_capacity_frames: usize,
    prefetch_lead_frames: FrameIndex,
    sent: FrameIndex,
    ready_until: FrameIndex,
    slots: VecDeque<FrameSlot>,
    plans: VecDeque<ObjectPlan>,
    actions: VecDeque<WindowAction>,
    next_plan_id: u64,
    source_frame_cursor: FrameIndex,
    source_eof: bool,
    actual_stream_end: Option<FrameIndex>,
}

impl<S> WindowController<S> {
    pub fn boundaries(&self) -> WindowBoundaries {
        let data_end = self.data_end();
        WindowBoundaries {
            sent: self.sent,
            ready_until: self.ready_until,
            data_end,
            prefetch_target: self.prefetch_target(data_end),
            planned_until: self.source_frame_cursor,
        }
    }

    pub fn plans(&self) -> &VecDeque<ObjectPlan> {
        &self.plans
    }

    pub fn desired_capacity_frames(&self) -> usize {
        self.desired_capacity_frames
    }

    pub fn allocated_capacity_frames(&self) -> usize {
        self.slots.len()
    }

    pub fn ready_frames(&self) -> FrameIndex {
        self.ready_until - self.sent
    }

    pub fn data_frames(&self) -> FrameIndex {
        self.data_end() - self.ready_until
    }

    pub fn take_actions(&mut self) -> Vec<WindowAction> {
        self.actions.drain(..).collect()
    }

    pub fn plan_zones(&self, plan_id: PlanId) -> Option<PlanZones> {
        let plan = self.plan(plan_id)?;
        let boundaries = self.boundaries();
        Some(PlanZones {
            ready: plan.to_local(boundaries.sent..boundaries.ready_until),
            data: plan.to_local(boundaries.ready_until..boundaries.data_end),
            prefetch: plan.to_local(boundaries.data_end..boundaries.prefetch_target),
        })
    }

    pub fn slot_snapshots(&self) -> Vec<FrameSlotSnapshot> {
        self.slots
            .iter()
            .map(|slot| {
                let state = if slot.payload.is_some() {
                    FrameSlotState::Ready
                } else if let Some(plan_id) = slot.plan_id {
                    match self.plan(plan_id).map(ObjectPlan::url_state) {
                        Some(UrlState::Ready) => FrameSlotState::Downloadable,
                        Some(UrlState::Requested | UrlState::Failed) => {
                            FrameSlotState::WaitingForUrl
                        }
                        None => FrameSlotState::Unplanned,
                    }
                } else {
                    FrameSlotState::Unplanned
                };
                FrameSlotSnapshot {
                    global_frame: slot.global_frame,
                    plan_id: slot.plan_id,
                    state,
                }
            })
            .collect()
    }

    fn data_end(&self) -> FrameIndex {
        self.sent + self.slots.len() as u64
    }

    fn prefetch_target(&self, data_end: FrameIndex) -> FrameIndex {
        let target = data_end.saturating_add(self.prefetch_lead_frames);
        self.known_end().map_or(target, |end| target.min(end))
    }

    fn known_end(&self) -> Option<FrameIndex> {
        match (self.requested_end, self.actual_stream_end) {
            (Some(requested), Some(actual)) => Some(requested.min(actual)),
            (Some(requested), None) => Some(requested),
            (None, Some(actual)) => Some(actual),
            (None, None) => None,
        }
    }

    fn plan(&self, id: PlanId) -> Option<&ObjectPlan> {
        self.plans.iter().find(|plan| plan.id == id)
    }

    fn plan_mut(&mut self, id: PlanId) -> Option<&mut ObjectPlan> {
        self.plans.iter_mut().find(|plan| plan.id == id)
    }

    fn advance_ready_boundary(&mut self) {
        while let Some(slot) = self.slots.get((self.ready_until - self.sent) as usize) {
            if slot.payload.is_none() {
                break;
            }
            self.ready_until += 1;
        }
    }

    fn schedule_download_actions(&mut self) {
        let data_end = self.data_end();
        let mut actions = Vec::new();
        for plan in &mut self.plans {
            if plan.url_state != UrlState::Ready || plan.useful_frames.start >= data_end {
                continue;
            }
            let allowed_absolute_end = plan.useful_frames.end.min(data_end);
            let allowed_local_end = (allowed_absolute_end - plan.global_frames.start) as u32;
            let useful_local = plan.local_useful_frames();

            match &mut plan.download_state {
                DownloadState::NotStarted => {
                    plan.download_state = DownloadState::Open {
                        authorized_local_end: allowed_local_end,
                        next_local_frame: useful_local.start,
                    };
                    actions.push(WindowAction::OpenDownload {
                        plan_id: plan.id,
                        object_id: plan.object.id.clone(),
                        full_local_range: useful_local,
                        authorized_local_end: allowed_local_end,
                    });
                }
                DownloadState::Open {
                    authorized_local_end,
                    ..
                } if allowed_local_end > *authorized_local_end => {
                    *authorized_local_end = allowed_local_end;
                    actions.push(WindowAction::AdvanceDownload {
                        plan_id: plan.id,
                        authorized_local_end: allowed_local_end,
                    });
                }
                DownloadState::Open { .. } | DownloadState::Complete => {}
            }
        }
        self.actions.extend(actions);
    }

    fn bind_slots_to_plans(&mut self) {
        for slot in &mut self.slots {
            if slot.plan_id.is_some() {
                continue;
            }
            slot.plan_id = self
                .plans
                .iter()
                .find(|plan| plan.useful_frames.contains(&slot.global_frame))
                .map(|plan| plan.id);
        }
    }

    fn grow_slots(&mut self) -> Result<(), WindowError<()>> {
        while self.slots.len() < self.desired_capacity_frames {
            let global_frame = self.data_end();
            if self.known_end().is_some_and(|end| global_frame >= end) {
                break;
            }
            if global_frame == FrameIndex::MAX {
                return Err(WindowError::FrameIndexOverflow);
            }
            let plan_id = self
                .plans
                .iter()
                .find(|plan| plan.useful_frames.contains(&global_frame))
                .map(|plan| plan.id);
            self.slots.push_back(FrameSlot {
                global_frame,
                plan_id,
                payload: None,
            });
        }
        Ok(())
    }

    fn truncate_slots_to_known_end(&mut self) {
        if let Some(end) = self.known_end() {
            while self
                .slots
                .back()
                .is_some_and(|slot| slot.global_frame >= end)
            {
                self.slots.pop_back();
            }
        }
    }

    fn evict_consumed_plans(&mut self) {
        while self
            .plans
            .front()
            .is_some_and(|plan| plan.useful_frames.end <= self.sent)
        {
            self.plans.pop_front();
        }
    }
}

impl<S, E> WindowController<S>
where
    S: Stream<Item = Result<ObjectMeta, E>> + Unpin,
{
    pub async fn new(source: S, config: WindowConfig) -> Result<Self, WindowError<E>> {
        let mut controller = Self {
            source,
            requested_start: config.start_frame,
            requested_end: config.end_frame_exclusive,
            desired_capacity_frames: config.capacity_frames,
            prefetch_lead_frames: config.prefetch_lead_frames,
            sent: config.start_frame,
            ready_until: config.start_frame,
            slots: VecDeque::new(),
            plans: VecDeque::new(),
            actions: VecDeque::new(),
            next_plan_id: 0,
            source_frame_cursor: 0,
            source_eof: false,
            actual_stream_end: None,
        };
        controller.reconcile().await?;
        Ok(controller)
    }

    pub fn url_ready(&mut self, plan_id: PlanId) -> Result<(), WindowError<E>> {
        let plan = self
            .plan_mut(plan_id)
            .ok_or(WindowError::UnknownPlan(plan_id))?;
        plan.url_state = UrlState::Ready;
        self.schedule_download_actions();
        Ok(())
    }

    pub fn url_failed(&mut self, plan_id: PlanId) -> Result<(), WindowError<E>> {
        let plan = self
            .plan_mut(plan_id)
            .ok_or(WindowError::UnknownPlan(plan_id))?;
        plan.url_state = UrlState::Failed;
        Ok(())
    }

    pub fn frame_ready(
        &mut self,
        plan_id: PlanId,
        local_frame: u32,
        payload: Bytes,
    ) -> Result<(), WindowError<E>> {
        let (global_frame, expected_local, authorized_local_end) = {
            let plan = self
                .plan(plan_id)
                .ok_or(WindowError::UnknownPlan(plan_id))?;
            let (authorized_local_end, next_local_frame) = match plan.download_state {
                DownloadState::Open {
                    authorized_local_end,
                    next_local_frame,
                } => (authorized_local_end, next_local_frame),
                _ => return Err(WindowError::DownloadNotOpen(plan_id)),
            };
            (
                plan.global_frames.start + u64::from(local_frame),
                next_local_frame,
                authorized_local_end,
            )
        };

        if local_frame != expected_local {
            return Err(WindowError::UnexpectedFrame {
                plan_id,
                expected_local,
                actual_local: local_frame,
            });
        }
        if local_frame >= authorized_local_end {
            return Err(WindowError::FrameNotAuthorized {
                plan_id,
                local_frame,
                authorized_local_end,
            });
        }

        let offset = global_frame
            .checked_sub(self.sent)
            .and_then(|value| usize::try_from(value).ok())
            .filter(|offset| *offset < self.slots.len())
            .ok_or(WindowError::FrameOutsideWindow(global_frame))?;
        let slot = &mut self.slots[offset];
        if slot.plan_id != Some(plan_id) {
            return Err(WindowError::FrameOutsidePlan {
                plan_id,
                global_frame,
            });
        }
        if slot.payload.is_some() {
            return Err(WindowError::DuplicateFrame(global_frame));
        }
        slot.payload = Some(payload);

        let plan = self
            .plan_mut(plan_id)
            .expect("plan was checked before mutating the slot");
        let next_local_frame = local_frame + 1;
        let useful = plan.local_useful_frames();
        plan.download_state = if next_local_frame == useful.end {
            DownloadState::Complete
        } else {
            DownloadState::Open {
                authorized_local_end,
                next_local_frame,
            }
        };

        self.advance_ready_boundary();
        Ok(())
    }

    pub async fn set_capacity_frames(
        &mut self,
        capacity_frames: usize,
    ) -> Result<(), WindowError<E>> {
        if capacity_frames == 0 {
            return Err(WindowError::ZeroCapacity);
        }
        self.desired_capacity_frames = capacity_frames;
        self.reconcile().await
    }

    pub async fn set_prefetch_lead_frames(
        &mut self,
        prefetch_lead_frames: FrameIndex,
    ) -> Result<(), WindowError<E>> {
        self.prefetch_lead_frames = prefetch_lead_frames;
        self.reconcile().await
    }

    pub async fn pop_ready(&mut self) -> Result<Option<Bytes>, WindowError<E>> {
        let Some(front) = self.slots.front_mut() else {
            return Ok(None);
        };
        let Some(payload) = front.payload.take() else {
            return Ok(None);
        };

        self.slots.pop_front();
        self.sent += 1;
        self.evict_consumed_plans();
        self.reconcile().await?;
        Ok(Some(payload))
    }

    async fn reconcile(&mut self) -> Result<(), WindowError<E>> {
        self.grow_slots().map_err(convert_unit_error)?;
        let target = self.prefetch_target(self.data_end());
        self.pull_source_until(target).await?;
        self.truncate_slots_to_known_end();
        self.bind_slots_to_plans();
        self.schedule_download_actions();
        Ok(())
    }

    async fn pull_source_until(&mut self, target: FrameIndex) -> Result<(), WindowError<E>> {
        while !self.source_eof && self.source_frame_cursor < target {
            let Some(result) = self.source.next().await else {
                self.source_eof = true;
                self.actual_stream_end = Some(self.source_frame_cursor);
                break;
            };
            let object = result.map_err(WindowError::Source)?;
            if object.frame_count == 0 {
                return Err(WindowError::EmptyObject(object.id));
            }
            let object_start = self.source_frame_cursor;
            let object_end = object_start
                .checked_add(u64::from(object.frame_count))
                .ok_or(WindowError::FrameIndexOverflow)?;
            self.source_frame_cursor = object_end;

            let useful_start = object_start.max(self.requested_start);
            let useful_end = self
                .requested_end
                .map_or(object_end, |end| object_end.min(end));
            if useful_start >= useful_end {
                continue;
            }

            let id = PlanId(self.next_plan_id);
            self.next_plan_id = self
                .next_plan_id
                .checked_add(1)
                .ok_or(WindowError::PlanIdOverflow)?;
            self.actions.push_back(WindowAction::FetchUrl {
                plan_id: id,
                object_id: object.id.clone(),
            });
            self.plans.push_back(ObjectPlan {
                id,
                object,
                global_frames: object_start..object_end,
                useful_frames: useful_start..useful_end,
                url_state: UrlState::Requested,
                download_state: DownloadState::NotStarted,
            });
        }
        Ok(())
    }
}

fn convert_unit_error<E>(error: WindowError<()>) -> WindowError<E> {
    match error {
        WindowError::FrameIndexOverflow => WindowError::FrameIndexOverflow,
        _ => unreachable!("grow_slots only reports frame index overflow"),
    }
}

fn intersect<T>(left: &Range<T>, right: &Range<T>) -> Option<Range<T>>
where
    T: Ord + Copy,
{
    let start = left.start.max(right.start);
    let end = left.end.min(right.end);
    (start < end).then_some(start..end)
}

#[derive(Debug, PartialEq, Eq)]
pub enum WindowError<E> {
    Source(E),
    ZeroCapacity,
    EmptyObject(ObjectId),
    UnknownPlan(PlanId),
    DownloadNotOpen(PlanId),
    UnexpectedFrame {
        plan_id: PlanId,
        expected_local: u32,
        actual_local: u32,
    },
    FrameNotAuthorized {
        plan_id: PlanId,
        local_frame: u32,
        authorized_local_end: u32,
    },
    FrameOutsideWindow(FrameIndex),
    FrameOutsidePlan {
        plan_id: PlanId,
        global_frame: FrameIndex,
    },
    DuplicateFrame(FrameIndex),
    FrameIndexOverflow,
    PlanIdOverflow,
}

impl<E: fmt::Display> fmt::Display for WindowError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(error) => write!(f, "object source failed: {error}"),
            Self::ZeroCapacity => f.write_str("Ready + Data must contain at least one frame"),
            Self::EmptyObject(id) => write!(f, "object {} contains no frames", id.as_str()),
            Self::UnknownPlan(id) => write!(f, "unknown object plan {}", id.get()),
            Self::DownloadNotOpen(id) => write!(f, "download {} is not open", id.get()),
            Self::UnexpectedFrame {
                plan_id,
                expected_local,
                actual_local,
            } => write!(
                f,
                "plan {} expected local frame {expected_local}, got {actual_local}",
                plan_id.get()
            ),
            Self::FrameNotAuthorized {
                plan_id,
                local_frame,
                authorized_local_end,
            } => write!(
                f,
                "plan {} frame {local_frame} is beyond authorized end {authorized_local_end}",
                plan_id.get()
            ),
            Self::FrameOutsideWindow(frame) => {
                write!(f, "frame {frame} is outside Ready + Data")
            }
            Self::FrameOutsidePlan {
                plan_id,
                global_frame,
            } => write!(
                f,
                "global frame {global_frame} does not belong to plan {}",
                plan_id.get()
            ),
            Self::DuplicateFrame(frame) => write!(f, "frame {frame} was delivered twice"),
            Self::FrameIndexOverflow => f.write_str("global frame index overflow"),
            Self::PlanIdOverflow => f.write_str("object plan identifier overflow"),
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
        frame_counts: &[u32],
    ) -> impl Stream<Item = Result<ObjectMeta, Infallible>> + Unpin + use<> {
        stream::iter(
            frame_counts
                .iter()
                .copied()
                .enumerate()
                .map(|(index, frames)| Ok(object(index, frames)))
                .collect::<Vec<_>>(),
        )
    }

    fn config(capacity: usize, prefetch: u64) -> WindowConfig {
        WindowConfig::new(0, None, capacity, prefetch).unwrap()
    }

    #[tokio::test]
    async fn lazily_materializes_only_objects_visible_to_prefetch() {
        let mut controller = WindowController::new(source(&[3, 3, 3, 3]), config(3, 3))
            .await
            .unwrap();

        assert_eq!(controller.plans().len(), 2);
        assert_eq!(controller.boundaries().planned_until, 6);
        assert_eq!(controller.take_actions().len(), 2);
    }

    #[tokio::test]
    async fn represents_an_object_in_multiple_zones_without_copying_it() {
        let mut controller = WindowController::new(source(&[3, 3]), config(3, 3))
            .await
            .unwrap();
        let first = controller.plans()[0].id();
        controller.take_actions();
        controller.url_ready(first).unwrap();
        controller.take_actions();

        controller
            .frame_ready(first, 0, Bytes::from_static(b"zero"))
            .unwrap();
        controller
            .frame_ready(first, 1, Bytes::from_static(b"one"))
            .unwrap();

        assert_eq!(controller.plans().len(), 2);
        assert_eq!(
            controller.plan_zones(first),
            Some(PlanZones {
                ready: Some(0..2),
                data: Some(2..3),
                prefetch: None,
            })
        );
    }

    #[tokio::test]
    async fn an_unresolved_url_cannot_open_a_download() {
        let mut controller = WindowController::new(source(&[3]), config(3, 0))
            .await
            .unwrap();
        let plan_id = controller.plans()[0].id();

        assert!(matches!(
            controller.take_actions().as_slice(),
            [WindowAction::FetchUrl { .. }]
        ));
        assert!(matches!(
            controller.slot_snapshots()[0].state,
            FrameSlotState::WaitingForUrl
        ));

        controller.url_ready(plan_id).unwrap();
        assert!(matches!(
            controller.take_actions().as_slice(),
            [WindowAction::OpenDownload {
                authorized_local_end: 3,
                ..
            }]
        ));
    }

    #[tokio::test]
    async fn out_of_order_objects_do_not_advance_the_contiguous_ready_boundary() {
        let mut controller = WindowController::new(source(&[3, 3]), config(6, 0))
            .await
            .unwrap();
        let first = controller.plans()[0].id();
        let second = controller.plans()[1].id();
        controller.take_actions();
        controller.url_ready(first).unwrap();
        controller.url_ready(second).unwrap();
        controller.take_actions();

        controller
            .frame_ready(second, 0, Bytes::from_static(b"future"))
            .unwrap();
        assert_eq!(controller.ready_frames(), 0);

        controller
            .frame_ready(first, 0, Bytes::from_static(b"current"))
            .unwrap();
        assert_eq!(controller.ready_frames(), 1);
        assert_eq!(controller.slot_snapshots()[3].state, FrameSlotState::Ready);
    }

    #[tokio::test]
    async fn consuming_a_frame_slides_data_and_advances_the_next_object() {
        let mut controller = WindowController::new(source(&[3, 3]), config(3, 3))
            .await
            .unwrap();
        let first = controller.plans()[0].id();
        let second = controller.plans()[1].id();
        controller.take_actions();
        controller.url_ready(first).unwrap();
        controller.url_ready(second).unwrap();
        controller.take_actions();
        for local in 0..3 {
            controller
                .frame_ready(first, local, Bytes::from(vec![local as u8]))
                .unwrap();
        }

        assert!(controller.pop_ready().await.unwrap().is_some());
        assert_eq!(controller.boundaries().data_end, 4);
        assert!(matches!(
            controller.take_actions().as_slice(),
            [WindowAction::OpenDownload {
                plan_id,
                authorized_local_end: 1,
                ..
            }] if *plan_id == second
        ));
    }

    #[tokio::test]
    async fn shrinking_drains_slots_before_reusing_them() {
        let mut controller = WindowController::new(source(&[6]), config(3, 0))
            .await
            .unwrap();
        let plan = controller.plans()[0].id();
        controller.take_actions();
        controller.url_ready(plan).unwrap();
        controller.take_actions();
        for local in 0..3 {
            controller
                .frame_ready(plan, local, Bytes::from(vec![local as u8]))
                .unwrap();
        }

        controller.set_capacity_frames(1).await.unwrap();
        assert_eq!(controller.allocated_capacity_frames(), 3);
        controller.pop_ready().await.unwrap();
        assert_eq!(controller.allocated_capacity_frames(), 2);
        controller.pop_ready().await.unwrap();
        assert_eq!(controller.allocated_capacity_frames(), 1);
    }

    #[tokio::test]
    async fn growing_immediately_extends_data_and_download_authorization() {
        let mut controller = WindowController::new(source(&[6]), config(1, 0))
            .await
            .unwrap();
        let plan = controller.plans()[0].id();
        controller.take_actions();
        controller.url_ready(plan).unwrap();
        controller.take_actions();

        controller.set_capacity_frames(4).await.unwrap();

        assert_eq!(controller.allocated_capacity_frames(), 4);
        assert_eq!(controller.boundaries().data_end, 4);
        assert!(matches!(
            controller.take_actions().as_slice(),
            [WindowAction::AdvanceDownload {
                plan_id,
                authorized_local_end: 4,
            }] if *plan_id == plan
        ));
    }

    #[tokio::test]
    async fn scans_variable_objects_to_the_requested_start_without_prefetching_them() {
        let config = WindowConfig::new(5, Some(8), 2, 1).unwrap();
        let mut controller = WindowController::new(source(&[2, 3, 4, 5]), config)
            .await
            .unwrap();

        assert_eq!(controller.plans().len(), 1);
        assert_eq!(controller.plans()[0].object().id.as_str(), "object-2");
        assert_eq!(controller.plans()[0].useful_frames(), 5..8);
        assert!(matches!(
            controller.take_actions().as_slice(),
            [WindowAction::FetchUrl { object_id, .. }] if object_id.as_str() == "object-2"
        ));
    }
}
