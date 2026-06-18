use std::error::Error;
use std::fmt;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug)]
pub struct FrameBudget {
    permits: Arc<Semaphore>,
    capacity: usize,
}

#[derive(Debug)]
pub struct FramePermit {
    permit: OwnedSemaphorePermit,
    frames: usize,
}

impl FrameBudget {
    pub fn new(capacity_frames: usize) -> Result<Self, FrameBudgetError> {
        if capacity_frames == 0 {
            return Err(FrameBudgetError::ZeroCapacity);
        }
        if capacity_frames > Semaphore::MAX_PERMITS {
            return Err(FrameBudgetError::TooManyFrames(capacity_frames));
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(capacity_frames)),
            capacity: capacity_frames,
        })
    }

    pub async fn reserve(&self, frames: usize) -> Result<FramePermit, FrameBudgetError> {
        if frames == 0 {
            return Err(FrameBudgetError::ZeroReservation);
        }
        if frames > self.capacity {
            return Err(FrameBudgetError::ReservationExceedsCapacity {
                requested: frames,
                capacity: self.capacity,
            });
        }
        let frames = u32::try_from(frames).map_err(|_| FrameBudgetError::TooManyFrames(frames))?;
        let permit = self
            .permits
            .clone()
            .acquire_many_owned(frames)
            .await
            .map_err(|_| FrameBudgetError::Closed)?;
        Ok(FramePermit {
            permit,
            frames: frames as usize,
        })
    }

    pub fn try_reserve(&self, frames: usize) -> Result<FramePermit, FrameBudgetError> {
        self.validate_reservation(frames)?;
        let frames = u32::try_from(frames).map_err(|_| FrameBudgetError::TooManyFrames(frames))?;
        let permit = self
            .permits
            .clone()
            .try_acquire_many_owned(frames)
            .map_err(|_| FrameBudgetError::Unavailable)?;
        Ok(FramePermit {
            permit,
            frames: frames as usize,
        })
    }

    fn validate_reservation(&self, frames: usize) -> Result<(), FrameBudgetError> {
        if frames == 0 {
            return Err(FrameBudgetError::ZeroReservation);
        }
        if frames > self.capacity {
            return Err(FrameBudgetError::ReservationExceedsCapacity {
                requested: frames,
                capacity: self.capacity,
            });
        }
        Ok(())
    }

    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn available(&self) -> usize {
        self.permits.available_permits()
    }
}

impl FramePermit {
    pub const fn frames(&self) -> usize {
        self.frames
    }

    pub fn merge(&mut self, other: Self) {
        self.frames += other.frames;
        self.permit.merge(other.permit);
    }

    pub fn shrink_to(&mut self, frames: usize) {
        let released = self.frames.saturating_sub(frames);
        if released == 0 {
            return;
        }
        drop(self.permit.split(released));
        self.frames -= released;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameBudgetError {
    ZeroCapacity,
    ZeroReservation,
    TooManyFrames(usize),
    ReservationExceedsCapacity { requested: usize, capacity: usize },
    Closed,
    Unavailable,
}

impl fmt::Display for FrameBudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => f.write_str("frame budget must be positive"),
            Self::ZeroReservation => f.write_str("frame reservation must be positive"),
            Self::TooManyFrames(frames) => write!(f, "too many frame permits: {frames}"),
            Self::ReservationExceedsCapacity {
                requested,
                capacity,
            } => write!(
                f,
                "reservation of {requested} frames exceeds capacity of {capacity} frames"
            ),
            Self::Closed => f.write_str("frame budget is closed"),
            Self::Unavailable => f.write_str("frame budget is currently unavailable"),
        }
    }
}

impl Error for FrameBudgetError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn permits_return_to_the_budget_on_drop() {
        let budget = FrameBudget::new(3).unwrap();
        let permit = budget.reserve(2).await.unwrap();
        assert_eq!(permit.frames(), 2);
        assert_eq!(budget.available(), 1);
        drop(permit);
        assert_eq!(budget.available(), 3);
    }

    #[test]
    fn permits_can_grow_and_shrink() {
        let budget = FrameBudget::new(3).unwrap();
        let mut permit = budget.try_reserve(1).unwrap();
        permit.merge(budget.try_reserve(2).unwrap());
        assert_eq!(budget.available(), 0);

        permit.shrink_to(1);
        assert_eq!(permit.frames(), 1);
        assert_eq!(budget.available(), 2);
    }
}
