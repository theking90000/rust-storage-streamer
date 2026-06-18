use std::error::Error;
use std::fmt;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A coarse, global budget for application-owned streaming memory.
///
/// Reservations are rounded up to `unit_size`. A caller should reserve the
/// expected body size plus its configured estimate of hidden transport costs
/// before opening a data request.
#[derive(Clone, Debug)]
pub struct MemoryBudget {
    permits: Arc<Semaphore>,
    unit_size: usize,
    total_units: u32,
}

#[derive(Debug)]
pub struct MemoryPermit {
    _permit: OwnedSemaphorePermit,
    requested_bytes: usize,
    reserved_bytes: usize,
}

impl MemoryBudget {
    pub fn new(total_bytes: usize, unit_size: usize) -> Result<Self, MemoryBudgetError> {
        if total_bytes == 0 {
            return Err(MemoryBudgetError::ZeroTotalBytes);
        }
        if unit_size == 0 {
            return Err(MemoryBudgetError::ZeroUnitSize);
        }

        let units = total_bytes / unit_size;
        if units == 0 {
            return Err(MemoryBudgetError::BudgetSmallerThanUnit {
                total_bytes,
                unit_size,
            });
        }
        let total_units =
            u32::try_from(units).map_err(|_| MemoryBudgetError::TooManyUnits { units })?;
        if units > Semaphore::MAX_PERMITS {
            return Err(MemoryBudgetError::TooManyUnits { units });
        }

        Ok(Self {
            permits: Arc::new(Semaphore::new(units)),
            unit_size,
            total_units,
        })
    }

    pub async fn reserve(&self, bytes: usize) -> Result<MemoryPermit, MemoryBudgetError> {
        if bytes == 0 {
            return Err(MemoryBudgetError::ZeroReservation);
        }

        let units = bytes.div_ceil(self.unit_size);
        if units > self.total_units as usize {
            return Err(MemoryBudgetError::ReservationExceedsBudget {
                requested_bytes: bytes,
                capacity_bytes: self.capacity_bytes(),
            });
        }
        let units = units as u32;
        let permit = self
            .permits
            .clone()
            .acquire_many_owned(units)
            .await
            .map_err(|_| MemoryBudgetError::Closed)?;

        Ok(MemoryPermit {
            _permit: permit,
            requested_bytes: bytes,
            reserved_bytes: units as usize * self.unit_size,
        })
    }

    pub const fn unit_size(&self) -> usize {
        self.unit_size
    }

    /// Effective capacity after rounding the configured byte budget down to
    /// whole units. It never exceeds the configured budget.
    pub const fn capacity_bytes(&self) -> usize {
        self.total_units as usize * self.unit_size
    }

    pub fn available_bytes(&self) -> usize {
        self.permits.available_permits() * self.unit_size
    }
}

impl MemoryPermit {
    pub const fn requested_bytes(&self) -> usize {
        self.requested_bytes
    }

    pub const fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemoryBudgetError {
    ZeroTotalBytes,
    ZeroUnitSize,
    BudgetSmallerThanUnit {
        total_bytes: usize,
        unit_size: usize,
    },
    ZeroReservation,
    TooManyUnits {
        units: usize,
    },
    ReservationExceedsBudget {
        requested_bytes: usize,
        capacity_bytes: usize,
    },
    Closed,
}

impl fmt::Display for MemoryBudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroTotalBytes => f.write_str("memory budget must be greater than zero"),
            Self::ZeroUnitSize => f.write_str("memory unit size must be greater than zero"),
            Self::BudgetSmallerThanUnit {
                total_bytes,
                unit_size,
            } => write!(
                f,
                "memory budget of {total_bytes} bytes cannot hold one {unit_size}-byte unit"
            ),
            Self::ZeroReservation => f.write_str("memory reservation must be greater than zero"),
            Self::TooManyUnits { units } => {
                write!(
                    f,
                    "memory budget requires too many semaphore units: {units}"
                )
            }
            Self::ReservationExceedsBudget {
                requested_bytes,
                capacity_bytes,
            } => write!(
                f,
                "reservation of {requested_bytes} bytes exceeds budget capacity of {capacity_bytes} bytes"
            ),
            Self::Closed => f.write_str("memory budget is closed"),
        }
    }
}

impl Error for MemoryBudgetError {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn rounds_reservations_up_to_whole_units() {
        let budget = MemoryBudget::new(200, 64).unwrap();
        let permit = budget.reserve(65).await.unwrap();

        assert_eq!(budget.capacity_bytes(), 192);
        assert_eq!(permit.requested_bytes(), 65);
        assert_eq!(permit.reserved_bytes(), 128);
        assert_eq!(budget.available_bytes(), 64);
    }

    #[tokio::test]
    async fn rounds_total_capacity_down_to_preserve_the_hard_limit() {
        let budget = MemoryBudget::new(100, 64).unwrap();

        assert_eq!(budget.capacity_bytes(), 64);
        assert!(matches!(
            budget.reserve(65).await,
            Err(MemoryBudgetError::ReservationExceedsBudget { .. })
        ));
    }

    #[tokio::test]
    async fn waits_until_an_existing_reservation_is_dropped() {
        let budget = MemoryBudget::new(128, 64).unwrap();
        let first = budget.reserve(128).await.unwrap();
        let waiting_budget = budget.clone();
        let waiter = tokio::spawn(async move { waiting_budget.reserve(64).await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiter.is_finished());

        drop(first);
        let second = waiter.await.unwrap().unwrap();
        assert_eq!(second.reserved_bytes(), 64);
    }

    #[tokio::test]
    async fn rejects_a_reservation_larger_than_the_whole_budget() {
        let budget = MemoryBudget::new(128, 64).unwrap();

        assert_eq!(
            budget.reserve(129).await.unwrap_err(),
            MemoryBudgetError::ReservationExceedsBudget {
                requested_bytes: 129,
                capacity_bytes: 128,
            }
        );
    }

    #[test]
    fn rejects_a_budget_smaller_than_one_unit() {
        assert_eq!(
            MemoryBudget::new(63, 64).unwrap_err(),
            MemoryBudgetError::BudgetSmallerThanUnit {
                total_bytes: 63,
                unit_size: 64,
            }
        );
    }
}
