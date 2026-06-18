//! Core primitives for bounded, framed HTTP streaming.

mod frame;
mod memory;
mod planning;
mod request;

pub use frame::{FrameAssembler, FrameDecoder, FrameError};
pub use memory::{MemoryBudget, MemoryBudgetError, MemoryPermit};
pub use planning::{ObjectReadPlan, PhysicalRange, ReadPlanner};
pub use request::{ByteRate, ObjectId, ObjectMeta, StreamRequest, StreamRequestError};
