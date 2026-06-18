//! Core primitives for bounded, framed HTTP streaming.

mod frame;
mod memory;
mod pacer;
mod planning;
mod request;
mod sizing;
mod window;

pub use frame::{FrameAssembler, FrameDecoder, FrameError};
pub use memory::{MemoryBudget, MemoryBudgetError, MemoryPermit};
pub use pacer::{OutputPacer, PacerError};
pub use planning::{ObjectReadPlan, PhysicalRange, ReadPlanner};
pub use request::{ByteRate, ObjectId, ObjectMeta, StreamRequest, StreamRequestError};
pub use sizing::{WindowSizing, WindowSizingError, WindowSizingInput};
pub use window::{
    DownloadState, FrameSlotSnapshot, FrameSlotState, ObjectPlan, PlanId, PlanZones, UrlState,
    WindowAction, WindowBoundaries, WindowConfig, WindowConfigError, WindowController, WindowError,
};
