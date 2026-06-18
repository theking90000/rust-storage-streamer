//! Core primitives for bounded, framed HTTP streaming.

mod frame;
mod memory;
mod pacer;
mod planning;
mod request;
mod sizing;
mod stream;

pub use frame::{FrameAssembler, FrameDecoder, FrameError};
pub use memory::{MemoryBudget, MemoryBudgetError, MemoryPermit};
pub use pacer::{OutputPacer, PacerError};
pub use planning::{ObjectReadPlan, PhysicalRange, ReadPlanner};
pub use request::{ByteRate, ObjectId, ObjectMeta, StreamRequest, StreamRequestError};
pub use sizing::{WindowSizing, WindowSizingError, WindowSizingInput};
pub use stream::{
    BoxError, FrameStream, ObjectPlan, SignedUrl, StreamBackend, StreamConfig, StreamSession,
    UrlTicket,
};
