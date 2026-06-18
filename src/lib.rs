//! Core primitives for bounded, framed HTTP streaming.

mod frame;
mod memory;
mod pacer;
mod request;
mod sizing;
mod stream;

pub use frame::{FrameAssembler, FrameDecoder, FrameError};
pub use memory::{FrameBudget, FrameBudgetError, FramePermit};
pub use pacer::{FramePacer, PacerError};
pub use request::{FrameRate, ObjectId, ObjectMeta, RequestError, StreamRequest};
pub use sizing::{WindowSizing, WindowSizingError, WindowSizingInput};
pub use stream::{
    BoxError, FrameStream, ObjectPlan, SignedUrl, StreamBackend, StreamConfig, StreamSession,
    UrlTicket,
};
