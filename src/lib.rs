//! Core primitives for bounded, framed HTTP streaming.

mod frame;
mod planning;
mod request;

pub use frame::{FrameAssembler, FrameDecoder, FrameError};
pub use planning::{ObjectReadPlan, PhysicalRange, ReadPlanner};
pub use request::{ByteRate, ObjectId, ObjectMeta, StreamRequest, StreamRequestError};
