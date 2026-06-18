//! Core primitives for bounded, framed HTTP streaming.

mod planning;
mod request;

pub use planning::{ObjectReadPlan, PhysicalRange, ReadPlanner};
pub use request::{ByteRate, ObjectId, ObjectMeta, StreamRequest, StreamRequestError};
