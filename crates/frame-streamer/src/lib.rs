//! Core primitives for bounded, framed HTTP streaming.

mod byte;
mod frame;
mod memory;
mod request;
mod sizing;
mod stream;

pub use byte::{ByteError, ByteRate, ByteRequest, ByteStream, ByteStreamConfig, ByteTransferModel};
pub use frame::{FrameAssembler, FrameDecoder, FrameError, TAG_SIZE};
pub use memory::{FrameBudget, FrameBudgetError, FramePermit};
pub use request::{DecryptKey, FrameRate, ObjectId, ObjectMeta, RequestError, StreamRequest};
pub use sizing::{TransferModel, WindowSizing, WindowSizingError};
pub use stream::{
    BoxError, EncryptedByteStream, EncryptedBytesBackend, FrameBackend, FrameStream, ObjectPlan,
    SignedUrl, StreamBackend, StreamConfig, StreamDriver, StreamSession, UrlTicket,
};
