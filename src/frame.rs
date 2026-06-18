use std::error::Error;
use std::fmt;

use bytes::{Bytes, BytesMut};

use crate::ObjectMeta;

/// Turns arbitrarily-sized HTTP body chunks into complete physical frames.
#[derive(Debug)]
pub struct FrameAssembler {
    frame_size: usize,
    pending: BytesMut,
}

impl FrameAssembler {
    pub fn new(frame_size: usize) -> Result<Self, FrameError> {
        if frame_size == 0 {
            return Err(FrameError::ZeroFrameSize);
        }

        Ok(Self {
            frame_size,
            pending: BytesMut::with_capacity(frame_size),
        })
    }

    pub fn push(&mut self, chunk: Bytes) {
        self.pending.extend_from_slice(&chunk);
    }

    pub fn next_frame(&mut self) -> Option<Bytes> {
        (self.pending.len() >= self.frame_size)
            .then(|| self.pending.split_to(self.frame_size).freeze())
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Verifies that the HTTP body ended exactly on a frame boundary.
    pub fn finish(self) -> Result<(), FrameError> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(FrameError::IncompleteFrame {
                expected: self.frame_size,
                actual: self.pending.len(),
            })
        }
    }
}

/// Cryptography boundary used by the streaming pipeline.
///
/// Implementations must authenticate the complete physical frame before
/// returning any plaintext. Authentication failure is a fatal stream error.
pub trait FrameDecoder {
    type Error: Error + Send + Sync + 'static;

    fn decode_frame(
        &self,
        encrypted_frame: &[u8],
        object: &ObjectMeta,
        global_frame_index: u64,
    ) -> Result<Bytes, Self::Error>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameError {
    ZeroFrameSize,
    IncompleteFrame { expected: usize, actual: usize },
    UnexpectedPayloadSize { expected: usize, actual: usize },
    FrameOutsidePlan { frame_index: u64 },
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroFrameSize => f.write_str("frame size must be greater than zero"),
            Self::IncompleteFrame { expected, actual } => write!(
                f,
                "HTTP body ended with an incomplete frame: expected {expected} bytes, got {actual}"
            ),
            Self::UnexpectedPayloadSize { expected, actual } => {
                write!(f, "decoded frame has {actual} bytes; expected {expected}")
            }
            Self::FrameOutsidePlan { frame_index } => {
                write!(f, "frame {frame_index} is outside this object read plan")
            }
        }
    }
}

impl Error for FrameError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assembles_frames_across_chunk_boundaries() {
        let mut assembler = FrameAssembler::new(4).unwrap();

        assembler.push(Bytes::from_static(&[0, 1, 2]));
        assert_eq!(assembler.next_frame(), None);

        assembler.push(Bytes::from_static(&[3, 4, 5, 6, 7, 8]));
        assert_eq!(
            assembler.next_frame(),
            Some(Bytes::from_static(&[0, 1, 2, 3]))
        );
        assert_eq!(
            assembler.next_frame(),
            Some(Bytes::from_static(&[4, 5, 6, 7]))
        );
        assert_eq!(assembler.pending_len(), 1);
    }

    #[test]
    fn reports_an_incomplete_final_frame() {
        let mut assembler = FrameAssembler::new(4).unwrap();
        assembler.push(Bytes::from_static(&[1, 2, 3]));

        assert_eq!(
            assembler.finish(),
            Err(FrameError::IncompleteFrame {
                expected: 4,
                actual: 3,
            })
        );
    }

    #[test]
    fn accepts_a_body_ending_on_a_frame_boundary() {
        let mut assembler = FrameAssembler::new(4).unwrap();
        assembler.push(Bytes::from_static(&[1, 2, 3, 4]));
        assert!(assembler.next_frame().is_some());
        assert_eq!(assembler.finish(), Ok(()));
    }
}
