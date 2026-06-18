use std::error::Error;
use std::fmt;

use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use bytes::{Bytes, BytesMut};

use crate::DecryptKey;

pub const TAG_SIZE: usize = 16;

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
        self.next_frame_mut().map(BytesMut::freeze)
    }

    pub fn next_frame_mut(&mut self) -> Option<BytesMut> {
        (self.pending.len() >= self.frame_size).then(|| self.pending.split_to(self.frame_size))
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

/// AES-256-GCM decoder for one object. Frames are `tag || ciphertext`.
pub struct FrameDecoder {
    frame_size: usize,
    cipher: Aes256Gcm,
}

impl FrameDecoder {
    pub fn new(frame_size: usize, key: &DecryptKey) -> Result<Self, FrameError> {
        if frame_size <= TAG_SIZE {
            return Err(FrameError::FrameTooSmall);
        }
        Ok(Self {
            frame_size,
            cipher: Aes256Gcm::new(key.as_bytes().into()),
        })
    }

    pub fn decode_frame(
        &self,
        mut encrypted_frame: BytesMut,
        local_frame_index: u64,
    ) -> Result<Bytes, FrameError> {
        if encrypted_frame.len() != self.frame_size {
            return Err(FrameError::InvalidFrameSize {
                expected: self.frame_size,
                actual: encrypted_frame.len(),
            });
        }

        let mut payload = encrypted_frame.split_off(TAG_SIZE);
        let mut nonce = [0; 12];
        nonce[4..].copy_from_slice(&local_frame_index.to_be_bytes());
        self.cipher
            .decrypt_in_place_detached(
                Nonce::from_slice(&nonce),
                b"",
                &mut payload,
                Tag::from_slice(&encrypted_frame),
            )
            .map_err(|_| FrameError::AuthenticationFailed)?;
        Ok(payload.freeze())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameError {
    ZeroFrameSize,
    FrameTooSmall,
    InvalidFrameSize { expected: usize, actual: usize },
    AuthenticationFailed,
    IncompleteFrame { expected: usize, actual: usize },
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroFrameSize => f.write_str("frame size must be greater than zero"),
            Self::FrameTooSmall => f.write_str("frame size must be greater than the 16-byte tag"),
            Self::InvalidFrameSize { expected, actual } => {
                write!(
                    f,
                    "invalid frame size: expected {expected} bytes, got {actual}"
                )
            }
            Self::AuthenticationFailed => f.write_str("frame authentication failed"),
            Self::IncompleteFrame { expected, actual } => write!(
                f,
                "HTTP body ended with an incomplete frame: expected {expected} bytes, got {actual}"
            ),
        }
    }
}

impl Error for FrameError {}

#[cfg(test)]
mod tests {
    use aes_gcm::aead::{AeadInPlace, KeyInit};

    use super::*;

    fn encrypt(key: &DecryptKey, index: u64, payload: &[u8]) -> BytesMut {
        let cipher = Aes256Gcm::new(key.as_bytes().into());
        let mut ciphertext = payload.to_vec();
        let mut nonce = [0; 12];
        nonce[4..].copy_from_slice(&index.to_be_bytes());
        let tag = cipher
            .encrypt_in_place_detached(Nonce::from_slice(&nonce), b"", &mut ciphertext)
            .unwrap();
        let mut frame = BytesMut::new();
        frame.extend_from_slice(&tag);
        frame.extend_from_slice(&ciphertext);
        frame
    }

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

    #[test]
    fn decrypts_and_authenticates_a_frame() {
        let key = DecryptKey::new([7; 32]);
        let decoder = FrameDecoder::new(24, &key).unwrap();
        let frame = encrypt(&key, 3, b"payload!");

        assert_eq!(decoder.decode_frame(frame, 3).unwrap(), &b"payload!"[..]);
    }

    #[test]
    fn rejects_a_wrong_key_or_tag() {
        let key = DecryptKey::new([7; 32]);
        let decoder = FrameDecoder::new(24, &DecryptKey::new([8; 32])).unwrap();
        assert_eq!(
            decoder.decode_frame(encrypt(&key, 3, b"payload!"), 3),
            Err(FrameError::AuthenticationFailed)
        );

        let decoder = FrameDecoder::new(24, &key).unwrap();
        let mut frame = encrypt(&key, 3, b"payload!");
        frame[0] ^= 1;
        assert_eq!(
            decoder.decode_frame(frame, 3),
            Err(FrameError::AuthenticationFailed)
        );
    }
}
