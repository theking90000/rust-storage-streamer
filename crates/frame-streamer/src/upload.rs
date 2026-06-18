use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_stream::try_stream;
use bytes::{Bytes, BytesMut};
use futures_util::{Stream, StreamExt};
use tokio::sync::oneshot;

use crate::{BoxError, DecryptKey, FrameEncoder, ObjectId, SignedUrl, TAG_SIZE};

pub type UploadByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send>>;
pub type UploadFuture = Pin<Box<dyn Future<Output = Result<StoredObject, BoxError>> + Send>>;
pub type DeleteFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

#[derive(Clone, Debug)]
pub struct UploadObject {
    pub id: ObjectId,
    pub key: DecryptKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredObject {
    pub uri: String,
    pub cached_url: Option<SignedUrl>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadResult {
    pub stored_object: StoredObject,
    pub plaintext_size: u64,
    pub frame_count: u32,
    pub checksum: [u8; 32],
}

/// Frame-native upload destination. Every input item is one full plaintext payload frame.
pub trait UploadBackend: Send + Sync {
    fn max_frames_per_segment(&self) -> u32;
    fn upload(
        &self,
        object: UploadObject,
        frame_count_hint: Option<u32>,
        frames: UploadByteStream,
    ) -> UploadFuture;
    fn delete(&self, object: StoredObject) -> DeleteFuture;
}

/// Lowest storage layer. It only sees complete encrypted physical frames.
pub trait EncryptedBytesUploadBackend: Send + Sync {
    fn max_physical_bytes_per_segment(&self) -> u64;
    fn upload(
        &self,
        id: ObjectId,
        physical_size_hint: Option<u64>,
        bytes: UploadByteStream,
    ) -> UploadFuture;
    fn delete(&self, object: StoredObject) -> DeleteFuture;
}

pub struct StreamUploadBackend {
    encrypted: Arc<dyn EncryptedBytesUploadBackend>,
    frame_size: usize,
}

impl StreamUploadBackend {
    pub fn new(
        encrypted: Arc<dyn EncryptedBytesUploadBackend>,
        frame_size: usize,
    ) -> Result<Self, BoxError> {
        if frame_size <= TAG_SIZE {
            return Err(Box::new(UploadError::FrameTooSmall));
        }
        Ok(Self {
            encrypted,
            frame_size,
        })
    }
}

impl UploadBackend for StreamUploadBackend {
    fn max_frames_per_segment(&self) -> u32 {
        (self.encrypted.max_physical_bytes_per_segment() / self.frame_size as u64)
            .min(u64::from(u32::MAX)) as u32
    }

    fn upload(
        &self,
        object: UploadObject,
        frame_count_hint: Option<u32>,
        mut frames: UploadByteStream,
    ) -> UploadFuture {
        let frame_size = self.frame_size;
        let encrypted = self.encrypted.clone();
        let id = object.id;
        let physical_size_hint = frame_count_hint.map(|count| u64::from(count) * frame_size as u64);
        let bytes = Box::pin(try_stream! {
            let encoder = FrameEncoder::new(frame_size, &object.key)?;
            let mut index = 0;
            while let Some(frame) = frames.next().await {
                yield encoder.encode_frame(BytesMut::from(&frame?[..]), index)?;
                index += 1;
            }
        });
        encrypted.upload(id, physical_size_hint, bytes)
    }

    fn delete(&self, object: StoredObject) -> DeleteFuture {
        self.encrypted.delete(object)
    }
}

pub struct ByteUpload {
    backend: Arc<dyn UploadBackend>,
    frame_size: usize,
}

impl ByteUpload {
    pub fn new(backend: Arc<dyn UploadBackend>, frame_size: usize) -> Result<Self, UploadError> {
        if frame_size <= TAG_SIZE {
            return Err(UploadError::FrameTooSmall);
        }
        if backend.max_frames_per_segment() == 0 {
            return Err(UploadError::ZeroCapacity);
        }
        Ok(Self {
            backend,
            frame_size,
        })
    }

    pub async fn upload<S, E>(
        &self,
        object: UploadObject,
        mut body: S,
        content_length: Option<u64>,
    ) -> Result<UploadResult, BoxError>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
        E: Error + Send + Sync + 'static,
    {
        let payload_size = self.frame_size - TAG_SIZE;
        let max_frames = self.backend.max_frames_per_segment();
        let max_bytes = u64::from(max_frames) * payload_size as u64;
        if matches!(content_length, Some(0)) {
            return Err(Box::new(UploadError::Empty));
        }
        if content_length.is_some_and(|size| size > max_bytes) {
            return Err(Box::new(UploadError::TooLarge { max_bytes }));
        }
        let frame_count_hint = content_length.map(|size| size.div_ceil(payload_size as u64) as u32);
        let (summary_tx, summary_rx) = oneshot::channel();
        let frames = Box::pin(try_stream! {
            let mut pending = BytesMut::with_capacity(payload_size);
            let mut hasher = blake3::Hasher::new();
            let mut size = 0_u64;
            let mut frame_count = 0_u32;

            while let Some(chunk) = body.next().await {
                let chunk = chunk.map_err(|error| -> BoxError { Box::new(error) })?;
                size = size.checked_add(chunk.len() as u64).ok_or(UploadError::TooLarge { max_bytes })?;
                if size > max_bytes {
                    Err(UploadError::TooLarge { max_bytes })?;
                }
                hasher.update(&chunk);
                let mut offset = 0;
                while offset < chunk.len() {
                    let take = (payload_size - pending.len()).min(chunk.len() - offset);
                    pending.extend_from_slice(&chunk[offset..offset + take]);
                    offset += take;
                    if pending.len() == payload_size {
                        frame_count += 1;
                        yield pending.split().freeze();
                    }
                }
            }

            if pending.is_empty() && frame_count == 0 {
                Err(UploadError::Empty)?;
            }
            if !pending.is_empty() {
                pending.resize(payload_size, 0);
                frame_count += 1;
                yield pending.freeze();
            }
            let _ = summary_tx.send((size, frame_count, *hasher.finalize().as_bytes()));
        });

        let stored_object = self
            .backend
            .upload(object, frame_count_hint, frames)
            .await?;
        let (plaintext_size, frame_count, checksum) = summary_rx
            .await
            .map_err(|_| Box::new(UploadError::Incomplete) as BoxError)?;
        if content_length.is_some_and(|expected| expected != plaintext_size) {
            let _ = self.backend.delete(stored_object.clone()).await;
            return Err(Box::new(UploadError::LengthMismatch {
                expected: content_length.unwrap(),
                actual: plaintext_size,
            }));
        }
        Ok(UploadResult {
            stored_object,
            plaintext_size,
            frame_count,
            checksum,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UploadError {
    FrameTooSmall,
    ZeroCapacity,
    Empty,
    TooLarge { max_bytes: u64 },
    LengthMismatch { expected: u64, actual: u64 },
    Incomplete,
}

impl fmt::Display for UploadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooSmall => f.write_str("frame size must be greater than the tag"),
            Self::ZeroCapacity => f.write_str("upload backend accepts no frames"),
            Self::Empty => f.write_str("segment body must not be empty"),
            Self::TooLarge { max_bytes } => write!(f, "segment exceeds {max_bytes} plaintext bytes"),
            Self::LengthMismatch { expected, actual } => write!(f, "Content-Length was {expected}, body contained {actual} bytes"),
            Self::Incomplete => f.write_str("upload backend stopped before consuming the body"),
        }
    }
}

impl Error for UploadError {}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures_util::stream;

    use super::*;
    use crate::{FrameDecoder, ObjectMeta};

    #[derive(Default)]
    struct MemoryBackend {
        bytes: Arc<Mutex<Vec<Bytes>>>,
        deleted: Arc<Mutex<Vec<String>>>,
    }

    impl EncryptedBytesUploadBackend for MemoryBackend {
        fn max_physical_bytes_per_segment(&self) -> u64 { 72 }

        fn upload(&self, _id: ObjectId, _hint: Option<u64>, mut bytes: UploadByteStream) -> UploadFuture {
            let output = self.bytes.clone();
            Box::pin(async move {
                while let Some(chunk) = bytes.next().await { output.lock().unwrap().push(chunk?); }
                Ok(StoredObject { uri: "memory://segment".into(), cached_url: None })
            })
        }

        fn delete(&self, object: StoredObject) -> DeleteFuture {
            let deleted = self.deleted.clone();
            Box::pin(async move { deleted.lock().unwrap().push(object.uri); Ok(()) })
        }
    }

    fn object() -> UploadObject {
        UploadObject { id: ObjectId::new("segment"), key: DecryptKey::new([7; 32]) }
    }

    #[tokio::test]
    async fn chunks_hashes_pads_and_encrypts() {
        let raw = Arc::new(MemoryBackend::default());
        let backend = Arc::new(StreamUploadBackend::new(raw.clone(), 24).unwrap());
        let upload = ByteUpload::new(backend, 24).unwrap();
        let result = upload.upload(
            object(),
            stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"abc")), Ok(Bytes::from_static(b"defghijk"))]),
            Some(11),
        ).await.unwrap();

        assert_eq!(result.plaintext_size, 11);
        assert_eq!(result.frame_count, 2);
        assert_eq!(result.checksum, *blake3::hash(b"abcdefghijk").as_bytes());
        let decoder = FrameDecoder::new(24, &DecryptKey::new([7; 32])).unwrap();
        let encrypted = raw.bytes.lock().unwrap();
        assert_eq!(decoder.decode_frame(BytesMut::from(&encrypted[0][..]), 0).unwrap(), &b"abcdefgh"[..]);
        assert_eq!(&decoder.decode_frame(BytesMut::from(&encrypted[1][..]), 1).unwrap()[..3], b"ijk");
    }

    #[tokio::test]
    async fn rejects_oversize_body() {
        let raw = Arc::new(MemoryBackend::default());
        let backend = Arc::new(StreamUploadBackend::new(raw, 24).unwrap());
        let upload = ByteUpload::new(backend, 24).unwrap();
        let error = upload.upload(object(), stream::iter([Ok::<_, std::io::Error>(Bytes::from(vec![0; 25]))]), None).await.unwrap_err();
        assert!(error.to_string().contains("exceeds 24"));
    }

    #[tokio::test]
    async fn deletes_object_after_length_mismatch() {
        let raw = Arc::new(MemoryBackend::default());
        let backend = Arc::new(StreamUploadBackend::new(raw.clone(), 24).unwrap());
        let upload = ByteUpload::new(backend, 24).unwrap();
        let error = upload.upload(object(), stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"abc"))]), Some(4)).await.unwrap_err();
        assert!(error.to_string().contains("Content-Length"));
        assert_eq!(raw.deleted.lock().unwrap().as_slice(), &["memory://segment"]);
    }

    #[allow(dead_code)]
    fn download_meta(result: &UploadResult) -> ObjectMeta {
        ObjectMeta {
            id: ObjectId::new("segment"),
            uri: result.stored_object.uri.clone(),
            frame_count: result.frame_count,
            decrypt_key: DecryptKey::new([7; 32]),
        }
    }
}
