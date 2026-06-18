use std::error::Error;
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::{Sink, Stream};

use crate::{
    BoxError, EncryptedBytesDownloadBackend, FrameBudget, FrameRate, ObjectMeta, StreamConfig,
    StreamDownloadBackend, StreamRequest, StreamSession, TAG_SIZE, TransferModel,
};

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct ByteRate(f64);

impl ByteRate {
    pub fn new(bytes_per_second: f64) -> Result<Self, ByteError> {
        if !bytes_per_second.is_finite() || bytes_per_second <= 0.0 {
            return Err(ByteError::InvalidRate);
        }
        Ok(Self(bytes_per_second))
    }

    pub const fn bytes_per_second(self) -> f64 {
        self.0
    }

    fn frames_per_second(self, bytes_per_frame: usize) -> Result<FrameRate, BoxError> {
        Ok(FrameRate::new(self.0 / bytes_per_frame as f64)?)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ByteRequest {
    bytes: Range<u64>,
    allocated_rate: ByteRate,
}

impl ByteRequest {
    pub fn new(bytes: Range<u64>, allocated_rate: ByteRate) -> Result<Self, ByteError> {
        if bytes.start >= bytes.end {
            return Err(ByteError::EmptyRange);
        }
        Ok(Self {
            bytes,
            allocated_rate,
        })
    }

    pub fn bytes(&self) -> Range<u64> {
        self.bytes.clone()
    }

    pub const fn allocated_rate(&self) -> ByteRate {
        self.allocated_rate
    }

    fn frame_request(&self, payload_size: usize) -> Result<StreamRequest, BoxError> {
        let payload_size = payload_size as u64;
        let frames = self.bytes.start / payload_size..self.bytes.end.div_ceil(payload_size);
        Ok(StreamRequest::new(
            frames,
            self.allocated_rate
                .frames_per_second(payload_size as usize)?,
        )?)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ByteTransferModel {
    /// Encrypted HTTP throughput, including the 16-byte tag in every frame.
    pub object_rate: ByteRate,
    pub data_ttfb: std::time::Duration,
    pub url_latency: std::time::Duration,
    pub frames_per_object: u32,
}

impl ByteTransferModel {
    fn frame_model(self, frame_size: usize) -> Result<TransferModel, BoxError> {
        Ok(TransferModel {
            object_rate: self.object_rate.frames_per_second(frame_size)?,
            data_ttfb: self.data_ttfb,
            url_latency: self.url_latency,
            frames_per_object: self.frames_per_object,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ByteStreamConfig {
    frame_size: usize,
    consumer_rate: ByteRate,
    transfer: ByteTransferModel,
}

impl ByteStreamConfig {
    pub fn new(
        frame_size: usize,
        consumer_rate: ByteRate,
        transfer: ByteTransferModel,
    ) -> Result<Self, ByteError> {
        if frame_size <= TAG_SIZE {
            return Err(ByteError::FrameTooSmall);
        }
        if transfer.frames_per_object == 0 {
            return Err(ByteError::ZeroFramesPerObject);
        }
        Ok(Self {
            frame_size,
            consumer_rate,
            transfer,
        })
    }

    pub const fn payload_size(self) -> usize {
        self.frame_size - TAG_SIZE
    }

    fn frame_config(self) -> Result<StreamConfig, BoxError> {
        StreamConfig::new(
            self.consumer_rate.frames_per_second(self.payload_size())?,
            self.transfer.frame_model(self.frame_size)?,
        )
    }
}

/// Byte-granular facade over the frame-native scheduler.
pub struct ByteStream {
    session: StreamSession,
    payload_size: usize,
    skip: usize,
    length: u64,
}

impl ByteStream {
    pub fn new(
        objects: impl Stream<Item = Result<ObjectMeta, BoxError>> + Send + 'static,
        backend: Arc<dyn EncryptedBytesDownloadBackend>,
        request: ByteRequest,
        budget: FrameBudget,
        config: ByteStreamConfig,
    ) -> Result<Self, BoxError> {
        let payload_size = config.payload_size();
        let skip = (request.bytes.start % payload_size as u64) as usize;
        let length = request.bytes.end - request.bytes.start;
        let frame_request = request.frame_request(payload_size)?;
        let backend = Arc::new(StreamDownloadBackend::new(backend, config.frame_size)?);
        let session = StreamSession::new(
            objects,
            backend,
            frame_request,
            budget,
            config.frame_config()?,
        )?;
        Ok(Self {
            session,
            payload_size,
            skip,
            length,
        })
    }

    pub fn set_consumer_rate(&mut self, rate: ByteRate) -> Result<(), BoxError> {
        self.session
            .set_consumer_rate(rate.frames_per_second(self.payload_size)?);
        Ok(())
    }

    pub fn set_transfer_model(&mut self, model: ByteTransferModel) -> Result<(), BoxError> {
        self.session
            .set_transfer_model(model.frame_model(self.payload_size + TAG_SIZE)?)
    }

    pub fn target_rate(&self) -> Result<ByteRate, BoxError> {
        Ok(ByteRate::new(
            self.session.target_rate()?.frames_per_second() * self.payload_size as f64,
        )?)
    }

    pub fn buffered_frames(&self) -> usize {
        self.session.buffered_frames()
    }

    pub fn capacity_frames(&self) -> usize {
        self.session.capacity_frames()
    }

    pub fn pipe_into<S>(self, output: S) -> impl Future<Output = Result<(), BoxError>>
    where
        S: Sink<Bytes>,
        S::Error: Error + Send + Sync + 'static,
    {
        self.session.pipe_into(ClippingSink {
            output: Box::pin(output),
            skip: self.skip,
            remaining: self.length,
        })
    }
}

struct ClippingSink<S> {
    output: Pin<Box<S>>,
    skip: usize,
    remaining: u64,
}

impl<S: Sink<Bytes>> Sink<Bytes> for ClippingSink<S> {
    type Error = S::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().output.as_mut().poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, frame: Bytes) -> Result<(), Self::Error> {
        let this = self.get_mut();
        let start = this.skip.min(frame.len());
        this.skip -= start;
        let remaining = usize::try_from(this.remaining).unwrap_or(usize::MAX);
        let end = start + remaining.min(frame.len() - start);
        this.remaining -= (end - start) as u64;
        this.output.as_mut().start_send(frame.slice(start..end))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().output.as_mut().poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().output.as_mut().poll_close(cx)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteError {
    EmptyRange,
    InvalidRate,
    FrameTooSmall,
    ZeroFramesPerObject,
}

impl fmt::Display for ByteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyRange => f.write_str("byte range must not be empty"),
            Self::InvalidRate => f.write_str("byte rate must be finite and positive"),
            Self::FrameTooSmall => f.write_str("frame size must be greater than the 16-byte tag"),
            Self::ZeroFramesPerObject => f.write_str("frames per object must be positive"),
        }
    }
}

impl Error for ByteError {}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use aes_gcm::aead::{AeadInPlace, KeyInit};
    use aes_gcm::{Aes256Gcm, Nonce};
    use futures_util::{SinkExt, sink, stream};

    use super::*;
    use crate::{DecryptKey, EncryptedByteStream, ObjectId, SignedUrl, UrlTicket};

    struct Backend {
        body: Bytes,
        range: Mutex<Option<Range<u64>>>,
    }

    impl EncryptedBytesDownloadBackend for Backend {
        fn resolve_url(&self, _object: &ObjectMeta) -> UrlTicket {
            Box::pin(async { Ok(SignedUrl::new("url", None)) })
        }

        fn download(
            &self,
            _object: &ObjectMeta,
            _url: SignedUrl,
            range: Range<u64>,
        ) -> EncryptedByteStream {
            *self.range.lock().unwrap() = Some(range.clone());
            let bytes = self.body.slice(range.start as usize..range.end as usize);
            Box::pin(stream::iter(
                bytes
                    .chunks(5)
                    .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                    .collect::<Vec<_>>(),
            ))
        }
    }

    fn encrypt(key: &DecryptKey, index: u64, payload: &[u8]) -> Vec<u8> {
        let cipher = Aes256Gcm::new(key.as_bytes().into());
        let mut ciphertext = payload.to_vec();
        let mut nonce = [0; 12];
        nonce[4..].copy_from_slice(&index.to_be_bytes());
        let tag = cipher
            .encrypt_in_place_detached(Nonce::from_slice(&nonce), b"", &mut ciphertext)
            .unwrap();
        let mut frame = Vec::from(tag.as_slice());
        frame.extend(ciphertext);
        frame
    }

    fn config() -> ByteStreamConfig {
        ByteStreamConfig::new(
            24,
            ByteRate::new(80.0).unwrap(),
            ByteTransferModel {
                object_rate: ByteRate::new(240.0).unwrap(),
                data_ttfb: Duration::ZERO,
                url_latency: Duration::ZERO,
                frames_per_object: 3,
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn maps_and_clips_an_unaligned_byte_range() {
        let key = DecryptKey::new([4; 32]);
        let mut body = Vec::new();
        body.extend(encrypt(&key, 0, b"abcdefgh"));
        body.extend(encrypt(&key, 1, b"ijklmnop"));
        body.extend(encrypt(&key, 2, b"qrstuvwx"));
        let backend = Arc::new(Backend {
            body: Bytes::from(body),
            range: Mutex::new(None),
        });
        let objects = stream::iter([Ok(ObjectMeta {
            id: ObjectId::new("object"),
            uri: "object".into(),
            frame_count: 3,
            decrypt_key: key,
        })]);
        let byte_stream = ByteStream::new(
            objects,
            backend.clone(),
            ByteRequest::new(3..19, ByteRate::new(80.0).unwrap()).unwrap(),
            FrameBudget::new(3).unwrap(),
            config(),
        )
        .unwrap();
        let output = Arc::new(Mutex::new(Vec::new()));
        let sink = sink::unfold(output.clone(), |output, bytes: Bytes| async move {
            output.lock().unwrap().extend_from_slice(&bytes);
            Ok::<_, std::io::Error>(output)
        });

        byte_stream.pipe_into(sink).await.unwrap();

        assert_eq!(&*output.lock().unwrap(), b"defghijklmnopqrs");
        assert_eq!(*backend.range.lock().unwrap(), Some(0..72));
    }

    #[test]
    fn converts_logical_and_physical_rates_to_frames() {
        let config = config();
        let frame_config = config.frame_config().unwrap();
        let request = ByteRequest::new(8..16, ByteRate::new(80.0).unwrap()).unwrap();

        assert_eq!(frame_config.consumer_rate.frames_per_second(), 10.0);
        assert_eq!(frame_config.transfer.object_rate.frames_per_second(), 10.0);
        assert_eq!(
            request
                .frame_request(config.payload_size())
                .unwrap()
                .allocated_rate()
                .frames_per_second(),
            10.0
        );
    }

    #[tokio::test]
    async fn keeps_an_aligned_payload_unchanged() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let sink = sink::unfold(output.clone(), |output, bytes: Bytes| async move {
            output.lock().unwrap().extend_from_slice(&bytes);
            Ok::<_, std::io::Error>(output)
        });
        let mut clipping = ClippingSink {
            output: Box::pin(sink),
            skip: 0,
            remaining: 8,
        };

        clipping
            .send(Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();

        assert_eq!(&*output.lock().unwrap(), b"abcdefgh");
    }
}
