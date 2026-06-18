use std::error::Error;
use std::fmt;

/// Size of the authentication tag appended to every encrypted frame.
pub const GCM_TAG_SIZE: u32 = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteRate(u64);

impl ByteRate {
    pub fn new(bytes_per_second: u64) -> Result<Self, StreamRequestError> {
        if bytes_per_second == 0 {
            return Err(StreamRequestError::ZeroMaxRate);
        }
        Ok(Self(bytes_per_second))
    }

    pub const fn bytes_per_second(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectId(String);

impl ObjectId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Metadata required by the pure read planner.
///
/// More transport and cryptographic metadata can be attached later without
/// changing the range calculations. `frame_count` is explicit because the
/// final object may be shorter than the nominal object size.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    pub id: ObjectId,
    pub uri: String,
    pub frame_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamRequest {
    offset: u64,
    size: u64,
    objects: Vec<ObjectMeta>,
    frame_size: u32,
    frames_per_object: u32,
    max_rate: ByteRate,
}

impl StreamRequest {
    pub fn new(
        offset: Option<u64>,
        size: u64,
        objects: Vec<ObjectMeta>,
        frame_size: Option<u32>,
        frames_per_object: u32,
        max_rate: ByteRate,
    ) -> Result<Self, StreamRequestError> {
        if size == 0 {
            return Err(StreamRequestError::ZeroSize);
        }
        if objects.is_empty() {
            return Err(StreamRequestError::NoObjects);
        }

        let frame_size = frame_size.unwrap_or(1 << 16);
        if frame_size <= GCM_TAG_SIZE {
            return Err(StreamRequestError::FrameTooSmall { frame_size });
        }
        if frames_per_object == 0 {
            return Err(StreamRequestError::ZeroFramesPerObject);
        }

        for (index, object) in objects.iter().enumerate() {
            if object.frame_count == 0 || object.frame_count > frames_per_object {
                return Err(StreamRequestError::InvalidObjectFrameCount {
                    object_index: index,
                    frame_count: object.frame_count,
                    maximum: frames_per_object,
                });
            }
            if index + 1 != objects.len() && object.frame_count != frames_per_object {
                return Err(StreamRequestError::ShortNonFinalObject {
                    object_index: index,
                    frame_count: object.frame_count,
                    expected: frames_per_object,
                });
            }
        }

        let request = Self {
            offset: offset.unwrap_or(0),
            size,
            objects,
            frame_size,
            frames_per_object,
            max_rate,
        };

        request
            .logical_len()
            .ok_or(StreamRequestError::LogicalLengthOverflow)?;
        request
            .offset
            .checked_add(request.size)
            .ok_or(StreamRequestError::RequestedRangeOverflow)?;

        Ok(request)
    }

    pub const fn frame_payload_size(&self) -> u64 {
        self.frame_size as u64 - GCM_TAG_SIZE as u64
    }

    pub const fn offset(&self) -> u64 {
        self.offset
    }

    pub const fn size(&self) -> u64 {
        self.size
    }

    pub fn objects(&self) -> &[ObjectMeta] {
        &self.objects
    }

    pub const fn frame_size(&self) -> u32 {
        self.frame_size
    }

    pub const fn frames_per_object(&self) -> u32 {
        self.frames_per_object
    }

    pub const fn max_rate(&self) -> ByteRate {
        self.max_rate
    }

    pub fn logical_len(&self) -> Option<u64> {
        let frames = self.objects.iter().try_fold(0_u64, |total, object| {
            total.checked_add(object.frame_count as u64)
        })?;
        frames.checked_mul(self.frame_payload_size())
    }

    pub fn requested_end(&self) -> u64 {
        // Validated by the constructor.
        self.offset + self.size
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamRequestError {
    ZeroSize,
    NoObjects,
    FrameTooSmall {
        frame_size: u32,
    },
    ZeroFramesPerObject,
    ZeroMaxRate,
    InvalidObjectFrameCount {
        object_index: usize,
        frame_count: u32,
        maximum: u32,
    },
    ShortNonFinalObject {
        object_index: usize,
        frame_count: u32,
        expected: u32,
    },
    LogicalLengthOverflow,
    RequestedRangeOverflow,
}

impl fmt::Display for StreamRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroSize => f.write_str("size must be greater than zero"),
            Self::NoObjects => f.write_str("objects must not be empty"),
            Self::FrameTooSmall { frame_size } => write!(
                f,
                "frame size {frame_size} must be greater than the {GCM_TAG_SIZE}-byte tag"
            ),
            Self::ZeroFramesPerObject => f.write_str("frames per object must be greater than zero"),
            Self::ZeroMaxRate => f.write_str("maximum rate must be greater than zero"),
            Self::InvalidObjectFrameCount {
                object_index,
                frame_count,
                maximum,
            } => write!(
                f,
                "object {object_index} has {frame_count} frames; expected 1..={maximum}"
            ),
            Self::ShortNonFinalObject {
                object_index,
                frame_count,
                expected,
            } => write!(
                f,
                "non-final object {object_index} has {frame_count} frames; expected {expected}"
            ),
            Self::LogicalLengthOverflow => f.write_str("logical stream length overflows u64"),
            Self::RequestedRangeOverflow => f.write_str("requested byte range overflows u64"),
        }
    }
}

impl Error for StreamRequestError {}
