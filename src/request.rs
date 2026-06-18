use std::error::Error;
use std::fmt;
use std::ops::Range;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct FrameRate(f64);

impl FrameRate {
    pub fn new(frames_per_second: f64) -> Result<Self, RequestError> {
        if !frames_per_second.is_finite() || frames_per_second <= 0.0 {
            return Err(RequestError::InvalidFrameRate);
        }
        Ok(Self(frames_per_second))
    }

    pub const fn frames_per_second(self) -> f64 {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    pub id: ObjectId,
    pub uri: String,
    pub frame_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamRequest {
    frames: Range<u64>,
}

impl StreamRequest {
    pub fn new(frames: Range<u64>) -> Result<Self, RequestError> {
        if frames.start >= frames.end {
            return Err(RequestError::EmptyFrameRange);
        }
        Ok(Self { frames })
    }

    pub fn frames(&self) -> Range<u64> {
        self.frames.clone()
    }

    pub fn frame_count(&self) -> u64 {
        self.frames.end - self.frames.start
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestError {
    EmptyFrameRange,
    InvalidFrameRate,
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyFrameRange => f.write_str("frame range must not be empty"),
            Self::InvalidFrameRate => f.write_str("frame rate must be finite and positive"),
        }
    }
}

impl Error for RequestError {}
