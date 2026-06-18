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

    pub fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
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

#[derive(Clone, PartialEq, Eq)]
pub struct DecryptKey([u8; 32]);

impl DecryptKey {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for DecryptKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DecryptKey([REDACTED])")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    pub id: ObjectId,
    pub uri: String,
    pub frame_count: u32,
    /// This key must not be reused for another object.
    pub decrypt_key: DecryptKey,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StreamRequest {
    frames: Range<u64>,
    allocated_rate: FrameRate,
}

impl StreamRequest {
    pub fn new(frames: Range<u64>, allocated_rate: FrameRate) -> Result<Self, RequestError> {
        if frames.start >= frames.end {
            return Err(RequestError::EmptyFrameRange);
        }
        Ok(Self {
            frames,
            allocated_rate,
        })
    }

    pub fn frames(&self) -> Range<u64> {
        self.frames.clone()
    }

    pub fn frame_count(&self) -> u64 {
        self.frames.end - self.frames.start
    }

    pub const fn allocated_rate(&self) -> FrameRate {
        self.allocated_rate
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_the_allocated_rate() {
        let request = StreamRequest::new(0..10, FrameRate::new(763.0).unwrap()).unwrap();
        assert_eq!(request.allocated_rate().frames_per_second(), 763.0);
    }

    #[test]
    fn redacts_decryption_keys() {
        assert_eq!(
            format!("{:?}", DecryptKey::new([7; 32])),
            "DecryptKey([REDACTED])"
        );
    }
}
