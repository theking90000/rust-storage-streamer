use std::error::Error;
use std::fmt;
use std::time::Duration;

use crate::ByteRate;

/// Inputs used by the policy layer to turn time and throughput constraints
/// into frame counts. The window controller itself only consumes those counts.
#[derive(Clone, Copy, Debug)]
pub struct WindowSizingInput {
    pub target_rate: ByteRate,
    pub object_download_rate: ByteRate,
    pub data_ttfb: Duration,
    pub url_fetch_latency: Duration,
    pub frame_payload_size: u64,
    pub frames_per_object: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowSizing {
    /// Minimum theoretical Ready + Data capacity for continuous delivery.
    pub ready_data_frames: u64,
    /// Additional logical look-ahead used to start URL resolution.
    pub url_prefetch_frames: u64,
}

impl WindowSizing {
    pub fn calculate(input: WindowSizingInput) -> Result<Self, WindowSizingError> {
        if input.frame_payload_size == 0 {
            return Err(WindowSizingError::ZeroFramePayloadSize);
        }
        if input.frames_per_object == 0 {
            return Err(WindowSizingError::ZeroFramesPerObject);
        }

        let frame_payload = input.frame_payload_size as f64;
        let target_rate = input.target_rate.bytes_per_second() as f64;
        let download_rate = input.object_download_rate.bytes_per_second() as f64;
        let object_size = frame_payload * f64::from(input.frames_per_object);
        let ttfb = input.data_ttfb.as_secs_f64();

        // The first constraint makes the first byte available before output
        // reaches the object. The second lets the tail keep downloading while
        // the object's prefix is already being delivered.
        let first_byte_bytes = target_rate * ttfb;
        let completion_bytes = target_rate * (ttfb + object_size / download_rate) - object_size;
        let ready_data_bytes = first_byte_bytes.max(completion_bytes).max(frame_payload);

        let ready_data_frames = ceil_frames(ready_data_bytes, frame_payload)?;
        let url_prefetch_frames = ceil_frames(
            target_rate * input.url_fetch_latency.as_secs_f64(),
            frame_payload,
        )?;

        Ok(Self {
            ready_data_frames,
            url_prefetch_frames,
        })
    }
}

fn ceil_frames(bytes: f64, frame_payload: f64) -> Result<u64, WindowSizingError> {
    let frames = (bytes / frame_payload).ceil();
    if !frames.is_finite() || frames > u64::MAX as f64 {
        return Err(WindowSizingError::FrameCountOverflow);
    }
    Ok(frames as u64)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowSizingError {
    ZeroFramePayloadSize,
    ZeroFramesPerObject,
    FrameCountOverflow,
}

impl fmt::Display for WindowSizingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroFramePayloadSize => f.write_str("frame payload size must be positive"),
            Self::ZeroFramesPerObject => f.write_str("frames per object must be positive"),
            Self::FrameCountOverflow => f.write_str("calculated frame count overflows u64"),
        }
    }
}

impl Error for WindowSizingError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn rate(bytes_per_second: u64) -> ByteRate {
        ByteRate::new(bytes_per_second).unwrap()
    }

    #[test]
    fn reproduces_the_reference_50_mb_per_second_window() {
        let sizing = WindowSizing::calculate(WindowSizingInput {
            target_rate: rate(50_000_000),
            object_download_rate: rate(20_000_000),
            data_ttfb: Duration::from_millis(300),
            url_fetch_latency: Duration::from_secs(1),
            frame_payload_size: 65_520,
            frames_per_object: 150,
        })
        .unwrap();

        assert_eq!(sizing.ready_data_frames, 454);
        assert_eq!(sizing.url_prefetch_frames, 764);
    }

    #[test]
    fn supports_the_conservative_download_assumptions() {
        let sizing = WindowSizing::calculate(WindowSizingInput {
            target_rate: rate(50_000_000),
            object_download_rate: rate(10_000_000),
            data_ttfb: Duration::from_secs(1),
            url_fetch_latency: Duration::from_secs(1),
            frame_payload_size: 65_520,
            frames_per_object: 150,
        })
        .unwrap();

        assert_eq!(sizing.ready_data_frames, 1_364);
        assert_eq!(sizing.url_prefetch_frames, 764);
    }

    #[test]
    fn always_keeps_at_least_one_frame() {
        let sizing = WindowSizing::calculate(WindowSizingInput {
            target_rate: rate(1),
            object_download_rate: rate(1_000_000),
            data_ttfb: Duration::ZERO,
            url_fetch_latency: Duration::ZERO,
            frame_payload_size: 65_520,
            frames_per_object: 150,
        })
        .unwrap();

        assert_eq!(sizing.ready_data_frames, 1);
        assert_eq!(sizing.url_prefetch_frames, 0);
    }
}
