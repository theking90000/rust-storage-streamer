use std::error::Error;
use std::fmt;
use std::time::Duration;

use crate::FrameRate;

#[derive(Clone, Copy, Debug)]
pub struct WindowSizingInput {
    pub target_rate: FrameRate,
    pub object_download_rate: FrameRate,
    pub data_ttfb: Duration,
    pub url_fetch_latency: Duration,
    pub frames_per_object: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowSizing {
    pub ready_data_frames: u64,
    pub url_prefetch_frames: u64,
}

impl WindowSizing {
    pub fn calculate(input: WindowSizingInput) -> Result<Self, WindowSizingError> {
        if input.frames_per_object == 0 {
            return Err(WindowSizingError::ZeroFramesPerObject);
        }
        let target = input.target_rate.frames_per_second();
        let download = input.object_download_rate.frames_per_second();
        let object = f64::from(input.frames_per_object);
        let ttfb = input.data_ttfb.as_secs_f64();

        let first_frame = target * ttfb;
        let complete_object = target * (ttfb + object / download) - object;

        Ok(Self {
            ready_data_frames: frames(first_frame.max(complete_object).max(1.0))?,
            url_prefetch_frames: frames(target * input.url_fetch_latency.as_secs_f64())?,
        })
    }
}

fn frames(value: f64) -> Result<u64, WindowSizingError> {
    let value = value.ceil();
    if !value.is_finite() || value > u64::MAX as f64 {
        return Err(WindowSizingError::FrameCountOverflow);
    }
    Ok(value as u64)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowSizingError {
    ZeroFramesPerObject,
    FrameCountOverflow,
}

impl fmt::Display for WindowSizingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroFramesPerObject => f.write_str("frames per object must be positive"),
            Self::FrameCountOverflow => f.write_str("calculated frame count overflows u64"),
        }
    }
}

impl Error for WindowSizingError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn rate(frames_per_second: f64) -> FrameRate {
        FrameRate::new(frames_per_second).unwrap()
    }

    #[test]
    fn sizes_the_reference_window_without_knowing_frame_bytes() {
        let sizing = WindowSizing::calculate(WindowSizingInput {
            target_rate: rate(50_000_000.0 / 65_520.0),
            object_download_rate: rate(20_000_000.0 / 65_520.0),
            data_ttfb: Duration::from_millis(300),
            url_fetch_latency: Duration::from_secs(1),
            frames_per_object: 150,
        })
        .unwrap();

        assert_eq!(sizing.ready_data_frames, 454);
        assert_eq!(sizing.url_prefetch_frames, 764);
    }
}
