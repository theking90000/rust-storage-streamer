use std::error::Error;
use std::fmt;
use std::time::Duration;

use crate::FrameRate;

#[derive(Clone, Copy, Debug)]
pub struct TransferModel {
    pub object_rate: FrameRate,
    pub data_ttfb: Duration,
    pub url_latency: Duration,
    pub frames_per_object: u32,
}

impl TransferModel {
    pub fn window_for(self, target: FrameRate) -> Result<WindowSizing, WindowSizingError> {
        if self.frames_per_object == 0 {
            return Err(WindowSizingError::ZeroFramesPerObject);
        }
        let rate = target.frames_per_second();
        let object = f64::from(self.frames_per_object);
        let ttfb = self.data_ttfb.as_secs_f64();
        let first_frame = rate * ttfb;
        let complete_object =
            rate * (ttfb + object / self.object_rate.frames_per_second()) - object;

        Ok(WindowSizing {
            ready_data_frames: frames(first_frame.max(complete_object).max(1.0))?,
            url_prefetch_frames: frames(rate * self.url_latency.as_secs_f64())?,
        })
    }

    pub fn max_rate_for(self, capacity_frames: usize) -> Result<FrameRate, WindowSizingError> {
        if capacity_frames == 0 {
            return Err(WindowSizingError::ZeroCapacity);
        }
        if self.frames_per_object == 0 {
            return Err(WindowSizingError::ZeroFramesPerObject);
        }
        let capacity = capacity_frames as f64;
        let object = f64::from(self.frames_per_object);
        let ttfb = self.data_ttfb.as_secs_f64();
        let first_frame_limit = if ttfb == 0.0 {
            f64::INFINITY
        } else {
            capacity / ttfb
        };
        let completion_limit =
            (capacity + object) / (ttfb + object / self.object_rate.frames_per_second());

        FrameRate::new(first_frame_limit.min(completion_limit))
            .map_err(|_| WindowSizingError::InvalidRate)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowSizing {
    pub ready_data_frames: usize,
    pub url_prefetch_frames: usize,
}

fn frames(value: f64) -> Result<usize, WindowSizingError> {
    let value = value.ceil();
    if !value.is_finite() || value > usize::MAX as f64 {
        return Err(WindowSizingError::FrameCountOverflow);
    }
    Ok(value as usize)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowSizingError {
    ZeroCapacity,
    ZeroFramesPerObject,
    FrameCountOverflow,
    InvalidRate,
}

impl fmt::Display for WindowSizingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => f.write_str("frame capacity must be positive"),
            Self::ZeroFramesPerObject => f.write_str("frames per object must be positive"),
            Self::FrameCountOverflow => f.write_str("calculated frame count overflows usize"),
            Self::InvalidRate => f.write_str("calculated frame rate is invalid"),
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
    fn sizes_and_inverts_the_reference_window() {
        let model = TransferModel {
            object_rate: rate(20_000_000.0 / 65_520.0),
            data_ttfb: Duration::from_millis(300),
            url_latency: Duration::from_secs(1),
            frames_per_object: 150,
        };
        let target = rate(50_000_000.0 / 65_520.0);
        let sizing = model.window_for(target).unwrap();

        assert_eq!(sizing.ready_data_frames, 454);
        assert_eq!(sizing.url_prefetch_frames, 764);
        assert!(model.max_rate_for(454).unwrap() >= target);
    }
}
