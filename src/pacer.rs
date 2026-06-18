use std::error::Error;
use std::fmt;
use std::time::Duration;

use tokio::time::Instant;

use crate::FrameRate;

#[derive(Debug)]
pub struct FramePacer {
    rate: FrameRate,
    burst: u64,
    tokens: f64,
    updated_at: Instant,
}

impl FramePacer {
    pub fn new(rate: FrameRate, burst_frames: u64) -> Result<Self, PacerError> {
        if burst_frames == 0 {
            return Err(PacerError::ZeroBurst);
        }
        Ok(Self {
            rate,
            burst: burst_frames,
            tokens: burst_frames as f64,
            updated_at: Instant::now(),
        })
    }

    pub async fn wait_for(&mut self, frames: u64) {
        let mut remaining = frames;
        while remaining > 0 {
            let segment = remaining.min(self.burst);
            loop {
                self.refill();
                if self.tokens >= segment as f64 {
                    self.tokens -= segment as f64;
                    break;
                }
                let missing = segment as f64 - self.tokens;
                tokio::time::sleep(Duration::from_secs_f64(
                    missing / self.rate.frames_per_second(),
                ))
                .await;
            }
            remaining -= segment;
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        self.tokens = (self.tokens
            + now.duration_since(self.updated_at).as_secs_f64() * self.rate.frames_per_second())
        .min(self.burst as f64);
        self.updated_at = now;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacerError {
    ZeroBurst,
}

impl fmt::Display for PacerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("frame burst must be positive")
    }
}

impl Error for PacerError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn paces_more_frames_than_one_burst() {
        let mut pacer = FramePacer::new(FrameRate::new(100.0).unwrap(), 100).unwrap();
        let started = Instant::now();
        pacer.wait_for(150).await;
        assert_eq!(Instant::now() - started, Duration::from_millis(500));
    }
}
