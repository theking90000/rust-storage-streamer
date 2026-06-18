use std::error::Error;
use std::fmt;
use std::time::Duration;

use tokio::time::Instant;

use crate::ByteRate;

/// Per-stream token bucket used immediately before yielding plaintext bytes.
#[derive(Debug)]
pub struct OutputPacer {
    max_rate: ByteRate,
    burst_bytes: usize,
    available_tokens: f64,
    last_update: Instant,
}

impl OutputPacer {
    pub fn new(max_rate: ByteRate, burst_bytes: usize) -> Result<Self, PacerError> {
        if burst_bytes == 0 {
            return Err(PacerError::ZeroBurst);
        }

        Ok(Self {
            max_rate,
            burst_bytes,
            available_tokens: burst_bytes as f64,
            last_update: Instant::now(),
        })
    }

    /// Waits until `bytes` may be emitted and returns the time spent sleeping.
    ///
    /// Requests larger than the bucket capacity are charged in burst-sized
    /// segments. This is important: waiting for the whole request at once
    /// would never complete because the bucket is capped at `burst_bytes`.
    pub async fn wait_for(&mut self, bytes: usize) -> Duration {
        let mut remaining = bytes;
        let mut slept = Duration::ZERO;

        while remaining > 0 {
            let segment = remaining.min(self.burst_bytes);
            loop {
                self.refill();
                if self.available_tokens >= segment as f64 {
                    self.available_tokens -= segment as f64;
                    break;
                }

                let missing = segment as f64 - self.available_tokens;
                let delay =
                    Duration::from_secs_f64(missing / self.max_rate.bytes_per_second() as f64);
                tokio::time::sleep(delay).await;
                slept += delay;
            }
            remaining -= segment;
        }

        slept
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        self.available_tokens = (self.available_tokens
            + elapsed * self.max_rate.bytes_per_second() as f64)
            .min(self.burst_bytes as f64);
        self.last_update = now;
    }

    pub const fn burst_bytes(&self) -> usize {
        self.burst_bytes
    }

    pub const fn max_rate(&self) -> ByteRate {
        self.max_rate
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PacerError {
    ZeroBurst,
}

impl fmt::Display for PacerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroBurst => f.write_str("pacer burst must be greater than zero"),
        }
    }
}

impl Error for PacerError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn allows_the_initial_burst_without_sleeping() {
        let rate = ByteRate::new(100).unwrap();
        let mut pacer = OutputPacer::new(rate, 100).unwrap();

        assert_eq!(pacer.wait_for(100).await, Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn paces_a_write_larger_than_the_bucket_in_segments() {
        let rate = ByteRate::new(100).unwrap();
        let mut pacer = OutputPacer::new(rate, 100).unwrap();
        let started = Instant::now();

        let slept = pacer.wait_for(150).await;

        assert_eq!(slept, Duration::from_millis(500));
        assert_eq!(Instant::now() - started, Duration::from_millis(500));
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_write_does_not_consume_tokens() {
        let rate = ByteRate::new(100).unwrap();
        let mut pacer = OutputPacer::new(rate, 100).unwrap();

        assert_eq!(pacer.wait_for(0).await, Duration::ZERO);
        assert_eq!(pacer.wait_for(100).await, Duration::ZERO);
    }
}
