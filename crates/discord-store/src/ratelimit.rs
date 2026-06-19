use std::sync::Mutex;
use std::time::Duration;

use dashmap::DashMap;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use tokio::time::Instant;

/// Fallback reset window used before the server tells us the real one.
const DEFAULT_WINDOW: Duration = Duration::from_secs(1);

/// Per-key (per webhook id) token-bucket state. Refilled lazily — never with a
/// background timer — so the limiter scales to 10k keys without a timer farm.
struct KeyState {
    limit: u32,
    remaining: u32,
    /// When the current window resets and `remaining` returns to `limit`.
    reset_at: Instant,
    /// Earliest instant the next request may be dispatched (smooths bursts).
    next_allowed_at: Instant,
    /// Minimum spacing between two requests = window / limit.
    spacing: Duration,
}

impl KeyState {
    fn new(now: Instant) -> Self {
        // Start optimistic: the real limit/remaining are unknown until the first
        // response, so a tiny initial guess must not pin `remaining` low under
        // the conservative `min` reconciliation rule.
        Self {
            limit: u32::MAX,
            remaining: u32::MAX,
            reset_at: now,
            next_allowed_at: now,
            spacing: Duration::ZERO,
        }
    }
}

/// Rate limiter keyed by webhook id, shared by upload and download. Upload picks
/// an interchangeable webhook then calls [`acquire`]; download has its key
/// imposed by the object URI and calls [`acquire`] directly.
pub(crate) struct KeyedRateLimiter {
    keys: DashMap<String, Mutex<KeyState>>,
}

impl KeyedRateLimiter {
    pub fn new() -> Self {
        Self {
            keys: DashMap::new(),
        }
    }

    /// Waits until one token is available for `id`, then consumes it. The bucket
    /// lock is never held across the `await`.
    pub async fn acquire(&self, id: &str) {
        loop {
            let wait = {
                let cell = self
                    .keys
                    .entry(id.to_owned())
                    .or_insert_with(|| Mutex::new(KeyState::new(Instant::now())));
                let mut state = cell.lock().unwrap();
                let now = Instant::now();
                if now >= state.reset_at {
                    state.remaining = state.limit;
                    state.reset_at = now + DEFAULT_WINDOW;
                }
                let mut wait = state.next_allowed_at.saturating_duration_since(now);
                if state.remaining == 0 {
                    wait = wait.max(state.reset_at.saturating_duration_since(now));
                }
                if wait.is_zero() {
                    state.remaining = state.remaining.saturating_sub(1);
                    state.next_allowed_at = now + state.spacing;
                    None
                } else {
                    Some(wait)
                }
            };
            match wait {
                None => return,
                Some(wait) => tokio::time::sleep(wait).await,
            }
        }
    }

    /// Reconciles bucket state with the server's `X-RateLimit-*` headers after a
    /// response. Conservative: `remaining` only ever shrinks within a window so
    /// concurrently-reserved tokens are never handed back.
    pub fn update_from_headers(&self, id: &str, headers: &HeaderMap, status: StatusCode) {
        let cell = self
            .keys
            .entry(id.to_owned())
            .or_insert_with(|| Mutex::new(KeyState::new(Instant::now())));
        let mut state = cell.lock().unwrap();
        let now = Instant::now();

        // Prefer Reset-After (relative, clock-skew free) over Retry-After.
        let reset_after = header_f64(headers, "x-ratelimit-reset-after")
            .or_else(|| header_f64(headers, "retry-after"));
        if let Some(secs) = reset_after {
            let window = Duration::from_secs_f64(secs.max(0.0));
            state.reset_at = now + window;
            if let Some(limit) = header_u32(headers, "x-ratelimit-limit") {
                state.limit = limit.max(1);
                state.spacing = window / state.limit;
            }
        }

        if status == StatusCode::TOO_MANY_REQUESTS {
            state.remaining = 0;
        } else if let Some(remaining) = header_u32(headers, "x-ratelimit-remaining") {
            state.remaining = state.remaining.min(remaining);
        }
    }
}

fn header_f64(headers: &HeaderMap, name: &str) -> Option<f64> {
    headers.get(name)?.to_str().ok()?.parse().ok()
}

fn header_u32(headers: &HeaderMap, name: &str) -> Option<u32> {
    headers.get(name)?.to_str().ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use reqwest::header::HeaderName;

    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            let name: HeaderName = name.parse().unwrap();
            map.insert(name, value.parse().unwrap());
        }
        map
    }

    #[tokio::test(start_paused = true)]
    async fn spaces_requests_after_learning_limit() {
        let limiter = KeyedRateLimiter::new();
        // 5 requests / 1s window -> 200ms spacing.
        limiter.acquire("hook").await;
        limiter.update_from_headers(
            "hook",
            &headers(&[
                ("x-ratelimit-limit", "5"),
                ("x-ratelimit-remaining", "4"),
                ("x-ratelimit-reset-after", "1"),
            ]),
            StatusCode::OK,
        );
        // The first request after learning the spacing is not itself delayed; the
        // smoothing applies to the gap before the *next* one.
        limiter.acquire("hook").await;
        let start = Instant::now();
        limiter.acquire("hook").await;
        assert_eq!(start.elapsed(), Duration::from_millis(200));
    }

    #[tokio::test(start_paused = true)]
    async fn blocks_until_reset_when_remaining_is_zero() {
        let limiter = KeyedRateLimiter::new();
        limiter.acquire("hook").await;
        limiter.update_from_headers(
            "hook",
            &headers(&[
                ("x-ratelimit-limit", "5"),
                ("x-ratelimit-remaining", "0"),
                ("x-ratelimit-reset-after", "2"),
            ]),
            StatusCode::OK,
        );
        let start = Instant::now();
        limiter.acquire("hook").await;
        assert_eq!(start.elapsed(), Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn a_429_forces_a_block_until_retry_after() {
        let limiter = KeyedRateLimiter::new();
        limiter.acquire("hook").await;
        limiter.update_from_headers(
            "hook",
            &headers(&[("retry-after", "3")]),
            StatusCode::TOO_MANY_REQUESTS,
        );
        let start = Instant::now();
        limiter.acquire("hook").await;
        assert_eq!(start.elapsed(), Duration::from_secs(3));
    }

    #[tokio::test(start_paused = true)]
    async fn out_of_order_responses_never_over_credit() {
        let limiter = KeyedRateLimiter::new();
        limiter.acquire("hook").await;
        // A stale response claiming many tokens left must not raise `remaining`
        // above what we already know locally (it was just consumed to 0-ish).
        limiter.update_from_headers(
            "hook",
            &headers(&[
                ("x-ratelimit-limit", "5"),
                ("x-ratelimit-remaining", "0"),
                ("x-ratelimit-reset-after", "5"),
            ]),
            StatusCode::OK,
        );
        limiter.update_from_headers(
            "hook",
            &headers(&[
                ("x-ratelimit-limit", "5"),
                ("x-ratelimit-remaining", "4"),
                ("x-ratelimit-reset-after", "5"),
            ]),
            StatusCode::OK,
        );
        // remaining stayed at 0 -> next acquire waits for the reset window.
        let start = Instant::now();
        limiter.acquire("hook").await;
        assert_eq!(start.elapsed(), Duration::from_secs(5));
    }
}
