//! Pure quota-scheduling core.
//!
//! No async, no I/O, no internal clock: every method takes `now` as a
//! parameter. This is what makes the whole thing unit-testable with plain
//! `#[test]` and trivially reusable across components -- the async runner that
//! sleeps until `send_at` and feeds back `reconcile` lives elsewhere.
//!
//! Model:
//!   - a `Bucket` is one GCRA-governed quota dimension (token bucket C, rho),
//!   - a `Resource` carries one or more buckets in conjunction (e.g. a webhook
//!     lane = the webhook bucket AND the channel bucket),
//!   - the engine holds `shared` buckets (always touched, e.g. global 50/s) and
//!     a list of selection `Pool`s (lanes, egress). A `Demand` pins each pool to
//!     Fixed(key) or Free; Free pools are resolved independently by "most rested"
//!     argmin and composed by `max`. That independence is exact as long as the
//!     axes are not coupled by affinity.

use std::time::{Duration, Instant};

/// Signed nanoseconds `a - b`, underflow-proof (Instant has no negative).
fn signed_nanos(a: Instant, b: Instant) -> i128 {
    if a >= b {
        a.duration_since(b).as_nanos() as i128
    } else {
        -(b.duration_since(a).as_nanos() as i128)
    }
}

/// One GCRA bucket with the §3.2 split: an *allocation* horizon `rtat` advanced
/// when a reservation is admitted, and a *commit* horizon `tat` advanced when a
/// commit is actually authorized (the only one the server enforces). Invariant:
/// `rtat >= tat` always (you cannot have committed past what you reserved).
#[derive(Clone, Debug)]
pub struct Bucket {
    pub label: &'static str,
    interval: Duration,  // T
    tolerance: Duration, // tau
    tat: Instant,        // commit horizon (authoritative spends)
    rtat: Instant,       // reservation / allocation horizon (admitted, maybe not committed)
}

impl Bucket {
    /// `safety` reserves headroom (effective capacity C - safety) to absorb the
    /// jitter in *when* the server actually consumes the token. This is the only
    /// knob the jitter corrector touches later.
    pub fn new(label: &'static str, capacity: u32, refill_per_sec: f64, safety: u32, now: Instant) -> Self {
        assert!(refill_per_sec > 0.0, "refill must be > 0");
        let interval = Duration::from_secs_f64(1.0 / refill_per_sec);
        let c_eff = capacity.saturating_sub(safety).max(1);
        let tolerance = interval.mul_f64((c_eff - 1) as f64);
        Self { label, interval, tolerance, tat: now, rtat: now }
    }

    fn slack_of(&self, horizon: Instant, now: Instant) -> i128 {
        signed_nanos(horizon, now) - self.tolerance.as_nanos() as i128
    }

    /// Allocation slack: used for admission AND for least-used selection, so a
    /// resource with many in-flight reservations stops being "most rested".
    fn reserve_slack(&self, now: Instant) -> i128 {
        self.slack_of(self.rtat, now)
    }

    /// Commit slack: the authoritative gate. Negative => may commit now.
    fn commit_slack(&self, now: Instant) -> i128 {
        self.slack_of(self.tat, now)
    }

    /// Admit a reservation at `t`: advance only the allocation horizon.
    fn advance_reserve(&mut self, t: Instant) {
        self.rtat = self.rtat.max(t) + self.interval;
    }

    /// Roll back one admitted-but-cancelled reservation (best-effort, on the
    /// client-side allocation horizon only -- never touches the committed `tat`).
    fn release_reserve(&mut self) {
        self.rtat = (self.rtat.checked_sub(self.interval)).unwrap_or(self.rtat).max(self.tat);
    }

    /// Authorize a commit at `t`: advance the real rate horizon, and keep the
    /// allocation horizon ahead of it.
    fn advance_commit(&mut self, t: Instant) {
        self.tat = self.tat.max(t) + self.interval;
        self.rtat = self.rtat.max(self.tat);
    }

    /// Re-anchor the commit horizon from an observed header, dead-reckoned to
    /// `now`. Tighten-only: a stale read may never *relax* a commitment already
    /// made, so we only push `tat` forward (and `rtat` with it).
    fn reconcile(&mut self, remaining: u32, capacity: u32, now: Instant) {
        let c_eff = capacity.max(1);
        let debt = c_eff.saturating_sub(remaining); // tokens in use
        let anchored = now + self.interval.mul_f64(debt as f64);
        self.tat = self.tat.max(anchored);
        self.rtat = self.rtat.max(self.tat);
    }
}

/// A selectable resource carrying its private buckets in conjunction.
#[derive(Clone, Debug)]
pub struct Resource<K> {
    pub key: K,
    pub alive: bool,
    buckets: Vec<Bucket>,
}

impl<K> Resource<K> {
    pub fn new(key: K, buckets: Vec<Bucket>) -> Self {
        Self { key, alive: true, buckets }
    }
    /// Conjunction => gated by the most-constraining bucket, on either horizon.
    fn reserve_slack(&self, now: Instant) -> i128 {
        self.buckets.iter().map(|b| b.reserve_slack(now)).max().unwrap_or(i128::MIN)
    }
    fn commit_slack(&self, now: Instant) -> i128 {
        self.buckets.iter().map(|b| b.commit_slack(now)).max().unwrap_or(i128::MIN)
    }
    fn advance_reserve(&mut self, t: Instant) {
        for b in &mut self.buckets {
            b.advance_reserve(t);
        }
    }
    fn advance_commit(&mut self, t: Instant) {
        for b in &mut self.buckets {
            b.advance_commit(t);
        }
    }
    fn release_reserve(&mut self) {
        for b in &mut self.buckets {
            b.release_reserve();
        }
    }
}

/// A selection axis: a family of equivalent resources.
#[derive(Clone, Debug)]
pub struct Pool<K> {
    pub resources: Vec<Resource<K>>,
}

impl<K: PartialEq> Pool<K> {
    pub fn new(resources: Vec<Resource<K>>) -> Self {
        Self { resources }
    }
    /// Resolve a pin to a resource index. Free => most-rested alive resource;
    /// ties broken by vec order (deterministic cold-start). None => no live
    /// resource / unknown fixed key.
    fn resolve(&self, pin: &Pin<K>, now: Instant) -> Option<usize> {
        match pin {
            Pin::Fixed(k) => self.resources.iter().position(|r| r.alive && &r.key == k),
            Pin::Free => self
                .resources
                .iter()
                .enumerate()
                .filter(|(_, r)| r.alive)
                .min_by_key(|(_, r)| r.reserve_slack(now))
                .map(|(i, _)| i),
        }
    }
}

/// Per-pool demand: leave the choice to the engine, or pin it.
#[derive(Clone, Debug)]
pub enum Pin<K> {
    Fixed(K),
    Free,
}

/// What the caller gets back: when to send the last byte, and what was chosen.
#[derive(Clone, Debug)]
pub struct Allocation<K> {
    pub send_at: Instant,
    pub picks: Vec<K>, // aligned to pool order
}

/// The engine. Owned single-threaded (one task); pure and synchronous.
pub struct QuotaEngine<K> {
    shared: Vec<Bucket>,
    pools: Vec<Pool<K>>,
}

impl<K: PartialEq + Clone> QuotaEngine<K> {
    pub fn new(shared: Vec<Bucket>, pools: Vec<Pool<K>>) -> Self {
        Self { shared, pools }
    }

    /// Phase 1 -- ADMISSION peek, on the ALLOCATION horizon. Selects the
    /// least-loaded resources (reserve_slack) and returns the earliest instant a
    /// reservation could be admitted, WITHOUT mutating. The caller checks it
    /// against its deadline; refusal spends nothing (monotone backpressure).
    pub fn peek_reserve(&self, demand: &[Pin<K>], now: Instant) -> Option<(Vec<usize>, Instant)> {
        assert_eq!(demand.len(), self.pools.len(), "one pin per pool");
        let mut overall = self.shared.iter().map(|b| b.reserve_slack(now)).max().unwrap_or(i128::MIN);
        let mut idxs = Vec::with_capacity(self.pools.len());
        for (pool, pin) in self.pools.iter().zip(demand) {
            let i = pool.resolve(pin, now)?;
            overall = overall.max(pool.resources[i].reserve_slack(now));
            idxs.push(i);
        }
        let reserve_at = now + Duration::from_nanos(overall.max(0) as u64);
        Some((idxs, reserve_at))
    }

    /// Admit the peeked reservation: advance the allocation horizon only. Holds
    /// the slot so concurrent admissions see the queue depth (§3.2 allocation).
    /// Must run under the same lock as its `peek_reserve`.
    pub fn admit(&mut self, idxs: &[usize], at: Instant) -> Vec<K> {
        for b in &mut self.shared {
            b.advance_reserve(at);
        }
        let mut picks = Vec::with_capacity(idxs.len());
        for (pool, &i) in self.pools.iter_mut().zip(idxs) {
            pool.resources[i].advance_reserve(at);
            picks.push(pool.resources[i].key.clone());
        }
        picks
    }

    /// THE COMMIT GATE. Earliest instant the held slot may actually commit, read
    /// from the live COMMIT horizon (tat) -- re-evaluated on every call, so a
    /// `reconcile` that arrived since admission is reflected here. Non-mutating.
    pub fn earliest_commit(&self, idxs: &[usize], now: Instant) -> Instant {
        let mut overall = self.shared.iter().map(|b| b.commit_slack(now)).max().unwrap_or(i128::MIN);
        for (pool, &i) in self.pools.iter().zip(idxs) {
            overall = overall.max(pool.resources[i].commit_slack(now));
        }
        now + Duration::from_nanos(overall.max(0) as u64)
    }

    /// Authorize the commit at `at` (the caller has waited until >= earliest):
    /// advance the real rate horizon. This is the only place the server-enforced
    /// quota is actually spent.
    pub fn do_commit(&mut self, idxs: &[usize], at: Instant) {
        for b in &mut self.shared {
            b.advance_commit(at);
        }
        for (pool, &i) in self.pools.iter_mut().zip(idxs) {
            pool.resources[i].advance_commit(at);
        }
    }

    /// Cancel an admitted-but-never-committed reservation: roll back the
    /// allocation horizon only. The committed `tat` is never touched, so this
    /// stays sound under concurrency (it only ever frees admission headroom).
    pub fn release(&mut self, idxs: &[usize]) {
        for b in &mut self.shared {
            b.release_reserve();
        }
        for (pool, &i) in self.pools.iter_mut().zip(idxs) {
            pool.resources[i].release_reserve();
        }
    }

    /// Convenience: admit immediately (allocation horizon only). Used by callers
    /// and tests that don't drive the explicit commit gate.
    pub fn claim(&mut self, demand: &[Pin<K>], now: Instant) -> Option<Allocation<K>> {
        let (idxs, send_at) = self.peek_reserve(demand, now)?;
        let picks = self.admit(&idxs, send_at);
        Some(Allocation { send_at, picks })
    }

    /// Feed back an observed rate-limit header for one bucket of one resource.
    pub fn reconcile(&mut self, pool: usize, key: &K, bucket: &'static str, remaining: u32, capacity: u32, now: Instant) {
        if let Some(r) = self.pools[pool].resources.iter_mut().find(|r| &r.key == key) {
            for b in r.buckets.iter_mut().filter(|b| b.label == bucket) {
                b.reconcile(remaining, capacity, now);
            }
        }
    }

    pub fn set_alive(&mut self, pool: usize, key: &K, alive: bool) {
        if let Some(r) = self.pools[pool].resources.iter_mut().find(|r| &r.key == key) {
            r.alive = alive;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lane(key: &str, now: Instant) -> Resource<String> {
        // webhook 5 @ 2.5/s, channel 30 @ 0.5/s -- the bijection lane.
        Resource::new(
            key.to_owned(),
            vec![
                Bucket::new("webhook", 5, 2.5, 0, now),
                Bucket::new("channel", 30, 0.5, 0, now),
            ],
        )
    }

    fn ms(a: Instant, b: Instant) -> i64 {
        signed_nanos(a, b) as i64 / 1_000_000
    }

    #[test]
    fn burst_then_pace_on_one_lane() {
        let t0 = Instant::now();
        let mut e = QuotaEngine::new(vec![], vec![Pool::new(vec![lane("w", t0)])]);
        // First 5 fit the webhook burst (sent immediately), 6th is paced at +400ms.
        let mut sends = vec![];
        for _ in 0..6 {
            let a = e.claim(&[Pin::Free], t0).unwrap();
            sends.push(ms(a.send_at, t0));
        }
        assert_eq!(&sends[..5], &[0, 0, 0, 0, 0]);
        assert_eq!(sends[5], 400);
    }

    #[test]
    fn channel_takes_over_after_burst() {
        // Drive the lane far enough that the 30-cap channel (2s spacing) bites.
        let t0 = Instant::now();
        let mut e = QuotaEngine::new(vec![], vec![Pool::new(vec![lane("w", t0)])]);
        let mut last = 0i64;
        for _ in 0..40 {
            let a = e.claim(&[Pin::Free], t0).unwrap();
            last = ms(a.send_at, t0);
        }
        // By the 40th, the steady regime is min(rho) = 0.5/s => 2000ms spacing.
        let a = e.claim(&[Pin::Free], t0).unwrap();
        assert_eq!(ms(a.send_at, t0) - last, 2000);
    }

    #[test]
    fn free_selection_is_least_used() {
        let t0 = Instant::now();
        let pool = Pool::new(vec![lane("a", t0), lane("b", t0)]);
        let mut e = QuotaEngine::new(vec![], vec![pool]);
        // Two identical idle lanes => strict alternation (round-robin).
        let picks: Vec<_> = (0..4).map(|_| e.claim(&[Pin::Free], t0).unwrap().picks[0].clone()).collect();
        assert_eq!(picks, vec!["a", "b", "a", "b"]);
    }

    #[test]
    fn capacity_weighted_when_lanes_differ() {
        let t0 = Instant::now();
        let weak = Resource::new("weak".to_owned(), vec![Bucket::new("webhook", 5, 2.5, 0, t0)]);
        let strong = Resource::new("strong".to_owned(), vec![Bucket::new("webhook", 10, 5.0, 0, t0)]);
        let mut e = QuotaEngine::new(vec![], vec![Pool::new(vec![weak, strong])]);
        let mut strong_n = 0;
        for _ in 0..30 {
            if e.claim(&[Pin::Free], t0).unwrap().picks[0] == "strong" {
                strong_n += 1;
            }
        }
        // The faster lane does strictly more work (not 50/50).
        assert!(strong_n > 15, "strong picked {strong_n}/30");
    }

    #[test]
    fn shared_global_floors_everyone() {
        let t0 = Instant::now();
        let global = Bucket::new("global", 2, 50.0, 0, t0); // tiny global for the test
        let pool = Pool::new(vec![lane("a", t0), lane("b", t0)]);
        let mut e = QuotaEngine::new(vec![global], vec![pool]);
        // Webhooks are wide open, but the global cap of 2 paces the aggregate.
        let s0 = e.claim(&[Pin::Free], t0).unwrap().send_at;
        let s1 = e.claim(&[Pin::Free], t0).unwrap().send_at;
        let s2 = e.claim(&[Pin::Free], t0).unwrap().send_at;
        assert_eq!(ms(s0, t0), 0);
        assert_eq!(ms(s1, t0), 0);
        assert_eq!(ms(s2, t0), 20); // 1/50s after global drains
    }

    #[test]
    fn reconcile_only_tightens() {
        let t0 = Instant::now();
        let mut e = QuotaEngine::new(vec![], vec![Pool::new(vec![lane("w", t0)])]);
        let before = e.claim(&[Pin::Free], t0).unwrap().send_at;
        // Server says the webhook bucket is empty: push future sends out.
        e.reconcile(0, &"w".to_owned(), "webhook", 0, 5, t0);
        let after = e.claim(&[Pin::Free], t0).unwrap().send_at;
        assert!(ms(after, t0) > ms(before, t0));
        // A stale "plenty remaining" read must NOT pull it back in.
        let pushed = e.claim(&[Pin::Free], t0).unwrap().send_at;
        e.reconcile(0, &"w".to_owned(), "webhook", 5, 5, t0);
        let still = e.claim(&[Pin::Free], t0).unwrap().send_at;
        assert!(ms(still, t0) >= ms(pushed, t0));
    }
}