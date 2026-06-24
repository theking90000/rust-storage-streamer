//! Async integration layer over the pure `QuotaEngine`.
//!
//! Single `std::sync::Mutex` (NOT `tokio::Mutex`): every critical section is
//! synchronous and microsecond-short, and no `.await` ever happens while the
//! guard is held. A `Notify` lets `update` wake every committer parked on the
//! gate, so a correction that revealed the prediction was wrong delays all
//! pending commits instead of letting them fire into a 429.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio::time::{Instant as TokioInstant, sleep_until};

use crate::cadence::{Pin, QuotaEngine};

struct Inner<K> {
    engine: Mutex<QuotaEngine<K>>,
    /// Pulsed whenever the committed horizon is tightened (a `do_commit` or an
    /// `update`), so parked committers re-read the live gate.
    bell: Notify,
}

pub struct QuotaHandle<K>(Arc<Inner<K>>);

impl<K> Clone for QuotaHandle<K> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// A held permit. The allocation horizon was advanced, so the slot counts
/// against admission until it is committed or released. It carries the pool
/// indices it reserved so `commit`/`release` act on exactly those resources.
pub struct Reservation<K> {
    pub picks: Vec<K>,
    pub expires_at: Instant, // validity ceiling for the prepared work (§7)
    inner: Arc<Inner<K>>,
    idxs: Vec<usize>,
    done: bool, // commit/release consumes it; Drop releases if neither happened
}

impl<K: PartialEq + Clone> QuotaHandle<K> {
    pub fn new(engine: QuotaEngine<K>) -> Self {
        Self(Arc::new(Inner {
            engine: Mutex::new(engine),
            bell: Notify::new(),
        }))
    }

    /// PHASE 1 -- PERMIT (sync). Admit a reservation iff a commit looks feasible
    /// before `deadline`, accounting for everything already reserved (allocation
    /// horizon). `None` => do NOT prepare, do NOT read more input: backpressure.
    /// Nothing is mutated on the `None` path.
    pub fn reserve(
        &self,
        demand: &[Pin<K>],
        deadline: Instant,
        validity: Duration,
    ) -> Option<Reservation<K>> {
        let mut engine = self.0.engine.lock().unwrap();
        let now = Instant::now();
        let (idxs, reserve_at) = engine.peek_reserve(demand, now)?;
        if reserve_at > deadline {
            return None; // too deep a backlog to finalize in time
        }
        let picks = engine.admit(&idxs, reserve_at);
        Some(Reservation {
            picks,
            idxs,
            inner: self.0.clone(),
            expires_at: now.max(reserve_at) + validity,
            done: false,
        })
    }

    /// PHASE 3 -- COMMIT (async, blocking). Re-asks the live commit gate every
    /// time it wakes; returns only once the commit is authorized NOW, advancing
    /// the real rate horizon at that instant. Wakes early when `update` rings the
    /// bell, so a tightened model pushes this call out instead of firing stale.
    /// The caller sends the last byte the moment this resolves.
    pub async fn commit(&self, r: &mut Reservation<K>) {
        loop {
            // Subscribe to the bell BEFORE reading the gate, so a tightening that
            // races between our read and our park is not lost.
            let bell = self.0.bell.notified();
            let target = {
                let mut engine = self.0.engine.lock().unwrap();
                let now = Instant::now();
                let earliest = engine.earliest_commit(&r.idxs, now);
                if earliest <= now {
                    engine.do_commit(&r.idxs, now);
                    r.done = true;
                    // Our commit moved the horizon: let the next committer re-check.
                    self.0.bell.notify_waiters();
                    return;
                }
                earliest
            }; // lock dropped before awaiting
            tokio::select! {
                _ = sleep_until(TokioInstant::from_std(target)) => {}
                _ = bell => {} // someone tightened tat: loop and re-read
            }
        }
    }

    /// PHASE 4 -- ADAPT. Compare the server's reported state to the prediction
    /// and tighten the commit horizon if we were behind. Rings the bell so any
    /// parked committer re-evaluates against the correction.
    pub fn update(
        &self,
        pool: usize,
        key: &K,
        bucket: &'static str,
        remaining: u32,
        capacity: u32,
    ) {
        self.0.engine.lock().unwrap().reconcile(
            pool,
            key,
            bucket,
            remaining,
            capacity,
            Instant::now(),
        );
        self.0.bell.notify_waiters();
    }

    pub fn set_alive(&self, pool: usize, key: &K, alive: bool) {
        self.0.engine.lock().unwrap().set_alive(pool, key, alive);
    }
}

/// If a reservation is dropped without committing (prep failed, task cancelled,
/// expired), give its admission headroom back -- the allocation horizon only,
/// never the committed rate. This is the §14 "resources released correctly".
impl<K> Drop for Reservation<K> {
    fn drop(&mut self) {
        if !self.done {
            self.inner.engine.lock().unwrap().release(&self.idxs);
            self.done = true;
        }
    }
}

impl<K: PartialEq + Clone> QuotaHandle<K> {
    /// Explicit cancel (prefer this over relying on Drop). Idempotent.
    pub fn release(&self, r: &mut Reservation<K>) {
        if !r.done {
            r.inner.engine.lock().unwrap().release(&r.idxs);
            r.done = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadence::{Bucket, Pool, Resource};
    use tokio::time::sleep;

    const SCALE: f64 = 100.0;

    fn one_slot_handle() -> QuotaHandle<String> {
        let now = Instant::now();
        QuotaHandle::new(QuotaEngine::new(
            vec![Bucket::new("global", 1, 1.0, 0, now)],
            vec![],
        ))
    }

    fn near_deadline() -> Instant {
        Instant::now() + Duration::from_millis(100)
    }

    fn lane(key: String, now: Instant) -> Resource<String> {
        Resource::new(
            key,
            vec![
                Bucket::new("webhook", 5, 2.5 * SCALE, 0, now),
                Bucket::new("channel", 30, 0.5 * SCALE, 0, now),
            ],
        )
    }

    fn egress(key: String, now: Instant) -> Resource<String> {
        Resource::new(
            key,
            vec![
                Bucket::new("ip", 10000, (1000.0 / 60.0) * SCALE, 0, now),
                Bucket::new("global", 50, 50.0 * SCALE, 0, now),
            ],
        )
    }

    fn matrix_handle(lanes: usize, egresses: usize) -> QuotaHandle<String> {
        let now = Instant::now();
        QuotaHandle::new(QuotaEngine::new(
            vec![],
            vec![
                Pool::new((0..lanes).map(|i| lane(format!("w{i}"), now)).collect()),
                Pool::new(
                    (0..egresses)
                        .map(|i| egress(format!("ip{i}"), now))
                        .collect(),
                ),
            ],
        ))
    }

    fn prep_delay(i: usize) -> Duration {
        Duration::from_millis(5 + ((i * 37) % 36) as u64)
    }

    async fn finish_request(
        handle: QuotaHandle<String>,
        mut r: Reservation<String>,
        i: usize,
    ) -> Vec<String> {
        let picks = r.picks.clone();
        sleep(prep_delay(i)).await;
        handle.commit(&mut r).await;
        sleep(Duration::from_millis(20)).await;
        handle.update(0, &picks[0], "webhook", 5, 5);
        handle.update(0, &picks[0], "channel", 30, 30);
        handle.update(1, &picks[1], "ip", 10000, 10000);
        handle.update(1, &picks[1], "global", 50, 50);
        picks
    }

    #[test]
    fn dropped_reservation_releases_admission_headroom() {
        let handle = one_slot_handle();
        let r = handle
            .reserve(&[], near_deadline(), Duration::from_secs(10))
            .unwrap();
        assert!(
            handle
                .reserve(&[], near_deadline(), Duration::from_secs(10))
                .is_none()
        );

        drop(r);

        assert!(
            handle
                .reserve(&[], near_deadline(), Duration::from_secs(10))
                .is_some()
        );
    }

    #[tokio::test]
    async fn committed_reservation_is_not_released_on_drop() {
        let handle = one_slot_handle();
        let mut r = handle
            .reserve(&[], near_deadline(), Duration::from_secs(10))
            .unwrap();
        handle.commit(&mut r).await;

        drop(r);

        assert!(
            handle
                .reserve(&[], near_deadline(), Duration::from_secs(10))
                .is_none()
        );
    }

    #[tokio::test]
    async fn matrix_one_lane_one_egress_with_scaled_sleeps() {
        let handle = matrix_handle(1, 1);
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut tasks = vec![];

        for i in 0..6 {
            let r = handle
                .reserve(&[Pin::Free, Pin::Free], deadline, Duration::from_secs(1))
                .unwrap();
            tasks.push(tokio::spawn(finish_request(handle.clone(), r, i)));
        }

        for task in tasks {
            assert_eq!(task.await.unwrap(), vec!["w0".to_owned(), "ip0".to_owned()]);
        }
    }

    #[tokio::test]
    async fn matrix_many_lanes_and_egresses_with_scaled_sleeps() {
        use std::collections::HashSet;

        let handle = matrix_handle(1000, 30);
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut first_1000_lanes = HashSet::new();
        let mut first_30_egresses = HashSet::new();
        let mut tasks = vec![];

        for i in 0..1501 {
            let r = handle
                .reserve(&[Pin::Free, Pin::Free], deadline, Duration::from_secs(1))
                .unwrap();
            if i < 1000 {
                first_1000_lanes.insert(r.picks[0].clone());
            }
            if i < 30 {
                first_30_egresses.insert(r.picks[1].clone());
            }
            tasks.push(tokio::spawn(finish_request(handle.clone(), r, i)));
        }

        assert_eq!(first_1000_lanes.len(), 1000);
        assert_eq!(first_30_egresses.len(), 30);
        for task in tasks {
            task.await.unwrap();
        }
    }
}
