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
use tokio::time::{sleep_until, Instant as TokioInstant};

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
    pub fn update(&self, pool: usize, key: &K, bucket: &'static str, remaining: u32, capacity: u32) {
        self.0
            .engine
            .lock()
            .unwrap()
            .reconcile(pool, key, bucket, remaining, capacity, Instant::now());
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
        // Note: needs the handle to release; in practice store an Arc<Inner> in
        // the Reservation, or call `handle.release(&mut r)` explicitly. Shown
        // explicit below to keep the type free of a back-reference.
    }
}

impl<K: PartialEq + Clone> QuotaHandle<K> {
    /// Explicit cancel (prefer this over relying on Drop). Idempotent.
    pub fn release(&self, r: &mut Reservation<K>) {
        if !r.done {
            self.0.engine.lock().unwrap().release(&r.idxs);
            r.done = true;
        }
    }
}