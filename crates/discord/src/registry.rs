use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Mutex;
use std::sync::atomic::Ordering;

use crate::webhook::{Webhook, WebhookSlot};

/// Heap key ordering webhooks by least-used-first (then index for determinism).
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct HeapEntry {
    used: u64,
    idx: u32,
}

/// Holds every webhook and hands out the least-used alive one for uploads.
///
/// A picked webhook is removed from the heap until [`finish`] reinserts it, so
/// concurrent uploads naturally fan out across distinct webhooks. Dead webhooks
/// are dropped from rotation.
pub(crate) struct WebhookRegistry {
    slots: Vec<WebhookSlot>,
    heap: Mutex<BinaryHeap<Reverse<HeapEntry>>>,
}

impl WebhookRegistry {
    pub fn new(webhooks: Vec<Webhook>) -> Self {
        let slots: Vec<WebhookSlot> = webhooks.into_iter().map(WebhookSlot::new).collect();
        let heap = slots
            .iter()
            .enumerate()
            .map(|(idx, _)| Reverse(HeapEntry { used: 0, idx: idx as u32 }))
            .collect();
        Self {
            slots,
            heap: Mutex::new(heap),
        }
    }

    pub fn slot(&self, idx: usize) -> &WebhookSlot {
        &self.slots[idx]
    }

    /// Pops the least-used alive webhook, removing it from rotation until
    /// [`finish`]. Returns `None` if no alive webhook is currently free.
    pub fn pick(&self) -> Option<usize> {
        let mut heap = self.heap.lock().unwrap();
        while let Some(Reverse(entry)) = heap.pop() {
            let idx = entry.idx as usize;
            if !self.slots[idx].dead.load(Ordering::Relaxed) {
                return Some(idx);
            }
        }
        None
    }

    /// Marks a webhook permanently unusable; it will not be reinserted.
    pub fn mark_dead(&self, idx: usize) {
        self.slots[idx].dead.store(true, Ordering::Relaxed);
    }

    /// Accounts for one attempt and returns the webhook to rotation if it is
    /// still alive.
    pub fn finish(&self, idx: usize) {
        let used = self.slots[idx].used.fetch_add(1, Ordering::Relaxed) + 1;
        if !self.slots[idx].dead.load(Ordering::Relaxed) {
            self.heap
                .lock()
                .unwrap()
                .push(Reverse(HeapEntry { used, idx: idx as u32 }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn webhooks(count: usize) -> Vec<Webhook> {
        (0..count)
            .map(|i| Webhook {
                id: format!("id{i}"),
                token: format!("tok{i}"),
            })
            .collect()
    }

    #[test]
    fn spreads_uniformly_across_webhooks() {
        let registry = WebhookRegistry::new(webhooks(3));
        let mut counts = [0u64; 3];
        for _ in 0..30 {
            let idx = registry.pick().unwrap();
            counts[idx] += 1;
            registry.finish(idx);
        }
        assert_eq!(counts, [10, 10, 10]);
    }

    #[test]
    fn skips_dead_webhooks() {
        let registry = WebhookRegistry::new(webhooks(2));
        let first = registry.pick().unwrap();
        registry.mark_dead(first);
        registry.finish(first);
        // Every subsequent pick avoids the dead one.
        for _ in 0..5 {
            let idx = registry.pick().unwrap();
            assert_ne!(idx, first);
            registry.finish(idx);
        }
    }

    #[test]
    fn returns_none_when_all_dead() {
        let registry = WebhookRegistry::new(webhooks(1));
        let idx = registry.pick().unwrap();
        registry.mark_dead(idx);
        registry.finish(idx);
        assert!(registry.pick().is_none());
    }
}
