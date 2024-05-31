use std::{
    collections::HashMap,
    sync::Arc,
    hash::Hash,
    cmp::Eq,
};

use chrono::Utc;

pub struct Limiter<T> where T: Eq + Hash {
    /// Duration in milliseconds after which items are considered expired.
    threshold: i64,
    list: Vec<(i64, Arc<T>)>,
    map: HashMap<Arc<T>, i64>,
}

impl<T> Limiter<T> where T: Eq + Hash {
    pub fn new(time: f64) -> Self {
        let threshold = (time * 1000.0) as i64;
        Limiter {
            threshold,
            list: Default::default(),
            map: Default::default(),
        }
    }

    /// Inserts an item and returns the timestamp if it already exists.
    /// Otherwise, returns `None`.
    pub fn put(&mut self, item: Arc<T>) -> Option<i64> {
        let now = Utc::now().timestamp_millis();
        self.remove(now);

        if let Some(&v) = self.map.get(&item) {
            Some(v)
        } else {
            self.list.push((now, Arc::clone(&item)));
            self.map.insert(item, now);
            None
        }
    }

    fn remove(&mut self, now: i64) {
        let limit = now - self.threshold; // correct calculation of expiry
        let idx = self.list.partition_point(|(recorded, _)| *recorded < limit);

        // Drain from `list` and also remove from `map`
        for (_, value) in self.list.drain(0..idx) {
            self.map.remove(&value);
        }
    }
}