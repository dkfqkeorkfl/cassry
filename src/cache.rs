use std::collections::{HashMap, LinkedList};
use std::sync::Arc;

use super::RwArc;
use tokio::sync::RwLock;

/// Represents a single cache entry with usage tracking
#[derive(Debug)]
struct Data<K, V> {
    key: K,
    used: u32,  // Keep u32 as it's sufficient for usage count
    data: Arc<V>,
    // Add a flag to track if the entry is in the ordered list
    // This helps avoid unnecessary list operations
    is_in_list: bool,
}
type DataRwArc<K, V> = RwArc<Data<K, V>>;

impl<K, V> Data<K, V> {
    /// Creates a new cache entry
    pub fn new(key: K, value: Arc<V>) -> DataRwArc<K, V> {
        Arc::new(RwLock::new(Data {
            key,
            used: 0,
            data: value,
            is_in_list: false,
        }))
    }
}

/// LFU (Least Frequently Used) Cache implementation
#[derive(Debug)]
struct Inner<K, V>
where
    K: Eq + std::hash::Hash,
{
    limit: usize,
    entries: HashMap<K, DataRwArc<K, V>>,
    ordered: LinkedList<DataRwArc<K, V>>,
    // Add a counter to track total entries
    // This helps avoid checking ordered.len() frequently
    count: usize,
}
type InnerRwArc<K, V> = RwArc<Inner<K, V>>;

impl<K, V> Inner<K, V>
where
    K: Eq + std::hash::Hash + Clone,
{
    /// Creates a new LFU cache with the specified size limit
    pub fn new(limit: usize) -> Self {
        Inner {
            limit,
            entries: HashMap::default(),
            ordered: LinkedList::default(),
            count: 0,
        }
    }

    /// Updates usage count and manages cache eviction
    async fn update_usage(&mut self, data: DataRwArc<K, V>) -> Option<Arc<V>> {
        let mut locked = data.write().await;
        locked.used += 1;
        
        // Only move to back if not already in list
        if !locked.is_in_list {
            locked.is_in_list = true;
            drop(locked); // Release the lock before list operation
            self.ordered.push_back(data.clone());
            self.count += 1;
        }

        // Check if we need to evict
        if self.count > self.limit {
            self.evict_least_used().await
        } else {
            None
        }
    }

    /// Evicts the least frequently used entry from the cache
    async fn evict_least_used(&mut self) -> Option<Arc<V>> {
        let first = self.ordered.pop_front()?;
        self.count -= 1;
        
        let mut locked = first.write().await;
        locked.used -= 1;
        locked.is_in_list = false;
        
        if locked.used == 0 {
            self.entries.remove(&locked.key)?;
            Some(locked.data.clone())
        } else {
            None
        }
    }

    /// Inserts or updates a value in the cache
    pub async fn insert(&mut self, key: K, value: Arc<V>) -> Option<Arc<V>> {
        let ptr = if let Some(old) = self.entries.get(&key) {
            let mut locked = old.write().await;
            locked.data = value;
            old.clone()
        } else {
            let ptr = Data::<K, V>::new(key.clone(), value);
            self.entries.insert(key, ptr.clone());
            ptr
        };

        self.update_usage(ptr).await
    }

    /// Retrieves a value from the cache and updates its usage count
    pub async fn get(&mut self, key: &K) -> Option<(Arc<V>, Option<Arc<V>>)> {
        let data = self.entries.get(key).cloned()?;
        let ret = data.read().await.data.clone();
        let evicted = self.update_usage(data).await;
        Some((ret, evicted))
    }

    /// Removes a value from the cache
    pub async fn remove(&mut self, key: &K) -> Option<Arc<V>> {
        let data = self.entries.remove(key)?;
        let mut locked = data.write().await;
        locked.used = 0;
        locked.is_in_list = false;
        self.count -= 1;
        Some(locked.data.clone())
    }
}

/// Thread-safe LFU cache implementation
#[derive(Debug, Clone)]
pub struct LfuCache<K, V>
where
    K: Eq + std::hash::Hash,
{
    inner: InnerRwArc<K, V>,
}

impl<K, V> LfuCache<K, V>
where
    K: Eq + std::hash::Hash + Clone,
{
    /// Creates a new thread-safe LFU cache with the specified size limit
    pub fn new(limit: usize) -> Self {
        LfuCache {
            inner: Arc::new(RwLock::new(Inner::<K, V>::new(limit))),
        }
    }

    /// Inserts a raw value into the cache
    pub async fn insert_raw(&self, key: K, value: V) -> Option<Arc<V>> {
        self.insert(key, Arc::new(value)).await
    }

    /// Inserts an Arc-wrapped value into the cache
    pub async fn insert(&self, key: K, value: Arc<V>) -> Option<Arc<V>> {
        self.inner.write().await.insert(key, value).await
    }

    /// Retrieves a value from the cache
    pub async fn get(&self, key: &K) -> Option<(Arc<V>, Option<Arc<V>>)> {
        self.inner.write().await.get(key).await
    }

    /// Removes a value from the cache
    pub async fn remove(&self, key: &K) -> Option<Arc<V>> {
        self.inner.write().await.remove(key).await
    }
}
