use std::collections::{HashMap, LinkedList};
use std::sync::Arc;

use super::RwArc;
use tokio::sync::RwLock;

#[derive(Debug)]
struct Data<K, V> {
    key: K,
    used: i32,
    data: Arc<V>,
}
type DataRwArc<K, V> = RwArc<Data<K, V>>;

impl<K, V> Data<K, V> {
    pub fn new(key: K, value: Arc<V>) -> DataRwArc<K, V> {
        Arc::new(RwLock::new(Data {
            key: key,
            used: 0,
            data: value,
        }))
    }
}

/* LFU(Least Frequently Used) - Cache Invalidation Strategy*/
#[derive(Debug)]
struct Inner<K, V>
where
    K: Eq + std::hash::Hash,
{
    limit: usize,
    entries: HashMap<K, DataRwArc<K, V>>,
    ordered: LinkedList<DataRwArc<K, V>>,
}
type InnerRwArc<K, V> = RwArc<Inner<K, V>>;

impl<K, V> Inner<K, V>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub fn new(limit: usize) -> Self {
        Inner {
            limit: limit,
            entries: HashMap::<K, DataRwArc<K, V>>::default(),
            ordered: LinkedList::<DataRwArc<K, V>>::default(),
        }
    }

    async fn eviction(&mut self, entry: DataRwArc<K, V>) -> Option<Arc<V>> {
        entry.write().await.used += 1;
        self.ordered.push_back(entry);
        if self.ordered.len() < self.limit {
            return None;
        }

        let first = self.ordered.pop_front()?;
        let used = {
            let mut locked = first.write().await;
            locked.used -= 1;
            locked.used
        };

        match used {
            0 => {
                let locked = first.read().await;
                self.entries.remove(&locked.key)?;
                Some(locked.data.clone())
            }
            i if i < 0 => Some(first.read().await.data.clone()),
            _ => None,
        }
    }

    pub async fn insert(&mut self, key: K, value: Arc<V>) -> Option<Arc<V>> {
        let entry = if let Some(ptr) = self.entries.get(&key) {
            let mut locked = ptr.write().await;
            locked.data = value;
            ptr.clone()
        } else {
            let ptr = Data::<K, V>::new(key.clone(), value);
            self.entries.insert(key, ptr.clone());
            ptr
        };

        self.eviction(entry).await
    }

    pub async fn get(&mut self, key: &K) -> Option<(Arc<V>, Option<Arc<V>>)> {
        let entry = self.entries.get(key).cloned()?;
        let data = entry.read().await.data.clone();
        let dump = self.eviction(entry).await;
        Some((data, dump))
    }

    pub async fn remove(&mut self, key: &K) -> Option<Arc<V>> {
        let data = self.entries.remove(key)?;
        let mut locked = data.write().await;
        locked.used = 0;
        Some(locked.data.clone())
    }
}

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
    pub fn new(limit: usize) -> Self {
        LfuCache {
            inner: Arc::new(RwLock::new(Inner::<K, V>::new(limit))),
        }
    }

    pub async fn insert_raw(&self, key: K, value: V) -> Option<Arc<V>> {
        self.insert(key, Arc::new(value)).await
    }
    pub async fn insert(&self, key: K, value: Arc<V>) -> Option<Arc<V>> {
        self.inner.write().await.insert(key, value).await
    }

    pub async fn get(&self, key: &K) -> Option<(Arc<V>, Option<Arc<V>>)> {
        self.inner.write().await.get(key).await
    }

    pub async fn remove(&self, key: &K) -> Option<Arc<V>> {
        self.inner.write().await.remove(key).await
    }
}
