use moka::future::Cache;
use serde::{de::DeserializeOwned, Serialize};

pub struct CacheCustomed {
    cache: Cache<String, String>,
}

impl CacheCustomed {
    pub fn new(cache: Cache<String, String>) -> Self {
        Self { cache }
    }

    /// Set a value with generic type T that implements Serialize
    pub async fn set<T: Serialize>(&self, key: String, value: T) -> anyhow::Result<()> {
        let serialized = serde_json::to_string(&value)?;
        self.cache.insert(key, serialized).await;
        Ok(())
    }

    /// Get a value with generic type T that implements DeserializeOwned
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> anyhow::Result<Option<T>> {
        if let Some(serialized) = self.cache.get(key).await {
            let deserialized: T = serde_json::from_str(&serialized)?;
            Ok(Some(deserialized))
        } else {
            Ok(None)
        }
    }

    /// Set a raw string value
    pub async fn set_raw(&self, key: String, value: String) {
        self.cache.insert(key, value).await;
    }

    /// Get a raw string value
    pub async fn get_raw(&self, key: &str) -> Option<String> {
        self.cache.get(key).await
    }

    /// Remove a key from cache
    pub async fn remove(&self, key: &str) {
        self.cache.remove(key).await;
    }
}
