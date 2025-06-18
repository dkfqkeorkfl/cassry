use chrono::{DateTime, Duration, TimeZone, Utc};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::localdb::LocalDB;

/// TTL을 기반으로 적절한 폴더명을 생성합니다.
/// TTL은 반드시 86400초(24시간)의 약수여야 합니다.
///
/// # Arguments
/// * `base_path` - 기본 경로
/// * `ttl` - Time To Live (Duration)
/// * `timestamp` - 기준 시간
///
/// # Returns
/// * `Ok(String)` - 생성된 폴더 경로
/// * `Err(anyhow::Error)` - TTL이 86400초의 약수가 아닌 경우
pub fn get_folder_name(
    base_path: &str,
    ttl: &Duration,
    timestamp: &DateTime<Utc>,
) -> anyhow::Result<String> {
    const SECONDS_IN_DAY: i64 = 24 * 60 * 60; // 86400 seconds
    let ttl_seconds = ttl.num_seconds();

    // 86400초의 약수인지 확인
    if SECONDS_IN_DAY % ttl_seconds != 0 {
        return Err(anyhow::anyhow!(
            "TTL must be a divisor of 86400 seconds (24 hours). Examples: 1s, 1m, 1h, 2h, 3h, 4h, 6h, 8h, 12h, 24h"
        ));
    }

    let seconds = timestamp.timestamp();
    let folder_timestamp = (seconds / ttl_seconds) * ttl_seconds;
    let folder_time = Utc.timestamp_opt(folder_timestamp, 0).unwrap();

    Ok(format!(
        "{}/{} {}",
        base_path,
        timestamp.format("%Y-%m-%d"),
        folder_time.format("%Y-%m-%d %H:%M:%S")
    ))
}

/// TimeWindowDB의 내부 구현
struct TimeWindowDBInner {
    path: String,
    ttl: Duration,
    delete_legacy: bool,

    current_db: Arc<LocalDB>,
    previous_db: Option<Arc<LocalDB>>,
    created: chrono::DateTime<Utc>,
}

impl TimeWindowDBInner {
    async fn new(base_path: &str, ttl: Duration, delete_legacy: bool) -> anyhow::Result<Self> {
        let current_time = Utc::now();

        let db_path = get_folder_name(base_path, &ttl, &current_time)?;
        let current_db = Arc::new(LocalDB::new(db_path).await?);

        Ok(Self {
            path: base_path.to_string(),
            ttl,
            delete_legacy,
            current_db,
            previous_db: None,
            created: current_time,
        })
    }

    fn should_rotate(&self) -> bool {
        (Utc::now() - self.created) >= self.ttl
    }

    async fn rotate_db(&mut self) -> anyhow::Result<()> {
        if !self.should_rotate() {
            return Ok(());
        }

        let current_time = Utc::now();
        let new_db_path = get_folder_name(&self.path, &self.ttl, &current_time)?;
        let new_db = Arc::new(LocalDB::new(new_db_path).await?);
        
        // Handle old DB
        if let Some(old_db) = self.previous_db.take() {
            if self.delete_legacy {
                // Delete old DB files
                let old_path = format!("{}/{}", self.path, old_db.get_path());
                std::fs::remove_dir_all(old_path)?;
            }
        }

        // Update state
        self.previous_db = Some(self.current_db.clone());
        self.current_db = new_db;
        self.created = current_time;

        Ok(())
    }

    async fn put(&mut self, key: String, value: String) -> anyhow::Result<()> {
        if self.should_rotate() {
            self.rotate_db().await?;
        }
        self.current_db.put(key, value).await
    }

    async fn get(&self, key: String) -> anyhow::Result<Option<String>> {
        // Try current DB first
        match self.current_db.get(key.clone()).await? {
            Some(value) => Ok(Some(value)),
            None => {
                // If not found in current DB, try previous DB
                if let Some(prev_db) = &self.previous_db {
                    prev_db.get(key).await
                } else {
                    Ok(None)
                }
            }
        }
    }

    async fn delete(&mut self, key: String) -> anyhow::Result<()> {
        if self.should_rotate() {
            self.rotate_db().await?;
        }

        // Delete from both current and previous DB
        self.current_db.delete(key.clone()).await?;
        if let Some(prev_db) = &self.previous_db {
            prev_db.delete(key).await?;
        }
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.current_db.flush().await?;
        if let Some(prev_db) = &self.previous_db {
            prev_db.flush().await?;
        }
        Ok(())
    }
}

/// 스레드 안전한 TimeWindowDB 래퍼
#[derive(Clone)]
pub struct TimeWindowDB {
    inner: Arc<RwLock<TimeWindowDBInner>>,
}

impl TimeWindowDB {
    pub async fn new(base_path: &str, ttl: Duration, delete_legacy: bool) -> anyhow::Result<Self> {
        let inner = TimeWindowDBInner::new(base_path, ttl, delete_legacy).await?;
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    pub async fn put(&self, key: String, value: String) -> anyhow::Result<()> {
        let mut inner = self.inner.write().await;
        inner.put(key, value).await
    }

    pub async fn get(&self, key: String) -> anyhow::Result<Option<String>> {
        let inner = self.inner.read().await;
        inner.get(key).await
    }

    pub async fn delete(&self, key: String) -> anyhow::Result<()> {
        let mut inner = self.inner.write().await;
        inner.delete(key).await
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        let inner = self.inner.read().await;
        inner.flush().await
    }
}
