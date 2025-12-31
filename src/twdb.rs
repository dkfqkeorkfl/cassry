use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::anyhowln;

use super::localdb::*;

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

#[derive(Clone)]
pub enum TimeWindowType {
    General,
    Log,
}

#[derive(Clone)]
pub struct TimeWindowDBConfig {
    pub base_path: String,
    pub ttl: Duration,
    pub delete_legacy: bool,
    pub ty: TimeWindowType,
}

impl TimeWindowDBConfig {
    fn get_folder_name(&self, timestamp: &DateTime<Utc>) -> anyhow::Result<String> {
        const SECONDS_IN_DAY: i64 = 24 * 60 * 60; // 86400 seconds
        let ttl_seconds = self.ttl.num_seconds();

        // 86400초의 약수인지 확인
        if ttl_seconds == 0 || SECONDS_IN_DAY % ttl_seconds != 0 {
            return Err(anyhowln!(
                "TTL must be a divisor of 86400 seconds (24 hours). Examples: 1s, 1m, 1h, 2h, 3h, 4h, 6h, 8h, 12h, 24h"
            ));
        }

        let seconds = timestamp.timestamp();
        let folder_timestamp = (seconds / ttl_seconds) * ttl_seconds;
        let folder_time = Utc.timestamp_opt(folder_timestamp, 0).unwrap();

        Ok(format!(
            "{}/{}",
            self.base_path,
            folder_time.format("%Y-%m-%d_%H-%M-%S")
        ))
    }

    async fn get_localdb(&self, timestamp: &DateTime<Utc>) -> anyhow::Result<LocalDB> {
        let path = self.get_folder_name(timestamp)?;
        match self.ty {
            TimeWindowType::General => LocalDB::new(config::General::new(path)).await,
            TimeWindowType::Log => LocalDB::new(config::Log::new(path)).await,
        }
    }
}

/// TimeWindowDB의 내부 구현
struct TimeWindowDBInner {
    config: TimeWindowDBConfig,

    current_db: LocalDB,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

impl TimeWindowDBInner {
    async fn new(config: TimeWindowDBConfig) -> anyhow::Result<Self> {
        let current_time = Utc::now();

        // Create base directory if it doesn't exist
        let base_path_obj = Path::new(&config.base_path);
        if !base_path_obj.exists() {
            fs::create_dir_all(base_path_obj)?;
        }

        let current_db = config.get_localdb(&current_time).await?;
        Ok(Self {
            config,
            current_db,
            created_at: current_time,
            updated_at: current_time,
        })
    }

    fn should_rotate(&self) -> bool {
        (Utc::now() - self.updated_at) >= self.config.ttl
    }

    async fn rotate_db(&mut self) -> anyhow::Result<()> {
        if !self.should_rotate() {
            return Ok(());
        }

        let current_time = Utc::now();
        let new_db = self.config.get_localdb(&current_time).await?;
        self.updated_at = current_time;

        let current_db_path = self.current_db.get_path().display().to_string();
        self.current_db = new_db;
        // Handle old DB
        if self.config.delete_legacy {
            if let Err(e) =
                tokio::task::spawn_blocking(move || std::fs::remove_dir_all(current_db_path)).await
            {
                return Err(anyhow::anyhow!("error deleting old db: {}", e));
            }
        }

        Ok(())
    }

    async fn put(&mut self, key: Arc<Vec<u8>>, value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        if self.should_rotate() {
            self.rotate_db().await?;
        }
        self.current_db.put(key, value).await
    }

    async fn get(&self, key: Arc<Vec<u8>>) -> anyhow::Result<Option<Vec<u8>>> {
        self.current_db.get(key).await
    }

    async fn delete(&mut self, key: Arc<Vec<u8>>) -> anyhow::Result<()> {
        if self.should_rotate() {
            self.rotate_db().await?;
        } else {
            self.current_db.delete(key).await?;
        }

        Ok(())
    }

    async fn put_json<T: Serialize>(&mut self, key: String, value: &T) -> anyhow::Result<()> {
        if self.should_rotate() {
            self.rotate_db().await?;
        }

        self.current_db.put_json(key, value).await
    }

    async fn get_json<T>(&self, key: String) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.current_db.get_json::<T>(key).await
    }

    async fn delete_json(&mut self, key: String) -> anyhow::Result<()> {
        if self.should_rotate() {
            self.rotate_db().await?;
        }
        self.current_db.delete_by_str(key).await
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.current_db.flush().await?;
        Ok(())
    }
}

/// 스레드 안전한 TimeWindowDB 래퍼
pub struct TimeWindowDB {
    inner: RwLock<TimeWindowDBInner>,
}

impl TimeWindowDB {
    pub async fn new(config: TimeWindowDBConfig) -> anyhow::Result<Self> {
        let inner = TimeWindowDBInner::new(config).await?;
        Ok(Self {
            inner: inner.into(),
        })
    }

    pub async fn put(&self, key: Arc<Vec<u8>>, value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        let mut inner = self.inner.write().await;
        inner.put(key, value).await
    }

    pub async fn get(&self, key: Arc<Vec<u8>>) -> anyhow::Result<Option<Vec<u8>>> {
        let inner = self.inner.read().await;
        inner.get(key).await
    }

    pub async fn delete(&self, key: Arc<Vec<u8>>) -> anyhow::Result<()> {
        let mut inner = self.inner.write().await;
        inner.delete(key).await
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        let inner = self.inner.read().await;
        inner.flush().await
    }

    pub async fn get_created_at(&self) -> chrono::DateTime<Utc> {
        let inner = self.inner.read().await;
        inner.created_at
    }

    pub async fn get_updated_at(&self) -> chrono::DateTime<Utc> {
        let inner = self.inner.read().await;
        inner.updated_at
    }

    pub async fn put_json<T: Serialize>(&self, key: String, value: &T) -> anyhow::Result<()> {
        self.inner.write().await.put_json(key, value).await
    }

    pub async fn get_json<T>(&self, key: String) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.inner.read().await.get_json::<T>(key).await
    }

    pub async fn delete_json(&self, key: String) -> anyhow::Result<()> {
        self.inner.write().await.delete_json(key).await
    }
}
