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
pub struct TimeWindowDB {
    config: TimeWindowDBConfig,
    created_at: chrono::DateTime<Utc>,

    current_db: RwLock<LocalDB>,
}

impl TimeWindowDB {
    pub fn get_created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }

    pub async fn new(config: TimeWindowDBConfig) -> anyhow::Result<Self> {
        let current_time = Utc::now();

        // Create base directory if it doesn't exist
        let base_path_obj = Path::new(&config.base_path);
        if !base_path_obj.exists() {
            fs::create_dir_all(base_path_obj)?;
        }

        let current_db = config.get_localdb(&current_time).await?;
        Ok(Self {
            config,
            created_at: current_time,
            current_db: current_db.into(),
        })
    }

    async fn try_rotate(&self) -> anyhow::Result<()> {
        let current_time = Utc::now();

        let mut current_db = self.current_db.write().await;
        if current_time < *current_db.get_created_at() + self.config.ttl {
            return Ok(());
        }

        let current_db_path = current_db.get_path().display().to_string();
        if self.config.delete_legacy {
            if let Err(e) =
                tokio::task::spawn_blocking(move || std::fs::remove_dir_all(current_db_path)).await
            {
                return Err(anyhow::anyhow!("error deleting old db: {}", e));
            }
        }

        let new_db = self.config.get_localdb(&current_time).await?;
        *current_db = new_db;
        Ok(())
    }

    pub async fn put(&self, key: Arc<Vec<u8>>, value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        self.try_rotate().await?;
        self.current_db.read().await.put(key, value).await
    }

    pub async fn get(&self, key: Arc<Vec<u8>>) -> anyhow::Result<Option<Vec<u8>>> {
        self.try_rotate().await?;
        self.current_db.read().await.get(key).await
    }

    pub async fn delete(&self, key: Arc<Vec<u8>>) -> anyhow::Result<()> {
        self.try_rotate().await?;
        self.current_db.read().await.delete(key).await
    }

    pub async fn put_raw(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        self.try_rotate().await?;
        self.current_db.read().await.put_raw(key, value).await
    }

    pub async fn get_raw(&self, key: Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
        self.try_rotate().await?;
        self.current_db.read().await.get_raw(key).await
    }

    pub async fn delete_raw(&self, key: Vec<u8>) -> anyhow::Result<()> {
        self.try_rotate().await?;
        self.current_db.read().await.delete_raw(key).await
    }
    
    pub async fn put_json(
        &self,
        key: &impl Serialize,
        value: &impl Serialize,
    ) -> anyhow::Result<()> {
        let key_str = serde_json::to_string(key)?;
        let json_str = serde_json::to_string(value)?;
        self.put_raw(key_str.into_bytes(), json_str.into_bytes())
            .await
    }

    pub async fn delete_json(
        &self,
        key: &impl Serialize,
    ) -> anyhow::Result<()> {
        let key_str = serde_json::to_string(key)?;
        self.delete_raw(key_str.into_bytes()).await
    }

    pub async fn get_json<T>(&self, key: &impl Serialize) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let key_str = serde_json::to_string(key)?;
        if let Some(data) = self.get_raw(key_str.into_bytes()).await.and_then(|ret| {
            ret.map(|v| String::from_utf8(v).map_err(anyhow::Error::from))
                .transpose()
        })? {
            let value: T = serde_json::from_str(&data)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub async fn put_bson(
        &self,
        key: &impl Serialize,
        value: &impl Serialize,
    ) -> anyhow::Result<()> {
        let ks = bson::serialize_to_vec(key)?;
        let bs = bson::serialize_to_vec(value)?;
        self.put_raw(ks, bs).await
    }

    pub async fn delete_bson(
        &self,
        key: &impl Serialize,
    ) -> anyhow::Result<()> {
        let ks = bson::serialize_to_vec(key)?;
        self.delete_raw(ks).await
    }

    pub async fn get_bson<T>(&self, key: &impl Serialize) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let ks = bson::serialize_to_vec(key)?;
        if let Some(data) = self.get_raw(ks).await? {
            let value: T = bson::deserialize_from_slice(&data)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub async fn put_postcard(
        &self,
        key: &impl Serialize,
        value: &impl Serialize,
    ) -> anyhow::Result<()> {
        let bs = postcard::to_stdvec(value)?;
        let ks = postcard::to_stdvec(key)?;
        self.put_raw(ks, bs).await
    }

    pub async fn delete_postcard(
        &self,
        key: &impl Serialize,
    ) -> anyhow::Result<()> {
        let ks = postcard::to_stdvec(key)?;
        self.delete_raw(ks).await
    }

    pub async fn get_postcard<T>(&self, key: &impl Serialize) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let ks = postcard::to_stdvec(key)?;
        if let Some(data) = self.get_raw(ks).await? {
            let value: T = postcard::from_bytes(&data)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }


}