use chrono::{DateTime, Duration, Local, TimeZone, Timelike, Utc};
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
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

impl TimeWindowType {
    pub async fn make_db(&self, path: &str) -> anyhow::Result<LocalDB> {
        match self {
            TimeWindowType::General => LocalDB::new(config::General::new(path.to_string())).await,
            TimeWindowType::Log => LocalDB::new(config::Log::new(path.to_string())).await,
        }
    }
}

#[derive(Clone)]
pub struct TimeWindowDBConfig {
    pub base_path: String,
    pub ttl: Duration,
    pub delete_legacy: bool,
    pub ty: TimeWindowType,
}

impl TimeWindowDBConfig {
    fn get_folder_name(
        &self,
        timestamp: &DateTime<Local>,
    ) -> anyhow::Result<(String, DateTime<Local>)> {
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
        let folder_time = Local.timestamp_opt(folder_timestamp, 0).unwrap();

        Ok((
            format!(
                "{}/{}",
                self.base_path,
                folder_time.format("%Y-%m-%d_%H-%M-%S")
            ),
            folder_time,
        ))
    }

    async fn get_localdb(
        &self,
        timestamp: &DateTime<Local>,
    ) -> anyhow::Result<(LocalDB, DateTime<Local>)> {
        let (path, folder_time) = self.get_folder_name(timestamp)?;
        let db = self.ty.make_db(&path).await?;
        Ok((db, folder_time))
    }
}

struct Context {
    current_db: LocalDB,
    folder_time: DateTime<Local>,
}
/// TimeWindowDB의 내부 구현
pub struct TimeWindowDB {
    config: TimeWindowDBConfig,
    created_at: chrono::DateTime<Local>,

    ctx: RwLock<Context>,
}

impl TimeWindowDB {
    pub fn get_created_at(&self) -> &DateTime<Local> {
        &self.created_at
    }

    pub fn get_config(&self) -> &TimeWindowDBConfig {
        &self.config
    }

    pub async fn get_current_path(&self) -> PathBuf {
        self.ctx.read().await.current_db.get_path().to_path_buf()
    }

    pub async fn get_current_dt(&self) -> DateTime<Local> {
        self.ctx.read().await.folder_time.clone()
    }

    pub async fn new(config: TimeWindowDBConfig) -> anyhow::Result<Self> {
        let current_time = Local::now();

        let (current_db, folder_time) = config.get_localdb(&current_time).await?;
        Ok(Self {
            config,
            created_at: current_time,
            ctx: RwLock::new(Context {
                current_db,
                folder_time,
            }),
        })
    }

    pub async fn try_rotate(&self) -> anyhow::Result<bool> {
        let current_time = Local::now();

        let mut ctx = self.ctx.write().await;
        if current_time < ctx.folder_time + self.config.ttl {
            return Ok(false);
        }

        let previous_is_empty = ctx.current_db.is_empty().await?;
        let previous_path = ctx.current_db.get_path().display().to_string();
        let (new_db, new_folder_time) = self.config.get_localdb(&current_time).await?;
        ctx.current_db = new_db;
        ctx.folder_time = new_folder_time;

        if self.config.delete_legacy || previous_is_empty {
            if let Err(e) = tokio::fs::remove_dir_all(previous_path).await {
                return Err(anyhow::anyhow!("error deleting old db: {}", e));
            }
        }
        Ok(true)
    }

    pub async fn commit(&self, batch: WriteBatch) -> anyhow::Result<()> {
        self.try_rotate().await?;
        self.ctx.read().await.current_db.commit(batch).await
    }

    pub async fn foreach<F>(&self, callback: F) -> anyhow::Result<()>
    where
        F: Fn(&[u8], &[u8]) -> anyhow::Result<()> + Send + Sync + 'static + Clone,
    {
        self.try_rotate().await?;
        self.ctx.read().await.current_db.foreach(callback).await
    }

    pub async fn put(&self, key: Arc<Vec<u8>>, value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        self.try_rotate().await?;
        self.ctx.read().await.current_db.put(key, value).await
    }

    pub async fn get(&self, key: Arc<Vec<u8>>) -> anyhow::Result<Option<Vec<u8>>> {
        self.try_rotate().await?;
        self.ctx.read().await.current_db.get(key).await
    }

    pub async fn delete(&self, key: Arc<Vec<u8>>) -> anyhow::Result<()> {
        self.try_rotate().await?;
        self.ctx.read().await.current_db.delete(key).await
    }

    pub async fn put_raw(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        self.try_rotate().await?;
        self.ctx.read().await.current_db.put_raw(key, value).await
    }

    pub async fn get_raw(&self, key: Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
        self.try_rotate().await?;
        self.ctx.read().await.current_db.get_raw(key).await
    }

    pub async fn delete_raw(&self, key: Vec<u8>) -> anyhow::Result<()> {
        self.try_rotate().await?;
        self.ctx.read().await.current_db.delete_raw(key).await
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

    pub async fn delete_json(&self, key: &impl Serialize) -> anyhow::Result<()> {
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

    pub async fn put_postcard(
        &self,
        key: &impl Serialize,
        value: &impl Serialize,
    ) -> anyhow::Result<()> {
        let bs = postcard::to_stdvec(value)?;
        let ks = postcard::to_stdvec(key)?;
        self.put_raw(ks, bs).await
    }

    pub async fn delete_postcard(&self, key: &impl Serialize) -> anyhow::Result<()> {
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

pub async fn test() -> anyhow::Result<()> {
    #[derive(Serialize, Deserialize, Debug)]
    struct Test {
        id: u32,
    }
    impl From<u32> for Test {
        fn from(id: u32) -> Self {
            Test { id }
        }
    }

    let interval = std::time::Duration::from_secs(1);
    let test = TimeWindowDBConfig {
        base_path: "/test/twdb".to_string(),
        ttl: Duration::seconds(5),
        delete_legacy: true,
        ty: TimeWindowType::Log,
    };
    let db = TimeWindowDB::new(test).await?;
    let mut src = Vec::<Test>::new();
    while src.len() < 20 {
        let start = std::time::Instant::now();
        src.push(Utc::now().second().into());
        let item = src.last().unwrap();
        db.put_json(&item.id, item).await?;
        if item.id % 2 == 0 {
            db.delete_json(&item.id).await?;
        }

        let mut results = Vec::<String>::new();
        for i in src.iter().rev() {
            let result = db.get_json::<Test>(&i.id).await?;
            if let Some(x) = result {
                results.push(format!(
                    "{}({})",
                    x.id,
                    db.get_current_path().await.display()
                ));
            } else {
                results.push(format!("{}(x)", i.id));
            }
        }
        println!("[twdb] json: {}", results.join(", "));

        let remaining = interval.saturating_sub(start.elapsed());
        tokio::time::sleep(remaining).await;
    }

    {
        let mut results = Vec::<String>::new();
        for i in src.iter().rev() {
            db.delete_json(&i.id).await?;
            let result = db.get_json::<Test>(&i.id).await?;
            if let Some(x) = result {
                results.push(format!("{}(o)", x.id));
            } else {
                results.push(format!("{}(x)", i.id));
            }
        }
        println!("[localdb] delete: {}", results.join(", "));
        src.clear();
    }

    while src.len() < 20 {
        let start = std::time::Instant::now();
        src.push(Utc::now().second().into());
        let item = src.last().unwrap();
        db.put_postcard(&item.id, item).await?;
        if item.id % 2 == 0 {
            db.delete_postcard(&item.id).await?;
        }

        let mut results = Vec::<String>::new();
        for i in src.iter().rev() {
            let result = db.get_postcard::<Test>(&i.id).await?;
            if let Some(x) = result {
                results.push(format!(
                    "{}({})",
                    x.id,
                    db.get_current_path().await.display()
                ));
            } else {
                results.push(format!("{}(x)", i.id));
            }
        }
        println!("[twdb] postcard: {}", results.join(", "));

        let remaining = interval.saturating_sub(start.elapsed());
        tokio::time::sleep(remaining).await;
    }

    {
        let mut results = Vec::<String>::new();
        for i in src.iter().rev() {
            db.delete_postcard(&i.id).await?;
            let result = db.get_postcard::<Test>(&i.id).await?;
            if let Some(x) = result {
                results.push(format!("{}(o)", x.id));
            } else {
                results.push(format!("{}(x)", i.id));
            }
        }
        println!("[localdb] delete: {}", results.join(", "));
        src.clear();
    }

    Ok(())
}
