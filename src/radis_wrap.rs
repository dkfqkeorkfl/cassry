use anyhow::{Context, Result};
use redis::{aio::ConnectionManager, AsyncCommands, Client};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Redis 연결 설정을 위한 구조체
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Redis 서버 URL (예: "redis://127.0.0.1:6379")
    pub url: String,
    /// 연결 타임아웃 (초)
    pub connection_timeout: u64,
    /// 명령 타임아웃 (초)
    pub command_timeout: u64,
    /// 재시도 횟수
    pub retry_count: u32,
    /// 재시도 간격 (밀리초)
    pub retry_interval: u64,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".to_string(),
            connection_timeout: 5,
            command_timeout: 3,
            retry_count: 3,
            retry_interval: 100,
        }
    }
}

/// Redis 연결을 관리하는 내부 구조체
struct RedisConnection {
    client: Client,
    manager: ConnectionManager,
}

impl RedisConnection {
    async fn new(config: &RedisConfig) -> Result<Self> {
        let client = Client::open(config.url.clone()).context("Failed to create Redis client")?;

        let manager = ConnectionManager::new(client.clone())
            .await
            .context("Failed to create connection manager")?;

        Ok(Self { client, manager })
    }

    async fn new_cluster(nodes: &[String], config: &RedisConfig) -> Result<Self> {
        let client =
            Client::open(nodes.join(",")).context("Failed to create Redis cluster client")?;

        let manager = ConnectionManager::new(client.clone())
            .await
            .context("Failed to create cluster connection manager")?;

        Ok(Self { client, manager })
    }
}

/// Redis 클라이언트 래퍼
#[derive(Clone)]
pub struct RedisClient {
    connection: Arc<RwLock<RedisConnection>>,
    config: RedisConfig,
}

impl RedisClient {
    /// 새로운 Redis 클라이언트를 생성합니다.
    pub async fn new(config: RedisConfig) -> Result<Self> {
        let connection = RedisConnection::new(&config).await?;
        Ok(Self {
            connection: Arc::new(RwLock::new(connection)),
            config,
        })
    }

    /// 클러스터 모드로 Redis 클라이언트를 생성합니다.
    pub async fn new_cluster(nodes: Vec<String>, config: RedisConfig) -> Result<Self> {
        let connection = RedisConnection::new_cluster(&nodes, &config).await?;
        Ok(Self {
            connection: Arc::new(RwLock::new(connection)),
            config,
        })
    }

    /// 키-값을 설정합니다.
    pub async fn set<K: AsRef<str>, V: AsRef<str>>(
        &self,
        key: K,
        value: V,
        expiration: Option<u64>,
    ) -> Result<()> {
        let mut conn = self.connection.write().await;
        let cmd = if let Some(exp) = expiration {
            conn.manager.set_ex::<_, _, ()>(key.as_ref(), value.as_ref(), exp)
        } else {
            conn.manager.set::<_, _, ()>(key.as_ref(), value.as_ref())
        };

        cmd.await.context("Failed to set key-value in Redis")?;

        Ok(())
    }

    /// 키의 값을 가져옵니다.
    pub async fn get<K: AsRef<str>>(&self, key: K) -> Result<Option<String>> {
        let mut conn = self.connection.write().await;
        conn.manager
            .get(key.as_ref())
            .await
            .context("Failed to get value from Redis")
    }

    /// 키를 삭제합니다.
    pub async fn del<K: AsRef<str>>(&self, key: K) -> Result<()> {
        let mut conn = self.connection.write().await;
        conn.manager
            .del::<_, ()>(key.as_ref())
            .await
            .context("Failed to delete key from Redis")?;

        Ok(())
    }

    /// 키가 존재하는지 확인합니다.
    pub async fn exists<K: AsRef<str>>(&self, key: K) -> Result<bool> {
        let mut conn = self.connection.write().await;
        conn.manager
            .exists(key.as_ref())
            .await
            .context("Failed to check key existence in Redis")
    }

    /// 키의 만료 시간을 설정합니다.
    pub async fn expire<K: AsRef<str>>(&self, key: K, expiration: i64) -> Result<bool> {
        let mut conn = self.connection.write().await;
        conn.manager
            .expire(key.as_ref(), expiration)
            .await
            .context("Failed to set expiration in Redis")
    }

    /// 키의 TTL을 가져옵니다.
    pub async fn ttl<K: AsRef<str>>(&self, key: K) -> Result<i64> {
        let mut conn = self.connection.write().await;
        conn.manager
            .ttl(key.as_ref())
            .await
            .context("Failed to get TTL from Redis")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_redis_operations() -> Result<()> {
        let config = RedisConfig::default();
        let client = RedisClient::new(config).await?;

        // Ping 테스트
        // let pong = client.ping().await?;
        // assert_eq!(pong, "PONG");

        // Set/Get 테스트
        client.set("test_key", "test_value", None).await?;
        let value = client.get("test_key").await?;
        assert_eq!(value, Some("test_value".to_string()));

        // Expiration 테스트
        client
            .set("expire_key", "expire_value", Some(1))
            .await?;
        let exists = client.exists("expire_key").await?;
        assert!(exists);

        // TTL 테스트
        let ttl = client.ttl("expire_key").await?;
        assert!(ttl > 0);

        // Delete 테스트
        client.del("test_key").await?;
        let exists = client.exists("test_key").await?;
        assert!(!exists);

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_operations() -> Result<()> {
        let config = RedisConfig::default();
        let client = RedisClient::new(config).await?;
        let client_clone = client.clone();

        // 여러 태스크에서 동시에 작업 수행
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let client = client_clone.clone();
                tokio::spawn(async move {
                    let key = format!("concurrent_key_{}", i);
                    client.set(&key, "value", None).await?;
                    let value = client.get(&key).await?;
                    assert_eq!(value, Some("value".to_string()));
                    client.del(&key).await?;
                    Result::<()>::Ok(())
                })
            })
            .collect();

        // 모든 태스크 완료 대기
        for handle in handles {
            handle.await??;
        }

        Ok(())
    }
}
