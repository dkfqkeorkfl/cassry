use chrono::{DateTime, Timelike, Utc};
use keyed_lock::r#async::KeyedLock;
use rocksdb::{IteratorMode, WriteBatch as RocksWriteBatch};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::usize;
use tokio::task::{self, JoinHandle};

use super::localdb::*;
use super::serialization::{datetime_milliseconds, duration_milliseconds};

/// TTL 인덱스 내부 블로킹 구현체
///
/// 이 구조체는 TTL 인덱스를 관리하며, 실제 TTL DB가 아닙니다.
/// RocksDB의 정렬 기능을 이용하여 expire_at(timestamp) → key 형태의 인덱스를 관리합니다.
/// Iterator를 통해 앞에서부터 만료된 항목들을 순차적으로 스캔하여
/// 키에 해당하는 데이터를 AccessDB에서 삭제하는 데 사용됩니다.

#[derive(Serialize, Deserialize)]
struct IndexSer {
    #[serde(with = "datetime_milliseconds")]
    pub expire_at: DateTime<Utc>,
    pub key: Arc<Vec<u8>>,
}

struct TTLIndex {
    db: Arc<LocalDB>,
}

impl TTLIndex {
    async fn new(path: String) -> anyhow::Result<Self> {
        let db = LocalDB::new(config::IndexTTL::new(path)).await?;
        Ok(Self { db: db.into() })
    }

    /// TTL 인덱스에 항목을 추가합니다.
    ///
    /// 지정된 만료 시간과 키를 조합하여 RocksDB에 저장합니다.
    /// 실제 데이터는 AccessDB에 저장되며, 이 인덱스는 만료 시간 기반으로
    /// 데이터를 찾기 위한 용도로만 사용됩니다.
    ///
    /// # Arguments
    /// * `expire_at` - 만료 시간
    /// * `key` - AccessDB의 키
    ///
    /// # RocksDB 저장 형식
    /// - **Key**: timestamp (8 bytes, big-endian) + key (variable length)
    /// - **Value**: 빈 값 (키 구별을 위한 용도만)
    async fn insert(&self, expire_at: DateTime<Utc>, key: Arc<Vec<u8>>) -> anyhow::Result<()> {
        let index_ser = IndexSer { expire_at, key };
        let index_ser_bytes = postcard::to_stdvec(&index_ser)?;
        self.db.put_raw(index_ser_bytes, Default::default()).await
    }

    /// TTL 인덱스에서 항목을 제거합니다.
    ///
    /// 지정된 만료 시간과 키에 해당하는 인덱스 항목을 삭제합니다.
    /// AccessDB의 데이터는 삭제하지 않으며, 인덱스 항목만 제거합니다.
    ///
    /// # Arguments
    /// * `expire_at` - 만료 시간 (삭제할 항목의 만료 시간)
    /// * `key` - AccessDB의 키
    async fn remove(&self, expire_at: DateTime<Utc>, key: Arc<Vec<u8>>) -> anyhow::Result<()> {
        let index_ser = IndexSer { expire_at, key };
        let index_ser_bytes = postcard::to_stdvec(&index_ser)?;
        self.db.delete_raw(index_ser_bytes).await
    }

    async fn remove_by_key(&self, combined_key: Arc<Vec<u8>>) -> anyhow::Result<()> {
        self.db.delete(combined_key).await
    }

    /// TTL 인덱스에서 항목이 존재하는지 확인합니다.
    ///
    /// # Arguments
    /// * `combined_key` - RocksDB 키 (timestamp + AccessDB key 조합)
    ///
    /// # Returns
    /// * `Ok(true)` - 항목이 존재하는 경우
    /// * `Ok(false)` - 항목이 존재하지 않는 경우
    async fn includes(&self, combined_key: Arc<Vec<u8>>) -> anyhow::Result<bool> {
        Ok(self.db.get(combined_key).await?.is_some())
    }

    /// WriteBatch를 커밋합니다.
    ///
    /// 배치에 등록된 모든 작업(insert, delete)을 원자적으로 수행합니다.
    ///
    /// # Arguments
    /// * `batch` - 커밋할 WriteBatch
    ///
    /// # Returns
    /// 커밋 성공 시 `Ok(())`, 실패 시 에러 반환
    async fn commit(&self, batch: WriteBatch) -> anyhow::Result<()> {
        self.db.commit(batch.into()).await
    }

    /// 만료된 항목들을 스캔합니다.
    ///
    /// RocksDB의 정렬 기능을 이용하여 앞에서부터 순차적으로 만료된 항목들을 스캔합니다.
    /// Iterator를 통해 `now` 이하의 `expire_at`을 가진 항목들을 반환합니다.
    /// timestamp가 정렬되어 있으므로 만료되지 않은 항목을 만나면 즉시 중단합니다.
    ///
    /// # Arguments
    /// * `now` - 현재 시간 (이 시간 이하의 expire_at을 가진 항목이 만료됨)
    ///
    /// # Returns
    /// * `Ok(HashMap)` - 만료된 항목들의 맵 (RocksDB key → AccessDB key)
    async fn scan_expired(
        &self,
        now: DateTime<Utc>,
        limit: Option<usize>,
    ) -> anyhow::Result<HashMap<Arc<Vec<u8>>, IndexSer>> {
        // Iterator를 사용하여 앞에서부터 순차적으로 만료된 항목들을 스캔
        // timestamp가 정렬되어 있으므로 now_timestamp 이하인 항목들만 수집
        let limit = limit.unwrap_or(usize::MAX);
        let ctx = (self.db.clone(), limit);

        let results = tokio::task::spawn_blocking(move || {
            let (db, limit) = ctx;
            let mut results: HashMap<Arc<Vec<u8>>, IndexSer> = HashMap::new();
            let iter = db.raw().iterator(IteratorMode::Start);

            for item in iter {
                let (db_key, _) = item?;
                let index_ser = postcard::from_bytes::<IndexSer>(&db_key)?;

                // now_timestamp 이하인 경우에만 추가
                if index_ser.expire_at <= now {
                    results.insert(db_key.into_vec().into(), index_ser);
                } else {
                    // timestamp가 정렬되어 있으므로 이후 항목은 모두 만료되지 않음
                    break;
                }

                if results.len() >= limit {
                    break;
                }
            }
            anyhow::Ok(results)
        })
        .await??;
        Ok(results)
    }
}

/// TTL 인덱스용 WriteBatch 래퍼
///
/// RocksDB의 `WriteBatch`를 래핑하여 TTL 인덱스 작업을 편리하게 수행할 수 있게 합니다.
/// `insert`와 `remove` 메서드를 통해 expire_at과 key를 전달하면 내부적으로
/// RocksDB 키 형식으로 변환하여 배치 작업에 추가합니다.
///
/// `Into<RocksWriteBatch>` 트레이트를 구현하여 `TTLIndexInner::commit()`에
/// 직접 전달할 수 있습니다.
#[derive(Default)]
struct WriteBatch {
    batch: RocksWriteBatch,
}

impl WriteBatch {
    /// TTL 인덱스에 항목을 추가합니다.
    ///
    /// 배치에 항목 추가 작업을 등록합니다. 실제 커밋은 `TTLIndexInner::commit()`을
    /// 호출할 때 수행됩니다.
    ///
    /// # Arguments
    /// * `expire_at` - 만료 시간
    /// * `key` - AccessDB의 키
    fn insert(&mut self, expire_at: DateTime<Utc>, key: Arc<Vec<u8>>) -> anyhow::Result<()> {
        let index_ser = IndexSer { expire_at, key };
        let index_ser_bytes = postcard::to_stdvec(&index_ser)?;
        self.batch.put(&index_ser_bytes, &[]);
        Ok(())
    }

    /// TTL 인덱스에서 항목을 제거합니다.
    ///
    /// 배치에 항목 삭제 작업을 등록합니다. 실제 커밋은 `TTLIndexInner::commit()`을
    /// 호출할 때 수행됩니다.
    ///
    /// # Arguments
    /// * `expire_at` - 만료 시간
    /// * `key` - AccessDB의 키
    fn remove(&mut self, expire_at: DateTime<Utc>, key: Arc<Vec<u8>>) -> anyhow::Result<()> {
        let index_ser = IndexSer { expire_at, key };
        let index_ser_bytes = postcard::to_stdvec(&index_ser)?;
        self.batch.delete(&index_ser_bytes);
        Ok(())
    }
}

impl Into<RocksWriteBatch> for WriteBatch {
    fn into(self) -> RocksWriteBatch {
        self.batch
    }
}

/// 만료 시간 타입
///
/// 데이터의 만료 정책을 정의합니다.
///
/// - **Idle**: 유휴 상태 기반 만료
///   - `updated_at`으로부터 지정된 `Duration`만큼 경과하면 만료
///   - 데이터 조회 시 `updated_at`이 갱신되어 만료 시간이 연장됨
///   - 캐시처럼 사용 빈도가 높은 데이터에 적합
///
/// - **Live**: 절대 시간 기반 만료
///   - 지정된 `DateTime<Utc>` 시간이 되면 만료
///   - 조회해도 만료 시간이 변경되지 않음
///   - 특정 시점에 만료되어야 하는 데이터에 적합
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExpiredAt {
    #[serde(with = "duration_milliseconds")]
    Idle(chrono::Duration),
    #[serde(with = "datetime_milliseconds")]
    Live(DateTime<Utc>),
}

impl ExpiredAt {
    /// 현재 시점을 기준으로 실제 만료 시간을 계산합니다.
    ///
    /// # Arguments
    /// * `now` - 현재 시간 (Idle 타입의 경우 `updated_at`과의 차이 계산에 사용)
    ///
    /// # Returns
    /// 계산된 만료 시간
    pub fn expire_at(&self, now: &DateTime<Utc>) -> DateTime<Utc> {
        match self {
            ExpiredAt::Idle(duration) => *now + *duration,
            ExpiredAt::Live(dt) => *dt,
        }
    }
}

/// AccessDB에 저장되는 데이터 스키마 (버전 2025121601)
///
/// `postcard`로 직렬화되어 AccessDB에 저장됩니다.
///
/// # 필드
/// * `ver` - 스키마 버전 (마이그레이션 시 사용)
/// * `expired_at` - 만료 정책 (Idle 또는 Live)
/// * `updated_at` - 마지막 업데이트 시간 (Idle 타입의 만료 시간 계산에 사용)
/// * `value` - 실제 저장된 데이터 (바이너리)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DBSchema2025121601 {
    pub ver: u64,
    pub expired_at: ExpiredAt,
    #[serde(with = "datetime_milliseconds")]
    pub updated_at: DateTime<Utc>,
    pub value: Vec<u8>,
}

impl DBSchema2025121601 {
    /// `updated_at`을 기준으로 실제 만료 시간을 계산합니다.
    ///
    /// Idle 타입인 경우 `updated_at + duration`을 반환하고,
    /// Live 타입인 경우 저장된 절대 시간을 반환합니다.
    pub fn expire_at(&self) -> DateTime<Utc> {
        self.expired_at.expire_at(&self.updated_at)
    }
}

/// 현재 사용 중인 데이터베이스 스키마 타입
type DBSchema = DBSchema2025121601;

/// LocalDBTTL의 내부 컨텍스트
///
/// AccessDB, TTL 인덱스, 그리고 키별 락을 관리합니다.
struct Context {
    access_db: LocalDB,
    ttl_index: TTLIndex,
    locks: KeyedLock<Arc<Vec<u8>>>,
}

/// TTL(Time To Live) 기능이 있는 로컬 데이터베이스
///
/// RocksDB를 기반으로 하며, 만료 시간이 있는 데이터를 자동으로 관리합니다.
/// AccessDB와 TTL 인덱스를 별도로 관리하여 효율적인 만료 처리를 수행합니다.
///
/// # 구조
/// - **AccessDB**: 실제 데이터를 저장하는 RocksDB 인스턴스
/// - **TTL Index**: 만료 시간 기반 인덱스를 관리하는 RocksDB 인스턴스
/// - **백그라운드 태스크**: 주기적으로 만료된 항목을 스캔하고 삭제
///
/// # 사용 예시
/// ```rust,no_run
/// use cassry::localdb_ttl::LocalDBTTL;
/// use cassry::localdb_ttl::ExpiredAt;
/// use chrono::{Utc, Duration};
///
/// # async fn example() -> anyhow::Result<()> {
/// let db = LocalDBTTL::new(
///     "db_path".to_string(),
///     std::time::Duration::from_secs(60)
/// ).await?;
///
/// // Idle 타입: 조회 시마다 만료 시간이 연장됨
/// db.put(
///     b"key1".to_vec(),
///     b"value1".to_vec(),
///     ExpiredAt::Idle(Duration::hours(1))
/// ).await?;
///
/// // Live 타입: 절대 시간에 만료
/// db.put(
///     b"key2".to_vec(),
///     b"value2".to_vec(),
///     ExpiredAt::Live(Utc::now() + Duration::hours(24))
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub struct LocalDBTTL {
    ctx: Arc<Context>,
    _task: JoinHandle<()>,
}

impl LocalDBTTL {
    /// 데이터베이스 스키마 버전
    pub const VERSION: u64 = 2025121601;

    /// 새로운 LocalDBTTL 인스턴스를 생성합니다.
    ///
    /// # Arguments
    /// * `path` - 데이터베이스 저장 경로
    /// * `interval` - 만료된 항목 삭제 작업 실행 간격
    ///
    /// # 설명
    /// 지정된 경로에 2개의 RocksDB 인스턴스를 생성합니다:
    /// - `path`: AccessDB용 (실제 데이터 저장)
    /// - `path/index`: TTL 인덱스용 (만료 시간 관리)
    ///
    /// 백그라운드 태스크가 주기적으로 만료된 항목을 스캔하고 삭제합니다.
    pub async fn new(
        path: String,
        interval: std::time::Duration,
        limit_scan: Option<usize>,
    ) -> anyhow::Result<Self> {
        let index_path = format!("{}/index", path);

        let access_db = LocalDB::new(config::General::new(path.clone())).await?;
        let ttl_index = TTLIndex::new(index_path).await?;

        let context = Arc::new(Context {
            access_db,
            ttl_index,
            locks: KeyedLock::new(),
        });

        let ctx = (Arc::downgrade(&context), interval, limit_scan);
        let task = task::spawn(async move {
            let (context, interval, limit_scan) = ctx;
            loop {
                let start = std::time::Instant::now();
                if let Some(context) = context.upgrade() {
                    if let Err(e) = Self::delete_expired(context, limit_scan).await {
                        crate::_private_logger::error(format!("Error in delete_expired: {}", e));
                    }
                } else {
                    break;
                }

                let remaining = interval.saturating_sub(start.elapsed());
                tokio::time::sleep(remaining).await;
            }
        });

        Ok(Self {
            ctx: context,
            _task: task,
        })
    }

    /// 만료된 항목들을 스캔하고 삭제합니다.
    ///
    /// TTL 인덱스에서 만료된 항목을 찾아 AccessDB에서 해당 데이터를 삭제하고,
    /// TTL 인덱스에서도 제거합니다. 이미 삭제된 항목은 건너뜁니다.
    ///
    /// # Arguments
    /// * `ctx` - LocalDBTTL 컨텍스트
    async fn delete_expired(ctx: Arc<Context>, limit_scan: Option<usize>) -> anyhow::Result<()> {
        let expired = ctx.ttl_index.scan_expired(Utc::now(), limit_scan).await?;
        for (key, index) in expired {
            let _guard = ctx.locks.lock(index.key.clone()).await;
            if ctx.ttl_index.includes(key.clone()).await? {
                ctx.access_db.delete(index.key.clone()).await?;
                ctx.ttl_index.remove_by_key(key).await?;
            }
        }
        Ok(())
    }

    /// 데이터를 저장합니다.
    ///
    /// 기존 키가 존재하는 경우 값을 업데이트하고, 만료 시간이 변경된 경우
    /// TTL 인덱스도 업데이트합니다. 새로운 키인 경우 TTL 인덱스에 항목을 추가합니다.
    ///
    /// # Arguments
    /// * `key` - 저장할 키
    /// * `value` - 저장할 값
    /// * `expired_at` - 만료 시간 설정 (Idle 또는 Live)
    ///
    /// # Returns
    /// 저장 성공 시 `Ok(())`, 실패 시 에러 반환
    pub async fn put(
        &self,
        key: Arc<Vec<u8>>,
        value: Vec<u8>,
        expired_at: ExpiredAt,
    ) -> anyhow::Result<()> {
        let new_item = DBSchema {
            ver: Self::VERSION,
            expired_at: expired_at.clone(),
            updated_at: Utc::now(),
            value,
        };
        let serialized = postcard::to_stdvec(&new_item)?;

        let mut batch = WriteBatch::default();
        let _guard = self.ctx.locks.lock(key.clone()).await;

        if let Some(bytes) = self.ctx.access_db.get(key.clone()).await? {
            let old_item: DBSchema = postcard::from_bytes(&bytes)?;
            if old_item.expire_at() != new_item.expire_at() {
                batch.remove(old_item.expire_at(), key.clone())?;
                batch.insert(new_item.expire_at(), key.clone())?;
                self.ctx.ttl_index.commit(batch).await?;
            }
        } else {
            self.ctx
                .ttl_index
                .insert(new_item.expire_at(), key.clone())
                .await?;
        }

        // AccessDB에 저장
        self.ctx.access_db.put(key, serialized.into()).await?;

        Ok(())
    }

    /// 데이터를 조회합니다.
    ///
    /// 만료된 항목인 경우 자동으로 삭제하고 `None`을 반환합니다.
    /// `ExpiredAt::Idle` 타입인 경우 조회 시 `updated_at`을 현재 시간으로 업데이트하여
    /// 만료 시간을 연장합니다. 이 경우 TTL 인덱스도 함께 업데이트됩니다.
    ///
    /// # Arguments
    /// * `key` - 조회할 키
    ///
    /// # Returns
    /// * `Ok(Some(value))` - 데이터가 존재하고 만료되지 않은 경우
    /// * `Ok(None)` - 데이터가 존재하지 않거나 만료된 경우
    /// * `Err(e)` - 조회 중 오류 발생
    pub async fn get(&self, key: Arc<Vec<u8>>) -> anyhow::Result<Option<Vec<u8>>> {
        let _guard = self.ctx.locks.lock(key.clone()).await;
        // AccessDB에서 조회
        let mut db_value = match self.ctx.access_db.get(key.clone()).await? {
            Some(data) => postcard::from_bytes::<DBSchema>(&data)?,
            None => return Ok(None),
        };

        let now = Utc::now();
        if now > db_value.expire_at() {
            self.ctx.access_db.delete(key.clone()).await?;

            self.ctx.ttl_index.remove(db_value.expire_at(), key).await?;
            return Ok(None);
        }

        // idle 상태이고 아직 만료되지 않았으면 updated_at 업데이트
        match &db_value.expired_at {
            ExpiredAt::Idle(_) => {
                let previous_expire_at = db_value.expire_at();

                db_value.updated_at = now;
                self.ctx
                    .access_db
                    .put(key.clone(), postcard::to_stdvec(&db_value)?.into())
                    .await?;
                if previous_expire_at != db_value.expire_at() {
                    let mut batch = WriteBatch::default();
                    batch.remove(previous_expire_at, key.clone())?;
                    batch.insert(db_value.expire_at(), key)?;
                    self.ctx.ttl_index.commit(batch).await?;
                }

                Ok(Some(db_value.value))
            }
            ExpiredAt::Live(_) => Ok(Some(db_value.value)),
        }
    }

    /// 데이터를 삭제합니다.
    ///
    /// AccessDB에서 데이터를 삭제하고, 해당 항목이 TTL 인덱스에 존재하는 경우
    /// TTL 인덱스에서도 제거합니다. 키가 존재하지 않는 경우에도 에러 없이 성공으로 처리됩니다.
    ///
    /// # Arguments
    /// * `key` - 삭제할 키
    ///
    /// # Returns
    /// 삭제 성공 시 `Ok(())`, 실패 시 에러 반환
    pub async fn delete(&self, key: Arc<Vec<u8>>) -> anyhow::Result<()> {
        let _guard = self.ctx.locks.lock(key.clone()).await;
        // AccessDB에서 조회하여 expired_at 확인
        if let Some(serialized) = self.ctx.access_db.get(key.clone()).await? {
            let db_value: DBSchema = postcard::from_bytes(&serialized)?;
            self.ctx.access_db.delete(key.clone()).await?;

            // TTL 인덱스에서 제거
            self.ctx.ttl_index.remove(db_value.expire_at(), key).await?;
        }

        Ok(())
    }

    pub async fn put_raw(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        expired_at: ExpiredAt,
    ) -> anyhow::Result<()> {
        self.put(key.into(), value.into(), expired_at).await
    }

    pub async fn get_raw(&self, key: Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
        self.get(key.into()).await
    }

    pub async fn delete_raw(&self, key: Vec<u8>) -> anyhow::Result<()> {
        self.delete(key.into()).await
    }

    pub async fn put_json(
        &self,
        key: &impl Serialize,
        value: &impl Serialize,
        expired_at: ExpiredAt,
    ) -> anyhow::Result<()> {
        let key_str = serde_json::to_string(key)?;
        let json_str = serde_json::to_string(value)?;
        self.put_raw(key_str.into_bytes(), json_str.into_bytes(), expired_at)
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

    pub async fn put_bson(
        &self,
        key: &impl Serialize,
        value: &impl Serialize,
        expired_at: ExpiredAt,
    ) -> anyhow::Result<()> {
        let ks = bson_key(key)?;
        let vs = bson::ser::serialize_to_vec(value)?;
        self.put_raw(ks, vs, expired_at).await
    }

    pub async fn delete_bson(&self, key: &impl Serialize) -> anyhow::Result<()> {
        let ks = bson_key(key)?;
        self.delete_raw(ks).await
    }

    pub async fn get_bson<T>(&self, key: &impl Serialize) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let ks = bson_key(key)?;
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
        expired_at: ExpiredAt,
    ) -> anyhow::Result<()> {
        let bs = postcard::to_stdvec(value)?;
        let ks = postcard::to_stdvec(key)?;
        self.put_raw(ks, bs, expired_at).await
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

    /// 데이터베이스 기본 경로를 반환합니다.
    ///
    /// # Returns
    /// AccessDB가 저장된 경로 (TTL 인덱스는 `{path}/index`에 저장됨)
    pub fn get_path(&self) -> &std::path::Path {
        self.ctx.access_db.get_path()
    }

    /// LocalDBTTL 인스턴스가 생성된 시간을 반환합니다.
    ///
    /// # Returns
    /// 인스턴스 생성 시점의 UTC 시간
    pub fn get_created_at(&self) -> &DateTime<Utc> {
        &self.ctx.access_db.get_created_at()
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
    let db = LocalDBTTL::new(
        "test_ttldb_live".to_string(),
        std::time::Duration::from_secs(1),
        None,
    )
    .await?;
    let mut src = Vec::<Test>::new();
    while src.len() < 20 {
        let start = std::time::Instant::now();
        src.push(Utc::now().second().into());
        let item = src.last().unwrap();
        db.put_json(
            &item.id,
            item,
            ExpiredAt::Live(Utc::now() + chrono::Duration::seconds(5)),
        )
        .await?;
        if item.id % 2 == 0 {
            db.delete_json(&item.id).await?;
        }

        let mut results = Vec::<String>::new();
        for i in src.iter().rev() {
            let result = db.get_json::<Test>(&i.id).await?;
            if let Some(x) = result {
                results.push(format!("{}(o)", x.id));
            } else {
                results.push(format!("{}(x)", i.id));
            }
        }
        println!("[ttldb] json: {}", results.join(", "));

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
        println!("[ttldb] delete: {}", results.join(", "));
        src.clear();
    }

    while src.len() < 20 {
        let start = std::time::Instant::now();
        src.push(Utc::now().second().into());
        let item = src.last().unwrap();
        db.put_bson(
            &item.id,
            item,
            ExpiredAt::Live(Utc::now() + chrono::Duration::seconds(5)),
        )
        .await?;
        if item.id % 2 == 0 {
            db.delete_bson(&item.id).await?;
        }

        let mut results = Vec::<String>::new();
        for i in src.iter().rev() {
            let result = db.get_bson::<Test>(&i.id).await?;
            if let Some(x) = result {
                results.push(format!("{}(o)", x.id));
            } else {
                results.push(format!("{}(x)", i.id));
            }
        }
        println!("[ttldb] bson: {}", results.join(", "));

        let remaining = interval.saturating_sub(start.elapsed());
        tokio::time::sleep(remaining).await;
    }

    {
        let mut results = Vec::<String>::new();
        for i in src.iter().rev() {
            db.delete_bson(&i.id).await?;
            let result = db.get_bson::<Test>(&i.id).await?;
            if let Some(x) = result {
                results.push(format!("{}(o)", x.id));
            } else {
                results.push(format!("{}(x)", i.id));
            }
        }
        println!("[ttldb] delete: {}", results.join(", "));
        src.clear();
    }

    while src.len() < 20 {
        let start = std::time::Instant::now();
        src.push(Utc::now().second().into());
        let item = src.last().unwrap();
        db.put_postcard(
            &item.id,
            item,
            ExpiredAt::Live(Utc::now() + chrono::Duration::seconds(5)),
        )
        .await?;
        if item.id % 2 == 0 {
            db.delete_postcard(&item.id).await?;
        }

        let mut results = Vec::<String>::new();
        for i in src.iter().rev() {
            let result = db.get_postcard::<Test>(&i.id).await?;
            if let Some(x) = result {
                results.push(format!("{}(o)", x.id));
            } else {
                results.push(format!("{}(x)", i.id));
            }
        }
        println!("[ttldb] postcard: {}", results.join(", "));

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
        println!("[ttldb] delete: {}", results.join(", "));
        src.clear();
    }















    
    while src.len() < 20 {
        let start = std::time::Instant::now();
        src.push(Utc::now().second().into());
        let item = src.last().unwrap();
        db.put_json(
            &item.id,
            item,
            ExpiredAt::Idle(chrono::Duration::seconds(5)),
        )
        .await?;

        let mut results = Vec::<String>::new();
        for (i, item) in src.iter().rev().enumerate() {
            if i % 2 == 0 {
                continue;
            }

            let result = db.get_json::<Test>(&item.id).await?;
            if let Some(x) = result {
                results.push(format!("{}(o)", x.id));
            } else {
                results.push(format!("{}(x)", item.id));
            }
        }
        println!("[ttldb] json: {}", results.join(", "));

        let remaining = interval.saturating_sub(start.elapsed());
        tokio::time::sleep(remaining).await;
    }

    {
        let mut results = Vec::<String>::new();
        for i in src.iter().rev() {
            let result = db.get_json::<Test>(&i.id).await?;
            if let Some(x) = result {
                results.push(format!("{}(o)", x.id));
            } else {
                results.push(format!("{}(x)", i.id));
            }
        }
        println!("[ttldb] idle: {}", results.join(", "));
    }
    Ok(())
}
