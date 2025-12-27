use chrono::{DateTime, Utc};
use keyed_lock::r#async::KeyedLock;
use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, DBWithThreadMode, IteratorMode, MultiThreaded,
    Options, ReadOptions, WriteBatch as RocksWriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::{path::Path, sync::Arc};
use tokio::task::{self, JoinHandle};

use super::localdb::LocalDB;
use super::serialization::{datetime_milliseconds, duration_milliseconds};
/// TTL 인덱스 내부 블로킹 구현체
///
/// 이 구조체는 TTL 인덱스를 관리하며, 실제 TTL DB가 아닙니다.
/// RocksDB의 정렬 기능을 이용하여 expire_at(timestamp) → key 형태의 인덱스를 관리합니다.
/// Iterator를 통해 앞에서부터 만료된 항목들을 순차적으로 스캔하여
/// 키에 해당하는 데이터를 AccessDB에서 삭제하는 데 사용됩니다.
struct TTLIndexInner {
    db: DBWithThreadMode<MultiThreaded>,
    write_opts: WriteOptions,
    read_opts: ReadOptions,
}

impl TTLIndexInner {
    fn new(path: String) -> anyhow::Result<Self> {
        let p = Path::new(&path);

        // TTL 인덱스용 최적화 설정: range scan + delete-heavy 워크로드
        //
        // 워크로드 특성:
        // - 앞에서부터 순차적으로 만료된 항목들을 스캔 (Iterator 기반)
        // - 작은 크기의 key-value (timestamp 8 bytes + key)
        // - delete-heavy (만료된 항목들을 자주 삭제)
        // - 정렬된 순회가 중요

        let mut block_opts = BlockBasedOptions::default();
        // Block cache 작게 설정 (range scan 위주이므로 큰 캐시가 불필요)
        let cache = Cache::new_lru_cache(16 * 1024 * 1024); // 16MB
        block_opts.set_block_cache(&cache);

        // Bloom filter OFF (range scan 위주이므로 도움이 안 됨)
        block_opts.set_bloom_filter(0.0, false);

        // Index와 filter block을 캐시하지 않음 (메모리 절약)
        block_opts.set_cache_index_and_filter_blocks(false);

        // 데이터베이스 옵션 설정
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_block_based_table_factory(&block_opts);

        // 성능 최적화 설정
        options.set_max_background_jobs(4);
        options.set_bytes_per_sync(1024 * 1024); // 1MB

        // Memtable 크기 작게 설정 (delete-heavy 워크로드, 작은 항목들)
        options.set_write_buffer_size(16 * 1024 * 1024); // 16MB
        options.set_max_write_buffer_number(2);
        options.set_min_write_buffer_number_to_merge(1);

        // Level size 설정 (작은 항목들이 많으므로 작게 설정)
        options.set_max_bytes_for_level_base(64 * 1024 * 1024); // 64MB
        options.set_target_file_size_base(16 * 1024 * 1024); // 16MB

        // 압축 OFF (range scan 위주이므로 압축 해제 오버헤드 회피)
        options.set_compression_type(DBCompressionType::None);
        options.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::None,
        ]);

        // Direct I/O OFF (range scan 위주이므로 일반 I/O가 더 나음)
        options.set_use_direct_reads(false);
        options.set_use_direct_io_for_flush_and_compaction(false);
        // Compaction readahead OFF (delete-heavy 워크로드)
        options.set_compaction_readahead_size(0);

        // 파일 관리 설정
        options.set_max_open_files(-1); // 제한 없음
        options.set_keep_log_file_num(10); // WAL 로그 파일 개수 제한
        options.set_max_manifest_file_size(128 * 1024 * 1024); // 128MB

        // 데이터 안정성 설정
        options.set_paranoid_checks(true); // 체크섬 검증 활성화
        options.set_manual_wal_flush(false); // 자동 WAL flush
        options.set_atomic_flush(true); // 원자적 flush

        // Write 옵션 설정
        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(false); // WAL 활성화 (데이터 안정성)
        write_opts.set_sync(false); // 비동기 쓰기 (성능 우선)

        // Read 옵션 설정
        let mut read_opts = ReadOptions::default();
        read_opts.set_verify_checksums(true); // 체크섬 검증
        read_opts.set_async_io(false); // 동기 I/O (range scan에는 동기 I/O가 적합)
        read_opts.set_readahead_size(0); // Iterator 기반 순차 스캔이므로 readahead 불필요

        let db = DBWithThreadMode::<MultiThreaded>::open(&options, p)?;

        Ok(TTLIndexInner {
            db,
            write_opts,
            read_opts,
        })
    }

    /// timestamp와 key를 조합하여 RocksDB key로 변환
    ///
    /// 형식: timestamp (8 bytes, big-endian) + key (variable length)
    /// RocksDB의 정렬 순서를 이용하기 위해 timestamp를 big-endian으로 변환합니다.
    /// 이렇게 하면 timestamp가 작은 값부터 큰 값 순으로 정렬되고,
    /// 같은 timestamp의 경우 key 순으로 정렬됩니다.
    fn timestamp_and_key_to_db_key(expire_at: &DateTime<Utc>, key: &[u8]) -> Vec<u8> {
        let mut db_key = expire_at.timestamp_millis().to_be_bytes().to_vec();
        db_key.extend_from_slice(key);
        db_key
    }

    /// RocksDB key에서 timestamp를 추출
    ///
    /// 형식: timestamp (8 bytes, big-endian) + key (variable length)
    fn db_key_to_timestamp(db_key: &[u8]) -> anyhow::Result<i64> {
        if db_key.len() < 8 {
            return Err(anyhow::anyhow!(
                "Invalid key length: expected at least 8 bytes, got {}",
                db_key.len()
            ));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&db_key[..8]);
        Ok(i64::from_be_bytes(bytes))
    }

    /// RocksDB key에서 원본 key를 추출
    ///
    /// 형식: timestamp (8 bytes, big-endian) + key (variable length)
    fn db_key_to_key(db_key: &[u8]) -> Vec<u8> {
        if db_key.len() <= 8 {
            Vec::new()
        } else {
            db_key[8..].to_vec()
        }
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
    fn insert(&self, expire_at: DateTime<Utc>, key: &[u8]) -> anyhow::Result<()> {
        let db_key = Self::timestamp_and_key_to_db_key(&expire_at, key);
        // value는 빈 값으로 저장 (키 구별을 위한 용도)
        self.db
            .put_opt(&db_key, &[], &self.write_opts)
            .map_err(anyhow::Error::from)
    }

    /// TTL 인덱스에서 항목을 제거합니다.
    ///
    /// 지정된 만료 시간과 키에 해당하는 인덱스 항목을 삭제합니다.
    /// AccessDB의 데이터는 삭제하지 않으며, 인덱스 항목만 제거합니다.
    ///
    /// # Arguments
    /// * `expire_at` - 만료 시간 (삭제할 항목의 만료 시간)
    /// * `key` - AccessDB의 키
    fn remove(&self, expire_at: DateTime<Utc>, key: &[u8]) -> anyhow::Result<()> {
        let db_key = Self::timestamp_and_key_to_db_key(&expire_at, key);
        self.remove_by_key(&db_key)
    }

    fn remove_by_key(&self, combined_key: &[u8]) -> anyhow::Result<()> {
        self.db
            .delete_opt(combined_key, &self.write_opts)
            .map_err(anyhow::Error::from)
    }

    /// TTL 인덱스에서 항목이 존재하는지 확인합니다.
    ///
    /// # Arguments
    /// * `combined_key` - RocksDB 키 (timestamp + AccessDB key 조합)
    ///
    /// # Returns
    /// * `Ok(true)` - 항목이 존재하는 경우
    /// * `Ok(false)` - 항목이 존재하지 않는 경우
    fn includes(&self, combined_key: &[u8]) -> anyhow::Result<bool> {
        match self.db.get_opt(&combined_key, &self.read_opts)? {
            Some(_) => Ok(true),
            None => Ok(false),
        }
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
    fn commit(&self, batch: WriteBatch) -> anyhow::Result<()> {
        self.db
            .write_opt(batch.into(), &self.write_opts)
            .map_err(anyhow::Error::from)
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
    fn scan_expired(&self, now: DateTime<Utc>) -> anyhow::Result<HashMap<Vec<u8>, Vec<u8>>> {
        let now = now.timestamp_millis();

        let mut results = HashMap::new();

        // Iterator를 사용하여 앞에서부터 순차적으로 만료된 항목들을 스캔
        // timestamp가 정렬되어 있으므로 now_timestamp 이하인 항목들만 수집
        let iter = self.db.iterator(IteratorMode::Start);

        for item in iter {
            let (db_key, _) = item?;
            let timestamp = Self::db_key_to_timestamp(&db_key)?;

            // now_timestamp 이하인 경우에만 추가
            if timestamp <= now {
                let key = Self::db_key_to_key(&db_key);
                results.insert(db_key.into_vec(), key);
            } else {
                // timestamp가 정렬되어 있으므로 이후 항목은 모두 만료되지 않음
                break;
            }
        }

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
    fn insert(&mut self, expire_at: &DateTime<Utc>, key: &[u8]) {
        let db_key = TTLIndexInner::timestamp_and_key_to_db_key(expire_at, key);
        // value는 빈 값으로 저장 (키 구별을 위한 용도)
        self.batch.put(&db_key, &[]);
    }

    /// TTL 인덱스에서 항목을 제거합니다.
    ///
    /// 배치에 항목 삭제 작업을 등록합니다. 실제 커밋은 `TTLIndexInner::commit()`을
    /// 호출할 때 수행됩니다.
    ///
    /// # Arguments
    /// * `expire_at` - 만료 시간
    /// * `key` - AccessDB의 키
    fn remove(&mut self, expire_at: &DateTime<Utc>, key: &[u8]) {
        let db_key = TTLIndexInner::timestamp_and_key_to_db_key(expire_at, key);
        self.batch.delete(&db_key);
    }
}

impl Into<RocksWriteBatch> for WriteBatch {
    fn into(self) -> RocksWriteBatch {
        self.batch
    }
}

/// TTL 인덱스를 관리하는 async 래퍼 클래스
///
/// 이 클래스는 TTL 인덱스를 관리하며, 실제 TTL DB가 아닙니다.
/// RocksDB의 정렬 기능을 이용하여 expire_at(timestamp) → key 형태의 인덱스를 관리합니다.
/// Iterator를 통해 앞에서부터 만료된 항목들을 순차적으로 스캔하여
/// 키에 해당하는 데이터를 AccessDB에서 삭제하는 데 사용됩니다.
#[derive(Clone)]
struct TTLIndex {
    inner: Arc<TTLIndexInner>,
}

impl TTLIndex {
    /// 새로운 TTL 인덱스를 생성합니다.
    ///
    /// 지정된 경로에 RocksDB 인스턴스를 생성하고 TTL 인덱스를 초기화합니다.
    /// 내부적으로 블로킹 작업이므로 async 컨텍스트에서 실행해야 합니다.
    ///
    /// # Arguments
    /// * `path` - TTL 인덱스용 RocksDB 데이터베이스 경로
    pub async fn new(path: String) -> anyhow::Result<Self> {
        let inner = task::spawn_blocking(move || TTLIndexInner::new(path)).await??;

        Ok(TTLIndex {
            inner: Arc::new(inner),
        })
    }

    /// TTL 인덱스에 항목을 추가합니다.
    ///
    /// 지정된 만료 시간과 키를 인덱스에 등록합니다.
    /// 내부적으로 블로킹 태스크로 실행됩니다.
    ///
    /// # Arguments
    /// * `expire_at` - 만료 시간
    /// * `key` - AccessDB의 키
    async fn insert(&self, expire_at: DateTime<Utc>, key: &[u8]) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        let key = key.to_vec();
        task::spawn_blocking(move || inner.insert(expire_at, &key)).await?
    }

    /// WriteBatch를 커밋합니다.
    ///
    /// 배치에 등록된 모든 작업을 원자적으로 수행합니다.
    /// 내부적으로 블로킹 태스크로 실행됩니다.
    ///
    /// # Arguments
    /// * `batch` - 커밋할 WriteBatch
    async fn commit(&self, batch: WriteBatch) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.commit(batch)).await?
    }

    /// TTL 인덱스에서 항목이 존재하는지 확인합니다.
    ///
    /// 지정된 combined_key (RocksDB 키)가 인덱스에 존재하는지 확인합니다.
    /// 내부적으로 블로킹 태스크로 실행됩니다.
    ///
    /// # Arguments
    /// * `key` - RocksDB 키 (timestamp + AccessDB key 조합)
    ///
    /// # Returns
    /// * `Ok(true)` - 항목이 존재하는 경우
    /// * `Ok(false)` - 항목이 존재하지 않는 경우
    async fn includes(&self, key: &[u8]) -> anyhow::Result<bool> {
        let inner = self.inner.clone();
        let key = key.to_vec();
        task::spawn_blocking(move || inner.includes(&key)).await?
    }

    /// TTL 인덱스에서 항목을 제거합니다.
    ///
    /// 지정된 만료 시간과 키에 해당하는 인덱스 항목을 삭제합니다.
    /// 내부적으로 블로킹 태스크로 실행됩니다.
    ///
    /// # Arguments
    /// * `expire_at` - 만료 시간
    /// * `key` - AccessDB의 키
    async fn remove(&self, expire_at: DateTime<Utc>, key: &[u8]) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        let key = key.to_vec();
        task::spawn_blocking(move || inner.remove(expire_at, &key)).await?
    }

    async fn remove_by_key(&self, key: Vec<u8>) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.remove_by_key(&key)).await?
    }

    /// 만료된 항목들을 스캔합니다.
    ///
    /// RocksDB의 정렬 기능을 이용하여 앞에서부터 순차적으로 만료된 항목들을 스캔합니다.
    /// 반환된 맵의 키는 RocksDB 키이고, 값은 AccessDB 키입니다.
    /// 내부적으로 블로킹 태스크로 실행됩니다.
    ///
    /// # Arguments
    /// * `now` - 현재 시간 (이 시간 이하의 expire_at을 가진 항목이 만료됨)
    ///
    /// # Returns
    /// * `Ok(HashMap)` - 만료된 항목들의 맵 (RocksDB key → AccessDB key)
    async fn scan_expired(&self, now: DateTime<Utc>) -> anyhow::Result<HashMap<Vec<u8>, Vec<u8>>> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.scan_expired(now)).await?
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
    pub ver: u32,
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
    locks: KeyedLock<Vec<u8>>,
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
    path: String,
    created_at: DateTime<Utc>,
    _task: JoinHandle<()>,
}

impl LocalDBTTL {
    /// 데이터베이스 스키마 버전
    pub const VERSION: u32 = 2025121601;

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
    pub async fn new(path: String, interval: std::time::Duration) -> anyhow::Result<Self> {
        let index_path = format!("{}/index", path);

        let access_db = LocalDB::new(path.clone()).await?;
        let ttl_index = TTLIndex::new(index_path).await?;

        let context = Arc::new(Context {
            access_db,
            ttl_index,
            locks: KeyedLock::new(),
        });

        let ctx = (Arc::downgrade(&context), interval);
        let task = task::spawn(async move {
            let (context, interval) = ctx;
            while let Some(context) = context.upgrade() {
                let start = std::time::Instant::now();
                if let Err(e) = Self::delete_expired(context).await {
                    crate::_private_logger::error(format!("Error in delete_expired: {}", e));
                }

                let remaining = interval.saturating_sub(start.elapsed());
                tokio::time::sleep(remaining).await;
            }
        });

        Ok(Self {
            ctx: context,
            path,
            created_at: Utc::now(),
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
    async fn delete_expired(ctx: Arc<Context>) -> anyhow::Result<()> {
        let expired = ctx.ttl_index.scan_expired(Utc::now()).await?;
        for (index, key) in expired {
            let _guard = ctx.locks.lock(key.clone()).await;
            if ctx.ttl_index.includes(&index).await? {
                ctx.access_db.delete(key).await?;
                ctx.ttl_index.remove_by_key(index).await?;
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
        key: Vec<u8>,
        value: Vec<u8>,
        expired_at: ExpiredAt,
    ) -> anyhow::Result<()> {
        // AccessDBValue 생성
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
                batch.remove(&old_item.expire_at(), &key);
                batch.insert(&new_item.expire_at(), &key);
                self.ctx.ttl_index.commit(batch).await?;
            }
        } else {
            self.ctx
                .ttl_index
                .insert(new_item.expire_at(), &key)
                .await?;
        }

        // AccessDB에 저장
        self.ctx.access_db.put(key, serialized).await?;

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
    pub async fn get(&self, key: &Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
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
                    .put(key.clone(), postcard::to_stdvec(&db_value)?)
                    .await?;
                if previous_expire_at != db_value.expire_at() {
                    let mut batch = WriteBatch::default();
                    batch.remove(&previous_expire_at, &key);
                    batch.insert(&db_value.expire_at(), &key);
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
    pub async fn delete(&self, key: &Vec<u8>) -> anyhow::Result<()> {
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

    /// 데이터베이스 기본 경로를 반환합니다.
    ///
    /// # Returns
    /// AccessDB가 저장된 경로 (TTL 인덱스는 `{path}/index`에 저장됨)
    pub fn get_base_path(&self) -> &str {
        &self.path
    }

    /// LocalDBTTL 인스턴스가 생성된 시간을 반환합니다.
    ///
    /// # Returns
    /// 인스턴스 생성 시점의 UTC 시간
    pub fn get_created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }
}
