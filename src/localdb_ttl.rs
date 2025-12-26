use chrono::{DateTime, Utc};
use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, DBWithThreadMode, IteratorMode, MultiThreaded,
    Options, ReadOptions, WriteOptions,
};
use std::{path::Path, sync::Arc};
use tokio::task;

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
    path: String,
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
            path,
        })
    }

    /// expire_at(i64)를 big-endian bytes로 변환
    /// 
    /// RocksDB의 정렬 순서를 이용하기 위해 big-endian으로 변환합니다.
    /// 이렇게 하면 timestamp가 작은 값부터 큰 값 순으로 정렬됩니다.
    fn timestamp_to_key(expire_at: i64) -> [u8; 8] {
        expire_at.to_be_bytes()
    }

    /// big-endian bytes를 i64로 변환
    fn key_to_timestamp(key: &[u8]) -> anyhow::Result<i64> {
        if key.len() != 8 {
            return Err(anyhow::anyhow!(
                "Invalid key length: expected 8 bytes, got {}",
                key.len()
            ));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(key);
        Ok(i64::from_be_bytes(bytes))
    }

    /// TTL 인덱스에 항목을 추가합니다.
    /// 
    /// # Arguments
    /// * `expire_at` - 만료 시간 (DateTime<Utc>)
    /// * `key` - AccessDB의 키 (Vec<u8>)
    /// 
    /// # RocksDB 구조
    /// - RocksDB key: expire_at(i64) as big-endian bytes (8 bytes)
    /// - RocksDB value: AccessDB key (Vec<u8>)
    fn insert(&self, expire_at: DateTime<Utc>, key: &[u8]) -> anyhow::Result<()> {
        let timestamp = expire_at.timestamp();
        let db_key = Self::timestamp_to_key(timestamp);
        self.db
            .put_opt(&db_key, key, &self.write_opts)
            .map_err(anyhow::Error::from)
    }

    /// TTL 인덱스에서 항목을 제거합니다.
    /// 
    /// # Arguments
    /// * `expire_at` - 만료 시간 (DateTime<Utc>)
    /// * `key` - AccessDB의 키 (Vec<u8>)
    fn remove(&self, expire_at: DateTime<Utc>, key: &[u8]) -> anyhow::Result<()> {
        let timestamp = expire_at.timestamp();
        let db_key = Self::timestamp_to_key(timestamp);

        // 현재 key에 대한 값을 조회하여 일치하는지 확인
        if let Some(existing_value) = self.db.get_opt(&db_key, &self.read_opts)? {
            if existing_value == key {
                // 값이 일치하면 삭제
                self.db
                    .delete_opt(&db_key, &self.write_opts)
                    .map_err(anyhow::Error::from)
            } else {
                // 값이 다르면 해당 expire_at에는 다른 key가 있음
                // 삭제하지 않음
                Ok(())
            }
        } else {
            // 이미 존재하지 않음
            Ok(())
        }
    }

    /// 만료된 항목들을 스캔합니다.
    /// 
    /// RocksDB의 정렬 기능을 이용하여 앞에서부터 순차적으로 만료된 항목들을 스캔합니다.
    /// Iterator를 통해 now.timestamp() 이하의 expire_at을 가진 항목들을 반환합니다.
    /// 
    /// # Arguments
    /// * `now` - 현재 시간 (DateTime<Utc>)
    /// 
    /// # Returns
    /// * `Vec<(i64, Vec<u8>)>` - 만료된 항목들의 리스트 (timestamp, AccessDB key)
    fn scan_expired(&self, now: DateTime<Utc>) -> anyhow::Result<Vec<(i64, Vec<u8>)>> {
        let now_timestamp = now.timestamp();
        let mut results = Vec::new();

        // Iterator를 사용하여 앞에서부터 순차적으로 만료된 항목들을 스캔
        // timestamp가 정렬되어 있으므로 now_timestamp 이하인 항목들만 수집
        let iter = self.db.iterator(IteratorMode::Start);

        for item in iter {
            let (key, value) = item?;
            let timestamp = Self::key_to_timestamp(&key)?;

            // now_timestamp 이하인 경우에만 추가
            if timestamp <= now_timestamp {
                results.push((timestamp, value.to_vec()));
            } else {
                // timestamp가 정렬되어 있으므로 이후 항목은 모두 만료되지 않음
                break;
            }
        }

        Ok(results)
    }

    /// 만료된 항목들을 삭제합니다.
    /// 
    /// # Arguments
    /// * `now` - 현재 시간 (DateTime<Utc>)
    /// 
    /// # Returns
    /// * `usize` - 삭제된 항목의 개수
    fn delete_expired(&self, now: DateTime<Utc>) -> anyhow::Result<usize> {
        let expired = self.scan_expired(now)?;
        let count = expired.len();

        // 삭제할 항목들을 WriteBatch로 일괄 삭제
        let mut batch = rocksdb::WriteBatch::default();
        for (timestamp, _) in &expired {
            let db_key = Self::timestamp_to_key(*timestamp);
            batch.delete(&db_key);
        }

        if count > 0 {
            self.db
                .write_opt(batch, &self.write_opts)
                .map_err(anyhow::Error::from)?;
        }

        Ok(count)
    }

    /// 데이터베이스 경로를 반환합니다.
    fn get_path(&self) -> &str {
        &self.path
    }

    /// 데이터베이스를 플러시합니다.
    fn flush(&self) -> anyhow::Result<()> {
        self.db.flush().map_err(anyhow::Error::from)
    }

    /// 데이터베이스를 컴팩트합니다.
    fn compact_range(&self) {
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
    }
}

/// TTL 인덱스를 관리하는 async 래퍼 클래스
/// 
/// 이 클래스는 TTL 인덱스를 관리하며, 실제 TTL DB가 아닙니다.
/// RocksDB의 정렬 기능을 이용하여 expire_at(timestamp) → key 형태의 인덱스를 관리합니다.
/// Iterator를 통해 앞에서부터 만료된 항목들을 순차적으로 스캔하여
/// 키에 해당하는 데이터를 AccessDB에서 삭제하는 데 사용됩니다.
#[derive(Clone)]
pub struct TTLIndex {
    inner: Arc<TTLIndexInner>,
}

impl TTLIndex {
    /// 새로운 TTL 인덱스를 생성합니다.
    /// 
    /// # Arguments
    /// * `path` - RocksDB 데이터베이스 경로
    pub async fn new(path: String) -> anyhow::Result<Self> {
        let inner = task::spawn_blocking(move || TTLIndexInner::new(path)).await??;

        Ok(TTLIndex {
            inner: Arc::new(inner),
        })
    }

    /// TTL 인덱스에 항목을 추가합니다.
    ///
    /// # Arguments
    /// * `expire_at` - 만료 시간 (DateTime<Utc>)
    /// * `key` - AccessDB의 키 (Vec<u8>)
    ///
    /// # Example
    /// ```rust
    /// use chrono::{DateTime, Utc, Duration};
    /// use cassry::ttl_index::TTLIndex;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let index = TTLIndex::new("ttl_index_db".to_string()).await?;
    /// let expire_at = Utc::now() + Duration::hours(1);
    /// let key = b"my_access_key".to_vec();
    ///
    /// index.insert(expire_at, &key).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn insert(&self, expire_at: DateTime<Utc>, key: &[u8]) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        let key = key.to_vec();
        task::spawn_blocking(move || inner.insert(expire_at, &key)).await?
    }

    /// TTL 인덱스에서 항목을 제거합니다.
    ///
    /// # Arguments
    /// * `expire_at` - 만료 시간 (DateTime<Utc>)
    /// * `key` - AccessDB의 키 (Vec<u8>)
    ///
    /// # Example
    /// ```rust
    /// use chrono::{DateTime, Utc, Duration};
    /// use cassry::ttl_index::TTLIndex;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let index = TTLIndex::new("ttl_index_db".to_string()).await?;
    /// let expire_at = Utc::now() + Duration::hours(1);
    /// let key = b"my_access_key".to_vec();
    ///
    /// index.remove(expire_at, &key).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove(&self, expire_at: DateTime<Utc>, key: &[u8]) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        let key = key.to_vec();
        task::spawn_blocking(move || inner.remove(expire_at, &key)).await?
    }

    /// 만료된 항목들을 스캔합니다.
    ///
    /// RocksDB의 정렬 기능을 이용하여 앞에서부터 순차적으로 만료된 항목들을 스캔합니다.
    /// 반환된 키들을 이용하여 AccessDB에서 해당 데이터를 삭제할 수 있습니다.
    ///
    /// # Arguments
    /// * `now` - 현재 시간 (DateTime<Utc>)
    ///
    /// # Returns
    /// * `Vec<(i64, Vec<u8>)>` - 만료된 항목들의 리스트 (timestamp, AccessDB key)
    ///
    /// # Example
    /// ```rust
    /// use chrono::Utc;
    /// use cassry::ttl_index::TTLIndex;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let index = TTLIndex::new("ttl_index_db".to_string()).await?;
    /// let now = Utc::now();
    ///
    /// let expired = index.scan_expired(now).await?;
    /// for (timestamp, key) in expired {
    ///     println!("Expired at {}: {:?}", timestamp, key);
    ///     // AccessDB에서 해당 key의 데이터를 삭제
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn scan_expired(&self, now: DateTime<Utc>) -> anyhow::Result<Vec<(i64, Vec<u8>)>> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.scan_expired(now)).await?
    }

    /// 만료된 항목들을 삭제합니다.
    ///
    /// # Arguments
    /// * `now` - 현재 시간 (DateTime<Utc>)
    ///
    /// # Returns
    /// * `usize` - 삭제된 항목의 개수
    ///
    /// # Example
    /// ```rust
    /// use chrono::Utc;
    /// use cassry::ttl_index::TTLIndex;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let index = TTLIndex::new("ttl_index_db".to_string()).await?;
    /// let now = Utc::now();
    ///
    /// let deleted_count = index.delete_expired(now).await?;
    /// println!("Deleted {} expired entries from index", deleted_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_expired(&self, now: DateTime<Utc>) -> anyhow::Result<usize> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.delete_expired(now)).await?
    }

    /// 데이터베이스 경로를 반환합니다.
    pub fn get_path(&self) -> &str {
        self.inner.get_path()
    }

    /// 데이터베이스를 플러시합니다.
    pub async fn flush(&self) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.flush()).await?
    }

    /// 데이터베이스를 컴팩트합니다.
    pub async fn compact_range(&self) {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.compact_range())
            .await
            .unwrap_or_else(|e| {
                eprintln!("Error in compact_range: {}", e);
            });
    }
}

