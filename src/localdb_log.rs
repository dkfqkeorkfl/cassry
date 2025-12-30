use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, DBWithThreadMode, IteratorMode, MultiThreaded,
    Options, ReadOptions, WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use tokio::task;

/// 로그용 RocksDB 내부 블로킹 구현체
///
/// 로그의 특성을 반영한 설정:
/// - append-heavy 워크로드 (주로 추가만 함)
/// - range scan 위주 (uid/timestamp 또는 timestamp/uid로 정렬된 조회)
/// - key 형식: `uid/timestamp`와 `timestamp/uid`
struct LocalDBLogInner {
    db: DBWithThreadMode<MultiThreaded>,
    write_opts: WriteOptions,
    read_opts: ReadOptions,
    path: String,
}

impl LocalDBLogInner {
    fn new(path: String) -> anyhow::Result<Self> {
        let p = Path::new(&path);

        // 로그용 최적화 설정: append-heavy + range scan 워크로드
        //
        // 워크로드 특성:
        // - append-only (로그는 주로 추가만 함, 삭제는 드묾)
        // - range scan 위주 (uid/timestamp 또는 timestamp/uid로 정렬된 조회)
        // - 시간 기반 조회가 많음
        // - 정렬된 순회가 중요

        let mut block_opts = BlockBasedOptions::default();
        // Block cache 설정 (range scan에 유용)
        let cache = Cache::new_lru_cache(128 * 1024 * 1024); // 128MB
        block_opts.set_block_cache(&cache);

        // Bloom filter ON (특정 uid나 timestamp 조회에 유용)
        block_opts.set_bloom_filter(10.0, false);

        // Index와 filter block을 캐시 (조회 성능 향상)
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);

        // 데이터베이스 옵션 설정
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_block_based_table_factory(&block_opts);

        // 성능 최적화 설정
        options.set_max_background_jobs(4);
        options.set_bytes_per_sync(1024 * 1024); // 1MB

        // Memtable 크기 설정 (append-heavy 워크로드에 적합)
        options.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        options.set_max_write_buffer_number(3);
        options.set_min_write_buffer_number_to_merge(2);

        // Level size 설정 (append-heavy이므로 큰 레벨 크기)
        options.set_max_bytes_for_level_base(256 * 1024 * 1024); // 256MB
        options.set_target_file_size_base(64 * 1024 * 1024); // 64MB

        // 압축 설정 (로그는 읽기보다 쓰기가 많지만, 저장 공간 절약을 위해 압축 사용)
        options.set_compression_type(DBCompressionType::Lz4);
        options.set_compression_per_level(&[
            DBCompressionType::None,           // L0: 압축 없음 (최신 데이터, 빠른 쓰기)
            DBCompressionType::None,            // L1: 빠른 압축
            DBCompressionType::Lz4,            // L2: 빠른 압축
            DBCompressionType::Lz4,            // L3: 빠른 압축
            DBCompressionType::Lz4,            // L4: 빠른 압축
            DBCompressionType::Lz4,            // L5: 빠른 압축
            DBCompressionType::Zstd,            // L6: 빠른 압축
        ]);

        // Direct I/O 설정 (append-heavy 워크로드에 유용)
        options.set_use_direct_reads(false); // range scan에는 일반 I/O가 더 나을 수 있음
        options.set_use_direct_io_for_flush_and_compaction(true); // flush/compaction은 direct I/O

        // Compaction readahead 설정 (range scan 성능 향상)
        options.set_compaction_readahead_size(4 * 1024 * 1024); // 4MB

        // 파일 관리 설정
        options.set_max_open_files(10000);
        options.set_keep_log_file_num(100); // WAL 로그 파일 개수 제한
        options.set_max_manifest_file_size(128 * 1024 * 1024); // 128MB

        // 데이터 안정성 설정
        options.set_paranoid_checks(true); // 체크섬 검증 활성화
        options.set_manual_wal_flush(false); // 자동 WAL flush
        options.set_atomic_flush(true); // 원자적 flush

        // Write 옵션 설정
        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(false); // WAL 활성화 (데이터 안정성)
        write_opts.set_sync(false); // 비동기 쓰기 (성능 우선, append-heavy에 적합)

        // Read 옵션 설정
        let mut read_opts = ReadOptions::default();
        read_opts.set_verify_checksums(true);
        read_opts.set_async_io(true);
        read_opts.set_readahead_size(4 * 1024 * 1024); // 4MB (range scan에 유용)

        // 데이터베이스 생성
        let db = DBWithThreadMode::<MultiThreaded>::open(&options, p)?;

        Ok(Self {
            db,
            write_opts,
            read_opts,
            path,
        })
    }
}

/// 로그용 RocksDB 래퍼 구조체
#[derive(Clone)]
pub struct LocalDBLog {
    inner: Arc<LocalDBLogInner>,
}

impl LocalDBLog {
    /// 새로운 로그용 데이터베이스를 생성합니다.
    pub fn new(path: String) -> anyhow::Result<Self> {
        let inner = LocalDBLogInner::new(path)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// uid/timestamp 형식의 키로 로그를 저장합니다.
    ///
    /// # Arguments
    /// * `uid` - 사용자 ID
    /// * `timestamp` - 타임스탬프 (밀리초)
    /// * `value` - 저장할 값 (JSON 문자열)
    pub async fn put_by_uid_timestamp(
        &self,
        uid: &str,
        timestamp: u64,
        value: &str,
    ) -> anyhow::Result<()> {
        let key = format!("{}/{}", uid, timestamp);
        let inner = self.inner.clone();
        let key_bytes = key.into_bytes();
        let value_bytes = value.as_bytes().to_vec();

        task::spawn_blocking(move || {
            inner
                .db
                .put_opt(key_bytes, value_bytes, &inner.write_opts)
                .map_err(anyhow::Error::from)
        })
        .await?
    }

    /// timestamp/uid 형식의 키로 로그를 저장합니다.
    ///
    /// # Arguments
    /// * `timestamp` - 타임스탬프 (밀리초)
    /// * `uid` - 사용자 ID
    /// * `value` - 저장할 값 (JSON 문자열)
    pub async fn put_by_timestamp_uid(
        &self,
        timestamp: u64,
        uid: &str,
        value: &str,
    ) -> anyhow::Result<()> {
        let key = format!("{}/{}", timestamp, uid);
        let inner = self.inner.clone();
        let key_bytes = key.into_bytes();
        let value_bytes = value.as_bytes().to_vec();

        task::spawn_blocking(move || {
            inner
                .db
                .put_opt(key_bytes, value_bytes, &inner.write_opts)
                .map_err(anyhow::Error::from)
        })
        .await?
    }

    /// uid/timestamp와 timestamp/uid 두 형식 모두로 로그를 저장합니다.
    ///
    /// # Arguments
    /// * `uid` - 사용자 ID
    /// * `timestamp` - 타임스탬프 (밀리초)
    /// * `value` - 저장할 값 (JSON 문자열)
    pub async fn put_both(
        &self,
        uid: &str,
        timestamp: u64,
        value: &str,
    ) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        let uid_key = format!("{}/{}", uid, timestamp);
        let timestamp_key = format!("{}/{}", timestamp, uid);
        let value_bytes = value.as_bytes().to_vec();

        task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();
            batch.put(uid_key.as_bytes(), &value_bytes);
            batch.put(timestamp_key.as_bytes(), &value_bytes);
            inner
                .db
                .write_opt(batch, &inner.write_opts)
                .map_err(anyhow::Error::from)
        })
        .await?
    }

    /// JSON 객체를 uid/timestamp와 timestamp/uid 두 형식 모두로 저장합니다.
    pub async fn put_json_both<T>(
        &self,
        uid: &str,
        timestamp: u64,
        value: &T,
    ) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let json_str = serde_json::to_string(value)?;
        self.put_both(uid, timestamp, &json_str).await
    }

    /// uid로 시작하는 모든 로그를 조회합니다 (정렬된 순서).
    ///
    /// # Arguments
    /// * `uid` - 사용자 ID
    /// * `limit` - 최대 반환 개수 (None이면 제한 없음)
    pub async fn get_by_uid(
        &self,
        uid: &str,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let prefix = format!("{}/", uid);
        let inner = self.inner.clone();
        let read_opts = inner.read_opts.clone();

        task::spawn_blocking(move || {
            let mut results = Vec::new();
            let iter = inner.db.iterator(IteratorMode::From(
                prefix.as_bytes(),
                rocksdb::Direction::Forward,
            ));

            for item in iter {
                let (key, value) = item?;
                let key_str = String::from_utf8(key.to_vec())?;

                // prefix로 시작하지 않으면 중단 (정렬되어 있으므로)
                if !key_str.starts_with(&prefix) {
                    break;
                }

                let value_str = String::from_utf8(value.to_vec())?;
                results.push((key_str, value_str));

                if let Some(limit) = limit {
                    if results.len() >= limit {
                        break;
                    }
                }
            }

            Ok::<_, anyhow::Error>(results)
        })
        .await?
    }

    /// timestamp로 시작하는 모든 로그를 조회합니다 (정렬된 순서).
    ///
    /// # Arguments
    /// * `timestamp` - 시작 타임스탬프 (밀리초)
    /// * `limit` - 최대 반환 개수 (None이면 제한 없음)
    pub async fn get_by_timestamp(
        &self,
        timestamp: u64,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let prefix = format!("{}/", timestamp);
        let inner = self.inner.clone();
        let read_opts = inner.read_opts.clone();

        task::spawn_blocking(move || {
            let mut results = Vec::new();
            let iter = inner.db.iterator(IteratorMode::From(
                prefix.as_bytes(),
                rocksdb::Direction::Forward,
            ));

            for item in iter {
                let (key, value) = item?;
                let key_str = String::from_utf8(key.to_vec())?;

                // prefix로 시작하지 않으면 중단 (정렬되어 있으므로)
                if !key_str.starts_with(&prefix) {
                    break;
                }

                let value_str = String::from_utf8(value.to_vec())?;
                results.push((key_str, value_str));

                if let Some(limit) = limit {
                    if results.len() >= limit {
                        break;
                    }
                }
            }

            Ok::<_, anyhow::Error>(results)
        })
        .await?
    }

    /// uid와 timestamp 범위로 로그를 조회합니다.
    ///
    /// # Arguments
    /// * `uid` - 사용자 ID
    /// * `start_timestamp` - 시작 타임스탬프 (밀리초)
    /// * `end_timestamp` - 종료 타임스탬프 (밀리초, None이면 제한 없음)
    /// * `limit` - 최대 반환 개수 (None이면 제한 없음)
    pub async fn get_by_uid_range(
        &self,
        uid: &str,
        start_timestamp: u64,
        end_timestamp: Option<u64>,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let start_key = format!("{}/{}", uid, start_timestamp);
        let inner = self.inner.clone();
        let read_opts = inner.read_opts.clone();

        task::spawn_blocking(move || {
            let mut results = Vec::new();
            let iter = inner.db.iterator(IteratorMode::From(
                start_key.as_bytes(),
                rocksdb::Direction::Forward,
            ));

            let uid_prefix = format!("{}/", uid);

            for item in iter {
                let (key, value) = item?;
                let key_str = String::from_utf8(key.to_vec())?;

                // uid prefix로 시작하지 않으면 중단
                if !key_str.starts_with(&uid_prefix) {
                    break;
                }

                // timestamp 추출
                if let Some(timestamp_str) = key_str.strip_prefix(&uid_prefix) {
                    if let Ok(timestamp) = timestamp_str.parse::<u64>() {
                        // end_timestamp 체크
                        if let Some(end) = end_timestamp {
                            if timestamp > end {
                                break;
                            }
                        }
                    }
                }

                let value_str = String::from_utf8(value.to_vec())?;
                results.push((key_str, value_str));

                if let Some(limit) = limit {
                    if results.len() >= limit {
                        break;
                    }
                }
            }

            Ok::<_, anyhow::Error>(results)
        })
        .await?
    }

    /// timestamp 범위로 로그를 조회합니다.
    ///
    /// # Arguments
    /// * `start_timestamp` - 시작 타임스탬프 (밀리초)
    /// * `end_timestamp` - 종료 타임스탬프 (밀리초, None이면 제한 없음)
    /// * `limit` - 최대 반환 개수 (None이면 제한 없음)
    pub async fn get_by_timestamp_range(
        &self,
        start_timestamp: u64,
        end_timestamp: Option<u64>,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let start_key = format!("{}/", start_timestamp);
        let inner = self.inner.clone();
        let read_opts = inner.read_opts.clone();

        task::spawn_blocking(move || {
            let mut results = Vec::new();
            let iter = inner.db.iterator(IteratorMode::From(
                start_key.as_bytes(),
                rocksdb::Direction::Forward,
            ));

            for item in iter {
                let (key, value) = item?;
                let key_str = String::from_utf8(key.to_vec())?;

                // timestamp prefix 추출
                if let Some(rest) = key_str.split('/').next() {
                    if let Ok(timestamp) = rest.parse::<u64>() {
                        // end_timestamp 체크
                        if let Some(end) = end_timestamp {
                            if timestamp > end {
                                break;
                            }
                        }
                    } else {
                        // timestamp로 시작하지 않으면 중단
                        break;
                    }
                } else {
                    break;
                }

                let value_str = String::from_utf8(value.to_vec())?;
                results.push((key_str, value_str));

                if let Some(limit) = limit {
                    if results.len() >= limit {
                        break;
                    }
                }
            }

            Ok::<_, anyhow::Error>(results)
        })
        .await?
    }

    /// 특정 키로 로그를 조회합니다.
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        let inner = self.inner.clone();
        let key_bytes = key.as_bytes().to_vec();

        task::spawn_blocking(move || {
            inner
                .db
                .get_opt(key_bytes, &inner.read_opts)
                .map_err(anyhow::Error::from)
                .and_then(|bytes| {
                    if let Some(bytes) = bytes {
                        Ok(Some(String::from_utf8(bytes)?))
                    } else {
                        Ok(None)
                    }
                })
        })
        .await?
    }

    /// 특정 키의 로그를 삭제합니다.
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        let key_bytes = key.as_bytes().to_vec();

        task::spawn_blocking(move || {
            inner
                .db
                .delete_opt(key_bytes, &inner.write_opts)
                .map_err(anyhow::Error::from)
        })
        .await?
    }

    /// uid/timestamp와 timestamp/uid 두 형식 모두를 삭제합니다.
    pub async fn delete_both(&self, uid: &str, timestamp: u64) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        let uid_key = format!("{}/{}", uid, timestamp);
        let timestamp_key = format!("{}/{}", timestamp, uid);

        task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();
            batch.delete(uid_key.as_bytes());
            batch.delete(timestamp_key.as_bytes());
            inner
                .db
                .write_opt(batch, &inner.write_opts)
                .map_err(anyhow::Error::from)
        })
        .await?
    }
}

