use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, DBWithThreadMode, MultiThreaded,
    Options, ReadOptions, WriteOptions,
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
            DBCompressionType::None,            // L2: 빠른 압축
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

impl LocalDBLogInner {
    fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.db
            .put_opt(key, value, &self.write_opts)
            .map_err(anyhow::Error::from)
    }

    fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.db.get_opt(key, &self.read_opts)?)
    }

    fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        self.db
            .delete_opt(key, &self.write_opts)
            .map_err(anyhow::Error::from)
    }
}

impl LocalDBLog {
    /// 새로운 로그용 데이터베이스를 생성합니다.
    pub async fn new(path: String) -> anyhow::Result<Self> {
        let inner = task::spawn_blocking(move || LocalDBLogInner::new(path)).await??;

        Ok(LocalDBLog {
            inner: Arc::new(inner),
        })
    }

    /// 키-값 쌍을 저장합니다.
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.put(&key, &value)).await?
    }

    /// 키로 값을 조회합니다.
    pub async fn get(&self, key: Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.get(&key)).await?
    }

    /// 키를 삭제합니다.
    pub async fn delete(&self, key: Vec<u8>) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.delete(&key)).await?
    }

    /// JSON 객체를 조회합니다.
    pub async fn get_json<T>(&self, key: String) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let inner = self.inner.clone();
        if let Some(result) = task::spawn_blocking(move || inner.get(key.as_bytes()))
            .await??
            .map(|v| String::from_utf8(v))
        {
            let value: T = serde_json::from_str(&result?)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// JSON 객체를 저장합니다.
    pub async fn put_json<T>(&self, key: String, value: &T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let json_str = serde_json::to_string(value)?;
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.put(key.as_bytes(), json_str.as_bytes())).await?
    }

    /// JSON 객체를 삭제합니다.
    pub async fn delete_json(&self, key: String) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.delete(key.as_bytes())).await?
    }

    /// 데이터베이스 경로를 반환합니다.
    pub fn get_path(&self) -> &str {
        &self.inner.path
    }
}

