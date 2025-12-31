use chrono::{DateTime, Utc};
use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, DBWithThreadMode, MultiThreaded, Options,
    ReadOptions, WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use tokio::task;

pub mod config {
    use super::*;

    pub trait Generator {
        fn generate(
            &self,
        ) -> anyhow::Result<(DBWithThreadMode<MultiThreaded>, WriteOptions, ReadOptions)>;
    }

    /// 일반 데이터베이스와 옵션을 생성하는 헬퍼 함수 (append-heavy + range scan 최적화)
    pub struct General(String);
    impl General {
        pub fn new(path: String) -> Self {
            Self(path)
        }
    }

    /// 로그 데이터베이스와 옵션을 생성하는 헬퍼 함수 (append-heavy + range scan 최적화)
    ///
    /// # Returns
    /// `(DBWithThreadMode<MultiThreaded>, WriteOptions, ReadOptions)`
    pub struct Log(String);
    impl Log {
        pub fn new(path: String) -> Self {
            Self(path)
        }
    }

    pub struct IndexTTL(String);
    impl IndexTTL {
        pub fn new(path: String) -> Self {
            Self(path)
        }
    }

    impl Generator for General {
        fn generate(
            &self,
        ) -> anyhow::Result<(DBWithThreadMode<MultiThreaded>, WriteOptions, ReadOptions)> {
            let mut block_opts = BlockBasedOptions::default();
            let cache = Cache::new_lru_cache(4 * 1024 * 1024 * 1024); // 4GB
            block_opts.set_block_cache(&cache);
            block_opts.set_bloom_filter(10.0, false);
            block_opts.set_cache_index_and_filter_blocks(true);

            let mut options = Options::default();
            options.create_if_missing(true);
            options.set_block_based_table_factory(&block_opts);
            options.set_max_background_jobs(4);
            options.set_bytes_per_sync(1024 * 1024);
            options.set_write_buffer_size(64 * 1024 * 1024);
            options.set_max_write_buffer_number(3);
            options.set_min_write_buffer_number_to_merge(2);
            options.set_max_bytes_for_level_base(256 * 1024 * 1024);
            options.set_target_file_size_base(64 * 1024 * 1024);

            options.set_compression_type(DBCompressionType::Lz4);
            options.set_compression_per_level(&[
                DBCompressionType::None,
                DBCompressionType::None,
                DBCompressionType::None,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
            ]);

            options.set_use_direct_reads(true);
            options.set_use_direct_io_for_flush_and_compaction(true);
            options.set_compaction_readahead_size(2 * 1024 * 1024);

            options.set_max_open_files(-1);
            options.set_keep_log_file_num(1000);
            options.set_max_manifest_file_size(1024 * 1024 * 1024);

            options.set_paranoid_checks(true);
            options.set_manual_wal_flush(false);
            options.set_atomic_flush(true);

            let mut write_opts = WriteOptions::default();
            write_opts.disable_wal(false);
            write_opts.set_sync(false);

            let mut read_opts = ReadOptions::default();
            read_opts.set_verify_checksums(true);
            read_opts.set_async_io(true);
            read_opts.set_readahead_size(2 * 1024 * 1024);

            let db = DBWithThreadMode::<MultiThreaded>::open(&options, &self.0)?;

            Ok((db, write_opts, read_opts))
        }
    }

    impl Generator for Log {
        fn generate(
            &self,
        ) -> anyhow::Result<(DBWithThreadMode<MultiThreaded>, WriteOptions, ReadOptions)> {
            let mut block_opts = BlockBasedOptions::default();
            let cache = Cache::new_lru_cache(128 * 1024 * 1024); // 128MB
            block_opts.set_block_cache(&cache);
            block_opts.set_bloom_filter(10.0, false);
            block_opts.set_cache_index_and_filter_blocks(true);
            block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);

            let mut options = Options::default();
            options.create_if_missing(true);
            options.set_block_based_table_factory(&block_opts);
            options.set_max_background_jobs(4);
            options.set_bytes_per_sync(1024 * 1024);
            options.set_write_buffer_size(64 * 1024 * 1024);
            options.set_max_write_buffer_number(3);
            options.set_min_write_buffer_number_to_merge(2);
            options.set_max_bytes_for_level_base(256 * 1024 * 1024);
            options.set_target_file_size_base(64 * 1024 * 1024);

            options.set_compression_type(DBCompressionType::Lz4);
            options.set_compression_per_level(&[
                DBCompressionType::None,
                DBCompressionType::None,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
            ]);

            options.set_use_direct_reads(false);
            options.set_use_direct_io_for_flush_and_compaction(true);
            options.set_compaction_readahead_size(4 * 1024 * 1024);

            options.set_max_open_files(10000);
            options.set_keep_log_file_num(100);
            options.set_max_manifest_file_size(128 * 1024 * 1024);

            options.set_paranoid_checks(true);
            options.set_manual_wal_flush(false);
            options.set_atomic_flush(true);

            let mut write_opts = WriteOptions::default();
            write_opts.disable_wal(false);
            write_opts.set_sync(false);

            let mut read_opts = ReadOptions::default();
            read_opts.set_verify_checksums(true);
            read_opts.set_async_io(true);
            read_opts.set_readahead_size(4 * 1024 * 1024);

            let db = DBWithThreadMode::<MultiThreaded>::open(&options, &self.0)?;
            Ok((db, write_opts, read_opts))
        }
    }

    impl Generator for IndexTTL {
        fn generate(
            &self,
        ) -> anyhow::Result<(DBWithThreadMode<MultiThreaded>, WriteOptions, ReadOptions)> {
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
            // Compaction readahead 설정 (delete-heavy 워크로드에서도 일부 readahead가 성능 향상)
            options.set_compaction_readahead_size(2 * 1024 * 1024); // 2MB

            // 파일 관리 설정
            options.set_max_open_files(10000); // 파일 핸들 개수 제한 (메모리 절약)
            options.set_keep_log_file_num(100); // WAL 로그 파일 개수 제한 (복구 시간 단축)
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

            let db = DBWithThreadMode::<MultiThreaded>::open(&options, &self.0)?;

            Ok((db, write_opts, read_opts))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DBStats {
    pub total_entries: u64,
    pub last_update: u64,
    pub memory_usage: u64,
    pub block_cache_usage: u64,
    pub memtable_usage: u64,
    pub sst_files_size: u64,
    pub read_amplification: u64,
    pub write_amplification: u64,
    pub compression_ratio: f64,
    pub pending_compaction_bytes: u64,
}

/// 내부 블로킹 구현체
struct LocalDBInner {
    db: DBWithThreadMode<MultiThreaded>,
    write_opts: WriteOptions,
    read_opts: ReadOptions,
}

impl LocalDBInner {
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

    fn commit(&self, batch: WriteBatch) -> anyhow::Result<()> {
        self.db
            .write_opt(batch, &self.write_opts)
            .map_err(anyhow::Error::from)
    }

    fn raw(&self) -> &DBWithThreadMode<MultiThreaded> {
        &self.db
    }

    fn get_stats(&self) -> DBStats {
        // 데이터베이스 속성 가져오기
        let props = self
            .db
            .property_value("rocksdb.estimate-num-keys")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let mem_usage = self
            .db
            .property_value("rocksdb.estimate-memory-usage")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let block_cache = self
            .db
            .property_value("rocksdb.block-cache-usage")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let memtable = self
            .db
            .property_value("rocksdb.cur-size-all-mem-tables")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let sst_size = self
            .db
            .property_value("rocksdb.total-sst-files-size")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let read_amp = self
            .db
            .property_value("rocksdb.estimate-table-readers-mem")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let write_amp = self
            .db
            .property_value("rocksdb.actual-delayed-write-rate")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let compression = self
            .db
            .property_value("rocksdb.compression-ratio-at-level0")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0)
            / 100.0;
        let pending = self
            .db
            .property_value("rocksdb.estimate-pending-compaction-bytes")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        // 마지막 업데이트 시간 찾기
        let mut last_update = 0;
        let mut iter = self.db.iterator(rocksdb::IteratorMode::End);
        if let Some(Ok((_, value))) = iter.next() {
            if let Ok(value_str) = String::from_utf8(value.to_vec()) {
                if let Ok(timestamp) = value_str.parse::<u64>() {
                    last_update = timestamp;
                }
            }
        }

        DBStats {
            total_entries: props,
            last_update,
            memory_usage: mem_usage,
            block_cache_usage: block_cache,
            memtable_usage: memtable,
            sst_files_size: sst_size,
            read_amplification: read_amp,
            write_amplification: write_amp,
            compression_ratio: compression,
            pending_compaction_bytes: pending,
        }
    }

    fn compact_range(&self) {
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
    }

    fn flush(&self) -> anyhow::Result<()> {
        self.db.flush().map_err(anyhow::Error::from)
    }

    fn get_property(&self, name: &str) -> anyhow::Result<Option<String>> {
        self.db.property_value(name).map_err(anyhow::Error::from)
    }
}

pub struct LocalDB {
    inner: Arc<LocalDBInner>,
    created_at: DateTime<Utc>,
}

impl LocalDB {
    pub async fn new<T: config::Generator + Send + Sync + 'static>(
        config: T,
    ) -> anyhow::Result<Self> {
        let (db, write_opts, read_opts) = task::spawn_blocking(move || config.generate()).await??;
        Ok(LocalDB {
            inner: Arc::new(LocalDBInner {
                db,
                write_opts,
                read_opts,
            }),
            created_at: Utc::now(),
        })
    }

    pub async fn commit(&self, batch: WriteBatch) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.commit(batch)).await?
    }

    pub async fn put(&self, key: Arc<Vec<u8>>, value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.put(&key, &value)).await?
    }

    pub async fn get(&self, key: Arc<Vec<u8>>) -> anyhow::Result<Option<Vec<u8>>> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.get(&key)).await?
    }

    pub async fn delete(&self, key: Arc<Vec<u8>>) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.delete(&key)).await?
    }

    pub async fn put_raw(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.put(&key, &value)).await?
    }

    pub async fn get_raw(&self, key: Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.get(&key)).await?
    }

    pub async fn delete_raw(&self, key: Vec<u8>) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.delete(&key)).await?
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


    pub fn raw(&self) -> &DBWithThreadMode<MultiThreaded> {
        &self.inner.raw()
    }

    pub async fn delete_by_str(&self, key: String) -> anyhow::Result<()> {
        self.delete_raw(key.into_bytes()).await
    }

    pub fn get_path(&self) -> &Path {
        self.inner.db.path()
    }

    pub async fn get_stats(&self) -> DBStats {
        let inner = self.inner.clone();

        task::spawn_blocking(move || inner.get_stats())
            .await
            .unwrap_or_else(|_| DBStats {
                total_entries: 0,
                last_update: 0,
                memory_usage: 0,
                block_cache_usage: 0,
                memtable_usage: 0,
                sst_files_size: 0,
                read_amplification: 0,
                write_amplification: 0,
                compression_ratio: 0.0,
                pending_compaction_bytes: 0,
            })
    }

    pub async fn compact_range(&self) {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.compact_range())
            .await
            .unwrap_or_else(|e| {
                eprintln!("Error in compact_range: {}", e);
            });
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.flush()).await?
    }

    pub async fn get_property(&self, name: String) -> anyhow::Result<Option<String>> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.get_property(&name)).await?
    }

    pub fn get_created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }
}