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

#[derive(Debug, Clone)]
enum Operation {
    Put { key: String, value: String },
    Delete { key: String },
}

/// 내부 블로킹 구현체
struct LocalDBInner {
    db: DBWithThreadMode<MultiThreaded>,
    write_opts: WriteOptions,
    read_opts: ReadOptions,
}

/// 트랜잭션을 위한 래퍼 구조체 (WriteBatch 기반)
pub struct TransactionWrapper {
    inner: Arc<LocalDBInner>,
    pending_operations: Vec<Operation>,
}

impl TransactionWrapper {
    /// 새로운 트랜잭션을 생성합니다.
    pub(self) fn new(inner: Arc<LocalDBInner>) -> Self {
        Self {
            inner,
            pending_operations: Vec::new(),
        }
    }

    /// 트랜잭션 내에서 키-값 쌍을 저장합니다.
    pub fn put(&mut self, key: String, value: String) -> anyhow::Result<()> {
        self.pending_operations.push(Operation::Put { key, value });
        Ok(())
    }

    /// 트랜잭션 내에서 JSON 객체를 저장합니다.
    pub fn put_json<T>(&mut self, key: String, value: &T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let json_str = serde_json::to_string(value)?;
        self.pending_operations.push(Operation::Put {
            key,
            value: json_str,
        });
        Ok(())
    }

    /// 트랜잭션 내에서 키로 값을 조회합니다.
    pub async fn get(&self, key: String) -> anyhow::Result<Option<String>> {
        // 먼저 pending operations에서 찾기
        for operation in &self.pending_operations {
            match operation {
                Operation::Put { key: op_key, value } => {
                    if op_key == &key {
                        return Ok(Some(value.clone()));
                    }
                }
                Operation::Delete { key: op_key } => {
                    if op_key == &key {
                        return Ok(None);
                    }
                }
            }
        }

        // pending operations에 없으면 데이터베이스에서 조회
        let inner = self.inner.clone();
        task::spawn_blocking(move || {
            let result = inner.get(key.as_bytes()).and_then(|bytes| {
                let result = if let Some(bytes) = bytes {
                    Some(String::from_utf8(bytes)?)
                } else {
                    None
                };
                Ok(result)
            })?;
            Ok(result)
        })
        .await?
    }

    /// 트랜잭션 내에서 JSON 객체를 조회합니다.
    pub async fn get_json<T>(&self, key: String) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        match self.get(key).await? {
            Some(json_str) => {
                let value = serde_json::from_str(&json_str)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// 트랜잭션 내에서 키를 삭제합니다.
    pub fn delete(&mut self, key: String) -> anyhow::Result<()> {
        self.pending_operations.push(Operation::Delete { key });
        Ok(())
    }

    /// 트랜잭션을 커밋합니다.
    pub async fn commit(self) -> anyhow::Result<()> {
        let mut batch = WriteBatch::default();

        // pending operations를 순서대로 batch에 추가
        for operation in self.pending_operations {
            match operation {
                Operation::Put { key, value } => {
                    batch.put(key.as_bytes(), value.as_bytes());
                }
                Operation::Delete { key } => {
                    batch.delete(key.as_bytes());
                }
            }
        }

        let inner = self.inner.clone();

        // WriteBatch를 데이터베이스에 적용
        task::spawn_blocking(move || {
            inner
                .db
                .write_opt(batch, &inner.write_opts)
                .map_err(anyhow::Error::from)
        })
        .await?
    }
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

#[derive(Clone)]
pub struct LocalDB {
    inner: Arc<LocalDBInner>,
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
        })
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.put(&key, &value)).await?
    }

    pub async fn get(&self, key: Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.get(&key)).await?
    }

    pub async fn delete(&self, key: Vec<u8>) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.delete(&key)).await?
    }

    pub async fn get_json<T>(&self, key: String) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        if let Some(data) = self.get(key.into_bytes()).await.and_then(|ret| {
            ret.map(|v| String::from_utf8(v).map_err(anyhow::Error::from))
                .transpose()
        })? {
            let value: T = serde_json::from_str(&data)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
    pub async fn put_json<T: Serialize>(&self, key: String, value: &T) -> anyhow::Result<()> {
        let json_str = serde_json::to_string(value)?;
        self.put(key.into_bytes(), json_str.into_bytes()).await
    }

    pub async fn delete_json(&self, key: String) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.delete(key.as_bytes())).await?
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

    /// 새로운 트랜잭션을 시작합니다.
    ///
    /// # Returns
    /// * `TransactionWrapper` - 트랜잭션 래퍼 객체
    ///
    /// # Example
    /// ```rust
    /// use cassry::leveldb::Leveldb;
    /// use serde::{Serialize, Deserialize};
    ///
    /// #[derive(Serialize, Deserialize, Debug)]
    /// struct User {
    ///     id: u32,
    ///     name: String,
    ///     balance: f64,
    /// }
    ///
    /// let db = Leveldb::new("test_db".to_string()).await?;
    ///
    /// // 트랜잭션 시작
    /// let mut tx = db.transaction();
    ///
    /// // 트랜잭션 내에서 여러 작업 수행
    /// let user1 = User { id: 1, name: "Alice".to_string(), balance: 100.0 };
    /// let user2 = User { id: 2, name: "Bob".to_string(), balance: 200.0 };
    ///
    /// tx.put_json("user:1", &user1)?;
    /// tx.put_json("user:2", &user2)?;
    ///
    /// // 기존 사용자 조회 및 업데이트
    /// if let Some(mut existing_user) = tx.get_json::<User>("user:1").await? {
    ///     existing_user.balance += 50.0;
    ///     tx.put_json("user:1", &existing_user)?;
    /// }
    ///
    /// // 트랜잭션 커밋
    /// tx.commit().await?;
    /// ```
    pub fn transaction(&self) -> TransactionWrapper {
        TransactionWrapper::new(self.inner.clone())
    }
}
