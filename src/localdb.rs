use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, DBWithThreadMode, MultiThreaded, Options,
    ReadOptions, WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use tokio::task;

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
    path: String,
}

/// 트랜잭션을 위한 래퍼 구조체 (WriteBatch 기반)
pub struct TransactionWrapper {
    batch: WriteBatch,
    inner: Arc<LocalDBInner>,
    pending_operations: Vec<(String, String)>, // key, value pairs for JSON operations
}

impl TransactionWrapper {
    /// 새로운 트랜잭션을 생성합니다.
    pub(self) fn new(inner: Arc<LocalDBInner>) -> Self {
        Self {
            batch: WriteBatch::default(),
            inner,
            pending_operations: Vec::new(),
        }
    }

    /// 트랜잭션 내에서 키-값 쌍을 저장합니다.
    pub fn put(&mut self, key: &str, value: &str) -> anyhow::Result<()> {
        self.batch.put(key.as_bytes(), value.as_bytes());
        Ok(())
    }

    /// 트랜잭션 내에서 JSON 객체를 저장합니다.
    pub fn put_json<T>(&mut self, key: &str, value: &T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let json_str = serde_json::to_string(value)?;
        self.pending_operations.push((key.to_string(), json_str));
        Ok(())
    }

    /// 트랜잭션 내에서 키로 값을 조회합니다.
    pub async fn get(&self, key: String) -> anyhow::Result<Option<String>> {
        let inner = self.inner.clone();

        task::spawn_blocking(move || {
            Ok(inner
                .db
                .get_opt(key.as_bytes(), &inner.read_opts)?
                .map(|bytes| String::from_utf8_lossy(&bytes).to_string()))
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
    pub fn delete(&mut self, key: &str) -> anyhow::Result<()> {
        self.batch.delete(key.as_bytes());
        Ok(())
    }

    /// 트랜잭션을 커밋합니다.
    pub async fn commit(mut self) -> anyhow::Result<()> {
        // 먼저 pending JSON operations를 batch에 추가
        for (key, value) in self.pending_operations {
            self.batch.put(key.as_bytes(), value.as_bytes());
        }

        let inner = self.inner.clone();
        let batch = self.batch;

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
    fn new(path: String) -> anyhow::Result<Self> {
        let p = Path::new(&path);

        // 블록 캐시 설정 (4GB)
        let mut block_opts = BlockBasedOptions::default();
        let cache = Cache::new_lru_cache(4 * 1024 * 1024 * 1024);
        block_opts.set_block_cache(&cache);
        block_opts.set_bloom_filter(10.0, false);
        block_opts.set_cache_index_and_filter_blocks(true);

        // 데이터베이스 옵션 설정
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_block_based_table_factory(&block_opts);

        // 성능 최적화 설정
        options.set_max_background_jobs(4);
        options.set_bytes_per_sync(1024 * 1024);
        options.set_write_buffer_size(64 * 1024 * 1024);
        options.set_max_write_buffer_number(3);
        options.set_min_write_buffer_number_to_merge(2);
        options.set_max_bytes_for_level_base(256 * 1024 * 1024);
        options.set_target_file_size_base(64 * 1024 * 1024);

        // 압축 설정
        options.set_compression_type(DBCompressionType::Lz4);
        options.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
        ]);

        // I/O 설정
        options.set_use_direct_reads(true);
        options.set_use_direct_io_for_flush_and_compaction(true);
        options.set_compaction_readahead_size(2 * 1024 * 1024);

        // 파일 관리 설정
        options.set_max_open_files(-1);
        options.set_keep_log_file_num(1000);
        options.set_max_manifest_file_size(1024 * 1024 * 1024);

        // 데이터 안정성 설정
        options.set_paranoid_checks(true);
        options.set_manual_wal_flush(false);
        options.set_atomic_flush(true);

        // Write 옵션 설정
        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(false);
        write_opts.set_sync(false);

        // Read 옵션 설정
        let mut read_opts = ReadOptions::default();
        read_opts.set_verify_checksums(true);
        read_opts.set_async_io(true);
        read_opts.set_readahead_size(2 * 1024 * 1024);

        let db = DBWithThreadMode::<MultiThreaded>::open(&options, p)?;

        Ok(LocalDBInner {
            db,
            write_opts,
            read_opts,
            path,
        })
    }

    fn put(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.db
            .put_opt(key.as_bytes(), value.as_bytes(), &self.write_opts)
            .map_err(anyhow::Error::from)
    }

    fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        Ok(self
            .db
            .get_opt(key.as_bytes(), &self.read_opts)?
            .map(|bytes| String::from_utf8_lossy(&bytes).to_string()))
    }

    fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.db
            .delete_opt(key.as_bytes(), &self.write_opts)
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
    pub async fn new(path: String) -> anyhow::Result<Self> {
        let inner = task::spawn_blocking(move || LocalDBInner::new(path)).await??;

        Ok(LocalDB {
            inner: Arc::new(inner),
        })
    }

    pub async fn put(&self, key: String, value: String) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.put(&key, &value)).await?
    }

    pub async fn get(&self, key: String) -> anyhow::Result<Option<String>> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.get(&key)).await?
    }

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

    pub async fn put_json<T>(&self, key: String, value: &T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let json_str = serde_json::to_string(value)?;
        self.put(key, json_str).await
    }

    pub async fn delete(&self, key: String) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.delete(&key)).await?
    }

    pub fn get_path(&self) -> &str {
        &self.inner.path
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