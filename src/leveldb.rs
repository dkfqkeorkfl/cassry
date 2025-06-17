use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, DBWithThreadMode,
    MultiThreaded, Options, ReadOptions, WriteOptions, WriteBatch,
};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};

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

#[derive(Clone)]
pub struct Leveldb {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    write_opts: Arc<WriteOptions>,
    read_opts: Arc<ReadOptions>,
    path: String,
}

/// 트랜잭션을 위한 래퍼 구조체 (WriteBatch 기반)
pub struct TransactionWrapper {
    batch: WriteBatch,
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    write_opts: Arc<WriteOptions>,
    read_opts: Arc<ReadOptions>,
    pending_operations: Vec<(String, String)>, // key, value pairs for JSON operations
}

impl TransactionWrapper {
    /// 새로운 트랜잭션을 생성합니다.
    pub fn new(db: Arc<DBWithThreadMode<MultiThreaded>>, write_opts: Arc<WriteOptions>, read_opts: Arc<ReadOptions>) -> Self {
        Self {
            batch: WriteBatch::default(),
            db,
            write_opts,
            read_opts,
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
    pub fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        Ok(self
            .db
            .get_opt(key.as_bytes(), &self.read_opts)?
            .map(|bytes| String::from_utf8_lossy(&bytes).to_string()))
    }

    /// 트랜잭션 내에서 JSON 객체를 조회합니다.
    pub fn get_json<T>(&self, key: &str) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        match self.get(key)? {
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
    pub fn commit(mut self) -> anyhow::Result<()> {
        // 먼저 pending JSON operations를 batch에 추가
        for (key, value) in self.pending_operations {
            self.batch.put(key.as_bytes(), value.as_bytes());
        }
        
        // WriteBatch를 데이터베이스에 적용
        self.db.write_opt(self.batch, &self.write_opts)
            .map_err(anyhow::Error::from)
    }
}

impl Leveldb {
    pub fn new(path: String) -> anyhow::Result<Self> {
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

        Ok(Leveldb {
            db: Arc::new(db),
            write_opts: Arc::new(write_opts),
            read_opts: Arc::new(read_opts),
            path: path,
        })
    }

    pub fn put(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.db
            .put_opt(key.as_bytes(), value.as_bytes(), &self.write_opts)
            .map_err(anyhow::Error::from)
    }

    pub fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        Ok(self
            .db
            .get_opt(key.as_bytes(), &self.read_opts)?
            .map(|bytes| String::from_utf8_lossy(&bytes).to_string()))
    }

    pub fn get_json<T>(&self, key: &str) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        match self.get(key)? {
            Some(json_str) => {
                let value = serde_json::from_str(&json_str)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub fn put_json<T>(&self, key: &str, value: &T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let json_str = serde_json::to_string(value)?;
        self.put(key, &json_str)
    }

    pub fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.db
            .delete_opt(key.as_bytes(), &self.write_opts)
            .map_err(anyhow::Error::from)
    }

    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(usize, String, String),
    {
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for (i, item) in iter.enumerate() {
            if let Ok((key, value)) = item {
                let key_str = String::from_utf8_lossy(&key).to_string();
                let value_str = String::from_utf8_lossy(&value).to_string();
                f(i, key_str, value_str);
            }
        }
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn get_stats(&self) -> DBStats {
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

    pub fn compact_range(&self) {
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
    }

    pub fn flush(&self) -> anyhow::Result<()> {
        self.db.flush().map_err(anyhow::Error::from)
    }

    pub fn get_property(&self, name: &str) -> anyhow::Result<Option<String>> {
        self.db.property_value(name).map_err(anyhow::Error::from)
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
    /// let db = Leveldb::new("test_db".to_string())?;
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
    /// if let Some(mut existing_user) = tx.get_json::<User>("user:1")? {
    ///     existing_user.balance += 50.0;
    ///     tx.put_json("user:1", &existing_user)?;
    /// }
    /// 
    /// // 트랜잭션 커밋
    /// tx.commit()?;
    /// ```
    pub fn transaction(&self) -> TransactionWrapper {
        TransactionWrapper::new(self.db.clone(), self.write_opts.clone(), self.read_opts.clone())
    }
}

impl Drop for Leveldb {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            eprintln!("Error flushing database: {}", e);
        }
    }
}
