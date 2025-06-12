use std::{path::Path, sync::Arc};
use rocksdb::{Options, BlockBasedOptions, Cache, WriteOptions, ReadOptions, DBCompressionType, DBWithThreadMode, MultiThreaded, Error as RocksError};
use serde::{Serialize, Deserialize};

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
}

impl Leveldb {
    pub fn new(path: &str) -> Result<Self, RocksError> {
        let p = Path::new(path);
        
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
        })
    }

    pub fn put(&self, key: &str, value: &str) -> Result<(), RocksError> {
        self.db.put_opt(key.as_bytes(), value.as_bytes(), &self.write_opts)
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, RocksError> {
        Ok(self.db.get_opt(key.as_bytes(), &self.read_opts)?
            .map(|bytes| String::from_utf8_lossy(&bytes).to_string()))
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

    pub fn get_stats(&self) -> DBStats {
        // 데이터베이스 속성 가져오기
        let props = self.db.property_value("rocksdb.estimate-num-keys")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let mem_usage = self.db.property_value("rocksdb.estimate-memory-usage")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let block_cache = self.db.property_value("rocksdb.block-cache-usage")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let memtable = self.db.property_value("rocksdb.cur-size-all-mem-tables")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let sst_size = self.db.property_value("rocksdb.total-sst-files-size")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let read_amp = self.db.property_value("rocksdb.estimate-table-readers-mem")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let write_amp = self.db.property_value("rocksdb.actual-delayed-write-rate")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let compression = self.db.property_value("rocksdb.compression-ratio-at-level0")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0) / 100.0;
        let pending = self.db.property_value("rocksdb.estimate-pending-compaction-bytes")
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

    pub fn flush(&self) -> Result<(), RocksError> {
        self.db.flush()
    }

    pub fn get_property(&self, name: &str) -> Result<Option<String>, RocksError> {
        self.db.property_value(name)
    }
}

impl Drop for Leveldb {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            eprintln!("Error flushing database: {}", e);
        }
    }
}
