use chrono::{DateTime, Utc};
use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, DBWithThreadMode, Direction,
    IteratorMode as RocksIteratorMode, MultiThreaded, Options, ReadOptions, WriteBatch,
    WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use tokio::{sync::Mutex, task};

pub mod config {
    use super::*;

    use std::path::PathBuf;

    pub use crate::Bool;

    pub trait Generator {
        fn generate(
            &self,
        ) -> anyhow::Result<(DBWithThreadMode<MultiThreaded>, WriteOptions, ReadOptions)>;
    }

    /// RocksDB 압축 타입 (직렬화 가능)
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum CompressionType {
        None,
        Snappy,
        Zlib,
        Bz2,
        Lz4,
        Lz4hc,
        Zstd,
        #[serde(other)]
        Unknown,
    }

    impl From<CompressionType> for DBCompressionType {
        fn from(ct: CompressionType) -> Self {
            match ct {
                CompressionType::None => DBCompressionType::None,
                CompressionType::Snappy => DBCompressionType::Snappy,
                CompressionType::Zlib => DBCompressionType::Zlib,
                CompressionType::Bz2 => DBCompressionType::Bz2,
                CompressionType::Lz4 => DBCompressionType::Lz4,
                CompressionType::Lz4hc => DBCompressionType::Lz4hc,
                CompressionType::Zstd => DBCompressionType::Zstd,
                CompressionType::Unknown => DBCompressionType::None,
            }
        }
    }

    /// BlockBasedOptions 설정
    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct BlockBasedConfig {
        /// 블록 캐시 크기 (바이트)
        pub cache_size: Option<usize>,

        /// Bloom filter bits per key
        pub bloom_filter_bits_per_key: Option<f64>,

        /// Index 및 filter block을 캐시할지 여부
        pub cache_index_and_filter_blocks: Option<bool>,

        /// L0 filter 및 index block을 캐시에 고정할지 여부
        pub pin_l0_filter_and_index_blocks_in_cache: Option<bool>,
    }

    impl BlockBasedConfig {
        /// BlockBasedOptions를 빌드합니다.
        pub fn build(&self) -> BlockBasedOptions {
            let mut block_opts = BlockBasedOptions::default();
            if let Some(cache_size) = self.cache_size {
                let cache = Cache::new_lru_cache(cache_size);
                block_opts.set_block_cache(&cache);
            }

            if let Some(bits) = self.bloom_filter_bits_per_key {
                block_opts.set_bloom_filter(bits, false);
            }
            if let Some(cache_index) = self.cache_index_and_filter_blocks {
                block_opts.set_cache_index_and_filter_blocks(cache_index);
            }
            if let Some(pin_l0) = self.pin_l0_filter_and_index_blocks_in_cache {
                block_opts.set_pin_l0_filter_and_index_blocks_in_cache(pin_l0);
            }
            block_opts
        }
    }

    /// WriteOptions 설정
    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct WriteOptionsConfig {
        /// WAL 비활성화 여부
        pub disable_wal: Option<bool>,

        /// 동기 쓰기 여부
        pub sync: Option<bool>,
    }

    impl WriteOptionsConfig {
        /// WriteOptions를 빌드합니다.
        pub fn build(&self) -> WriteOptions {
            let mut write_opts = WriteOptions::default();
            if let Some(disable_wal) = self.disable_wal {
                write_opts.disable_wal(disable_wal);
            }
            if let Some(sync) = self.sync {
                write_opts.set_sync(sync);
            }
            write_opts
        }
    }

    /// ReadOptions 설정
    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct ReadOptionsConfig {
        /// 체크섬 검증 여부
        pub verify_checksums: Option<bool>,

        /// 비동기 I/O 사용 여부
        pub async_io: Option<bool>,

        /// Readahead 크기 (바이트)
        pub readahead_size: Option<u64>,
    }

    impl ReadOptionsConfig {
        /// ReadOptions를 빌드합니다.
        pub fn build(&self) -> ReadOptions {
            let mut read_opts = ReadOptions::default();
            if let Some(verify_checksums) = self.verify_checksums {
                read_opts.set_verify_checksums(verify_checksums);
            }
            if let Some(async_io) = self.async_io {
                read_opts.set_async_io(async_io);
            }
            if let Some(readahead_size) = self.readahead_size {
                read_opts.set_readahead_size(readahead_size as usize);
            }
            read_opts
        }
    }

    /// RocksDB 전체 설정 구조체
    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct DBConfig {
        /// BlockBasedOptions 설정
        pub block_based: Option<BlockBasedConfig>,

        /// 최대 백그라운드 작업 수
        pub max_background_jobs: Option<i32>,

        /// Sync당 바이트 수
        pub bytes_per_sync: Option<u64>,

        /// Write buffer 크기 (바이트)
        pub write_buffer_size: Option<u64>,

        /// 최대 write buffer 개수
        pub max_write_buffer_number: Option<i32>,

        /// Merge에 필요한 최소 write buffer 개수
        pub min_write_buffer_number_to_merge: Option<i32>,

        /// Level 0 기준 최대 바이트 수
        pub max_bytes_for_level_base: Option<u64>,

        /// 파일 크기 기준 (바이트)
        pub target_file_size_base: Option<u64>,

        /// 기본 압축 타입
        pub compression_type: Option<CompressionType>,

        /// 레벨별 압축 타입
        pub compression_per_level: Option<Vec<CompressionType>>,

        /// Direct reads 사용 여부
        pub use_direct_reads: Option<bool>,

        /// Flush 및 compaction에 Direct I/O 사용 여부
        pub use_direct_io_for_flush_and_compaction: Option<bool>,

        /// Compaction readahead 크기 (바이트)
        pub compaction_readahead_size: Option<u64>,

        /// 최대 열린 파일 개수 (-1은 무제한)
        pub max_open_files: Option<i32>,

        /// 유지할 로그 파일 개수
        pub keep_log_file_num: Option<usize>,

        /// Manifest 파일 최대 크기 (바이트)
        pub max_manifest_file_size: Option<u64>,

        /// Paranoid checks 활성화 여부
        pub paranoid_checks: Option<bool>,

        /// 수동 WAL flush 여부
        pub manual_wal_flush: Option<bool>,

        /// 원자적 flush 활성화 여부
        pub atomic_flush: Option<bool>,

        /// WriteOptions 설정
        pub write_options: Option<WriteOptionsConfig>,

        /// ReadOptions 설정
        pub read_options: Option<ReadOptionsConfig>,
    }

    impl DBConfig {
        /// Options를 빌드합니다.
        pub fn build_db_opt(&self) -> anyhow::Result<Options> {
            let mut options = Options::default();
            if let Some(block_based) = &self.block_based {
                let block_opts = block_based.build();
                options.set_block_based_table_factory(&block_opts);
            }

            if let Some(max_background_jobs) = self.max_background_jobs {
                options.set_max_background_jobs(max_background_jobs);
            }
            if let Some(bytes_per_sync) = self.bytes_per_sync {
                options.set_bytes_per_sync(bytes_per_sync);
            }
            if let Some(write_buffer_size) = self.write_buffer_size {
                options.set_write_buffer_size(write_buffer_size as usize);
            }
            if let Some(max_write_buffer_number) = self.max_write_buffer_number {
                options.set_max_write_buffer_number(max_write_buffer_number);
            }
            if let Some(min_write_buffer_number_to_merge) = self.min_write_buffer_number_to_merge {
                options.set_min_write_buffer_number_to_merge(min_write_buffer_number_to_merge);
            }
            if let Some(max_bytes_for_level_base) = self.max_bytes_for_level_base {
                options.set_max_bytes_for_level_base(max_bytes_for_level_base);
            }
            if let Some(target_file_size_base) = self.target_file_size_base {
                options.set_target_file_size_base(target_file_size_base);
            }

            if let Some(compression_type) = self.compression_type {
                options.set_compression_type(compression_type.into());
            }
            if let Some(ref compression_per_level) = self.compression_per_level {
                let compression_levels: Vec<DBCompressionType> =
                    compression_per_level.iter().map(|&ct| ct.into()).collect();
                options.set_compression_per_level(&compression_levels);
            }

            if let Some(use_direct_reads) = self.use_direct_reads {
                options.set_use_direct_reads(use_direct_reads);
            }
            if let Some(use_direct_io) = self.use_direct_io_for_flush_and_compaction {
                options.set_use_direct_io_for_flush_and_compaction(use_direct_io);
            }
            if let Some(compaction_readahead_size) = self.compaction_readahead_size {
                options.set_compaction_readahead_size(compaction_readahead_size as usize);
            }

            if let Some(max_open_files) = self.max_open_files {
                options.set_max_open_files(max_open_files);
            }
            if let Some(keep_log_file_num) = self.keep_log_file_num {
                options.set_keep_log_file_num(keep_log_file_num);
            }
            if let Some(max_manifest_file_size) = self.max_manifest_file_size {
                options.set_max_manifest_file_size(max_manifest_file_size as usize);
            }

            if let Some(paranoid_checks) = self.paranoid_checks {
                options.set_paranoid_checks(paranoid_checks);
            }
            if let Some(manual_wal_flush) = self.manual_wal_flush {
                options.set_manual_wal_flush(manual_wal_flush);
            }
            if let Some(atomic_flush) = self.atomic_flush {
                options.set_atomic_flush(atomic_flush);
            }

            Ok(options)
        }

        /// WriteOptions를 빌드합니다.
        pub fn build_write_opt(&self) -> WriteOptions {
            self.write_options
                .as_ref()
                .map(|cfg| cfg.build())
                .unwrap_or_default()
        }

        /// ReadOptions를 빌드합니다.
        pub fn build_read_opt(&self) -> ReadOptions {
            self.read_options
                .as_ref()
                .map(|cfg| cfg.build())
                .unwrap_or_default()
        }
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

    #[derive(Clone)]
    pub struct Loader<B: Bool>(PathBuf, PathBuf, std::marker::PhantomData<B>);
    impl<B: Bool> Loader<B> {
        pub fn new(path: PathBuf, cfg: PathBuf) -> Self {
            Self(path, cfg, std::marker::PhantomData::<B>)
        }

        pub fn parse_tag(path: &PathBuf, cfg_root: &PathBuf) -> anyhow::Result<Self> {
            let (db_path, cfg_path) = if let Some((db_name, cfg_name)) =
                path.file_stem().and_then(|stem| {
                    stem.to_string_lossy()
                        .split_once('@')
                        .map(|(v1, v2)| (v1.to_string(), v2.to_string()))
                }) {
                let mut cfg_path = cfg_root.join(cfg_name);
                cfg_path.add_extension("json");

                let db_path = if let Some(parent) = path.parent() {
                    parent.join(db_name)
                } else {
                    PathBuf::from(db_name)
                };

                (db_path, cfg_path)
            } else {
                return Err(anyhow::anyhow!("Invalid filename: {}", path.display()));
            };

            Ok(Self(db_path, cfg_path, std::marker::PhantomData::<B>))
        }
    }

    impl<B: Bool> Generator for Loader<B> {
        fn generate(
            &self,
        ) -> anyhow::Result<(DBWithThreadMode<MultiThreaded>, WriteOptions, ReadOptions)> {
            let Loader(db_path, cfg_path, _) = self;
            let cfg = serde_json::from_slice::<DBConfig>(&std::fs::read(cfg_path)?).unwrap();
            let options = {
                let mut options = cfg.build_db_opt()?;
                options.create_if_missing(B::as_bool());
                options
            };

            let (db_stem, cfg_stem) =
                db_path
                    .file_name()
                    .zip(cfg_path.file_stem())
                    .ok_or(anyhow::anyhow!(
                        "DB path is not a file: {}",
                        db_path.display()
                    ))?;
            let filename = format!(
                "{}@{}",
                db_stem.to_string_lossy(),
                cfg_stem.to_string_lossy()
            );
            let mut fullpath = db_path.clone();
            fullpath.set_file_name(filename);

            let db = DBWithThreadMode::<MultiThreaded>::open(&options, &fullpath)?;
            let write_opts = cfg.build_write_opt();
            let read_opts = cfg.build_read_opt();
            Ok((db, write_opts, read_opts))
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
            options.set_min_write_buffer_number_to_merge(1);
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
            options.set_min_write_buffer_number_to_merge(1);
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

pub enum IteratorMode {
    Start,
    End,
    From(Vec<u8>, Direction),
}

impl IteratorMode {
    fn to_rocksdb_mode<'a>(&'a self) -> RocksIteratorMode<'a> {
        match self {
            IteratorMode::Start => RocksIteratorMode::Start,
            IteratorMode::End => RocksIteratorMode::End,
            IteratorMode::From(key, direction) => {
                RocksIteratorMode::From(key.as_slice(), *direction)
            }
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

pub struct TakeResult {
    pub items: Vec<(Vec<u8>, Vec<u8>)>,
    pub last_key: Option<Vec<u8>>,
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
        let mut iter = self.db.iterator(RocksIteratorMode::End);
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

    fn flush(&self) -> anyhow::Result<()> {
        self.db.flush().map_err(anyhow::Error::from)
    }

    fn get_property(&self, name: &str) -> anyhow::Result<Option<String>> {
        self.db.property_value(name).map_err(anyhow::Error::from)
    }

    pub fn foreach<F>(&self, callback: F) -> anyhow::Result<()>
    where
        F: Fn(&[u8], &[u8]) -> anyhow::Result<()> + Send + Sync + 'static + Clone,
    {
        let iter = self.raw().iterator(RocksIteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            callback(&key, &value)?;
        }
        Ok(())
    }

    pub fn take(&self, iter: IteratorMode, cnt: usize) -> anyhow::Result<TakeResult> {
        let mut result = Vec::new();
        let mut last_key = None;
        let iter = self.raw().iterator(iter.to_rocksdb_mode());
        for item in iter.take(cnt + 1) {
            let (key, value) = item?;
            if result.len() < cnt {
                result.push((key.to_vec(), value.to_vec()));
            } else {
                last_key = Some(key.to_vec());
            }
        }

        Ok(TakeResult {
            items: result,
            last_key,
        })
    }

    /// 내부 store의 총 아이템 개수를 반환합니다 (근사치).
    /// RocksDB의 `rocksdb.estimate-num-keys` 속성을 사용합니다.
    ///
    /// # 성능
    /// - 매우 빠름 (O(1))
    /// - 근사치이므로 정확한 개수와 다를 수 있습니다
    /// - 중복/삭제가 없는 경우 일반적으로 ±5% 이내의 정확도를 가집니다
    ///
    /// # 용도
    /// 배치 임계값 체크 등 근사치로 충분한 경우에 사용하세요.
    // fn count(&self) -> anyhow::Result<u64> {
    //     let count = self
    //         .db
    //         .property_value("rocksdb.estimate-num-keys")
    //         .map_err(anyhow::Error::from)?
    //         .and_then(|v| v.parse::<u64>().ok())
    //         .unwrap_or(0);
    //     Ok(count)
    // }

    /// 컴팩트 후 근사치를 반환합니다.
    /// `count()`보다 정확하지만 여전히 근사치입니다.
    ///
    /// # 성능
    /// - 컴팩트 작업으로 인해 느릴 수 있습니다 (수 초 ~ 수십 초)
    /// - 컴팩트 후 estimate가 더 정확해집니다 (일반적으로 ±1% 이내)
    ///
    /// # 용도
    /// 더 정확한 근사치가 필요한 경우에 사용하세요.
    // fn count_with_compact(&self) -> anyhow::Result<u64> {
    //     // 전체 범위 컴팩트 (None은 전체 범위를 의미)
    //     // RocksDB의 compact_range는 Option<&[u8]> 타입을 받습니다
    //     self.db.compact_range(None::<&[u8]>, None::<&[u8]>);

    //     // 컴팩트 후 estimate 조회
    //     self.count()
    // }

    /// 내부 store가 비어있는지 확인합니다.
    /// Iterator에서 첫 번째 아이템을 확인하여 정확하게 판단합니다.
    ///
    /// # 성능
    /// - 빠름 (O(1), 첫 번째 아이템만 확인)
    /// - `count()`보다 정확합니다 (estimate가 아닌 실제 확인)
    ///
    /// # 용도
    /// 정확한 empty 체크가 필요한 경우에 사용하세요.
    fn is_empty(&self) -> anyhow::Result<bool> {
        let mut iter = self.db.iterator(RocksIteratorMode::Start);
        Ok(iter.next().is_none())
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

    pub async fn is_empty(&self) -> anyhow::Result<bool> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.is_empty()).await?
    }

    pub async fn commit(&self, batch: WriteBatch) -> anyhow::Result<()> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.commit(batch)).await?
    }

    pub async fn foreach<F>(&self, callback: F) -> anyhow::Result<()>
    where
        F: Fn(&[u8], &[u8]) -> anyhow::Result<()> + Send + Sync + 'static + Clone,
    {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.foreach(callback)).await?
    }

    pub async fn take(&self, iter: IteratorMode, cnt: usize) -> anyhow::Result<TakeResult> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || inner.take(iter, cnt)).await?
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
        let ks = serde_json::to_string(key)?;
        let vs = serde_json::to_string(value)?;
        self.put_raw(ks.into_bytes(), vs.into_bytes()).await
    }

    pub async fn delete_json(&self, key: &impl Serialize) -> anyhow::Result<()> {
        let ks = serde_json::to_string(key)?;
        self.delete_raw(ks.into_bytes()).await
    }

    pub async fn get_json<T>(&self, key: &impl Serialize) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let ks = serde_json::to_string(key)?;
        if let Some(data) = self.get_raw(ks.into_bytes()).await.and_then(|ret| {
            ret.map(|v| String::from_utf8(v).map_err(anyhow::Error::from))
                .transpose()
        })? {
            let value: T = serde_json::from_str(&data)?;
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

pub struct DrainerCommit {
    db: Arc<LocalDB>,
    last_key: Option<Vec<u8>>,
}

impl DrainerCommit {
    pub fn new(db: Arc<LocalDB>, last_key: Option<Vec<u8>>) -> Self {
        Self { db, last_key }
    }

    pub async fn commit(&self) -> anyhow::Result<()> {
        self.db
            .put_raw(
                Default::default(),
                self.last_key.clone().unwrap_or_default(),
            )
            .await
    }
}

pub struct LocalDBDrainer {
    db: Mutex<Arc<LocalDB>>,
}

impl LocalDBDrainer {
    pub fn open(db: LocalDB) -> Self {
        Self {
            db: Mutex::new(Arc::new(db)),
        }
    }

    pub async fn set_offset(&self, offset: Option<Vec<u8>>) -> anyhow::Result<()> {
        self.db
            .lock()
            .await
            .put_raw(Default::default(), offset.unwrap_or_default())
            .await
    }

    pub async fn get_offset(&self) -> anyhow::Result<Option<Vec<u8>>> {
        self.db.lock().await.get_raw(Default::default()).await
    }

    pub async fn reset(&self) -> anyhow::Result<()> {
        self.db.lock().await.delete_raw(Default::default()).await
    }

    pub async fn consume<F, Fut>(&self, cnt: usize, callback: F) -> anyhow::Result<Option<Vec<u8>>>
    where
        F: FnOnce(Vec<(Vec<u8>, Vec<u8>)>, Arc<DrainerCommit>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let db = self.db.lock().await;
        let offset = db.get_raw(Default::default()).await?;
        let iter = if let Some(offset) = offset {
            if offset.is_empty() {
                return Ok(None);
            } else {
                IteratorMode::From(offset, Direction::Forward)
            }
        } else {
            IteratorMode::Start
        };

        let result = db.take(iter, cnt).await?;
        if !result.items.is_empty() {
            let commitor = Arc::new(DrainerCommit::new(db.clone(), result.last_key.clone()));
            callback(result.items, commitor).await?;
        }

        Ok(result.last_key)
    }
}

pub async fn test() -> anyhow::Result<()> {
    #[derive(Serialize, Deserialize, Debug)]
    struct Test {
        id: usize,
    }
    impl From<usize> for Test {
        fn from(id: usize) -> Self {
            Test { id }
        }
    }

    let cache_root = std::path::Path::new("/test");
    if !cache_root.exists() {
        tokio::fs::create_dir_all(cache_root).await?;
    }

    let interval = std::time::Duration::from_secs(1);
    let db = LocalDB::new(config::General::new("/test/localdb".to_string())).await?;
    let mut src = Vec::<Test>::new();
    while src.len() < 20 {
        let start = std::time::Instant::now();
        src.push(src.len().into());
        let item = src.last().unwrap();
        db.put_json(&item.id, item).await?;
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
        println!("[localdb] json: {}", results.join(", "));

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
        println!("[localdb] delete: {}", results.join(", "));
        src.clear();
    }

    while src.len() < 20 {
        let start = std::time::Instant::now();
        src.push(src.len().into());
        let item = src.last().unwrap();
        db.put_postcard(&item.id, item).await?;
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
        println!("[localdb] postcard: {}", results.join(", "));

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
        println!("[localdb] delete: {}", results.join(", "));
        src.clear();
    }

    Ok(())
}
