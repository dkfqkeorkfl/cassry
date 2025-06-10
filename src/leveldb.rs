// use leveldb::;
use std::{path::Path, sync::Arc};

use rocksdb::{DB, Options};

#[derive(Debug, Clone)]
pub struct Leveldb {
    db: Arc<DB>,
}

impl Leveldb {
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let p = Path::new(path);
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, p)?;
        Ok(Leveldb {
            db: Arc::new(db),
        })
    }

    pub fn put(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.db.put(key.as_bytes(), value.as_bytes())
            .map_err(anyhow::Error::from)
    }

    pub fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        Ok(self.db.get(key.as_bytes())
            .map_err(anyhow::Error::from)?
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
}
