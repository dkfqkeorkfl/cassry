use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

pub type RwArc<T> = Arc<RwLock<T>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Line<T>(T, T);

impl<T> Line<T> {
    pub fn new(min: T, max: T) -> Self {
        Line(min, max)
    }

    pub fn min(&self) -> &T {
        &self.0
    }

    pub fn max(&self) -> &T {
        &self.1
    }
}

pub trait ErrCode {
    fn status(&self) -> u16;
    fn value(&self) -> u64;
    fn to_response(&self) -> (u16, serde_json::Value);
}

