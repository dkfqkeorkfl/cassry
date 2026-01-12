use std::{ops::{Add, Div, Mul, Sub}, sync::Arc};

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
    fn as_response(&self) -> (u16, serde_json::Value);
}

pub trait Arithmetic:
    Add<Output = Self> + Sub<Output = Self> + Mul<Output = Self> + Div<Output = Self> + Copy
{
}

impl<T> Arithmetic for T where
    T: Add<Output = T> + Sub<Output = T> + Mul<Output = T> + Div<Output = T> + Copy
{
}

pub trait Bool {
    fn as_bool() -> bool;
}

pub struct True;
impl Bool for True {
    fn as_bool() -> bool {
        true
    }
}

pub struct False;
impl Bool for False {
    fn as_bool() -> bool {
        false
    }
}