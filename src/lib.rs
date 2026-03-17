pub use tokio;

pub use postcard;
pub use serde;
pub use serde_json;

pub use anyhow;
pub use chrono;
pub use futures;
pub use reqwest;

pub use argon2;
pub use base64;
pub use blake3;
pub use hex;
pub use jsonwebtoken;
pub use keyed_lock;
pub use moka;
pub use regex;
pub use ring;
pub use rocksdb;
pub use rust_decimal;
pub use secrecy;
pub use uuid;
pub use zeroize;

pub mod cache;
pub mod cache_customed;
pub mod float;
pub mod json;
pub mod localdb;
pub mod localdb_ttl;
pub mod peak_timer;
pub mod secret_loader;
pub mod slack;
pub mod timelimiter;
pub mod types;
pub mod util;

pub mod goosheet;
pub mod serialization;
pub mod twdb;

pub use cache::*;
pub use float::*;
pub use localdb::*;
pub use types::*;
// pub use rust_decimal::Decimal;

pub mod _private_logger {
    pub fn trace(msg: String) {
        tracing::trace!("{}", msg);
    }

    pub fn debug(msg: String) {
        tracing::debug!("{}", msg);
    }

    pub fn info(msg: String) {
        tracing::info!("{}", msg);
    }

    pub fn warn(msg: String) {
        tracing::warn!("{}", msg);
        // crate::SLACK.put(log::Level::Warn, msg);
    }

    pub fn error(msg: String) {
        tracing::error!("{}", msg);
        // crate::SLACK.put(log::Level::Error, msg);
    }
}

#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) => ({
        let formatted = format!("{} ({}:{})", format_args!($($arg)*), file!(), line!());
        cassry::_private_logger::trace(formatted);
    })
}

#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => ({
        let formatted = format!("{} ({}:{})", format_args!($($arg)*), file!(), line!());
        cassry::_private_logger::debug(formatted);
    })
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => ({
        let formatted = format!("{} ({}:{})", format_args!($($arg)*), file!(), line!());
        cassry::_private_logger::info(formatted);
    })
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => ({
        let formatted = format!("{} ({}:{})", format_args!($($arg)*), file!(), line!());
        cassry::_private_logger::warn(formatted);
    })
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => ({
        let formatted = format!("{} ({}:{})", format_args!($($arg)*), file!(), line!());
        cassry::_private_logger::error(formatted);
    })
}

#[macro_export]
macro_rules! anyhowln {
    ($($arg:tt)*) => ({
        let formatted = format!("{} ({}:{})", format_args!($($arg)*), file!(), line!());
        anyhow::anyhow!("{}", formatted)
    })
}

#[macro_export]
macro_rules! cleanup {
    ($job:expr, $cleanup:block) => {{
        let __result = $job.await;
        $cleanup;
        __result
    }};
}

// fn maybe_send_to_slack(level: &str, msg: &str) {
//     if level == "ERROR" {  // For example, only send errors to Slack
//         let slack_message = format!("[{}]: {}", level, msg);
//         send_to_slack(&slack_message);
//     }
// }
