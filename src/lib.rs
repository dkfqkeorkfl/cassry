pub use tokio;

pub use serde;
pub use serde_json;
pub use serde_urlencoded;
pub use postcard;

pub use reqwest;
pub use chrono;
pub use anyhow;
pub use futures;

pub use once_cell;
pub use hex;
pub use jsonwebtoken;
pub use ring;
pub use argon2;
pub use rust_decimal;
pub use regex;
pub use secrecy;
pub use zeroize;
pub use moka;
pub use blake3;
pub use base64;

pub mod cache;
pub mod cache_customed;
pub mod json;
pub mod localdb;
pub mod localdb_log;
pub mod localdb_ttl;
pub mod slack;
pub mod types;
pub mod util;
pub mod float;
pub mod timelimiter;
pub mod secret_loader;

pub mod twdb;
pub mod goosheet;
pub mod serialization;

pub use float::*;
pub use cache::*;
pub use localdb::*;
pub use types::*;
pub use log::Level as LogLvl;
pub use log::LevelFilter as LogLvlFilter;
// pub use rust_decimal::Decimal;

static SLACK: once_cell::sync::Lazy<slack::Slack> =
    once_cell::sync::Lazy::new(|| slack::Slack::default());

pub async fn init(
    path: &str,
    slack: slack::SlackParams
) -> anyhow::Result<()> {
    SLACK.init(slack).await;
    log4rs::init_file(path, Default::default())
}

pub mod _private_logger {
    pub fn trace(msg: String) {
        log::trace!("{}", msg);
    }

    pub fn debug(msg: String) {
        log::debug!("{}", msg);
    }

    pub fn info(msg: String) {
        log::info!("{}", msg);
    }

    pub fn warn(msg: String) {
        log::warn!("{}", msg);
        crate::SLACK.put(log::Level::Warn, msg);
    }

    pub fn error(msg: String) {
        log::error!("{}", msg);
        crate::SLACK.put(log::Level::Error, msg);
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
// fn maybe_send_to_slack(level: &str, msg: &str) {
//     if level == "ERROR" {  // For example, only send errors to Slack
//         let slack_message = format!("[{}]: {}", level, msg);
//         send_to_slack(&slack_message);
//     }
// }
