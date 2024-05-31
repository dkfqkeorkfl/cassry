use std::{
    collections::HashMap,
    sync::Arc,
};

use chrono::Local;
use regex::Regex;
use reqwest::{Client, Response};
use tokio::sync::RwLock;
use super::timelimiter::Limiter;

struct Inner {
    token: String,
    channels: [String; log::Level::Trace as usize + 1],
    messages: Vec<(log::Level, String)>,
    client: Client,

    limiter : Limiter<String>
}

impl Inner {
    // pub fn convert_usize_to_level(v: usize) -> Option<log::Level> {
    //     match v {
    //         1 => Some(log::Level::Error),
    //         2 => Some(log::Level::Warn),
    //         3 => Some(log::Level::Info),
    //         4 => Some(log::Level::Debug),
    //         5 => Some(log::Level::Trace),
    //         _ => None,
    //     }
    // }

    pub fn new(token: String, channels: HashMap<log::Level, String>, cooldown: f64) -> Self {
        let mut items = [
            String::default(),
            String::default(),
            String::default(),
            String::default(),
            String::default(),
            String::default(),
        ];
        for (k, v) in channels.into_iter() {
            let i = k as usize;
            items[i] = v;
        }

        Inner {
            token,
            channels: items,
            messages: Default::default(),
            client: Default::default(),
            limiter:Limiter::new(cooldown)
        }
    }

    pub async fn send(&self, item: &(log::Level, String)) -> anyhow::Result<Response> {
        let channel = &self.channels[item.0 as usize];
        let res = self
            .client
            .post("https://slack.com/api/chat.postMessage")
            .bearer_auth(self.token.clone())
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&serde_json::json!({
                "channel": channel,
                "text": item.1
            }))
            .send()
            .await?
            .error_for_status()?;

        Ok(res)
    }

    pub async fn put(&mut self, lvl: log::Level, msg: String) {
        let mut i = 0;
        let i = loop {
            if i < self.messages.len() {
                if let Ok(_res) = self.send(&self.messages[i]).await {
                    i += 1;
                } else {
                    break i;
                }
            } else {
                break i;
            }
        };
        self.messages.drain(0..i);

        let key = self.normalize_message(&msg);
        if self.limiter.put(key.into()).is_some() {
            return;
        }

        let message = format!(
            "[{}][{}] - {}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            lvl.to_string(),
            msg
        );
        let item = (lvl, message);
        let remain = if self.messages.is_empty() {
            if let Ok(_res) = self.send(&item).await {
                None
            } else {
                Some(item)
            }
        } else {
            Some(item)
        };

        if let Some(item) = remain {
            self.messages.push(item);
        }
    }

    fn normalize_message(&self, msg: &str) -> String {
        let re = Regex::new(r"\d").unwrap();
        re.replace_all(msg, "").to_string()
    }
}

pub struct SlackParams {
    pub token: String,
    pub channels: HashMap<log::Level, String>,
    pub cooldown: f64,
}

#[derive(Default)]
pub struct Slack {
    inner: Arc<RwLock<Option<Inner>>>,
}

impl Slack {
    pub async fn init(
        &self,
        params : SlackParams,
    ) {
        let mut inner = self.inner.write().await;
        *inner = Some(Inner::new(params.token, params.channels, params.cooldown));
    }

    pub fn put(&self, lvl: log::Level, msg: String) {
        let cloned = self.inner.clone();
        tokio::spawn(async move {
            if let Some(inner) = cloned.write().await.as_mut() {
                if !inner.channels[lvl as usize].is_empty() {
                    inner.put(lvl, msg).await;
                }
            }
        });
    }
}
