use std::{collections::HashMap, sync::Arc};

use reqwest::Client;
use tokio::sync::RwLock;

struct Inner {
    token: String,
    channels: HashMap<tracing::Level, String>,
    messages: HashMap<tracing::Level, Vec<String>>,
    client: Client,
    // 제약이 필요하면 moka::Cache를 사용하자
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

    pub fn new(token: String, channels: HashMap<tracing::Level, String>) -> Self {
        Inner {
            token,
            channels,
            messages: Default::default(),
            client: Default::default(),
        }
    }

    pub async fn send(&mut self, lvl: tracing::Level, msg: String) -> anyhow::Result<()> {
        let channel = if let Some(channel) = self.channels.get(&lvl) {
            channel.clone()
        } else {
            return Ok(());
        };

        let res = self
            .client
            .post("https://slack.com/api/chat.postMessage")
            .bearer_auth(self.token.clone())
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&serde_json::json!({
                "channel": channel,
                "text": msg
            }))
            .send()
            .await?;

        let json = res.json::<serde_json::Value>().await?;
        let success = json["ok"].as_bool().unwrap_or(false);
        if !success {
            eprint!("Failed to send message to Slack: {}", json.to_string());
            self.messages.entry(lvl).or_insert(Vec::new()).push(msg);
        }

        Ok(())
    }

    pub async fn put(&mut self, _lvl: tracing::Level, _msg: String) {
        // let message = format!("[{}] - {}", Local::now().format("%Y-%m-%d %H:%M:%S"), msg);
        // self.messages.entry(lvl).or_insert(Vec::new()).push(message);
    }

    fn flash_msg(&mut self, lvl: &tracing::Level) -> String {
        if let Some(v) = self.messages.remove(&lvl) {
            v.join("\n")
        } else {
            Default::default()
        }
    }
}

pub struct SlackParams {
    pub token: String,
    pub channels: HashMap<tracing::Level, String>,
}

pub struct Slack {
    inner: Arc<RwLock<Inner>>,
    _handler: RwLock<tokio::task::JoinHandle<()>>,
}

impl Slack {
    pub async fn new(params: SlackParams) -> Self {
        let inner = Arc::new(RwLock::new(Inner::new(params.token, params.channels)));

        let ctx = (Arc::downgrade(&inner), std::time::Duration::from_secs(1));
        let h = tokio::spawn(async move {
            let (wpt, dur) = ctx;
            loop {
                if let Some(inner) = wpt.upgrade() {
                    let mut inner = inner.write().await;
                    let keys = inner.channels.keys().cloned().collect::<Vec<_>>();
                    for lvl in keys {
                        let msg = inner.flash_msg(&lvl);
                        if !msg.is_empty() {
                            if let Err(e) = inner.send(lvl, msg).await {
                                eprint!("Failed to send message to Slack: {}", e.to_string());
                            }
                        }
                    }
                } else {
                    break;
                }
                tokio::time::sleep(dur).await;
            }
        });

        Slack {
            inner,
            _handler: RwLock::new(h),
        }
    }

    pub async fn put(&self, lvl: tracing::Level, msg: String) {
        let mut inner = self.inner.write().await;
        if inner.channels.get(&lvl).is_some() {
            inner.put(lvl, msg).await;
        }
    }
}
