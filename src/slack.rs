use std::{collections::HashMap, sync::Arc};

use chrono::Local;
use regex::Regex;
use reqwest::{Client, Response};
use tokio::sync::RwLock;

struct Inner {
    token: String,
    channels: [String; log::Level::Trace as usize + 1],
    messages: HashMap<log::Level, Vec<String>>,
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

    pub fn new(token: String, channels: HashMap<log::Level, String>) -> Self {
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
        }
    }

    pub async fn send(&mut self, lvl: log::Level, msg: String) -> anyhow::Result<()> {
        let channel = &self.channels[lvl as usize];
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

    pub async fn put(&mut self, lvl: log::Level, msg: String) {
        // let message = format!("[{}] - {}", Local::now().format("%Y-%m-%d %H:%M:%S"), msg);
        // self.messages.entry(lvl).or_insert(Vec::new()).push(message);
    }

    fn flash_msg(&mut self, lvl: log::Level) -> String {
        if let Some(v) = self.messages.remove(&lvl) {
            v.join("\n")
        } else {
            Default::default()
        }
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
    handler: RwLock<Option<tokio::task::JoinHandle<()>>>,
}

impl Slack {
    pub async fn init(&self, params: SlackParams) {
        {
            let mut inner = self.inner.write().await;
            *inner = Some(Inner::new(params.token, params.channels));
        }

        {
            let wpt = Arc::downgrade(&self.inner);
            let dur = std::time::Duration::from_secs(1);

            let h = tokio::spawn(async move {
                loop {
                    if let Some(inner) = wpt.upgrade() {
                        let mut inner = inner.write().await;
                        if let Some(inner) = inner.as_mut() {
                            for lvl in log::Level::iter() {
                                let msg = inner.flash_msg(lvl);
                                if msg.is_empty() {
                                    continue;
                                }
                                
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

            let mut handler = self.handler.write().await;
            *handler = Some(h);
        }
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
