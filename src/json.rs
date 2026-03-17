use std::{borrow::Cow, collections::HashMap};

use anyhow::*;

pub fn urlencode_for_form(obj: &impl serde::Serialize) -> Result<String> {
    let v = serde_json::to_value(obj)?;

    if let Option::Some(obj) = v.as_object() {
        let map = obj
            .iter()
            .map(|(key, value)| {
                let val_str = match value {
                    serde_json::Value::String(s) => Cow::Borrowed(s),
                    _ => Cow::Owned(value.to_string()),
                };
                (key, val_str)
            })
            .collect::<HashMap<_, _>>();
        let encoded = serde_urlencoded::to_string(&map)?;
        return Ok(encoded);
    } else {
        Err(crate::anyhowln!("failed to convert from object to map"))
    }
}

pub fn urlencode_for_sign(v: &serde_json::Map<String, serde_json::Value>) -> Result<String> {
    let query = v
        .iter()
        .map(|(k, v)| {
            let val_str = match v {
                serde_json::Value::String(s) => s.clone(),
                _ => v.to_string(),
            };
            format!(
                "{}={}",
                urlencoding::encode(k),
                urlencoding::encode(&val_str)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    Ok(query)
}

pub fn urlencode_for_sign_vec(v: Vec<(&str, &str)>) -> Result<String> {
    let query = v
        .iter()
        .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
        .collect::<Vec<_>>()
        .join("&");
    Ok(query)
}
