use std::collections::BTreeMap;

use anyhow::*;
use serde::Serialize;

pub fn url_encode_object<T: Serialize>(v: &T) -> Result<String> {
    let v = serde_json::to_value(v)?;
    url_encode(&v)
}

pub fn url_encode(v: &serde_json::Value) -> Result<String> {
    if v.is_null() {
        return Ok(String::default());
    } else if let Option::Some(obj) = v.as_object() {
        let mut sorted_vec: Vec<(&str, String)> = Vec::with_capacity(obj.len());
        for (key, value) in obj {
            let val_str = match value {
                serde_json::Value::String(s) => s.clone(),
                _ => value.to_string(),
            };


            // 이진 탐색으로 삽입 위치 찾기
            let pos = sorted_vec
                .binary_search_by(|(k, _)| (*k).cmp(key.as_str()))
                .unwrap_or_else(|e| e);
            sorted_vec.insert(pos, (key, val_str));
        }

        let encoded = serde_urlencoded::to_string(&sorted_vec)?;
        return Ok(encoded);
    }

    return Err(crate::anyhowln!("invalid param"));
}
