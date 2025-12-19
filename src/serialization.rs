pub mod datetime_milliseconds {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(dt.timestamp_millis())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = i64::deserialize(deserializer)?;
        Ok(crate::util::make_datetime_from_millis(millis))
    }
}

pub mod anyhow_error {
    use serde::Deserialize;

    pub fn serialize<S>(error: &anyhow::Error, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&error.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<anyhow::Error, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let message = String::deserialize(deserializer)?;
        Ok(crate::anyhowln!("{}", message))
    }
}

pub mod ipaddr_bytes {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    /// IpAddr를 바이트 배열로 직렬화합니다.
    ///
    /// 형식: [버전(1바이트), IP 주소 바이트]
    /// - IPv4: [4, a, b, c, d] (총 5바이트)
    /// - IPv6: [6, a1, a2, ..., a16] (총 17바이트)
    pub fn serialize<S>(ip: &IpAddr, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = match ip {
            IpAddr::V4(ipv4) => {
                let mut result = vec![4u8];
                result.extend_from_slice(&ipv4.octets());
                result
            }
            IpAddr::V6(ipv6) => {
                let mut result = vec![6u8];
                result.extend_from_slice(&ipv6.octets());
                result
            }
        };
        serializer.serialize_bytes(&bytes)
    }

    /// 바이트 배열에서 IpAddr로 역직렬화합니다.
    ///
    /// 형식: [버전(1바이트), IP 주소 바이트]
    /// - IPv4: [4, a, b, c, d] (총 5바이트)
    /// - IPv6: [6, a1, a2, ..., a16] (총 17바이트)
    pub fn deserialize<'de, D>(deserializer: D) -> Result<IpAddr, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let bytes: Vec<u8> = Vec::<u8>::deserialize(deserializer)?;

        if bytes.is_empty() {
            return Err(Error::custom("IP address bytes cannot be empty"));
        }

        let version = bytes[0];
        let ip_bytes = &bytes[1..];

        match version {
            4 => {
                if ip_bytes.len() != 4 {
                    return Err(Error::custom(format!(
                        "IPv4 address must be 4 bytes, got {} bytes",
                        ip_bytes.len()
                    )));
                }
                let octets: [u8; 4] = [ip_bytes[0], ip_bytes[1], ip_bytes[2], ip_bytes[3]];
                Ok(IpAddr::V4(Ipv4Addr::from(octets)))
            }
            6 => {
                if ip_bytes.len() != 16 {
                    return Err(Error::custom(format!(
                        "IPv6 address must be 16 bytes, got {} bytes",
                        ip_bytes.len()
                    )));
                }
                let octets: [u8; 16] = [
                    ip_bytes[0],
                    ip_bytes[1],
                    ip_bytes[2],
                    ip_bytes[3],
                    ip_bytes[4],
                    ip_bytes[5],
                    ip_bytes[6],
                    ip_bytes[7],
                    ip_bytes[8],
                    ip_bytes[9],
                    ip_bytes[10],
                    ip_bytes[11],
                    ip_bytes[12],
                    ip_bytes[13],
                    ip_bytes[14],
                    ip_bytes[15],
                ];
                Ok(IpAddr::V6(Ipv6Addr::from(octets)))
            }
            _ => Err(Error::custom(format!(
                "Invalid IP version: {}. Expected 4 (IPv4) or 6 (IPv6)",
                version
            ))),
        }
    }
}

/// Postcard + Base64 직렬화/역직렬화 모듈
///
/// T 타입을 postcard로 직렬화하여 바이트로 만들고, 이를 base64로 인코딩합니다.
pub mod postcard_base64 {
    use serde::{
        de::{DeserializeOwned, Error as DeError},
        ser::Error as SerError,
        Deserialize, Deserializer, Serialize, Serializer,
    };

    /// T를 postcard로 직렬화하고 base64로 인코딩합니다.
    ///
    /// 1. postcard::to_allocvec를 사용하여 T를 바이트 배열로 직렬화
    /// 2. base64::engine::general_purpose::URL_SAFE_NO_PAD로 인코딩
    /// 3. 결과를 문자열로 직렬화
    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Serialize,
        S: Serializer,
        S::Error: SerError,
    {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use base64::Engine;

        let bytes = postcard::to_stdvec(value)
            .map_err(|e| S::Error::custom(format!("postcard serialization failed: {}", e)))?;

        let base64_str = URL_SAFE_NO_PAD.encode(&bytes);
        serializer.serialize_str(&base64_str)
    }

    /// Base64로 인코딩된 문자열을 디코딩하고 postcard로 역직렬화합니다.
    ///
    /// 1. base64 문자열을 디코딩하여 바이트 배열로 변환
    /// 2. postcard::from_bytes를 사용하여 T로 역직렬화
    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: DeserializeOwned,
        D: Deserializer<'de>,
        D::Error: DeError,
    {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use base64::Engine;

        let base64_str = String::deserialize(deserializer)?;

        let bytes = URL_SAFE_NO_PAD
            .decode(&base64_str)
            .map_err(|e| D::Error::custom(format!("base64 decoding failed: {}", e)))?;

        postcard::from_bytes(&bytes)
            .map_err(|e| D::Error::custom(format!("postcard deserialization failed: {}", e)))
    }
}
