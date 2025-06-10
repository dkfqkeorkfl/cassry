use std::sync::Arc;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// use serde::{
//     de::{self, SeqAccess, Visitor},
//     ser::SerializeTuple,
//     Deserialize, Deserializer, Serialize, Serializer,
// };
use tokio::sync::RwLock;

pub fn to_decimal(s: &str) -> anyhow::Result<rust_decimal::Decimal> {
    let (integer_part, fractional_part) = if let Some(pos) = s.find('.') {
        (&s[..pos], &s[pos + 1..])
    } else {
        (s, "")
    };

    if fractional_part.len() > 8 {
        let trimmed = format!("{}.{}", integer_part, &fractional_part[..8]);

        if trimmed.contains('e') || trimmed.contains('E') {
            Decimal::from_scientific(&trimmed)
        } else {
            Decimal::from_str_exact(&trimmed)
        }
    } else {
        if s.contains('e') || s.contains('E') {
            Decimal::from_scientific(s)
        } else {
            Decimal::from_str_exact(s)
        }
    }
    .map_err(anyhow::Error::from)
}

pub fn to_decimal_with_json(v: &serde_json::Value) -> anyhow::Result<rust_decimal::Decimal> {
    if let Some(s) = v.as_str() {
        to_decimal(s)
    } else {
        to_decimal(&v.to_string())
    }
}

#[derive(Default, Debug, Clone)]
pub struct LazyDecimal {
    number: Arc<RwLock<Option<Decimal>>>,
    string: String,
}
pub type LazyDecimalPtr = Arc<LazyDecimal>;

impl From<&str> for LazyDecimal {
    fn from(s: &str) -> Self {
        LazyDecimal::new(s.into())
    }
}

impl From<String> for LazyDecimal {
    fn from(s: String) -> Self {
        LazyDecimal::new(s)
    }
}

impl Serialize for LazyDecimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.string.serialize(serializer)
    }
}

// Custom deserialization
impl<'de> Deserialize<'de> for LazyDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str: String = Deserialize::deserialize(deserializer)?;
        Ok(LazyDecimal {
            number: Arc::new(RwLock::new(None)),
            string: str,
        })
    }
}

impl LazyDecimal {
    /// `new_with_strings`는 문자열 표현으로 새 인스턴스를 생성합니다.
    ///
    /// 이 함수는 10진수를 문자열로 변환하는 데 따른 오버헤드를 발생시키지 않습니다.
    pub fn new(str: String) -> Self {
        Self {
            number: Arc::new(RwLock::new(None)),
            string: str,
        }
    }

    /// `new_with_decimal` creates a new instance with decimal values.
    ///
    /// Note: This function incurs overhead due to the conversion of decimals to strings.
    /// Consider using `new_with_strings` if you already have string representations.
    pub fn new_with_decimal(num: Decimal) -> Self {
        Self {
            number: Arc::new(Some(num).into()),
            string: num.to_string(),
        }
    }

    pub async fn number(&self) -> anyhow::Result<Decimal> {
        let result = async {
            let reader = self.number.read().await;
            if let Some(numbers) = reader.as_ref() {
                Some(numbers.clone())
            } else {
                None
            }
        }
        .await;

        if let Some(numbers) = result {
            Ok(numbers)
        } else {
            let num = to_decimal(&self.string)?;
            let mut writer = self.number.write().await;
            *writer = Some(num.clone());
            Ok(num)
        }
    }

    pub fn string(&self) -> &String {
        &self.string
    }
}