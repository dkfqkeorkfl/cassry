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

// #[serde_as]
// #[derive(Debug, Clone, Serialize, Deserialize)]
// #[serde(untagged)]
// pub enum LazyDecimal {
//     Str(String),
//     Num(Decimal),
// }

// impl Default for LazyDecimal {
//     fn default() -> Self {
//         LazyDecimal::Num(Decimal::ZERO)
//     }
// }

// impl LazyDecimal {
//     pub fn get_num(&self) -> Option<&Decimal> {
//         if let LazyDecimal::Num(s) = self {
//             Some(s)
//         } else {
//             None
//         }
//     }

//     pub fn transf_num(&mut self) -> &Decimal {
//         if let LazyDecimal::Str(s) = self {
//             let n = Decimal::to_decimal(s).unwrap_or(Decimal::ZERO);
//             *self = LazyDecimal::Num(n);
//         }

//         if let LazyDecimal::Num(s) = self {
//             s
//         } else {
//             &Decimal::ZERO
//         }
//     }

//     pub fn transf_str(&mut self) -> &str {
//         if let LazyDecimal::Num(n) = self {
//             let s = n.to_string();
//             *self = LazyDecimal::Str(s);
//         }

//         if let LazyDecimal::Str(s) = self {
//             s.as_str()
//         } else {
//             ""
//         }
//     }

//     pub fn to_num(&self) -> anyhow::Result<rust_decimal::Decimal> {
//         match self {
//             LazyDecimal::Str(s) => {
//                 let n = Decimal::to_decimal(s)?;
//                 Ok(n)
//             }
//             LazyDecimal::Num(n) => Ok(n.clone()),
//         }
//     }

//     pub fn to_string(&self) -> String {
//         match self {
//             LazyDecimal::Str(s) => s.clone(),
//             LazyDecimal::Num(n) => n.to_string(),
//         }
//     }
// }

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

// #[derive(Default, Debug, Clone)]
// pub struct LazyDecimalPair {
//     numbers: Arc<RwLock<Option<(Decimal, Decimal)>>>,
//     strings: (String, String),
// }

// // Custom serialization
// impl Serialize for LazyDecimalPair {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         self.strings.serialize(serializer)
//     }
// }

// // Custom deserialization
// impl<'de> Deserialize<'de> for LazyDecimalPair {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         struct LazyDecimalPairVisitor;

//         impl<'de> Visitor<'de> for LazyDecimalPairVisitor {
//             type Value = LazyDecimalPair;

//             fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
//                 formatter.write_str("an array of two strings")
//             }

//             fn visit_seq<A>(self, mut seq: A) -> Result<LazyDecimalPair, A::Error>
//             where
//                 A: SeqAccess<'de>,
//             {
//                 let s1 = seq
//                     .next_element()?
//                     .ok_or_else(|| de::Error::invalid_length(0, &self))?;
//                 let s2 = seq
//                     .next_element()?
//                     .ok_or_else(|| de::Error::invalid_length(1, &self))?;
//                 Ok(LazyDecimalPair {
//                     numbers: Arc::new(RwLock::new(None)),
//                     strings: (s1, s2),
//                 })
//             }
//         }

//         deserializer.deserialize_seq(LazyDecimalPairVisitor)
//     }
// }

// impl LazyDecimalPair {
//     /// `new_with_strings`는 문자열 표현으로 새 인스턴스를 생성합니다.
//     ///
//     /// 이 함수는 10진수를 문자열로 변환하는 데 따른 오버헤드를 발생시키지 않습니다.
//     pub fn new(price: String, amount: String) -> Self {
//         Self {
//             numbers: Arc::new(RwLock::new(None)),
//             strings: (price, amount),
//         }
//     }

//     /// `new_with_decimal` creates a new instance with decimal values.
//     ///
//     /// Note: This function incurs overhead due to the conversion of decimals to strings.
//     /// Consider using `new_with_strings` if you already have string representations.
//     pub fn new_with_decimal(price: Decimal, amount: Decimal) -> Self {
//         let strings = (price.to_string(), amount.to_string());
//         let numbers = (price, amount);
//         Self {
//             numbers: Arc::new(Some(numbers).into()),
//             strings: strings,
//         }
//     }

//     pub async fn get_numbers(&self) -> anyhow::Result<(Decimal, Decimal)> {
//         let result = async {
//             let reader = self.numbers.read().await;
//             if let Some(numbers) = reader.as_ref() {
//                 Some(numbers.clone())
//             } else {
//                 None
//             }
//         }
//         .await;

//         if let Some(numbers) = result {
//             Ok(numbers)
//         } else {
//             let price = to_decimal(&self.strings.0)?;
//             let amount = to_decimal(&self.strings.1)?;
//             let numbers = (price, amount);

//             let mut writer = self.numbers.write().await;
//             *writer = Some(numbers.clone());
//             Ok(numbers)
//         }
//     }

//     pub fn get_strings(&self) -> &(String, String) {
//         &self.strings
//     }
// }
