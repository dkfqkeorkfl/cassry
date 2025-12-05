use serde::Deserialize;

pub fn serialize_anyhow_error<S>(error: &anyhow::Error, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&error.to_string())
}

pub fn deserialize_anyhow_error<'de, D>(deserializer: D) -> Result<anyhow::Error, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let message = String::deserialize(deserializer)?;
    Ok(crate::anyhowln!("{}", message))
}