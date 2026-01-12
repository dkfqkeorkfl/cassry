use std::collections::HashMap;

use jsonwebtoken;
use reqwest;
use secrecy::*;
use serde::{de::DeserializeOwned, *};
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Deserialize, ZeroizeOnDrop)]
pub struct AccountKey {
    pub client_email: SecretString,
    pub private_key: SecretString,

    #[serde(rename = "type")]
    #[serde(default)]
    pub service_type: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub private_key_id: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub auth_uri: Option<String>,
    #[serde(default)]
    pub token_uri: Option<String>,
    #[serde(default)]
    pub auth_provider_x509_cert_url: Option<String>,
    #[serde(default)]
    pub client_x509_cert_url: Option<String>,
    #[serde(default)]
    pub universe_domain: Option<String>,
}

#[derive(Deserialize, ZeroizeOnDrop)]
pub struct AccessToken {
    pub token_type: String,
    pub expires_in: i64,

    pub access_token: SecretString,
}

fn deserialize_trimmed_secret_string_vec_vec<'de, D>(
    deserializer: D,
) -> Result<Vec<Vec<SecretString>>, D::Error>
where
    D: Deserializer<'de>,
{
    let vec_vec_string: Vec<Vec<String>> = Vec::deserialize(deserializer)?;
    Ok(vec_vec_string
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|mut s| {
                    let result = SecretString::new(s.trim().into());
                    s.zeroize();
                    result
                })
                .collect()
        })
        .collect())
}

#[derive(Deserialize, ZeroizeOnDrop)]
pub struct SheetValue {
    pub range: String,
    #[serde(rename = "majorDimension")]
    pub major_dimension: String,

    #[serde(deserialize_with = "deserialize_trimmed_secret_string_vec_vec")]
    pub values: Vec<Vec<SecretString>>,
}

#[derive(Default, Serialize, Deserialize)]
pub struct SheetTable<T: Default> {
    pub headers: Vec<String>,
    pub rows: Vec<T>,
}

pub async fn fetch_access_token(account: &AccountKey, exp: i64) -> anyhow::Result<AccessToken> {
    let claims = serde_json::json!({
        "iss": account.client_email.expose_secret().to_string(),
        "scope": "https://www.googleapis.com/auth/spreadsheets.readonly".to_string(),
        "aud": "https://oauth2.googleapis.com/token".to_string(),
        "exp": chrono::Utc::now().timestamp() + exp,
        "iat": chrono::Utc::now().timestamp(),
    });

    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &claims,
        &jsonwebtoken::EncodingKey::from_rsa_pem(account.private_key.expose_secret().as_bytes())?,
    )?;

    let body = serde_json::json!({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": token
    });

    let client = reqwest::Client::new();
    let response: AccessToken = client
        .post("https://oauth2.googleapis.com/token")
        .json(&body)
        .send()
        .await?
        .json()
        .await?;

    Ok(response)
}

pub async fn fetch_sheet(
    access_token: &AccessToken,
    sheet: &str,
    range: &str,
) -> anyhow::Result<SheetValue> {
    let url = format!(
        "https://sheets.googleapis.com/v4/spreadsheets/{}/values/{}",
        sheet, range
    );

    let client = reqwest::Client::new();
    let response: SheetValue = client
        .get(&url)
        .bearer_auth(access_token.access_token.expose_secret())
        .send()
        .await?
        .json()
        .await?;

    Ok(response)
}

pub async fn fetch_sheet_to_dictionary(
    access_token: &AccessToken,
    sheet: &str,
    range: &str,
) -> anyhow::Result<HashMap<String, SecretString>> {
    let mut sheet_value = fetch_sheet(access_token, sheet, range).await?;

    let values = sheet_value
        .values
        .iter_mut()
        .skip(1) // for header
        .filter(|row| !row.is_empty())
        .map(|row| {
            if row.len() != 2 {
                return Err(anyhow::anyhow!("row is not correct"));
            }

            let key = row.get(0).unwrap().expose_secret().to_string();
            let value = row.remove(1);
            Ok((key, value))
        })
        .collect::<anyhow::Result<HashMap<_, _>>>()?;
    Ok(values)
}
pub async fn fetch_sheet_as_each_map(
    access_token: &AccessToken,
    sheet: &str,
    range: &str,
) -> anyhow::Result<Vec<HashMap<String, SecretString>>> {
    let sheet_value = fetch_sheet(access_token, sheet, range).await?;
    let headers = sheet_value
        .values
        .iter()
        .next()
        .ok_or(anyhow::anyhow!("headers not found"))?
        .iter()
        .map(|h| h.expose_secret().to_string())
        .collect::<Vec<String>>();

    let values = sheet_value
        .values
        .iter()
        .skip(1)
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(index, value)| {
                    let header = headers
                        .get(index)
                        .ok_or(anyhow::anyhow!("header not found"))?;
                    Ok((header.clone(), value.clone()))
                })
                .collect::<anyhow::Result<HashMap<_, _>>>()
        })
        .collect::<anyhow::Result<Vec<HashMap<String, SecretString>>>>()?;
    Ok(values)
}

pub async fn fetch_sheet_as_each_obj<T: Default + DeserializeOwned>(
    access_token: &AccessToken,
    sheet: &str,
    range: &str,
) -> anyhow::Result<SheetTable<T>> {
    let sheet_value = fetch_sheet(access_token, sheet, range).await?;

    let mut iter = sheet_value.values.iter();
    let headers = if let Some(headers) = iter.next().filter(|h| !h.is_empty()) {
        headers
            .iter()
            .map(|h| h.expose_secret().to_string())
            .collect::<Vec<String>>()
    } else {
        return Ok(Default::default());
    };

    let rows = iter
        .map(|row| {
            let values = row
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    let value = value.expose_secret().to_string();
                    let header = headers
                        .get(index)
                        .ok_or(anyhow::anyhow!("header not found"))?;
                    Ok((header, value))
                })
                .collect::<anyhow::Result<HashMap<_, _>>>()?;

            serde_json::from_value::<T>(serde_json::json!(values)).map_err(anyhow::Error::from)
        })
        .collect::<anyhow::Result<Vec<T>>>()?;

    Ok(SheetTable { headers, rows })
}
