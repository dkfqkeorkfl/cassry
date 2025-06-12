use argon2::{PasswordHasher, PasswordVerifier};
use chrono::{DateTime, TimeZone, Utc};
use futures::Future;
use ring::aead;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

pub fn serialize_chrono_duration<S>(
    dur: &chrono::Duration,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let std = dur.to_std().map_err(serde::ser::Error::custom)?;
    std.serialize(serializer)
}

pub fn deserialize_chrono_duration<'de, D>(deserializer: D) -> Result<chrono::Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let std = std::time::Duration::deserialize(deserializer)?;
    chrono::Duration::from_std(std).map_err(serde::de::Error::custom)
}

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

#[derive(serde::Serialize)]
struct GoogleClaims {
    iss: String, // issuer
    scope: String,
    aud: String, // audience
    exp: usize,  // expiration
    iat: usize,  // issued at
}

pub async fn req_access_token_for_google_service_account(
    account_json: &str,
    exp: i64,
) -> anyhow::Result<secrecy::SecretString> {
    let key_data: serde_json::Value = serde_json::from_str(account_json)?;

    let private_key = key_data["private_key"]
        .as_str()
        .ok_or(crate::anyhowln!("invalid private key"))?;
    let client_email = key_data["client_email"]
        .as_str()
        .ok_or(crate::anyhowln!("invalid client email"))?;

    let my_claims = GoogleClaims {
        iss: client_email.to_string(),
        scope: "https://www.googleapis.com/auth/spreadsheets.readonly".to_string(),
        aud: "https://oauth2.googleapis.com/token".to_string(),
        exp: (chrono::Utc::now().timestamp() + exp) as usize,
        iat: chrono::Utc::now().timestamp() as usize,
    };

    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &my_claims,
        &jsonwebtoken::EncodingKey::from_rsa_pem(private_key.as_bytes()).unwrap(),
    )?;

    let body = serde_json::json!({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": token
    });

    let client = reqwest::Client::new();
    let response: serde_json::Value = client
        .post("https://oauth2.googleapis.com/token")
        .json(&body)
        .send()
        .await?
        .json()
        .await?;

    let access_token = response["access_token"]
        .as_str()
        .ok_or(crate::anyhowln!("invalid access token in response"))?;
    Ok(access_token.into())
}

pub async fn req_sheet_with_access_token(
    access_token: &str,
    sheet: &str,
    range: &str,
) -> anyhow::Result<String> {
    let url = format!(
        "https://sheets.googleapis.com/v4/spreadsheets/{}/values/{}",
        sheet, range
    );

    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .bearer_auth(access_token)
        .send()
        .await?
        .text()
        .await?;
    Ok(response)
}

pub async fn download_google_sheet(
    sheet: &str,
    tab: &str,
    key: &str,
) -> anyhow::Result<serde_json::Value> {
    let url = format!(
        "https://sheets.googleapis.com/v4/spreadsheets/{}/values/{}?key={}",
        sheet, tab, key,
    );

    let res = reqwest::Client::new().get(url).send().await?;
    let json = res.json().await?;
    return Ok(json);
}

pub fn get_epoch_first() -> DateTime<Utc> {
    Utc.timestamp_opt(0, 0).unwrap()
}

pub fn f64_to_duration(secs: f64) -> chrono::Duration {
    let millis = (secs * 1000.0) as i64;
    chrono::Duration::milliseconds(millis)
}

pub fn verify_password(origin: &str, hashed: &str) -> anyhow::Result<()> {
    let parsed_hash = argon2::password_hash::PasswordHash::new(hashed)
        .map_err(|e| crate::anyhowln!("{}", e.to_string()))?;
    let argon = argon2::Argon2::default();
    let ret = argon
        .verify_password(origin.as_bytes(), &parsed_hash)
        .map_err(|e| crate::anyhowln!("{}", e.to_string()))?;
    Ok(ret)
}

pub fn hash_password(data: &str, salt: &str) -> anyhow::Result<String> {
    let ss = argon2::password_hash::SaltString::from_b64(salt)
        .map_err(|e| crate::anyhowln!("{}", e.to_string()))?;

    let argon = argon2::Argon2::default();
    let hash = argon
        .hash_password(data.as_bytes(), &ss)
        .map_err(|e| crate::anyhowln!("{}", e.to_string()))?
        .to_string();
    Ok(hash)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedData {
    cipher: String,
    tag: String,
}

pub fn decrypt_aes_gcm_128(key: &str, iv: &str, data: EncryptedData) -> anyhow::Result<secrecy::SecretString> {
    let key_bytes = hex::decode(key)?;
    let nonce_bytes = hex::decode(iv)?;
    let ciphertext_bytes = hex::decode(data.cipher)?;
    let tag_bytes = hex::decode(data.tag)?;

    let nonce = aead::Nonce::try_assume_unique_for_key(&nonce_bytes)
        .map_err(|_| crate::anyhowln!("Occur Error from aead::Nonce::try_assume_unique_for_key"))?;
    let unbound_key = aead::UnboundKey::new(&aead::AES_128_GCM, &key_bytes)
        .map_err(|_| crate::anyhowln!("Occur Error from aead::UnboundKey::new"))?;
    let safe_key = aead::LessSafeKey::new(unbound_key);

    let mut in_out = ciphertext_bytes.to_vec();
    in_out.extend_from_slice(&tag_bytes);
    safe_key
        .open_in_place(nonce, aead::Aad::empty(), &mut in_out)
        .map_err(|_| crate::anyhowln!("Occur Error from safe_key.open_in_place"))?;

    // 이제 in_out 벡터의 처음부터 암호화된 데이터의 길이까지가 복호화된 데이터입니다.
    let decrypted = String::from_utf8_lossy(&in_out[..ciphertext_bytes.len()]);
    Ok(decrypted.into_owned().into())
}

pub fn encrypt_aes_gcm_128(key: &str, iv: &str, plaintext: &str) -> anyhow::Result<EncryptedData> {
    let key_bytes = hex::decode(key)?;
    let nonce_bytes = hex::decode(iv)?;
    let plaintext_bytes = plaintext.as_bytes();

    // Nonce 객체를 생성합니다.
    let nonce = aead::Nonce::try_assume_unique_for_key(&nonce_bytes)
        .map_err(|_| crate::anyhowln!("Error from aead::Nonce::try_assume_unique_for_key"))?;

    // 암호화 키를 생성합니다.
    let unbound_key = aead::UnboundKey::new(&aead::AES_128_GCM, &key_bytes)
        .map_err(|_| crate::anyhowln!("Error from aead::UnboundKey::new"))?;
    let safe_key = aead::LessSafeKey::new(unbound_key);

    // 암호화 과정을 실행합니다.
    let mut in_out = plaintext_bytes.to_vec();
    in_out.extend_from_slice(&[0; 16]); // 태그 길이 추가 (AES-GCM 128의 태그 길이는 16 바이트)
    safe_key
        .seal_in_place_append_tag(nonce, aead::Aad::empty(), &mut in_out)
        .map_err(|_| crate::anyhowln!("Error from safe_key.seal_in_place_append_tag"))?;

    // 결과 데이터를 헥사스트링으로 변환합니다.
    let cipher = hex::encode(&in_out[..plaintext_bytes.len()]);
    let tag = hex::encode(&in_out[plaintext_bytes.len()..]);

    Ok(EncryptedData {
        cipher: cipher,
        tag: tag,
    })
}

/// Performs an asynchronous binary search.
///
/// `slice`: The sorted slice to search through.
/// `predicate`: A closure that returns a future, which resolves to an Ordering.
pub async fn async_binary_search<T, F, Fut>(slice: &[T], mut predicate: F) -> Result<usize, usize>
where
    F: FnMut(&T) -> Fut,
    Fut: Future<Output = Ordering> + Send + 'static, // Note the 'static bound here
{
    let mut low = 0;
    let mut high = slice.len();

    while low < high {
        let mid = low + (high - low) / 2;
        let ordering = predicate(&slice[mid]).await;

        match ordering {
            Ordering::Equal => return Ok(mid),
            Ordering::Less => high = mid,
            Ordering::Greater => low = mid + 1,
        }
    }

    Err(low)
}
