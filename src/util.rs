use argon2::{password_hash::rand_core, PasswordHasher, PasswordVerifier};
use chrono::{DateTime, TimeZone, Utc};
use futures::Future;
use ring::aead;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use zeroize::Zeroize;

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

pub fn hash_password(data: &str) -> anyhow::Result<String> {
    let rng = rand_core::OsRng;
    let ss = argon2::password_hash::SaltString::generate(rng);

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
    // 1. 디코딩
    let mut key_bytes = hex::decode(key)?;
    let mut nonce_bytes = hex::decode(iv)?;
    let mut ciphertext = hex::decode(data.cipher)?;
    let tag = hex::decode(data.tag)?;

    // 2. 입력 유효성 검사
    if key_bytes.len() != 16 {
        return Err(crate::anyhowln!("AES-128 requires 16-byte key"));
    }
    if nonce_bytes.len() != 12 {
        return Err(crate::anyhowln!("GCM requires 12-byte nonce"));
    }

    // 3. tag를 ciphertext 뒤에 붙이기
    ciphertext.extend_from_slice(&tag);

    // 4. 복호화
    let nonce = aead::Nonce::try_assume_unique_for_key(&nonce_bytes)
        .map_err(|_| crate::anyhowln!("Invalid nonce"))?;
    let unbound_key = aead::UnboundKey::new(&aead::AES_128_GCM, &key_bytes)
        .map_err(|_| crate::anyhowln!("Invalid key"))?;
    let key = aead::LessSafeKey::new(unbound_key);

    let decrypted = key
        .open_in_place(nonce, aead::Aad::empty(), &mut ciphertext)
        .map_err(|_| crate::anyhowln!("Decryption failed"))?;

    // 5. 복호화 결과를 안전하게 반환
    let secret_string = String::from_utf8_lossy(decrypted).into_owned();
    let result = secrecy::SecretString::new(secret_string.into());

    // 6. 민감한 버퍼들 메모리에서 제거
    key_bytes.zeroize();
    nonce_bytes.zeroize();
    ciphertext.zeroize();

    Ok(result)
}

pub fn encrypt_aes_gcm_128(key: &str, iv: &str, plaintext: &str) -> anyhow::Result<EncryptedData> {
    let mut key_bytes = hex::decode(key)?;
    let mut nonce_bytes = hex::decode(iv)?;
    let plaintext_bytes = plaintext.as_bytes();

    if key_bytes.len() != 16 {
        return Err(crate::anyhowln!("AES-128 requires 16-byte key"));
    }
    if nonce_bytes.len() != 12 {
        return Err(crate::anyhowln!("GCM requires 12-byte nonce"));
    }

    let nonce = aead::Nonce::try_assume_unique_for_key(&nonce_bytes)
        .map_err(|_| crate::anyhowln!("Invalid nonce"))?;

    let unbound_key = aead::UnboundKey::new(&aead::AES_128_GCM, &key_bytes)
        .map_err(|_| crate::anyhowln!("Invalid key"))?;
    let safe_key = aead::LessSafeKey::new(unbound_key);

    let mut in_out = plaintext_bytes.to_vec(); // ❗ no 0 padding
    safe_key
        .seal_in_place_append_tag(nonce, aead::Aad::empty(), &mut in_out)
        .map_err(|_| crate::anyhowln!("Encryption failed"))?;

    // 태그는 끝 16바이트
    let tag_start = in_out.len() - 16;
    let cipher = hex::encode(&in_out[..tag_start]);
    let tag = hex::encode(&in_out[tag_start..]);

    // 민감 데이터 정리
    in_out.zeroize();
    key_bytes.zeroize();
    nonce_bytes.zeroize();

    Ok(EncryptedData { cipher, tag })
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
