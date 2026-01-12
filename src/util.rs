use argon2::{password_hash::rand_core, PasswordHasher, PasswordVerifier};
use blake3;
use chrono::{DateTime, Duration, TimeZone, Utc};
use futures::Future;
use ring::aead;
use std::cmp::Ordering;
use std::path::PathBuf;
use zeroize::Zeroize;

pub fn datetime_epoch_first() -> DateTime<Utc> {
    Utc.timestamp_opt(0, 0).unwrap()
}

/// 밀리초 단위의 i64 값을 받아서 DateTime<Utc>로 변환
pub fn datetime_from_millis(millis: i64) -> DateTime<Utc> {
    // 밀리초 단위 Duration을 더해줌 (음수도 지원)
    datetime_epoch_first() + Duration::milliseconds(millis)
}

pub fn datetime_floor(datetime: &DateTime<Utc>, by: &Duration) -> anyhow::Result<DateTime<Utc>> {
    const SECONDS_IN_DAY: i64 = 24 * 60 * 60 * 1000; // 86400000 milliseconds
    let dur_ms = by.num_milliseconds();
    if dur_ms == 0 || SECONDS_IN_DAY % dur_ms != 0 {
        return Err(anyhow::anyhow!("TTL must be a divisor of 86400 seconds (24 hours). Examples: 1s, 1m, 1h, 2h, 3h, 4h, 6h, 8h, 12h, 24h"));
    }
    let seconds = datetime.timestamp();
    let floored_timestamp = (seconds / dur_ms) * dur_ms;
    let floored_time = Utc.timestamp_opt(floored_timestamp, 0).unwrap();
    Ok(floored_time)
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

/// 가변 길이 Blake3 기반 키드 해시 함수
///
/// 1. 임의 길이의 키를 Blake3로 해시하여 32바이트 키를 생성합니다.
/// 2. 그 키로 Blake3 keyed hash를 수행하여 지정된 길이의 해시를 생성합니다.
/// `output_len`은 바이트 단위입니다 (1 이상, Blake3는 무제한 길이 지원).
///
/// 참고: Blake3는 keyed hashing을 직접 지원하므로 HMAC과 유사한 보안성을 제공합니다.
/// 키는 내부적으로 Blake3로 해시되어 32바이트로 변환되며, XOF(Extendable Output Function)를
/// 사용하여 원하는 길이의 출력을 생성합니다.
pub fn hmac_blake3_with_len(
    key: &[u8; 32],
    message: &[u8],
    output_len: usize,
) -> anyhow::Result<Vec<u8>> {
    if output_len == 0 {
        return Err(crate::anyhowln!("output_len must be greater than 0"));
    }

    // 2단계: Blake3 keyed hash를 사용하여 메시지 해시
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(message);

    // 3단계: XOF(Extendable Output Function)를 사용하여 지정된 길이만큼 출력
    let mut output = vec![0u8; output_len];
    hasher.finalize_xof().fill(&mut output);

    Ok(output)
}

pub fn decrypt_str_by_aes_gcm_128(
    key: &[u8],
    iv: &[u8],
    mut cipher: Vec<u8>,
) -> anyhow::Result<secrecy::SecretString> {
    // 2. 입력 유효성 검사
    if key.len() != 16 {
        return Err(crate::anyhowln!("AES-128 requires 16-byte key"));
    }
    if iv.len() != 12 {
        return Err(crate::anyhowln!("GCM requires 12-byte nonce"));
    }

    // 4. 복호화
    let nonce = aead::Nonce::try_assume_unique_for_key(&iv)
        .map_err(|_| crate::anyhowln!("Invalid nonce"))?;
    let unbound_key = aead::UnboundKey::new(&aead::AES_128_GCM, &key)
        .map_err(|_| crate::anyhowln!("Invalid key"))?;
    let key = aead::LessSafeKey::new(unbound_key);

    let decrypted = key
        .open_in_place(nonce, aead::Aad::empty(), &mut cipher)
        .map_err(|_| crate::anyhowln!("Decryption failed"))?;

    // 5. 복호화 결과를 안전하게 반환
    let decrypted = String::from_utf8(decrypted.to_vec())?;
    let secret = secrecy::SecretString::new(decrypted.into());
    cipher.zeroize();
    Ok(secret)
}

pub fn encrypt_str_by_aes_gcm_128(
    key: &[u8],
    iv: &[u8],
    plaintext: &str,
) -> anyhow::Result<Vec<u8>> {
    if key.len() != 16 {
        return Err(crate::anyhowln!("AES-128 requires 16-byte key"));
    }
    if iv.len() != 12 {
        return Err(crate::anyhowln!("GCM requires 12-byte nonce"));
    }

    let nonce = aead::Nonce::try_assume_unique_for_key(&iv)
        .map_err(|_| crate::anyhowln!("Invalid nonce"))?;

    let unbound_key = aead::UnboundKey::new(&aead::AES_128_GCM, &key)
        .map_err(|_| crate::anyhowln!("Invalid key"))?;
    let safe_key = aead::LessSafeKey::new(unbound_key);

    let mut in_out = plaintext.as_bytes().to_vec(); // ❗ no 0 padding
    safe_key
        .seal_in_place_append_tag(nonce, aead::Aad::empty(), &mut in_out)
        .map_err(|_| crate::anyhowln!("Encryption failed"))?;

    Ok(in_out)
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

/// 지정된 경로의 디렉토리를 지정된 깊이까지 나열합니다.
///
/// `path`: 탐색을 시작할 경로
/// `lvl`: 탐색 깊이 (0이면 직접 하위 디렉토리만, 1이면 하위의 하위만만, 등)
///
/// # 예시
/// ```
/// // lvl 0: path 하위의 직접 디렉토리들만
/// // lvl 1: path 하위 디렉토리들의 하위 디렉토리만
/// ```
pub async fn list_directories(path: &PathBuf, lvl: u32) -> anyhow::Result<Vec<PathBuf>> {
    let metadata = tokio::fs::metadata(&path).await?;
    if !metadata.is_dir() {
        return Err(anyhow::anyhow!(
            "Path is not a directory: {}",
            path.display()
        ));
    }

    if lvl == 0 {
        // 직접 하위 디렉토리만 반환
        let mut dirs = Vec::new();
        let mut read_dir = tokio::fs::read_dir(&path).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                dirs.push(entry.path());
            }
        }
        return Ok(dirs);
    }

    // lvl > 0인 경우: 재귀적으로 하위 디렉토리들을 수집
    let mut result = Vec::new();
    let mut read_dir = tokio::fs::read_dir(&path).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let metadata = entry.metadata().await?;
        if metadata.is_dir() {
            let sub_path = entry.path();
            // 다음 레벨로 재귀
            let sub_dirs = Box::pin(list_directories(&sub_path, lvl - 1)).await?;
            result.extend(sub_dirs);
        }
    }

    Ok(result)
}
