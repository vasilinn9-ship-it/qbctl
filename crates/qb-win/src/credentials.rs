#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredCredential {
    pub username: String,
    pub secret: String,
}

#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    #[error("credential '{0}' was not found")]
    NotFound(String),
    #[error("credential '{0}' contains invalid text")]
    InvalidText(String),
    #[error("Credential Manager error: {0}")]
    Platform(std::io::Error),
    #[error("Windows Credential Manager is unavailable on this platform")]
    UnsupportedPlatform,
}

#[cfg(windows)]
pub fn read_generic(target: &str) -> Result<StoredCredential, CredentialError> {
    use std::{ptr, slice};

    use windows_sys::Win32::{
        Foundation::{GetLastError, ERROR_NOT_FOUND},
        Security::Credentials::{CredFree, CredReadW, CREDENTIALW, CRED_TYPE_GENERIC},
    };

    let wide_target: Vec<u16> = target.encode_utf16().chain(std::iter::once(0)).collect();
    let mut raw: *mut CREDENTIALW = ptr::null_mut();

    let ok = unsafe { CredReadW(wide_target.as_ptr(), CRED_TYPE_GENERIC, 0, &mut raw) };
    if ok == 0 {
        let code = unsafe { GetLastError() };
        if code == ERROR_NOT_FOUND {
            return Err(CredentialError::NotFound(target.to_string()));
        }
        return Err(CredentialError::Platform(
            std::io::Error::from_raw_os_error(code as i32),
        ));
    }

    if raw.is_null() {
        return Err(CredentialError::Platform(std::io::Error::other(
            "CredReadW returned a null credential",
        )));
    }

    let credential = unsafe { &*raw };
    let username = unsafe { read_wide_z(credential.UserName) };
    let blob = if credential.CredentialBlob.is_null() || credential.CredentialBlobSize == 0 {
        Vec::new()
    } else {
        unsafe {
            slice::from_raw_parts(
                credential.CredentialBlob,
                credential.CredentialBlobSize as usize,
            )
            .to_vec()
        }
    };

    unsafe {
        CredFree(raw.cast());
    }

    let username = username.ok_or_else(|| CredentialError::InvalidText(target.to_string()))?;
    let secret =
        decode_secret(&blob).ok_or_else(|| CredentialError::InvalidText(target.to_string()))?;

    Ok(StoredCredential { username, secret })
}

#[cfg(windows)]
unsafe fn read_wide_z(pointer: *mut u16) -> Option<String> {
    if pointer.is_null() {
        return Some(String::new());
    }

    let mut length = 0_usize;
    while unsafe { *pointer.add(length) } != 0 {
        length = length.checked_add(1)?;
        if length > 32 * 1024 {
            return None;
        }
    }

    let units = unsafe { std::slice::from_raw_parts(pointer, length) };
    String::from_utf16(units).ok()
}

#[cfg(windows)]
fn decode_secret(blob: &[u8]) -> Option<String> {
    if let Ok(value) = std::str::from_utf8(blob) {
        return Some(value.to_string());
    }

    if !blob.len().is_multiple_of(2) {
        return None;
    }

    let (pairs, remainder) = blob.as_chunks::<2>();
    if !remainder.is_empty() {
        return None;
    }

    let utf16: Vec<u16> = pairs
        .iter()
        .map(|pair| u16::from_le_bytes(*pair))
        .collect();
    String::from_utf16(&utf16)
        .ok()
        .map(|value| value.trim_end_matches('\0').to_string())
}

#[cfg(not(windows))]
pub fn read_generic(_target: &str) -> Result<StoredCredential, CredentialError> {
    Err(CredentialError::UnsupportedPlatform)
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn decodes_utf8_and_utf16_secret_blobs() {
        assert_eq!(decode_secret(b"secret").as_deref(), Some("secret"));

        let utf16 = "secret"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        assert_eq!(decode_secret(&utf16).as_deref(), Some("secret"));
    }
}
