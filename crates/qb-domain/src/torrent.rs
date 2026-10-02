use std::{error::Error, fmt, str::FromStr};

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct TorrentId(String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TorrentIdError;

impl TorrentId {
    pub fn new(value: impl AsRef<str>) -> Result<Self, TorrentIdError> {
        let value = value.as_ref();
        let valid_length = matches!(value.len(), 40 | 64);
        let valid_hex = value.bytes().all(|byte| byte.is_ascii_hexdigit());

        if valid_length && valid_hex {
            Ok(Self(value.to_ascii_lowercase()))
        } else {
            Err(TorrentIdError)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TorrentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for TorrentId {
    type Err = TorrentIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl fmt::Display for TorrentIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("torrent id must be a 40- or 64-character hexadecimal info-hash")
    }
}

impl Error for TorrentIdError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TorrentState {
    Downloading,
    StalledDownloading,
    QueuedDownloading,
    Checking,
    Stopped,
    Uploading,
    StalledUploading,
    QueuedUploading,
    Error,
    Unknown,
}

impl TorrentState {
    pub const fn is_stopped(self) -> bool {
        matches!(self, Self::Stopped)
    }

    pub const fn is_downloading(self) -> bool {
        matches!(
            self,
            Self::Downloading | Self::StalledDownloading | Self::QueuedDownloading
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TorrentIdentity {
    pub v1: Option<[u8; 20]>,
    pub v2: Option<[u8; 32]>,
}

impl TorrentIdentity {
    pub fn new(v1: Option<[u8; 20]>, v2: Option<[u8; 32]>) -> Option<Self> {
        if v1.is_none() && v2.is_none() {
            None
        } else {
            Some(Self { v1, v2 })
        }
    }

    pub const fn is_hybrid(&self) -> bool {
        self.v1.is_some() && self.v2.is_some()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestFile {
    pub path: String,
    pub size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TorrentManifest {
    pub files: Vec<ManifestFile>,
    pub total_size: u64,
}

impl TorrentManifest {
    pub fn new(files: Vec<ManifestFile>) -> Option<Self> {
        let mut total_size = 0_u64;
        for file in &files {
            total_size = total_size.checked_add(file.size)?;
        }
        Some(Self { files, total_size })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn torrent_id_normalizes_hex_case() {
        let id = TorrentId::new("ABCDEF0123456789ABCDEF0123456789ABCDEF01").expect("valid id");
        assert_eq!(id.as_str(), "abcdef0123456789abcdef0123456789abcdef01");
    }

    #[test]
    fn torrent_id_rejects_non_hash_input() {
        assert!(TorrentId::new("not-a-hash").is_err());
        assert!(TorrentId::new("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_err());
    }

    #[test]
    fn manifest_total_is_checked() {
        let manifest = TorrentManifest::new(vec![
            ManifestFile {
                path: "a".into(),
                size: 10,
            },
            ManifestFile {
                path: "b".into(),
                size: 20,
            },
        ])
        .expect("manifest");

        assert_eq!(manifest.total_size, 30);
    }
}
