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

        if !valid_length || !valid_hex {
            return Err(TorrentIdError);
        }

        // qBittorrent's public `hash` selector is its 160-bit TorrentID.
        // For a v2-only torrent qBittorrent derives that ID from the first
        // 160 bits of the SHA-256 info-hash, so canonicalize a full v2 hash
        // at the domain boundary instead of sending an unsupported 64-hex
        // value to WebAPI or fingerprinting the same torrent two ways.
        let qbit_id = if value.len() == 64 {
            &value[..40]
        } else {
            value
        };
        Ok(Self(qbit_id.to_ascii_lowercase()))
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
        f.write_str(
            "torrent selector must be a 40-character qBittorrent ID or 64-character v2 info-hash",
        )
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

    pub fn shares_alias_with(&self, other: &Self) -> bool {
        self.v1
            .zip(other.v1)
            .is_some_and(|(left, right)| left == right)
            || self
                .v2
                .zip(other.v2)
                .is_some_and(|(left, right)| left == right)
    }

    pub fn qbit_selector_ids(&self) -> Vec<TorrentId> {
        let mut ids = Vec::with_capacity(2);

        if let Some(v2) = self.v2 {
            ids.push(
                TorrentId::new(hex_lower(&v2))
                    .expect("SHA-256 info hash always forms a valid qBittorrent selector"),
            );
        } else if let Some(v1) = self.v1 {
            ids.push(
                TorrentId::new(hex_lower(&v1))
                    .expect("SHA-1 info hash always forms a valid qBittorrent selector"),
            );
        }

        if self.v2.is_some() {
            if let Some(v1) = self.v1 {
                let alternate = TorrentId::new(hex_lower(&v1))
                    .expect("SHA-1 info hash always forms a valid qBittorrent selector");
                if ids.iter().all(|id| id != &alternate) {
                    ids.push(alternate);
                }
            }
        }

        ids
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestFile {
    pub path: String,
    pub size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TorrentMetainfo {
    pub identity: TorrentIdentity,
    pub manifest: TorrentManifest,
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
    fn torrent_id_canonicalizes_full_v2_hash_to_qbit_id() {
        let id = TorrentId::new("ABCDEF0123456789ABCDEF0123456789ABCDEF01112233445566778899AABBCC")
            .expect("valid v2 hash");
        assert_eq!(id.as_str(), "abcdef0123456789abcdef0123456789abcdef01");
    }

    #[test]
    fn torrent_id_rejects_non_hash_input() {
        assert!(TorrentId::new("not-a-hash").is_err());
        assert!(TorrentId::new("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_err());
    }

    #[test]
    fn torrent_identity_matches_any_shared_v1_or_v2_alias() {
        let v1 = [0x11; 20];
        let v2 = [0x22; 32];
        let v1_only = TorrentIdentity::new(Some(v1), None).expect("v1");
        let v2_only = TorrentIdentity::new(None, Some(v2)).expect("v2");
        let hybrid = TorrentIdentity::new(Some(v1), Some(v2)).expect("hybrid");
        let other = TorrentIdentity::new(Some([0x33; 20]), Some([0x44; 32])).expect("other");

        assert!(v1_only.shares_alias_with(&hybrid));
        assert!(v2_only.shares_alias_with(&hybrid));
        assert!(!v1_only.shares_alias_with(&v2_only));
        assert!(!hybrid.shares_alias_with(&other));
    }

    #[test]
    fn torrent_identity_exposes_qbit_primary_and_hybrid_alternate_ids() {
        let v1 = [0x11; 20];
        let mut v2 = [0x22; 32];
        v2[20] = 0x33;

        let v1_only = TorrentIdentity::new(Some(v1), None).expect("v1");
        assert_eq!(
            v1_only
                .qbit_selector_ids()
                .iter()
                .map(TorrentId::as_str)
                .collect::<Vec<_>>(),
            vec!["1111111111111111111111111111111111111111"]
        );

        let v2_only = TorrentIdentity::new(None, Some(v2)).expect("v2");
        assert_eq!(
            v2_only
                .qbit_selector_ids()
                .iter()
                .map(TorrentId::as_str)
                .collect::<Vec<_>>(),
            vec!["2222222222222222222222222222222222222222"]
        );

        let hybrid = TorrentIdentity::new(Some(v1), Some(v2)).expect("hybrid");
        assert_eq!(
            hybrid
                .qbit_selector_ids()
                .iter()
                .map(TorrentId::as_str)
                .collect::<Vec<_>>(),
            vec![
                "2222222222222222222222222222222222222222",
                "1111111111111111111111111111111111111111",
            ]
        );
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
